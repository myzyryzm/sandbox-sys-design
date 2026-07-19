// LLM Worker — custom Edit tab (embedded body; NodeEditModal owns the chrome).
//
// Three sections:
//   1. Live state — active sequences (progress to target_len) + cached chats with age.
//   2. Tunables — ttl_seconds / max_active / chat_db, written to the worker's mounted
//      worker.json via the config route: the container mtime-polls it, so edits apply
//      LIVE with no rebuild (TTL takes effect at the next reaper tick).
//   3. on_cache_evict hook — describe what should run when the reaper evicts a chat's
//      prefix cache; "Author hook" launches a Claude session (edit queue) that follows
//      the sandbox-llm-worker skill: it writes <worker>/hooks.py, restarts the worker,
//      and sets implemented:true in hook.json. Resume reopens that session.
// Worker count (manual + autoscaling policy) lives on the Scaling tab.
import { useEffect, useState } from 'react'
import type { EditTabProps } from '../../types/customTypes'
import type { LlmWorkerState } from './DiagramBody'

const STATE_URL = (sys: string) => `/api/custom/llm-worker/state?system=${encodeURIComponent(sys)}`

// The tunables form; number inputs hold the raw string while the user types,
// Number()-ed on save.
interface TunablesForm {
  ttl_seconds: number
  max_active: number | string
  chat_db: string
}

// Prompt seeding the launched session. The repeatable procedure lives in the
// sandbox-llm-worker skill, so this stays short.
function buildHookPrompt({ systemId, worker, description, editing, priorDescription, instances = [] }: {
  systemId: string
  worker: string
  description: string
  editing: boolean
  priorDescription?: string
  instances?: string[]
}): string {
  // Every replica bind-mounts the BASE worker's hooks.py, so a restart must cover the
  // whole group (base + instances) or the instances keep the old hook.
  const group = [worker, ...instances]
  const lines = [
    `Use the sandbox-llm-worker skill to ${editing ? 'UPDATE' : 'IMPLEMENT'} the on_cache_evict hook`,
    `of the LLM worker "${worker}" in the "${systemId}" system.`,
    '',
  ]
  if (editing) {
    lines.push(
      `The hook ALREADY EXISTS in systems/${systemId}/${worker}/hooks.py. FIRST read it, then modify it`,
      `in place. Current behavior (existing description):`,
      (priorDescription || '').trim() || '(none recorded)',
      '',
    )
  }
  lines.push(
    `What it should do when a chat's prefix-cache entry is evicted:`,
    (description || '').trim(),
    '',
    `Per the skill:`,
    `- Edit ONLY systems/${systemId}/${worker}/hooks.py — keep the on_cache_evict(entry) signature;`,
    `  app.py / model.py are off-limits.`,
    `- hooks.py is bind-mounted, so apply it with a restart (no rebuild).`,
    instances.length
      ? `  This worker is scaled to ${group.length} instances that ALL bind-mount ${worker}/hooks.py — restart the whole group:`
      : `  Restart the worker:`,
    `    docker compose -f systems/${systemId}/docker-compose.yml restart ${group.join(' ')}`,
    `- Verify per the skill's Verify section (drop the TTL, drive one AddPrompt, watch the eviction).`,
    `- Then set "implemented": true in systems/${systemId}/${worker}/hook.json.`,
  )
  return lines.join('\n')
}

export default function WorkerTab({ systemId, node, manifest, onClose, onLaunch, onBusyChange }: EditTabProps) {
  const [state, setState] = useState<LlmWorkerState | null>(null) // { live, config, hook } for THIS node
  const [form, setForm] = useState<TunablesForm | null>(null) // tunables form; seeded once from the registry
  const [hookText, setHookText] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [savedAt, setSavedAt] = useState(0)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // Poll the aggregate state; seed the tunables form from the on-disk config once.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(STATE_URL(systemId))
        const data = (await res.json()) as { ok: boolean; nodes: Record<string, LlmWorkerState> }
        if (cancelled || !data.ok) return
        const s = data.nodes[node.id] || null
        setState(s)
        if (s?.config) {
          setForm((f) => f || {
            ttl_seconds: s.config!.ttl_seconds ?? 30,
            max_active: s.config!.max_active ?? 5,
            chat_db: s.config!.chat_db ?? '',
          })
        }
      } catch {
        /* keep last good */
      }
    }
    tick()
    const t = setInterval(tick, 1500)
    return () => { cancelled = true; clearInterval(t) }
  }, [systemId, node.id])

  const postgres = (manifest?.nodes || []).filter((n) => n.type === 'postgres').map((n) => n.id)
  const live = state?.live
  const hook = state?.hook

  async function saveConfig() {
    if (!form) return
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/custom/llm-worker/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          node: node.id,
          ttl_seconds: Number(form.ttl_seconds),
          max_active: Number(form.max_active),
          chat_db: form.chat_db || null,
        }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setSavedAt(Date.now())
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  async function authorHook() {
    const description = hookText.trim()
    if (!description) { setError('describe what the hook should do'); return }
    const conversationId = crypto.randomUUID()
    setBusy(true)
    setError(null)
    try {
      // 1. Persist the description (records history; the session owns `implemented`).
      const res = await fetch('/api/custom/llm-worker/hook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: node.id, description, conversationId }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)

      // 2. Launch the authoring session (queued; runs in the one terminal).
      // NodeEditModal always passes onLaunch (App wires it to enqueueSession).
      onLaunch!({
        sessionId: conversationId,
        mode: 'new',
        prompt: buildHookPrompt({
          systemId,
          worker: node.id,
          description,
          editing: hook?.implemented === true,
          priorDescription: hook?.description,
          instances: node.replicas?.instances || [],
        }),
      }, { kind: 'llm-worker', target: node.id, title: 'on_cache_evict' })
      onClose()
    } catch (err) {
      setError((err as Error).message)
      setBusy(false)
    }
  }

  return (
    <div>
      <p className="sim-desc">
        Simulated LLM inference: prompts arrive over gRPC (<code>Worker.AddPrompt</code>), the batch
        decodes continuously, and every token streams to <code>{node.llm?.stream || `${node.id}-stream`}</code>{' '}
        (<code>tokens:&lt;user_message_id&gt;</code>, END = 26). Finished sequences keep their KV caches
        in a TTL prefix cache keyed by chat.
      </p>

      {/* Live state */}
      <div className="form-section">
        <div className="form-section-head"><span>Live state</span></div>
        {!live ? (
          <p className="sim-desc">worker not reachable yet…</p>
        ) : (
          <>
            <div className="form-row">
              <span>Batch</span>
              <code>{live.active_count}/{live.config?.max_active} active · {live.cached_count} cached</code>
            </div>
            {(live.active || []).map((s) => (
              <div className="form-row" key={s.seq_id}>
                <span>seq {s.seq_id}</span>
                <code>
                  msg {s.user_message_id}{s.chat != null ? ` · chat ${s.chat}` : ''} ·{' '}
                  {s.prefilled ? `${s.generated}/${s.target_len} tokens` : 'prefilling…'}
                </code>
              </div>
            ))}
            {(live.cached || []).map((c) => (
              <div className="form-row" key={`c${c.chat}`}>
                <span>cached</span>
                <code>chat {c.chat} · {c.age_s}s old</code>
              </div>
            ))}
          </>
        )}
      </div>

      {/* Tunables (live, no rebuild) */}
      <div className="form-section">
        <div className="form-section-head"><span>Tunables</span></div>
        {!form ? (
          <p className="sim-desc">loading config…</p>
        ) : (
          <>
            <label className="form-row">
              <span>Cache TTL</span>
              <span style={{ display: 'flex', alignItems: 'center', gap: 8, flex: 1 }}>
                <input
                  type="range"
                  min={0}
                  max={60}
                  value={form.ttl_seconds}
                  onChange={(e) => setForm({ ...form, ttl_seconds: Number(e.target.value) })}
                  disabled={busy}
                  style={{ flex: 1 }}
                />
                <code>{Number(form.ttl_seconds) === 0 ? 'off' : `${form.ttl_seconds}s`}</code>
              </span>
            </label>
            <label className="form-row">
              <span>Max active</span>
              <input
                type="number"
                min={1}
                max={32}
                value={form.max_active}
                onChange={(e) => setForm({ ...form, max_active: e.target.value })}
                disabled={busy}
              />
            </label>
            <label className="form-row">
              <span>Chat history DB</span>
              <select
                value={form.chat_db || ''}
                onChange={(e) => setForm({ ...form, chat_db: e.target.value })}
                disabled={busy}
              >
                <option value="">— none —</option>
                {postgres.map((id) => <option key={id} value={id}>{id}</option>)}
              </select>
            </label>
            <div className="modal-actions">
              <button type="button" className="primary" onClick={saveConfig} disabled={busy}>
                Save tunables
              </button>
              {savedAt > 0 && Date.now() - savedAt < 4000 && <span className="sim-desc">applied live — no rebuild</span>}
            </div>
            <p className="sim-desc">
              Applies live (mtime-polled config): TTL at the next reaper tick, max active / chat DB at
              the next AddPrompt. TTL 0 disables caching entirely.
            </p>
          </>
        )}
      </div>

      {/* on_cache_evict hook */}
      <div className="form-section">
        <div className="form-section-head">
          <span>On cache evict</span>
          {hook?.description ? (
            <span className={hook.implemented ? 'llm-implemented' : 'scenario-pending'}>
              {hook.implemented ? 'implemented' : 'pending'}
            </span>
          ) : null}
        </div>
        {hook?.description ? (
          <p className="sim-desc" style={{ whiteSpace: 'pre-wrap' }}>{hook.description}</p>
        ) : (
          <p className="sim-desc">
            No hook yet — the worker just logs each eviction. Describe what should happen when a
            chat's prefix cache expires (e.g. persist a summary row, XADD an eviction event, bump a
            metric) and a Claude session will author it into <code>{node.id}/hooks.py</code>.
          </p>
        )}
        <textarea
          rows={3}
          placeholder="e.g. write an eviction record with the chat id and generated text into the evictions redis stream"
          value={hookText}
          onChange={(e) => setHookText(e.target.value)}
          disabled={busy}
          style={{ width: '100%' }}
        />
        <div className="modal-actions">
          <button type="button" className="primary" onClick={authorHook} disabled={busy || !hookText.trim()}>
            {hook?.description ? 'Update hook (Claude session)' : 'Author hook (Claude session)'}
          </button>
          {hook?.conversationId ? (
            <button
              type="button"
              onClick={() => onLaunch!({ sessionId: hook.conversationId!, mode: 'resume', prompt: '' })}
              disabled={busy}
            >
              Resume session
            </button>
          ) : null}
        </div>
      </div>

      {error && <p className="modal-error">{error}</p>}
    </div>
  )
}

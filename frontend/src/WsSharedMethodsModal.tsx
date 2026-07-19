import { useState } from 'react'
import ConfirmDelete from './ConfirmDelete'
import type { Manifest } from './types/manifest'
import type { OutageInfo, WebsocketsFile, WsMethodEntry, WsMethodRecord } from './types/registries'
import type { LaunchSession } from './types/customTypes'

/**
 * The websocket tier's SHARED editing surface, opened from the shared-methods panel
 * the diagram draws below the server fleet (the individual server nodes carry no
 * Edit button — the fleet is edited as one unit):
 *
 * - Methods — the two shared server methods (onMessage / onSend) every relay runs
 *   from ws-shared/hooks.js. The BASE implementation is fixed; a saved description
 *   entry only ADDS behavior: POST /api/websockets/methods writes the registry entry
 *   (implemented:false), then a launched Claude session (sandbox-websocket skill)
 *   authors the hook code, restarts the servers, and flips implemented back — the
 *   consumers.json contract.
 * - Shutdown — per-server timed outage, multi-select: pick which server(s) to kill
 *   for N seconds (one POST /api/outage per pick; the diagram paints them orange).
 * - Delete — the tier is one deletion unit; embeds the lb's ConfirmDelete (cascade
 *   warning + the real delete), since no tier member can be deleted on its own.
 */

// Hook signatures shown on the method headers (mirrors ws-shared/hooks.js).
const METHOD_SIG: Record<string, string> = {
  onMessage: 'onMessage(msg, ctx)',
  onSend: 'onSend(clientId, payload, ctx)',
}

// Prompt seeding the launched session that authors the hook. The repeatable procedure
// lives in the sandbox-websocket skill ("Shared methods"), so this stays short. The
// registry entry (implemented:false) is already written by POST /api/websockets/methods;
// the session writes ws-shared/hooks.js, restarts the servers, and flips the flag.
function buildWsMethodPrompt({
  systemId,
  lb,
  servers,
  method,
  base,
  entries,
}: {
  systemId: string
  lb: string
  servers: string[]
  method: string
  base?: string
  entries: WsMethodEntry[]
}): string {
  const lines = [
    `Use the sandbox-websocket skill to IMPLEMENT the shared "${method}" hook for the "${lb}" websocket tier in the "${systemId}" system.`,
    '',
    `Every server (${servers.join(', ')}) runs this method from the ONE shared hooks file`,
    `systems/${systemId}/ws-shared/hooks.js (mounted read-only at /app/shared/hooks.js).`,
    '',
    `Base behavior (FIXED — never modify server.js or the base routing/delivery path):`,
    (base || '').trim() || '(see the skill)',
    '',
    `Additive behaviors this hook must implement (ALL of them, in order):`,
    ...entries.map((e, i) => `${i + 1}. ${e.text}${e.at ? ` (added ${new Date(e.at).toLocaleString()})` : ''}`),
    '',
    `Per the skill's "Shared methods" section:`,
    `- Edit ONLY systems/${systemId}/ws-shared/hooks.js — the exported ${method}(…) function. If the`,
    `  servers' server.js predates the hooks loader (no fireHook in it), add the loader per the`,
    `  skill and use \`up -d --build\` so the just-added ws-shared volume mounts.`,
    `- Hooks are fire-and-forget ADDITIVE side effects: never block or veto routing/delivery, and`,
    `  never touch the six ws_* metric names.`,
    `- A new npm dependency needs every systems/${systemId}/ws-server-*/package.json +`,
    `  \`up -d --build\` of all servers — prefer dep-free code, fetch, or the ctx redis handles.`,
    `- Reload: docker compose -f systems/${systemId}/docker-compose.yml restart ${servers.join(' ')}`,
    `- Verify per the skill (pool run: delivered ≈ sent; server logs free of "hooks" errors), then`,
    `  set "implemented": true on methods.${method} in systems/${systemId}/${lb}/websockets.json.`,
  ]
  return lines.join('\n')
}

const TABS = [
  { id: 'methods', label: 'Methods' },
  { id: 'shutdown', label: 'Shutdown' },
  { id: 'delete', label: 'Delete', danger: true },
]

interface WsSharedMethodsModalProps {
  systemId: string
  tier: WebsocketsFile
  manifest?: Manifest | null
  outages?: Record<string, OutageInfo>
  onClose: () => void
  onLaunch?: LaunchSession
}

export default function WsSharedMethodsModal({ systemId, tier, manifest, outages = {}, onClose, onLaunch }: WsSharedMethodsModalProps) {
  const [active, setActive] = useState('methods')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // Methods tab: which method's "add behavior" editor is open, and its draft text.
  const [adding, setAdding] = useState<string | null>(null)
  const [text, setText] = useState('')
  // Shutdown tab: picked server ids + outage duration (raw input text while editing).
  const [picked, setPicked] = useState<Set<string>>(() => new Set())
  const [seconds, setSeconds] = useState<number | string>(30)

  const methods = tier.methods || {}
  const methodNames = Object.keys(METHOD_SIG)
  const servers = tier.servers || []
  const lbNode = (manifest?.nodes || []).find((n) => n.id === tier.lb)

  const dismiss = busy ? undefined : onClose

  // ------------------------------------------------------------------ Methods
  async function saveEntry(method: string) {
    const t = text.trim()
    if (!t) {
      setError('Describe the behavior to add first.')
      return
    }
    setBusy(true)
    setError(null)
    const conversationId = crypto.randomUUID()
    try {
      const res = await fetch('/api/websockets/methods', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, method, text: t, conversationId }),
      })
      const data = (await res.json().catch(() => ({}))) as {
        ok?: boolean
        error?: string
        methods?: Record<string, WsMethodRecord>
      }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      // The response carries the updated methods block — prompt with the FULL entry
      // list so the session converges the hook on everything accumulated so far.
      const m = data.methods?.[method] || methods[method] || {}
      onLaunch?.(
        {
          sessionId: conversationId,
          mode: 'new',
          prompt: buildWsMethodPrompt({
            systemId,
            lb: tier.lb,
            servers,
            method,
            base: m.base,
            entries: m.entries || [{ text: t }],
          }),
        },
        { kind: 'ws-method', target: tier.lb, title: method },
      )
      onClose()
    } catch (err) {
      setBusy(false)
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  function renderMethods() {
    return (
      <>
        <p className="sim-desc">
          Two shared methods every <code>{tier.lb}</code> server runs from the one mounted{' '}
          <code>ws-shared/hooks.js</code>. The <strong>base behavior is fixed</strong> — an
          entry you add only <em>adds</em> a side effect on top (authored into real code by a
          launched Claude session, then applied to all {servers.length} servers with a
          restart, no rebuild).
        </p>
        {methodNames.map((name) => {
          const m = methods[name] || {}
          const entries = m.entries || []
          const pending = m.implemented === false
          return (
            <div className="form-section" key={name}>
              <div className="form-section-head">
                <code className="scenario-fn-sig">ƒ {METHOD_SIG[name]}</code>
                {pending && (
                  <span
                    className="scenario-pending"
                    title="Hook code not authored yet — open or resume the Claude session"
                  >
                    pending
                  </span>
                )}
                {m.conversationId && (
                  <button
                    type="button"
                    className="link"
                    disabled={busy}
                    onClick={() => onLaunch?.({ sessionId: m.conversationId!, mode: 'resume', prompt: '' })}
                  >
                    Resume
                  </button>
                )}
              </div>
              <p className="sim-desc">{m.base || ''}</p>
              {entries.length > 0 && (
                <ul className="scenario-steps">
                  {entries.map((e, i) => (
                    <li className="scenario-step" key={`${e.at || ''}-${i}`}>
                      <div className="scenario-step-head">
                        <span className="scenario-step-num">{i + 1}</span>
                        <span className="scenario-path">{e.text}</span>
                        {e.at && (
                          <span className="scenario-stepcount">{new Date(e.at).toLocaleString()}</span>
                        )}
                      </div>
                    </li>
                  ))}
                </ul>
              )}
              {adding === name ? (
                <>
                  <div className="form-row form-row-stack">
                    <label htmlFor={`ws-method-text-${name}`}>Behavior to add</label>
                    <textarea
                      id={`ws-method-text-${name}`}
                      className="desc-input"
                      rows={3}
                      value={text}
                      disabled={busy}
                      placeholder={
                        name === 'onMessage'
                          ? 'e.g. if the target of a message does not exist, update some database'
                          : 'e.g. stamp each delivered payload with the serverId that sent it'
                      }
                      onChange={(e) => setText(e.target.value)}
                    />
                  </div>
                  {error && <p className="modal-error">{error}</p>}
                  <div className="modal-actions">
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => {
                        setAdding(null)
                        setText('')
                        setError(null)
                      }}
                    >
                      Cancel
                    </button>
                    <button type="button" className="primary" disabled={busy} onClick={() => saveEntry(name)}>
                      {busy ? 'Saving…' : 'Save & implement'}
                    </button>
                  </div>
                </>
              ) : (
                <button
                  type="button"
                  className="link"
                  disabled={busy}
                  onClick={() => {
                    setAdding(name)
                    setText('')
                    setError(null)
                  }}
                >
                  ＋ Add behavior
                </button>
              )}
            </div>
          )
        })}
      </>
    )
  }

  // ----------------------------------------------------------------- Shutdown
  function togglePick(sid: string) {
    setPicked((prev) => {
      const next = new Set(prev)
      if (next.has(sid)) next.delete(sid)
      else next.add(sid)
      return next
    })
  }

  async function shutDownPicked() {
    const n = Number(seconds)
    if (!Number.isInteger(n) || n < 1 || n > 300) {
      setError('Choose a whole number of seconds between 1 and 300.')
      return
    }
    const targets = servers.filter((sid) => picked.has(sid) && !outages[sid])
    if (!targets.length) {
      setError('Pick at least one running server.')
      return
    }
    setBusy(true)
    setError(null)
    try {
      for (const sid of targets) {
        const res = await fetch('/api/outage', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ system: systemId, node: sid, duration_seconds: n }),
        })
        const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
        if (!res.ok || !data.ok) throw new Error(`${sid}: ${data.error || `HTTP ${res.status}`}`)
      }
      onClose() // the outage poll paints them orange on the diagram
    } catch (err) {
      setBusy(false)
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  async function bringBack(sid: string) {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/outage', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: sid }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      setBusy(false)
    } catch (err) {
      setBusy(false)
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  function renderShutdown() {
    const pickedCount = servers.filter((sid) => picked.has(sid) && !outages[sid]).length
    return (
      <>
        <p className="sim-desc">
          Temporarily stops the picked server container(s) so they refuse all connections —{' '}
          <code>{tier.lb}</code> shifts sessions to the survivors — then each restarts
          automatically when its timer runs out.
        </p>
        {servers.map((sid) => {
          const down = outages[sid]
          return (
            <div className="form-row" key={sid}>
              {down ? (
                <>
                  <span>
                    <code>{sid}</code> — down, back in <strong>{down.remaining_seconds}s</strong>
                  </span>
                  <button type="button" className="link" disabled={busy} onClick={() => bringBack(sid)}>
                    Bring back now
                  </button>
                </>
              ) : (
                <label>
                  <input
                    type="checkbox"
                    checked={picked.has(sid)}
                    disabled={busy}
                    onChange={() => togglePick(sid)}
                  />{' '}
                  <code>{sid}</code>
                </label>
              )}
            </div>
          )
        })}
        <div className="form-row">
          <label htmlFor="ws-outage-seconds">Duration (seconds)</label>
          <input
            id="ws-outage-seconds"
            type="number"
            min={1}
            max={300}
            value={seconds}
            disabled={busy}
            onChange={(e) => setSeconds(e.target.value)}
          />
        </div>
        <input
          type="range"
          min={1}
          max={300}
          value={Number(seconds) || 1}
          disabled={busy}
          onChange={(e) => setSeconds(e.target.value)}
          aria-label="Duration in seconds"
        />
        {error && <p className="modal-error">{error}</p>}
        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button type="button" className="danger" disabled={busy || !pickedCount} onClick={shutDownPicked}>
            {busy
              ? 'Shutting down…'
              : `Shut down ${pickedCount || ''} server${pickedCount === 1 ? '' : 's'} for ${seconds || 0}s`}
          </button>
        </div>
      </>
    )
  }

  // ------------------------------------------------------------------- Delete
  function renderDelete() {
    if (!lbNode) return <p className="sim-desc">Load balancer node not found.</p>
    return (
      <>
        <p className="sim-desc">
          Individual servers can't be deleted — the tier is one unit, removed via its load
          balancer <code>{tier.lb}</code>:
        </p>
        <ConfirmDelete
          embedded
          systemId={systemId}
          node={lbNode}
          manifest={manifest!}
          onClose={onClose}
          onBusyChange={setBusy}
        />
      </>
    )
  }

  return (
    <div className="modal-overlay" onClick={dismiss}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>
            Shared methods · <code>{tier.lb}</code>
          </h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>
            ✕
          </button>
        </header>

        <div className="modal-tabs" role="tablist">
          {TABS.map((t) => (
            <button
              key={t.id}
              type="button"
              role="tab"
              aria-selected={active === t.id}
              className={['modal-tab', active === t.id ? 'active' : '', t.danger ? 'danger' : '']
                .filter(Boolean)
                .join(' ')}
              disabled={busy && active !== t.id}
              onClick={() => {
                setActive(t.id)
                setError(null)
              }}
            >
              {t.label}
            </button>
          ))}
        </div>

        {active === 'methods' && renderMethods()}
        {active === 'shutdown' && renderShutdown()}
        {active === 'delete' && renderDelete()}
      </div>
    </div>
  )
}

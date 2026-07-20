// Persistence Readers — custom Edit tab on the READER GROUP node (embedded body;
// NodeEditModal owns the chrome).
//
// Three sections:
//   1. Reader state — the registry entry (persist target, implemented/pending badge,
//      description) and each member's live /reader/state (authored by the session;
//      "not implemented yet" until then), plus a Resume button for the session.
//   2. Update — append to the description and/or move the table/field target; a
//      target change launches a session to re-author the loop (a description-only
//      edit is registry-only). The DATABASE is fixed at creation (delete + re-add).
//   3. Members — the manual "set member count" input (the shared replica reconciler;
//      no autoscaler for this type — redis divides announcements across members).
import { useEffect, useState } from 'react'
import type { EditTabProps } from '../../types/customTypes'
import { buildPersistencePrompt } from './prompt'

// ─── The state route's per-node runtime entry this module reads ─────────────

// The node's persistence.json registry entry.
export interface PersistenceRegistryEntry {
  service?: string
  worker?: string
  stream?: string
  group?: string
  db?: string
  table?: string
  field?: string
  freeform?: string | null
  description?: string
  implemented?: boolean
  conversationId?: string
}

// A member's live /reader/state counters (authored by the session).
export interface PersistenceReaderLive {
  active?: number
  persisted?: number
  runs?: number
  consumer?: string
}

export interface PersistenceReaderState {
  registry?: PersistenceRegistryEntry | null
  live?: PersistenceReaderLive | null
}

const STATE_URL = (sys: string) => `/api/custom/persistence-reader/state?system=${encodeURIComponent(sys)}`
const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_.-]*$/

export default function ReadersTab({ systemId, node, onClose, onLaunch, onBusyChange }: EditTabProps) {
  const [nodes, setNodes] = useState<Record<string, PersistenceReaderState> | null>(null) // the state route's full node map
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [memberCount, setMemberCount] = useState<number | string>(1 + (node.replicas?.instances?.length || 0))
  const [addDescription, setAddDescription] = useState('')
  const [table, setTable] = useState(node.persistence?.table || '')
  const [field, setField] = useState(node.persistence?.field || '')

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(STATE_URL(systemId))
        const data = (await res.json()) as { ok: boolean; nodes: Record<string, PersistenceReaderState> }
        if (!cancelled && data.ok) setNodes(data.nodes)
      } catch {
        /* keep last good */
      }
    }
    tick()
    const t = setInterval(tick, 2000)
    return () => { cancelled = true; clearInterval(t) }
  }, [systemId, node.id])

  const entry = nodes?.[node.id]?.registry || null
  const memberIds = [node.id, ...(node.replicas?.instances || [])]
  const structured = !!(entry ? entry.db : node.persistence?.db)
  const currentTotal = 1 + (node.replicas?.instances?.length || 0)
  const mc = Number(memberCount)
  const memberCountErr = !Number.isInteger(mc) || mc < 1 || mc > 8
  const targetChanged = structured && entry
    && (table !== (entry.table || '') || field !== (entry.field || ''))

  async function applyUpdate() {
    setError(null)
    if (structured && (!IDENT_RE.test(table) || !IDENT_RE.test(field))) {
      return setError('table and field must be plain identifiers')
    }
    if (!addDescription.trim() && !targetChanged) return
    const description = [entry?.description || '', addDescription.trim()].filter(Boolean).join('\n\n')
    const conversationId = targetChanged ? crypto.randomUUID() : entry?.conversationId || ''
    setBusy(true)
    try {
      const res = await fetch('/api/custom/persistence-reader/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          system: systemId,
          node: node.id,
          description,
          ...(structured ? { table, field } : {}),
          ...(targetChanged ? { conversationId } : {}),
        }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      // Moving the persist target is a CODE change — re-author via a session.
      // (targetChanged implies the registry entry loaded; NodeEditModal always
      // passes onLaunch.)
      if (targetChanged) {
        onLaunch!({
          sessionId: conversationId,
          mode: 'new',
          prompt: buildPersistencePrompt({
            systemId,
            service: node.id,
            worker: entry!.worker,
            stream: entry!.stream,
            group: entry!.group,
            db: entry!.db,
            table,
            field,
            freeform: entry!.freeform,
            description,
            editing: true,
            priorDescription: entry!.description,
          }),
        }, { kind: 'persistence', target: node.id, title: 'update readers' })
        onClose()
        return
      }
      setAddDescription('')
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  async function scaleMembers() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/custom/persistence-reader/scale', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: node.id, instances: mc }),
      })
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string }
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      onClose()
    } catch (err) {
      setError((err as Error).message)
      setBusy(false)
    }
  }

  return (
    <div>
      <p className="sim-desc">
        <code>{node.id}</code> runs as N member containers forming redis consumer group{' '}
        <code>{node.persistence?.group}</code> on <code>{node.persistence?.announce}</code> (redis{' '}
        <code>{node.persistence?.stream}</code>): each member claims announced runs, accumulates the
        run's token stream, and persists the finished output
        {structured ? <> to <code>{node.persistence?.db}</code></> : ' per its specialized spec'}.
      </p>

      {/* Reader state */}
      <div className="form-section">
        <div className="form-section-head">
          <span>Reader state</span>
          {entry && !entry.implemented && <span className="scenario-pending">pending implementation</span>}
        </div>
        {!entry ? (
          <p className="sim-desc">loading registry…</p>
        ) : (
          <>
            <div className="form-row">
              <span>Persist target</span>
              <code>{entry.db ? `${entry.db} · ${entry.table}.${entry.field}` : 'specialized (freeform spec)'}</code>
            </div>
            <div className="form-row">
              <span>Worker</span>
              <code>{entry.worker} → {entry.stream}</code>
            </div>
            {memberIds.map((id) => {
              const live = nodes?.[id]?.live
              return (
                <div className="form-row" key={id}>
                  <span>{id}</span>
                  <code>
                    {live
                      ? `active ${live.active ?? 0} · persisted ${live.persisted ?? live.runs ?? 0}${live.consumer ? ` · ${live.consumer}` : ''}`
                      : entry.implemented ? 'not reachable' : 'loop not authored yet'}
                  </code>
                </div>
              )
            })}
            {entry.conversationId && (
              <div className="modal-actions">
                <button
                  type="button"
                  onClick={() => { onLaunch!({ sessionId: entry.conversationId!, mode: 'resume' }, { kind: 'persistence', target: node.id, title: 'readers' }); onClose() }}
                  disabled={busy}
                >
                  Resume session
                </button>
              </div>
            )}
          </>
        )}
      </div>

      {/* Update the spec */}
      <div className="form-section">
        <div className="form-section-head"><span>Update</span></div>
        {structured && (
          <>
            <label className="form-row">
              <span>Table</span>
              <input value={table} onChange={(e) => setTable(e.target.value)} disabled={busy} />
            </label>
            <label className="form-row">
              <span>Field</span>
              <input value={field} onChange={(e) => setField(e.target.value)} disabled={busy} />
            </label>
          </>
        )}
        <textarea
          rows={3}
          placeholder="Describe the change (appended to the description)…"
          value={addDescription}
          onChange={(e) => setAddDescription(e.target.value)}
          disabled={busy}
        />
        <small className="form-hint">
          {targetChanged
            ? 'Moving the target re-authors the loop (launches a session + rebuild).'
            : 'A description-only update is registry-only — no session, no rebuild. The database itself is fixed at creation: delete + re-add the readers to change it.'}
        </small>
        <div className="modal-actions">
          <button
            type="button"
            className="primary"
            onClick={applyUpdate}
            disabled={busy || (!addDescription.trim() && !targetChanged)}
          >
            {targetChanged ? 'Apply + re-author' : 'Append description'}
          </button>
        </div>
      </div>

      {/* Manual member count */}
      <div className="form-section">
        <div className="form-section-head"><span>Members</span></div>
        <label className="form-row">
          <span>Total members</span>
          <input
            type="number"
            min={1}
            max={8}
            value={memberCount}
            onChange={(e) => setMemberCount(e.target.value)}
            disabled={busy}
          />
        </label>
        {memberCountErr ? (
          <small className="field-error">Between 1 and 8 members</small>
        ) : (
          <small className="form-hint">
            {mc >= 2 ? `Creates: ${node.id} + ${node.id}-2…${mc}` : '1 = a single member (no replicas)'}
            {' — all members share the group, so redis divides announced runs across them (one reader per run).'}
          </small>
        )}
        <div className="modal-actions">
          <button
            type="button"
            className="primary"
            onClick={scaleMembers}
            disabled={busy || memberCountErr || mc === currentTotal}
          >
            {busy ? 'Applying… (building instances can take a minute)' : 'Apply'}
          </button>
        </div>
      </div>

      {error && <p className="modal-error">{error}</p>}
    </div>
  )
}

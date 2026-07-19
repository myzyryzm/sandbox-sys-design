/**
 * The edit queue panel — a top-left card (under the header/actions bar) listing the
 * Claude edit sessions waiting to run. Sessions run one at a time; the queue
 * auto-advances ~10s after the running one finishes (see App.jsx). Each row shows the
 * target node + a short description and a per-status action button.
 *
 * Props:
 *   items     [{ id, meta:{ kind, target, title }, status }]  (status pending|running|done)
 *   countdown  number|null   seconds left in the post-completion hold (for the done row)
 *   onRemove(id)             cancel a pending item / stop the running one
 *   onNext()                 skip the 10s hold and start the next edit now
 *   onClose()                hide the panel
 */
import type { SessionMeta } from './types/customTypes'

export interface EditQueueItem {
  id: string
  sessionId?: string
  prompt?: string
  meta?: SessionMeta
  status: 'pending' | 'running' | 'done'
}

interface EditQueuePanelProps {
  items: EditQueueItem[]
  countdown: number | null
  onRemove: (id: string) => void
  onNext: () => void
  onClose: () => void
}

export default function EditQueuePanel({ items, countdown, onRemove, onNext, onClose }: EditQueuePanelProps) {
  const pendingCount = items.filter((it) => it.status === 'pending').length

  return (
    <div className="edit-queue-panel">
      <header className="edit-queue-head">
        <span className="edit-queue-title">Edit queue</span>
        <span className="edit-queue-count">{pendingCount} pending</span>
        <button className="edit-queue-close" onClick={onClose} title="Hide queue">
          ✕
        </button>
      </header>
      <div className="edit-queue-body">
        {items.map((it) => {
          const kind = it.meta?.kind || 'edit'
          const target = it.meta?.target || ''
          const title = it.meta?.title || '(edit)'
          return (
            <div key={it.id} className={`edit-queue-row status-${it.status}`}>
              <span className={`edit-queue-dot ${it.status}`} />
              <span className="edit-queue-kind">{kind}</span>
              <span className="edit-queue-desc">
                {target ? <code className="edit-queue-target">{target}</code> : null}
                <span className="edit-queue-titletext">{title}</span>
              </span>
              {it.status === 'running' && (
                <button
                  className="edit-queue-action"
                  onClick={() => onRemove(it.id)}
                  title="Stop this session and skip to the next"
                >
                  Skip ▸
                </button>
              )}
              {it.status === 'done' && (
                <button
                  className="edit-queue-action primary"
                  onClick={onNext}
                  title="Start the next edit now"
                >
                  {countdown != null ? `next in ${countdown}s ▸` : 'next ▸'}
                </button>
              )}
              {it.status === 'pending' && (
                <button
                  className="edit-queue-action"
                  onClick={() => onRemove(it.id)}
                  title="Remove from queue"
                >
                  ✕
                </button>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

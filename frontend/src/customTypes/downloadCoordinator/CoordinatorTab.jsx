// Download Coordinator — custom Edit tab (embedded body; NodeEditModal owns the chrome).
//
// Coordinator node: Add node + Run distribution (from a URL or a pre-staged local file)
// + live aggregate status. Worker node: its own live download status. Live state comes
// from the same aggregate endpoint App polls, fetched here so the tab updates on its own.
import { useCallback, useEffect, useState } from 'react'

const STATE_URL = (sys) => `/api/custom/download-coordinator/state?system=${encodeURIComponent(sys)}`

// Chunk size is chosen in MB (powers of two) but sent to the coordinator in bytes.
const MB = 1024 * 1024
const CHUNK_MB_OPTIONS = [2, 4, 8, 16, 32, 64, 128, 256]
const DEFAULT_CHUNK_MB = 64
// Derive the dropdown's initial MB value from the node's stored byte size, snapping
// to a known option (falling back to the default if it doesn't line up).
function initialChunkMB(node) {
  const bytes = node.coordinator?.chunk_size
  const mb = bytes ? Math.round(bytes / MB) : DEFAULT_CHUNK_MB
  return CHUNK_MB_OPTIONS.includes(mb) ? mb : DEFAULT_CHUNK_MB
}

function bytesLabel(n) {
  if (!n || typeof n !== 'number') return '—'
  if (n >= 1024 * 1024) return `${Math.round(n / (1024 * 1024))} MB`
  if (n >= 1024) return `${Math.round(n / 1024)} KB`
  return `${n} B`
}

// A compact held/missing grid (held = green). Distinct from the SVG diagram body.
function Bitmap({ bitmap, count }) {
  const n = count || (bitmap ? bitmap.length : 0)
  if (!n) return <span className="sim-desc">—</span>
  const cells = []
  for (let i = 0; i < n; i++) {
    const held = bitmap && bitmap[i] === 1
    cells.push(<span key={i} className={held ? 'dc-cell held' : 'dc-cell'} />)
  }
  return <span className="dc-bitmap">{cells}</span>
}

export default function CoordinatorTab({ systemId, node, onBusyChange }) {
  const isCoordinator = node.service_type === 'download_coordinator'

  const [live, setLive] = useState(null) // aggregate state for THIS node
  const [allWorkers, setAllWorkers] = useState([]) // [{id, ...}] for a coordinator
  const [sources, setSources] = useState([])
  const [srcMode, setSrcMode] = useState('local') // 'local' | 'url'
  const [localFile, setLocalFile] = useState('')
  const [url, setUrl] = useState('')
  const [chunkMB, setChunkMB] = useState(() => initialChunkMB(node))
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  useEffect(() => onBusyChange?.(busy), [busy, onBusyChange])

  // Poll the aggregate state and pick out this node + (for a coordinator) its workers.
  useEffect(() => {
    let cancelled = false
    const tick = async () => {
      try {
        const res = await fetch(STATE_URL(systemId))
        const data = await res.json()
        if (cancelled || !data.ok) return
        setLive(data.nodes[node.id] || null)
        if (isCoordinator) {
          const workers = Object.entries(data.nodes)
            .filter(([, v]) => v.role === 'worker')
            .map(([id, v]) => ({ id, ...v }))
          workers.sort((a, b) => a.id.localeCompare(b.id))
          setAllWorkers(workers)
        }
      } catch {
        /* keep last good */
      }
    }
    tick()
    const t = setInterval(tick, 1500)
    return () => { cancelled = true; clearInterval(t) }
  }, [systemId, node.id, isCoordinator])

  const loadSources = useCallback(() => {
    fetch(`/api/custom/download-coordinator/sources?system=${encodeURIComponent(systemId)}&node=${encodeURIComponent(node.id)}`)
      .then((r) => r.json())
      .then((d) => { if (d.ok) setSources(d.sources || []) })
      .catch(() => {})
  }, [systemId, node.id])

  useEffect(() => { if (isCoordinator) loadSources() }, [isCoordinator, loadSources])

  async function addWorker() {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/custom/download-coordinator/add-node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, coordinator: node.id }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  async function runDistribution() {
    setError(null)
    const source = srcMode === 'url' ? { type: 'url', value: url.trim() } : { type: 'local', value: localFile }
    if (!source.value) { setError(srcMode === 'url' ? 'enter a URL' : 'pick a local file'); return }
    setBusy(true)
    try {
      const res = await fetch('/api/custom/download-coordinator/distribute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ system: systemId, node: node.id, source, chunk_size: chunkMB * MB }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
    } catch (err) {
      setError(err.message)
    } finally {
      setBusy(false)
    }
  }

  if (!isCoordinator) {
    // Worker view — its own live status.
    return (
      <div>
        <p className="sim-desc">
          A download worker. It registers with <code>{node.coordinatorId || 'its coordinator'}</code>,
          pulls chunks (serving them to peers as soon as it holds them), and persists its bitmap to
          disk so a restart resumes where it left off.
        </p>
        <div className="form-section">
          <div className="form-section-head"><span>Status</span></div>
          <div className="form-row"><span>State</span><code>{live?.status || '—'}</code></div>
          <div className="form-row">
            <span>Chunks</span>
            <code>{live ? `${live.held}/${live.chunk_count || '?'}` : '—'}</code>
          </div>
          <div style={{ marginTop: 6 }}><Bitmap bitmap={live?.bitmap} count={live?.chunk_count} /></div>
        </div>
      </div>
    )
  }

  const pct = live?.progress != null ? Math.round(live.progress * 100) : 0

  return (
    <div>
      <p className="sim-desc">
        Seeds a large file and orchestrates worker nodes that pull chunks from each other
        (star → mesh). Add workers, then run a distribution from a URL or a pre-staged local file.
      </p>

      {/* Nodes */}
      <div className="form-section">
        <div className="form-section-head"><span>Workers</span></div>
        <div className="modal-actions" style={{ marginTop: 0 }}>
          <button type="button" className="primary" onClick={addWorker} disabled={busy}>
            {busy ? 'Working… (can take a minute)' : '＋ Add node'}
          </button>
        </div>
        {allWorkers.length === 0 ? (
          <p className="sim-desc">No workers yet.</p>
        ) : (
          <ul className="dc-worker-list">
            {allWorkers.map((w) => (
              <li key={w.id}>
                <code>{w.id}</code>
                <span className="dc-worker-status">{w.status}{w.alive ? '' : ' · stale'}</span>
                <span className="dc-worker-count">{w.held}/{w.chunk_count || '?'}</span>
                <Bitmap bitmap={w.bitmap} count={w.chunk_count} />
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* Run distribution */}
      <div className="form-section">
        <div className="form-section-head"><span>Run distribution</span></div>
        <div className="form-row">
          <span>Source</span>
          <span>
            <label className="dc-radio"><input type="radio" name="src" checked={srcMode === 'local'} onChange={() => setSrcMode('local')} disabled={busy} /> Local file</label>
            <label className="dc-radio"><input type="radio" name="src" checked={srcMode === 'url'} onChange={() => setSrcMode('url')} disabled={busy} /> URL</label>
          </span>
        </div>
        {srcMode === 'local' ? (
          <label className="form-row">
            <span>File</span>
            <span style={{ display: 'flex', gap: 6 }}>
              <select value={localFile} onChange={(e) => setLocalFile(e.target.value)} disabled={busy}>
                <option value="">— pick a pre-staged file —</option>
                {sources.map((s) => <option key={s} value={s}>{s}</option>)}
              </select>
              <button type="button" className="link" onClick={loadSources} disabled={busy}>refresh</button>
            </span>
          </label>
        ) : (
          <label className="form-row">
            <span>URL</span>
            <input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://…" disabled={busy} />
          </label>
        )}
        <label className="form-row">
          <span>Chunk size</span>
          <select value={chunkMB} onChange={(e) => setChunkMB(Number(e.target.value))} disabled={busy}>
            {CHUNK_MB_OPTIONS.map((mb) => <option key={mb} value={mb}>{mb} MB</option>)}
          </select>
        </label>
        <div className="modal-actions">
          <button type="button" className="primary" onClick={runDistribution} disabled={busy}>
            Run distribution
          </button>
        </div>
      </div>

      {/* Live status */}
      <div className="form-section">
        <div className="form-section-head"><span>Status</span></div>
        <div className="form-row"><span>Phase</span><code>{live?.phase || '—'}{live?.error ? ` · ${live.error}` : ''}</code></div>
        <div className="form-row"><span>File</span><code>{live?.ready ? `${live.chunk_count} chunks · ${bytesLabel(live.file_size)}` : 'no distribution yet'}</code></div>
        <div className="form-row"><span>Overall</span><code>{pct}%</code></div>
        <div className="form-row"><span>Coordinator</span><Bitmap bitmap={live?.bitmap} count={live?.chunk_count} /></div>
      </div>

      {error && <p className="modal-error">{error}</p>}
    </div>
  )
}

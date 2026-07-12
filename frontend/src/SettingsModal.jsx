import { useState } from 'react'
import {
  DEFAULT_PREFIX_COLORS,
  PREFIX_ROLE_LABELS,
  HEX_RE,
  applyBadgeColors,
} from './prefixColors.js'
import { DEFAULT_NODE_COLORS, NODE_ROLE_LABELS } from './nodeColors.js'

// Order the color roles are shown in the modal, per section.
const PREFIX_ORDER = [
  'http', 'function', 'consumer', 'grpc', 'etcdKey', 'redisKey',
  'cdcInsert', 'cdcUpdate', 'cdcDelete',
  'etcdEdge', // edge-only (no badge) — kept last
]
const NODE_ORDER = ['load_balancer']

/**
 * Global app settings (repo-root settings.json via /api/settings). Three today:
 *  - Prefix colors: the diagram's row-prefix badge colors (HTTP verbs, ƒ, PULL,
 *    KEY/SUB, WATCH, RPC) plus the matching edges they trace. Applied live to the
 *    --badge-* CSS vars + passed to SystemDiagram as edge colors on save.
 *  - Node colors: the color of a node no health rule paints — today just the nginx
 *    load balancer. Passed to SystemDiagram, which paints its header + outline.
 *  - Dangerously skip permissions: adds --dangerously-skip-permissions to every
 *    Claude session the app launches.
 * More settings will slot in here later.
 */
export default function SettingsModal({ settings, onSave, onClose }) {
  const prefix = useColorMap(DEFAULT_PREFIX_COLORS, settings?.prefixColors)
  const nodes = useColorMap(DEFAULT_NODE_COLORS, settings?.nodeColors)
  const [skipPerms, setSkipPerms] = useState(!!settings?.dangerouslySkipPermissions)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  // Block Save while any hex field in either section is mid-edit / invalid.
  const hasInvalid = prefix.hasInvalid || nodes.hasInvalid

  const save = async () => {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prefixColors: prefix.colors,
          nodeColors: nodes.colors,
          dangerouslySkipPermissions: skipPerms,
        }),
      })
      const data = await res.json()
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      applyBadgeColors(data.settings.prefixColors) // re-tint badges immediately
      onSave?.(data.settings) // lift into App state so the diagram edges + nodes re-render too
      onClose?.()
    } catch (err) {
      setError(err.message)
      setBusy(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={busy ? undefined : onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Settings</h2>
          <button className="modal-close" onClick={onClose} disabled={busy}>✕</button>
        </header>

        <ColorSection
          title="Prefix colors"
          hint="Colors of the diagram's row-prefix badges — and the matching edges they trace."
          order={PREFIX_ORDER}
          labels={PREFIX_ROLE_LABELS}
          map={prefix}
          busy={busy}
        />

        <ColorSection
          title="Node colors"
          hint="Color of a node nothing scrapes, so no health rule paints it."
          order={NODE_ORDER}
          labels={NODE_ROLE_LABELS}
          map={nodes}
          busy={busy}
        />

        <section className="settings-section">
          <h3>Claude Code</h3>
          <div className="form-row form-row-check">
            <input
              id="settings-skip-perms"
              type="checkbox"
              checked={skipPerms}
              onChange={(e) => setSkipPerms(e.target.checked)}
              disabled={busy}
            />
            <div className="check-field">
              <label htmlFor="settings-skip-perms">Dangerously skip permissions</label>
              <p className="form-hint">
                Launches every Claude session from this app with{' '}
                <code>--dangerously-skip-permissions</code>, so it never pauses to ask for tool
                approval. Convenient, but Claude can then run any command unprompted — only enable
                on a machine you trust.
              </p>
            </div>
          </div>
        </section>

        {error && <p className="modal-error">{error}</p>}

        <div className="modal-actions">
          <button type="button" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button type="button" className="primary" onClick={save} disabled={busy || hasInvalid}>
            {busy ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )
}

/**
 * State for one editable color map: the live, always-valid `colors` (what Save posts) plus a
 * per-role text `drafts` buffer, so a mid-edit invalid hex ("#12") doesn't clobber `colors`.
 * Both start merged over the defaults, so a settings object missing a role still renders every
 * picker.
 */
function useColorMap(defaults, initial) {
  const [colors, setColors] = useState(() => ({ ...defaults, ...(initial || {}) }))
  const [drafts, setDrafts] = useState(() => ({ ...defaults, ...(initial || {}) }))

  return {
    colors,
    drafts,
    defaults,
    hasInvalid: Object.keys(defaults).some((role) => !HEX_RE.test(drafts[role])),
    set(role, value) {
      setDrafts((d) => ({ ...d, [role]: value }))
      if (HEX_RE.test(value)) setColors((c) => ({ ...c, [role]: value }))
    },
    reset(role) {
      setColors((c) => ({ ...c, [role]: defaults[role] }))
      setDrafts((d) => ({ ...d, [role]: defaults[role] }))
    },
    resetAll() {
      setColors({ ...defaults })
      setDrafts({ ...defaults })
    },
  }
}

/** One section of swatch + hex + revert rows over a useColorMap. */
function ColorSection({ title, hint, order, labels, map, busy }) {
  return (
    <section className="settings-section">
      <div className="settings-section-head">
        <h3>{title}</h3>
        <button type="button" className="link" onClick={map.resetAll} disabled={busy}>
          Reset all
        </button>
      </div>
      <p className="sim-desc">{hint}</p>
      <div className="settings-colors">
        {order.map((role) => {
          const draft = map.drafts[role]
          const valid = HEX_RE.test(draft)
          const isDefault = map.colors[role] === map.defaults[role]
          return (
            <div className="settings-color-row" key={role}>
              <label className="settings-color-label">{labels[role]}</label>
              <input
                type="color"
                className="settings-color-swatch"
                value={valid ? draft : map.colors[role]}
                onChange={(e) => map.set(role, e.target.value)}
                disabled={busy}
                aria-label={`${labels[role]} color`}
              />
              <input
                type="text"
                className={`settings-color-hex${valid ? '' : ' invalid'}`}
                value={draft}
                onChange={(e) => map.set(role, e.target.value.trim())}
                disabled={busy}
                spellCheck={false}
                maxLength={7}
                aria-label={`${labels[role]} hex`}
              />
              <button
                type="button"
                className="link settings-color-reset"
                onClick={() => map.reset(role)}
                disabled={busy || isDefault}
                title="Reset to default"
              >
                ↺
              </button>
            </div>
          )
        })}
      </div>
    </section>
  )
}

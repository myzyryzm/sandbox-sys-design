import { useState } from 'react'
import {
  DEFAULT_PREFIX_COLORS,
  PREFIX_ROLE_LABELS,
  HEX_RE,
  applyBadgeColors,
} from './prefixColors.js'

// Order the color roles are shown in the modal.
const ROLE_ORDER = ['http', 'function', 'consumer', 'grpc', 'etcdKey', 'etcdEdge']

/**
 * Global app settings (repo-root settings.json via /api/settings). The first two:
 *  - Prefix colors: the diagram's row-prefix badge colors (HTTP verbs, ƒ, PULL,
 *    KEY/SUB, WATCH, RPC) plus the matching edges they trace. Applied live to the
 *    --badge-* CSS vars + passed to SystemDiagram as edge colors on save.
 *  - Dangerously skip permissions: adds --dangerously-skip-permissions to every
 *    Claude session the app launches.
 * More settings will slot in here later.
 */
export default function SettingsModal({ settings, onSave, onClose }) {
  // Live, validated colors (always full #rrggbb). Merge over defaults so a settings
  // object missing a role still renders every picker.
  const [colors, setColors] = useState(() => ({
    ...DEFAULT_PREFIX_COLORS,
    ...(settings?.prefixColors || {}),
  }))
  // Per-role text buffer, so a mid-edit invalid hex ("#12") doesn't clobber `colors`.
  const [drafts, setDrafts] = useState(() => ({
    ...DEFAULT_PREFIX_COLORS,
    ...(settings?.prefixColors || {}),
  }))
  const [skipPerms, setSkipPerms] = useState(!!settings?.dangerouslySkipPermissions)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState(null)

  const setColor = (role, value) => {
    setDrafts((d) => ({ ...d, [role]: value }))
    if (HEX_RE.test(value)) setColors((c) => ({ ...c, [role]: value }))
  }
  const resetRole = (role) => {
    setColors((c) => ({ ...c, [role]: DEFAULT_PREFIX_COLORS[role] }))
    setDrafts((d) => ({ ...d, [role]: DEFAULT_PREFIX_COLORS[role] }))
  }
  const resetAll = () => {
    setColors({ ...DEFAULT_PREFIX_COLORS })
    setDrafts({ ...DEFAULT_PREFIX_COLORS })
  }

  // Block Save while any hex field is mid-edit / invalid.
  const hasInvalid = ROLE_ORDER.some((r) => !HEX_RE.test(drafts[r]))

  const save = async () => {
    setBusy(true)
    setError(null)
    try {
      const res = await fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ prefixColors: colors, dangerouslySkipPermissions: skipPerms }),
      })
      const data = await res.json()
      if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${res.status}`)
      applyBadgeColors(data.settings.prefixColors) // re-tint badges immediately
      onSave?.(data.settings) // lift into App state so the diagram edges re-render too
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

        <section className="settings-section">
          <div className="settings-section-head">
            <h3>Prefix colors</h3>
            <button type="button" className="link" onClick={resetAll} disabled={busy}>
              Reset all
            </button>
          </div>
          <p className="sim-desc">
            Colors of the diagram's row-prefix badges — and the matching edges they trace.
          </p>
          <div className="settings-colors">
            {ROLE_ORDER.map((role) => {
              const draft = drafts[role]
              const valid = HEX_RE.test(draft)
              const isDefault = colors[role] === DEFAULT_PREFIX_COLORS[role]
              return (
                <div className="settings-color-row" key={role}>
                  <label className="settings-color-label">{PREFIX_ROLE_LABELS[role]}</label>
                  <input
                    type="color"
                    className="settings-color-swatch"
                    value={valid ? draft : colors[role]}
                    onChange={(e) => setColor(role, e.target.value)}
                    disabled={busy}
                    aria-label={`${PREFIX_ROLE_LABELS[role]} color`}
                  />
                  <input
                    type="text"
                    className={`settings-color-hex${valid ? '' : ' invalid'}`}
                    value={draft}
                    onChange={(e) => setColor(role, e.target.value.trim())}
                    disabled={busy}
                    spellCheck={false}
                    maxLength={7}
                    aria-label={`${PREFIX_ROLE_LABELS[role]} hex`}
                  />
                  <button
                    type="button"
                    className="link settings-color-reset"
                    onClick={() => resetRole(role)}
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

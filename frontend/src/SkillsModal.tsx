import './SkillsModal.css'
import { useEffect, useState } from 'react'

interface Skill {
  name: string
  description?: string
  body?: string
}

type SkillsState =
  | { status: 'loading' }
  | { status: 'error'; error?: string }
  | { status: 'ok'; skills: Skill[] }

/**
 * One skill: name + description always visible; the full SKILL.md procedure is
 * collapsed behind a disclosure arrow to save vertical space, expanded on click.
 */
function SkillItem({ skill }: { skill: Skill }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="skill-item">
      <div className="skill-name">{skill.name}</div>
      {skill.description && <div className="skill-desc">{skill.description}</div>}
      {skill.body && (
        <>
          <button
            type="button"
            className="skill-toggle"
            aria-expanded={open}
            onClick={() => setOpen((v) => !v)}
          >
            <span className={`skill-caret${open ? ' open' : ''}`}>▶</span>
            {open ? 'Hide details' : 'Show details'}
          </button>
          {open && <pre className="skill-body">{skill.body}</pre>}
        </>
      )}
    </div>
  )
}

/**
 * Read-only list of the project's Claude Code skills, fetched live from
 * GET /api/skills (see frontend/server/skills.js). These are the skills under
 * .claude/skills/ that a Claude session launched from this app auto-loads, so the
 * modal is a window into what Claude already knows how to do here. Each skill
 * shows its name + description plus the full SKILL.md procedure verbatim.
 */
export default function SkillsModal({ onClose }: { onClose: () => void }) {
  const [state, setState] = useState<SkillsState>({ status: 'loading' })

  useEffect(() => {
    let cancelled = false
    fetch('/api/skills')
      .then((r) => r.json() as Promise<{ ok?: boolean; error?: string; skills?: Skill[] }>)
      .then((d) => {
        if (cancelled) return
        if (!d.ok) setState({ status: 'error', error: d.error })
        else setState({ status: 'ok', skills: d.skills || [] })
      })
      .catch((err: Error) => !cancelled && setState({ status: 'error', error: err.message }))
    return () => {
      cancelled = true
    }
  }, [])

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={(e) => e.stopPropagation()}>
        <header className="modal-head">
          <h2>Skills available to Claude</h2>
          <button className="modal-close" onClick={onClose}>✕</button>
        </header>

        <p className="sim-desc">
          These skills live in <code>.claude/skills/</code> and are loaded automatically by
          the Claude sessions you launch from this app. Each one teaches Claude a repeatable
          procedure for working on this sandbox.
        </p>

        {state.status === 'loading' && <p className="sim-desc">Reading skills…</p>}
        {state.status === 'error' && <p className="modal-error">{state.error}</p>}

        {state.status === 'ok' &&
          (state.skills.length === 0 ? (
            <p className="sim-desc">No skills found.</p>
          ) : (
            <div className="skill-list">
              {state.skills.map((s) => (
                <SkillItem key={s.name} skill={s} />
              ))}
            </div>
          ))}
      </div>
    </div>
  )
}

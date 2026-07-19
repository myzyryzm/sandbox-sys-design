import { useEffect, useRef, useState } from 'react'

// A single "＋ Add ▾" header button that opens a dropdown of the various things a user
// can add, grouped under section headers. Collapses what used to be a row of ＋ buttons.
// The app has no menu/popover primitive, so click-outside + Escape dismissal are handled
// here inline (mousedown listener scoped to this menu's wrapper).
//
// Props:
//   groups: [{ label, items: [{ label, onClick }] }]

interface AddMenuItem {
  label: string
  onClick: () => void
}

interface AddMenuGroup {
  label: string
  items: AddMenuItem[]
}

interface AddMenuProps {
  groups: AddMenuGroup[]
}

export default function AddMenu({ groups }: AddMenuProps) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!open) return
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  return (
    <div className="add-menu" ref={ref}>
      <button
        className={`header-btn no-auto add-menu-trigger ${open ? 'open' : ''}`}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        aria-haspopup="menu"
      >
        ＋ Add ▾
      </button>
      {open && (
        <div className="add-menu-panel" role="menu">
          {groups.map((group, gi) => (
            <div className="add-menu-group" key={group.label}>
              {gi > 0 && <div className="add-menu-divider" />}
              <div className="add-menu-group-label">{group.label}</div>
              {group.items.map((item) => (
                <button
                  className="add-menu-item"
                  key={item.label}
                  role="menuitem"
                  onClick={() => {
                    item.onClick()
                    setOpen(false)
                  }}
                >
                  {item.label}
                </button>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

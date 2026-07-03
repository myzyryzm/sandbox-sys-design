// Vite dev-server plugin: list the project's Claude Code skills.
//
//   GET /api/skills
//     -> { ok, skills: [{ name, description, body }] }
//
// Skills live under <repoRoot>/.claude/skills/<name>/SKILL.md. Because the
// terminal spawns `claude` with cwd = repoRoot, these project skills are exactly
// the ones a launched session auto-loads. We read them live so the UI reflects
// whatever skills currently exist — add a SKILL.md and it shows up, no code
// change. Each SKILL.md is YAML frontmatter (name, description) + a markdown body.
import fs from 'node:fs'
import path from 'node:path'
import { parse } from 'yaml'
import { repoRoot } from './systems.js'

const skillsRoot = path.join(repoRoot, '.claude', 'skills')

// Split a SKILL.md into its frontmatter object and markdown body. Tolerates a
// file with no frontmatter (whole file becomes the body, empty frontmatter).
function parseSkill(dirName, raw) {
  let front = {}
  let body = raw
  const m = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/.exec(raw)
  if (m) {
    try {
      front = parse(m[1]) || {}
    } catch {
      front = {}
    }
    body = m[2]
  }
  return {
    name: typeof front.name === 'string' && front.name.trim() ? front.name.trim() : dirName,
    description: typeof front.description === 'string' ? front.description.trim() : '',
    body: body.trim(),
  }
}

function readSkills() {
  let entries
  try {
    entries = fs.readdirSync(skillsRoot, { withFileTypes: true })
  } catch {
    return [] // no skills dir yet
  }
  const skills = []
  for (const ent of entries) {
    if (!ent.isDirectory()) continue
    const file = path.join(skillsRoot, ent.name, 'SKILL.md')
    let raw
    try {
      raw = fs.readFileSync(file, 'utf8')
    } catch {
      continue // a dir without a SKILL.md isn't a skill
    }
    skills.push(parseSkill(ent.name, raw))
  }
  skills.sort((a, b) => a.name.localeCompare(b.name))
  return skills
}

export default function skills() {
  return {
    name: 'skills',
    configureServer(server) {
      server.middlewares.use('/api/skills', (req, res, next) => {
        if (req.method !== 'GET') return next()
        res.setHeader('Content-Type', 'application/json')
        try {
          res.end(JSON.stringify({ ok: true, skills: readSkills() }))
        } catch (err) {
          res.statusCode = 500
          res.end(JSON.stringify({ ok: false, error: err.message }))
        }
      })
    },
  }
}

// The shared autoscale APPLY loop for worker-group custom types (Kafka consumer
// groups, LLM workers). Each type's scaler is a real sidecar container that only
// COMPUTES a desired member count — the control plane owns docker, so applying a
// decision happens here. Every APPLY_MS: for each base of a registered type that has
// a scaler, read the scaler's /state through the lb (only the active system's lb
// answers — a failed fetch just means "not running, skip") and, when its desired
// count differs from the manifest's actual member count, run the SAME idempotent
// setGroupReplicas the manual Scaling tab uses. Serialization against manual scales
// and other rebuilds comes from withSystemLock inside scaleRebuild — this loop never
// wraps a second lock. A per-group in-flight flag + apply cooldown keeps one slow
// docker rebuild from stacking follow-ups.
//
// Types register via startAutoscaleLoop(server, spec) from their onServerStart:
//   tag         — log prefix + key namespace (e.g. 'consumer-group')
//   disabledEnv — env var that, when '1', keeps this type out of the loop
//   replicaCfg  — the type's replicaGroup cfg (serviceType doubles as the base filter)
//   scalerIdOf  — base id -> its scaler's node id
//   matchesBase — (state, baseNode, system) -> boolean identity guard: the lb only
//                 serves the ACTIVE system, so a same-named scaler from a different
//                 system may answer — reject its state instead of applying it.
// One interval serves every registered type; a Vite restart (config/plugin edit)
// closes the old server — its close event stops the old loop so the fresh plugin
// instances own the only one.
import fs from 'node:fs'
import path from 'node:path'
import { setGroupReplicas } from './replicaGroup.js'
import { isValidSystem, systemDir, systemsDir } from './systems.js'

const LB = 'http://localhost:8080' // the system's load balancer (compose maps 8080:80)
const APPLY_MS = 10_000
const APPLY_COOLDOWN_MS = 60_000
const SKIP_REBUILD = () => process.env.CREATE_SVC_SKIP_REBUILD === '1'

const readManifest = (system) =>
  JSON.parse(fs.readFileSync(path.join(systemDir(system), 'manifest.json'), 'utf8'))

const _specs = []
const _inFlight = new Set()
const _lastApplied = new Map()
let _timer = null

async function tickSpec(spec) {
  let systems = []
  try {
    systems = fs.readdirSync(systemsDir).filter((s) => isValidSystem(s))
  } catch {
    return
  }
  for (const system of systems) {
    let manifest
    try {
      manifest = readManifest(system)
    } catch {
      continue
    }
    const bases = manifest.nodes.filter(
      (n) => n.service_type === spec.replicaCfg.serviceType && !n.instanceOf,
    )
    for (const base of bases) {
      const scalerId = spec.scalerIdOf(base.id)
      if (!manifest.nodes.some((n) => n.id === scalerId)) continue
      const key = `${spec.tag}:${system}/${base.id}`
      if (_inFlight.has(key)) continue
      if ((_lastApplied.get(key) || 0) > Date.now() - APPLY_COOLDOWN_MS) continue

      let state
      try {
        const r = await fetch(`${LB}/${scalerId}/state`, { signal: AbortSignal.timeout(3000) })
        if (!r.ok) continue
        state = await r.json()
      } catch {
        continue // system not active / scaler down — nothing to apply
      }
      if (!state?.ok || state.enabled === false || !Number.isInteger(state.desired)) continue
      if (!spec.matchesBase(state, base, system)) continue

      // Re-read just before applying so a manual scale that already landed makes
      // this a no-op instead of a fight.
      let fresh
      try {
        fresh = readManifest(system).nodes.find((n) => n.id === base.id)
      } catch {
        continue
      }
      if (!fresh) continue
      const current = 1 + (fresh.replicas?.instances?.length || 0)
      if (state.desired === current) continue

      _inFlight.add(key)
      try {
        await setGroupReplicas(spec.replicaCfg, { system, node: base.id, instances: state.desired })
        console.log(
          `[${spec.tag}] autoscaled ${system}/${base.id}: ${current} → ${state.desired} (${state.lastDecision?.reason || 'policy'})`,
        )
      } catch (err) {
        console.error(`[${spec.tag}] autoscale ${system}/${base.id} failed: ${err.message}`)
      } finally {
        _lastApplied.set(key, Date.now()) // back off after success AND failure
        _inFlight.delete(key)
      }
    }
  }
}

async function tick() {
  if (SKIP_REBUILD()) return
  for (const spec of _specs) {
    try {
      await tickSpec(spec)
    } catch (err) {
      console.error(`[${spec.tag}] autoscale tick failed: ${err.message}`)
    }
  }
}

export function startAutoscaleLoop(server, spec) {
  if (spec.disabledEnv && process.env[spec.disabledEnv] === '1') return
  if (_specs.some((s) => s.tag === spec.tag)) return
  _specs.push(spec)
  if (_timer) return
  _timer = setInterval(tick, APPLY_MS)
  _timer.unref?.()
  server?.httpServer?.once?.('close', () => {
    if (_timer) clearInterval(_timer)
    _timer = null
    _specs.length = 0
  })
}

// The custom-service-type plugin contract (frontend half of the Part-1
// mechanism): what a customTypes/<type>/index module exports and the prop
// seams App / NodeEditModal / SystemDiagram drive it through. Also the
// launched-session seam every modal calls (App's enqueueSession).

import type { ComponentType } from 'react'
import type { Manifest, ManifestNode } from './manifest'
import type { DiscoveredEndpoint } from './registries'

// ─── Launched Claude sessions (App's edit queue) ────────────────────────────

export interface SessionLaunch {
  sessionId: string
  mode?: 'new' | 'resume'
  prompt?: string
}

// Renders the queue row; missing meta degrades to a generic label.
export interface SessionMeta {
  kind?: string
  target?: string
  title?: string
}

export type LaunchSession = (cfg: SessionLaunch, meta?: SessionMeta) => void

// ─── Custom-type runtime state ──────────────────────────────────────────────

// Per-node payload of a module's runtime poll ({ nodes: { [id]: state } }).
// Deliberately open: each module narrows to its own state interface.
export type CustomNodeState = Record<string, unknown>

export type CustomStateMap = Record<string, CustomNodeState>

// ─── Edit-tab injection (NodeEditModal) ─────────────────────────────────────

// The full prop set NodeEditModal passes every Component-rendered tab (both
// the built-in Component tabs and custom-type injected ones).
export interface EditTabProps {
  embedded?: boolean
  systemId: string
  node: ManifestNode
  // The node's live poll state (metrics/health/custom runtime), if any.
  current?: unknown
  manifest: Manifest
  onClose: () => void
  onLaunch?: LaunchSession
  onBusyChange?: (busy: boolean) => void
}

export interface EditTabSpec {
  id: string
  label: string
  Component: ComponentType<EditTabProps>
}

// ─── Diagram hooks (SystemDiagram) ──────────────────────────────────────────

export interface DiagramBodyProps {
  node: ManifestNode
  // The node's entry in the customState map; modules narrow it.
  runtime: CustomNodeState | undefined
  width: number
  top: number
}

// A live edge a module contributes (richer than a manifest edge).
export interface DiagramEdgeSpec {
  from: string
  to: string
  className?: string
  label?: string
}

export interface EndpointPolicyResult {
  visibility: string // 'internal' | 'hidden' | …
  locked: boolean
}

export interface CustomRuntime {
  url: (systemId: string) => string
}

export interface CustomTypeModule {
  // Manifest `service_type`s this module owns.
  serviceTypes: string[]
  // Members of this type stack into a dotted worker-group box.
  workerGroup?: (node: ManifestNode) => boolean
  // Tabs injected into NodeEditModal between the kind tabs and Shutdown/Delete.
  editTabs?: (node: ManifestNode) => EditTabSpec[]
  // Live runtime poll App merges into the customState map.
  runtime?: CustomRuntime
  // Endpoint visibility/lock policy for this type's own routes. The generic seam
  // (src/endpointPolicy.ts) also passes the full endpoint row; current modules
  // classify by service-local path alone. The result may be partial — the seam
  // spreads it over the public/unlocked defaults.
  endpointPolicy?: (
    node: ManifestNode,
    path: string,
    endpoint?: DiscoveredEndpoint,
  ) => Partial<EndpointPolicyResult> | null
  DiagramBody?: ComponentType<DiagramBodyProps>
  // Height (px) the custom body needs; MUST match what DiagramBody draws.
  diagramHeight?: (node: ManifestNode, runtime: CustomNodeState | undefined, width: number) => number
  // Live edges (e.g. chunk-source star → mesh).
  diagramEdges?: (ctx: { manifest: Manifest; customState: CustomStateMap }) => DiagramEdgeSpec[]
}

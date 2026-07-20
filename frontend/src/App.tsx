import { useEffect, useRef, useState } from "react";
import AddMenu from "./AddMenu";
import ConnectionResilienceModal from "./ConnectionResilienceModal";
import CreateClient from "./CreateClient";
import CreateDatabase from "./CreateDatabase";
import CreateEtcd from "./CreateEtcd";
import CreateEventStream from "./CreateEventStream";
import CreateExternalService from "./CreateExternalService";
import CreateService from "./CreateService";
import CreateWebsockets from "./CreateWebsockets";
import EditQueuePanel from "./EditQueuePanel";
import EndToEndModal from "./EndToEndModal";
import GrpcContractsModal from "./GrpcContractsModal";
import InterviewPanel from "./InterviewPanel";
import ModelsModal from "./ModelsModal";
import NodeEditModal from "./NodeEditModal";
import SettingsModal from "./SettingsModal";
import SkillsModal from "./SkillsModal";
import SystemDiagram from "./SystemDiagram";
import Terminal from "./Terminal";
import WsSharedMethodsModal from "./WsSharedMethodsModal";
import { CUSTOM_RUNTIMES } from "./customTypes/index";
import { pickColor } from "./health";
import { DEFAULT_PREFIX_COLORS, applyBadgeColors } from "./prefixColors";
import { DEFAULT_NODE_COLORS } from "./nodeColors";
import { queryInstant, queryVector } from "./prometheus";
import { deriveFunctionTrace } from "./scenarioBank";
import type { Manifest, ManifestNode, PersistenceBlock } from "./types/manifest";
import type {
  CdcRule,
  ConsumerEntry,
  DiscoveredEndpoint,
  DiscoveredGrpcContract,
  EndToEndProcess,
  EtcdKeyspace,
  OutageInfo,
  ScenarioFunction,
  VectorSample,
  WebsocketsFile,
} from "./types/registries";
import type { CustomStateMap, SessionLaunch, SessionMeta } from "./types/customTypes";
import type {
  CdcTraceState,
  ConnectionPoolLive,
  ConnectionResilienceLive,
  ConsumerTraceState,
  DiagramClientFunction,
  DiagramConsumerFn,
  DiagramFunctionTrace,
  KeyspaceTraceState,
  MemberLiveState,
  NodeLiveData,
  RedisTraceState,
  RpcTraceState,
  WsClientStats,
} from "./SystemDiagram";
import type { InterviewState } from "./InterviewPanel";
import type { AppSettings } from "./SettingsModal";
import type { EditQueueItem } from "./EditQueuePanel";
import type { WsMethodRecord } from "./types/registries";

const SYSTEM_ID = import.meta.env.VITE_SYSTEM_ID;

// GET /api/websockets: the tier registry + the pool client's builtin method
// descriptors + its last run's delivery stats.
interface WsInfo {
  ok?: boolean;
  tier?: (WebsocketsFile & { methods?: Record<string, WsMethodRecord> }) | null;
  clientMethods?: Array<{ name: string; args?: Array<{ name: string; type: string }> }>;
  stats?: WsClientStats | null;
}

// The 4-direction "move" glyph for the drag-mode toggle (four arrows out from center).
// Inline SVG (the app uses no icon library); inherits the button's text color.
function MoveIcon() {
  return (
    <svg
      className="move-icon"
      viewBox="0 0 24 24"
      width="14"
      height="14"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <polyline points="5 9 2 12 5 15" />
      <polyline points="9 5 12 2 15 5" />
      <polyline points="15 19 12 22 9 19" />
      <polyline points="19 9 22 12 19 15" />
      <line x1="2" y1="12" x2="22" y2="12" />
      <line x1="12" y1="2" x2="12" y2="22" />
    </svg>
  );
}

function GearIcon() {
  return (
    <svg
      className="move-icon"
      viewBox="0 0 24 24"
      width="14"
      height="14"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </svg>
  );
}

/**
 * Poll every node's metric + health queries once and return a map:
 *   { [nodeId]: { metrics: { [label]: value|null }, color } }
 */
async function pollSystem(manifest: Manifest): Promise<Record<string, NodeLiveData>> {
  // No Prometheus node on the diagram ⇒ observability is "off": skip every query so all
  // nodes go gray and their metrics dropdowns read "no metrics" (matches the diagram gate).
  if (!manifest.nodes.some((n) => n.type === "prometheus")) return {};
  const base = manifest.prometheus_base;
  const state: Record<string, NodeLiveData> = {};

  await Promise.all(
    manifest.nodes.map(async (node) => {
      const metrics: Record<string, number | null> = {};

      // Each displayed metric is an independent instant query.
      await Promise.all(
        (node.metrics || []).map(async (m) => {
          try {
            metrics[m.label] = await queryInstant(base, m.query);
          } catch (err) {
            console.warn(
              `metric "${m.label}" on ${node.id} failed:`,
              err instanceof Error ? err.message : err,
            );
            metrics[m.label] = null;
          }
        }),
      );

      // Health query drives node color. Nodes without a health block stay
      // neutral (gray).
      let color = "gray";
      if (node.health?.query) {
        try {
          const value = await queryInstant(base, node.health.query);
          color = pickColor(node.health.rules, value);
        } catch (err) {
          console.warn(`health on ${node.id} failed:`, err instanceof Error ? err.message : err);
        }
      }

      // An etcd cluster node additionally reads PER-MEMBER series (all N `up` and
      // `is_leader` series of its job) to drive the member-dot strip under the node.
      let members: Record<string, MemberLiveState> | null = null;
      if (node.type === "etcd") {
        try {
          const [ups, leaders] = await Promise.all([
            queryVector(base, `up{job="${node.id}"}`),
            queryVector(base, `etcd_server_is_leader{job="${node.id}"}`),
          ]);
          members = {};
          for (const s of ups) {
            const m = (s.labels.instance || "").split(":")[0];
            if (m) members[m] = { up: s.value === 1, leader: false };
          }
          for (const s of leaders) {
            const m = (s.labels.instance || "").split(":")[0];
            if (members[m]) members[m].leader = s.value === 1;
          }
        } catch (err) {
          console.warn(`etcd members on ${node.id} failed:`, err instanceof Error ? err.message : err);
        }
      }

      // A redis node with a Topology block reads per-member series the same way.
      // Targets are exporter sidecars (`<member>-exporter:9121`), so the member name
      // is the instance label minus that suffix. Cluster mode rings the shard
      // masters (redis_instance_info role="master" — the etcd leader-ring analogue);
      // sentinel mode has no leader among the sentinels themselves.
      if (node.type === "redis" && (node.sentinel || node.redisCluster)) {
        const job = node.redisCluster ? node.id : `${node.id}-sentinel`;
        const memberOf = (s: VectorSample) =>
          (s.labels.instance || "").split(":")[0].replace(/-exporter$/, "");
        try {
          const [ups, masters] = await Promise.all([
            queryVector(base, `up{job="${job}"}`),
            // last_over_time + max by(instance): redis_instance_info carries a
            // run_id label that churns on every container recreate, and the stale
            // pre-recreate series would ring demoted members for the full 5m
            // lookback after a topology change — the 30s window (scrape is 5s)
            // keeps only live role reports.
            node.redisCluster
              ? queryVector(
                  base,
                  `max by (instance) (last_over_time(redis_instance_info{job="${job}",role="master"}[30s]))`,
                )
              : Promise.resolve([]),
          ]);
          members = {};
          for (const s of ups) {
            const m = memberOf(s);
            if (m) members[m] = { up: s.value === 1, leader: false };
          }
          for (const s of masters) {
            const m = memberOf(s);
            if (members[m]) members[m].leader = s.value === 1;
          }
        } catch (err) {
          console.warn(`redis members on ${node.id} failed:`, err instanceof Error ? err.message : err);
        }
      }

      // A replicated postgres node reads its member roles from the failover WATCHER's own
      // series (job `<id>-failover`, one `member` label per container). Nothing else can
      // tell you this: the postgres exporter has no notion of who is primary, and after a
      // failover the manifest does not either — the primary is whichever member the watcher
      // promoted. `leader` rings the live primary; `fenced` marks a stale ex-primary that is
      // up but read-only (so writers correctly skip it).
      if (node.type === "postgres" && node.postgresHa) {
        const job = node.postgresHa.watcher || `${node.id}-failover`;
        try {
          const [ups, primaries, fenced] = await Promise.all([
            queryVector(base, `pg_ha_member_up{job="${job}"}`),
            queryVector(base, `pg_ha_is_primary{job="${job}"}`),
            queryVector(base, `pg_ha_is_fenced{job="${job}"}`),
          ]);
          members = {};
          for (const s of ups) {
            const m = s.labels.member;
            if (m) members[m] = { up: s.value === 1, leader: false };
          }
          for (const s of primaries) {
            const m = s.labels.member;
            if (members[m]) members[m].leader = s.value === 1;
          }
          for (const s of fenced) {
            const m = s.labels.member;
            if (members[m]) members[m].fenced = s.value === 1;
          }
        } catch (err) {
          console.warn(`postgres members on ${node.id} failed:`, err instanceof Error ? err.message : err);
        }
      }

      state[node.id] = members
        ? { metrics, color, members }
        : { metrics, color };
    }),
  );

  return state;
}

export default function App() {
  const [manifest, setManifest] = useState<Manifest | null>(null);
  const [nodeData, setNodeData] = useState<Record<string, NodeLiveData>>({});
  const [error, setError] = useState<string | null>(null);
  const [showTerminal, setShowTerminal] = useState(false);
  // Drag mode: when on, nodes can be repositioned and the system boundary box moved/resized
  // on the diagram (and the normal click actions are suppressed). The toggle turns green.
  const [dragMode, setDragMode] = useState(false);
  const [showCreateDb, setShowCreateDb] = useState(false);
  const [showCreateSvc, setShowCreateSvc] = useState(false);
  const [showCreateExternal, setShowCreateExternal] = useState(false);
  const [showCreateClient, setShowCreateClient] = useState(false);
  const [showCreateEventStream, setShowCreateEventStream] = useState(false);
  const [showCreateEtcd, setShowCreateEtcd] = useState(false);
  const [showCreateWebsockets, setShowCreateWebsockets] = useState(false);
  const [showGrpcContracts, setShowGrpcContracts] = useState(false);
  const [showModels, setShowModels] = useState(false);
  const [connectionTarget, setConnectionTarget] = useState<{ from: string; to: string } | null>(null);
  const [resilienceState, setResilienceState] = useState<Record<string, ConnectionResilienceLive>>({});
  const [poolState, setPoolState] = useState<Record<string, ConnectionPoolLive>>({});
  const [outages, setOutages] = useState<Record<string, OutageInfo>>({});
  // Set of event-stream cluster ids whose consumers are currently paused (drives a
  // diagram badge). Polled from /api/consumer-pause, mirroring the outage poll.
  const [pausedConsumers, setPausedConsumers] = useState<Set<string>>(() => new Set());
  const [showEndToEnd, setShowEndToEnd] = useState(false);
  // Interview mode: the chat drawer's visibility, the polled GET /api/interview payload
  // (interview.json + the in-flight-turn flag + the skip-permissions gate), and — while
  // an interview exists — the endtoend processes whose lastRun verdicts badge the
  // requirement rows in the diagram's FR/NFR text boxes.
  const [showInterview, setShowInterview] = useState(false);
  const [interviewInfo, setInterviewInfo] = useState<{
    interview: InterviewState | null;
    turnInFlight: boolean;
    skipPermissions: boolean;
  }>({
    interview: null,
    turnInFlight: false,
    skipPermissions: false,
  });
  const [interviewProcesses, setInterviewProcesses] = useState<EndToEndProcess[]>([]);
  const [showSkills, setShowSkills] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  // Global app settings (repo-root settings.json via /api/settings). Fetched once on mount;
  // prefixColors re-tint the diagram's badges (via CSS vars) and edges (passed to SystemDiagram),
  // nodeColors paint the nodes no health rule colors (the nginx LB).
  const [settings, setSettings] = useState<AppSettings>({
    prefixColors: DEFAULT_PREFIX_COLORS,
    nodeColors: DEFAULT_NODE_COLORS,
    dangerouslySkipPermissions: false,
  });
  const [endpoints, setEndpoints] = useState<DiscoveredEndpoint[]>([]);
  // The node whose tabbed "Edit" modal is open (endpoints / gRPC / calls / schema /
  // topics / shutdown / delete — whichever tabs apply to that node's kind).
  const [editTarget, setEditTarget] = useState<ManifestNode | null>(null);
  // A service method selected from its Edit ▸ Calls tab, traced on the main diagram
  // (service → the nodes it calls, with request/response schema arrows). Mutually
  // exclusive with the load balancer's own endpoint selection. Cleared on canvas click.
  const [methodTrace, setMethodTrace] = useState<DiscoveredEndpoint | null>(null);
  // The per-system "client function" bank (systems/<id>/scenarios.json), polled below.
  // Resolved per-client into clickable rows on each client node.
  const [scenarios, setScenarios] = useState<ScenarioFunction[]>([]);
  // A client function selected on the diagram, traced client → LB → its called services →
  // their downstreams (with each called method highlighted). Mutually exclusive with the
  // method/LB selections. Cleared on canvas click.
  const [functionTrace, setFunctionTrace] = useState<DiagramFunctionTrace | null>(null);
  // The per-system consumer-function registry (systems/<id>/consumers.json), polled below.
  // Grouped per service into clickable CONS rows on each service node.
  const [consumers, setConsumers] = useState<ConsumerEntry[]>([]);
  // The websocket tier info (GET /api/websockets: registry + the pool client's builtin
  // method descriptors + its last run's delivery stats), polled below only while the
  // manifest actually contains a tier. Drives the ws client node's ƒ rows + stat rows.
  const [wsInfo, setWsInfo] = useState<WsInfo | null>(null);
  // The websocket tier (lb id) whose SHARED editing modal is open — the shared methods
  // (onMessage/onSend) + per-server shutdown + tier delete. Opened from the Edit button
  // on the shared-methods panel the diagram draws below the server fleet.
  const [wsMethodsTier, setWsMethodsTier] = useState<string | null>(null);
  // A consumer function selected on the diagram, traced cluster → consuming service. Mutually
  // exclusive with the method/function/LB selections. Cleared on canvas click.
  const [consumerTrace, setConsumerTrace] = useState<ConsumerTraceState | null>(null);
  // The per-etcd keyspace registry (systems/<id>/etcd.json via GET /api/etcd?live=0), keyed by
  // etcd node id. Rendered as clickable KEY rows on the etcd cluster node.
  const [etcdKeyspaces, setEtcdKeyspaces] = useState<Record<string, EtcdKeyspace[]>>({});
  // A keyspace selected on the etcd node, traced registrant → etcd (the lease-put) and
  // etcd → each listener (the watch push). Mutually exclusive with the other traces.
  const [keyspaceTrace, setKeyspaceTrace] = useState<KeyspaceTraceState | null>(null);
  // The gRPC contract registry (systems/<id>/grpc/_registry.json via GET /api/grpc-contracts),
  // as [{ name, methods, … }]. A service that SERVES a contract lists its methods as RPC rows.
  const [grpcContracts, setGrpcContracts] = useState<DiscoveredGrpcContract[]>([]);
  // A served RPC method selected on a server service, traced each caller → this server.
  // Mutually exclusive with the other traces.
  const [rpcTrace, setRpcTrace] = useState<RpcTraceState | null>(null);
  // A redis keyspace row selected on a redis node (the node's manifest `keyspaces`
  // block), traced each declared writer → redis and redis → each declared reader.
  // Mutually exclusive with the other traces.
  const [redisTrace, setRedisTrace] = useState<RedisTraceState | null>(null);
  // The per-database CDC rule registry (systems/<id>/<db>/cdc.json via
  // GET /api/db-cdc?live=0), keyed by the CDC WORKER's node id (not the database's) — the
  // worker is the node that draws them. Rendered as clickable rule rows, badged with the
  // operations that fire them.
  const [cdcRules, setCdcRules] = useState<Record<string, CdcRule[]>>({});
  // A CDC rule row selected on a cdc worker node, traced worker → its stream ("publishes
  // <entity> to <topic>") and onward through every consumer function that pulls that topic.
  // Mutually exclusive with the other traces.
  const [cdcTrace, setCdcTrace] = useState<CdcTraceState | null>(null);
  // Live runtime state for custom service types (e.g. Download Coordinator worker
  // bitmaps / distribution progress), keyed by node id. Filled by the poll below.
  const [customState, setCustomState] = useState<CustomStateMap>({});
  const [terminalSession, setTerminalSession] = useState<SessionLaunch | null>(null);
  // Edit queue: Claude sessions to run one at a time instead of clobbering each other.
  // Items: { id, sessionId, prompt, meta:{kind,target,title}, status }, status is
  // 'pending' | 'running' | 'done' ('done' = finished, in the ~10s pre-advance hold).
  const [queue, setQueue] = useState<EditQueueItem[]>([]);
  const [showQueue, setShowQueue] = useState(false);
  const [doneCountdown, setDoneCountdown] = useState<number | null>(null);
  const advanceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const countdownTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const manifestJsonRef = useRef<string | null>(null);

  // Load the manifest, then keep re-fetching it so components added from the
  // terminal (which edits manifest.json) appear in the diagram without a reload.
  // Only swap state when the JSON actually changed.
  useEffect(() => {
    let cancelled = false;

    const loadManifest = async () => {
      try {
        const res = await fetch(`/systems/${SYSTEM_ID}/manifest.json`);
        if (!res.ok) throw new Error(`manifest HTTP ${res.status}`);
        const text = await res.text();
        if (cancelled || text === manifestJsonRef.current) return;
        manifestJsonRef.current = text;
        setManifest(JSON.parse(text) as Manifest);
        setError(null);
      } catch (err) {
        if (!manifestJsonRef.current)
          setError(`Failed to load manifest: ${err instanceof Error ? err.message : String(err)}`);
      }
    };

    loadManifest();
    const id = setInterval(loadManifest, 3000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Load global settings once on mount (colors + the dangerously-skip-permissions flag). These
  // rarely change, so no polling — the Settings modal lifts saves back via onSave(setSettings).
  useEffect(() => {
    let cancelled = false;
    fetch("/api/settings")
      .then((r) => r.json() as Promise<{ ok?: boolean; settings?: AppSettings }>)
      .then((d) => {
        if (!cancelled && d.ok && d.settings) setSettings(d.settings);
      })
      .catch(() => {
        /* keep the built-in defaults */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Push the configured prefix colors onto the --badge-* CSS vars so every badge across the
  // SVG diagram + HTML lists re-tints. Edge colors are threaded to SystemDiagram via props below.
  useEffect(() => {
    applyBadgeColors(settings.prefixColors);
  }, [settings.prefixColors]);

  // Poll metrics on the manifest's cadence once the manifest is loaded.
  useEffect(() => {
    if (!manifest) return;

    let cancelled = false;
    const tick = async () => {
      const state = await pollSystem(manifest);
      if (!cancelled) setNodeData(state);
    };

    tick(); // immediate first poll
    const interval = manifest.poll_interval_ms || 4000;
    timerRef.current = setInterval(tick, interval);

    return () => {
      cancelled = true;
      clearInterval(timerRef.current ?? undefined);
    };
  }, [manifest]);

  // Poll the interview state (systems/<id>/interview.json + turn flag + permissions
  // gate). Runs even with the drawer closed — the diagram renders the FR/NFR text
  // boxes from it. Also called directly by the panel right after a mutating action.
  const refreshInterview = async () => {
    try {
      const res = await fetch(`/api/interview?system=${SYSTEM_ID}`);
      const data = (await res.json()) as {
        ok?: boolean;
        interview?: InterviewState | null;
        turnInFlight?: boolean;
        skipPermissions?: boolean;
      };
      if (data.ok)
        setInterviewInfo({
          interview: data.interview ?? null,
          turnInFlight: !!data.turnInFlight,
          skipPermissions: !!data.skipPermissions,
        });
    } catch {
      /* keep the last good state */
    }
  };
  useEffect(() => {
    refreshInterview();
    const id = setInterval(refreshInterview, 3000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // While an interview exists, poll the endtoend processes so requirement rows (panel
  // + diagram boxes) can badge their linked test's last PASS/FAIL. Gated like the ws
  // poll: no interview, no traffic.
  const hasInterview = !!interviewInfo.interview;
  useEffect(() => {
    if (!hasInterview) {
      setInterviewProcesses([]);
      return;
    }
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/endtoend?system=${SYSTEM_ID}`);
        const data = (await res.json()) as { ok?: boolean; processes?: EndToEndProcess[] };
        if (!cancelled && data.ok) setInterviewProcesses(data.processes || []);
      } catch {
        /* keep the last good list */
      }
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [hasInterview]);

  // Poll the system's live, routable endpoints (shown on the LB node). They
  // change as services are added or as a service gains endpoints.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/endpoints?system=${SYSTEM_ID}`);
        const data = (await res.json()) as { ok?: boolean; endpoints?: DiscoveredEndpoint[] };
        if (!cancelled && data.ok) setEndpoints(data.endpoints || []);
      } catch {
        /* leave the last good list in place */
      }
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Poll the per-client functions registry so each client's function rows (and their traces)
  // update as functions are authored/edited from the Functions tab.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/scenarios?system=${SYSTEM_ID}`);
        const data = (await res.json()) as { ok?: boolean; functions?: ScenarioFunction[] };
        if (!cancelled && data.ok) setScenarios(data.functions || []);
      } catch {
        /* keep the last good list */
      }
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Poll the websocket tier (registry + builtin client methods + last pool-run stats).
  // Gated on the manifest containing a tier, so tier-less systems never hit the route;
  // the gate flipping (tier created / deleted) starts and stops the poll automatically.
  const hasWsTier = !!manifest?.nodes?.some(
    (n) => n.origin === "create-websockets",
  );
  useEffect(() => {
    if (!hasWsTier) {
      setWsInfo(null);
      return;
    }
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/websockets?system=${SYSTEM_ID}`);
        const data = (await res.json()) as WsInfo;
        if (!cancelled && data.ok) setWsInfo(data);
      } catch {
        /* keep the last good response */
      }
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [hasWsTier]);

  // Poll the per-service consumer-function registry so each service's CONS rows (and their
  // cluster→service traces) update as consumer functions are authored/edited/deleted.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/consumers?system=${SYSTEM_ID}`);
        const data = (await res.json()) as { ok?: boolean; consumers?: ConsumerEntry[] };
        if (!cancelled && data.ok) setConsumers(data.consumers || []);
      } catch {
        /* keep the last good list */
      }
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Poll each etcd cluster's keyspace registry (registry-only, ?live=0 — no docker
  // probing) so the cluster node's KEY rows and their traces update as keyspaces /
  // listeners are added. Gated on the manifest containing an etcd node.
  const etcdIds = (manifest?.nodes || [])
    .filter((n) => n.type === "etcd")
    .map((n) => n.id)
    .join(",");
  useEffect(() => {
    if (!etcdIds) {
      setEtcdKeyspaces({});
      return;
    }
    let cancelled = false;
    const tick = async () => {
      const entries = await Promise.all(
        etcdIds.split(",").map(async (id): Promise<[string, EtcdKeyspace[]] | null> => {
          try {
            const res = await fetch(
              `/api/etcd?system=${SYSTEM_ID}&id=${encodeURIComponent(id)}&live=0`,
            );
            const data = (await res.json()) as { ok?: boolean; keyspaces?: EtcdKeyspace[] };
            return data.ok ? [id, data.keyspaces || []] : null;
          } catch {
            return null;
          }
        }),
      );
      if (!cancelled)
        setEtcdKeyspaces(Object.fromEntries(entries.filter((e): e is [string, EtcdKeyspace[]] => !!e)));
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [etcdIds]);

  // Poll each CDC worker's rule registry (registry-only, ?live=0 — the live read probes the
  // database container for its schema and 502s when it's down, which would blank the rows on a
  // stopped DB) so the worker node's rule rows and their traces update as rules are added /
  // retargeted / removed. Gated on the manifest containing a cdc node. Rules are stored on the
  // DATABASE (cdc.json lives under <db>/), so we fetch by `cdcOf` and key by the worker node id.
  const cdcPairs = (manifest?.nodes || [])
    .filter((n) => n.type === "cdc" && n.cdcOf)
    .map((n) => `${n.id}:${n.cdcOf}`)
    .join(",");
  useEffect(() => {
    if (!cdcPairs) {
      setCdcRules({});
      return;
    }
    let cancelled = false;
    const tick = async () => {
      const entries = await Promise.all(
        cdcPairs.split(",").map(async (pair): Promise<[string, CdcRule[]] | null> => {
          const [nodeId, dbId] = pair.split(":");
          try {
            const res = await fetch(
              `/api/db-cdc?system=${SYSTEM_ID}&id=${encodeURIComponent(dbId)}&live=0`,
            );
            const data = (await res.json()) as { ok?: boolean; rules?: CdcRule[] };
            return data.ok ? [nodeId, data.rules || []] : null;
          } catch {
            return null;
          }
        }),
      );
      if (!cancelled) setCdcRules(Object.fromEntries(entries.filter((e): e is [string, CdcRule[]] => !!e)));
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [cdcPairs]);

  // Poll the gRPC contract registry so a server service's RPC rows (contract name in its manifest
  // `grpc.servers`) stay in sync as methods are added/removed. Gated on the manifest having any
  // node that serves a contract, so systems with no gRPC servers never hit the route.
  const hasGrpcServers = (manifest?.nodes || []).some(
    (n) => (n.grpc?.servers || []).length > 0,
  );
  useEffect(() => {
    if (!hasGrpcServers) {
      setGrpcContracts([]);
      return;
    }
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/grpc-contracts?system=${SYSTEM_ID}`);
        const data = (await res.json()) as { ok?: boolean; contracts?: DiscoveredGrpcContract[] };
        if (!cancelled && data.ok) setGrpcContracts(data.contracts || []);
      } catch {
        /* keep the last good list */
      }
    };
    tick();
    const id = setInterval(tick, 4000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [hasGrpcServers]);

  // Fast-poll the in-memory resilience state (breaker/retry) so the diagram can show
  // a breaker trip live, faster than Prometheus' scrape. The aggregator returns {}
  // until services are wired, so this is harmless before any policy exists.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/resilience-state?system=${SYSTEM_ID}`);
        const data = (await res.json()) as {
          ok?: boolean;
          connections?: Record<string, ConnectionResilienceLive>;
        };
        if (!cancelled && data.ok) setResilienceState(data.connections || {});
      } catch {
        /* keep the last good state */
      }
    };
    tick();
    const id = setInterval(tick, 750);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Fast-poll live connection-pool state (active/idle counts) so the diagram can show a
  // pool badge on the line. Empty until a service exposes /pool/state, so it's harmless
  // before any pool is wired.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(
          `/api/connection-pool-state?system=${SYSTEM_ID}`,
        );
        const data = (await res.json()) as {
          ok?: boolean;
          connections?: Record<string, ConnectionPoolLive>;
        };
        if (!cancelled && data.ok) setPoolState(data.connections || {});
      } catch {
        /* keep the last good state */
      }
    };
    tick();
    const id = setInterval(tick, 750);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Poll active outages (nodes temporarily shut down) once a second so the diagram
  // can paint them orange and tick down the remaining time. Reduced to a node-keyed
  // map for O(1) lookup in the diagram.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/outage?system=${SYSTEM_ID}`);
        const data = (await res.json()) as { ok?: boolean; outages?: OutageInfo[] };
        if (!cancelled && data.ok) {
          setOutages(
            Object.fromEntries((data.outages || []).map((o) => [o.node, o])),
          );
        }
      } catch {
        /* keep the last good state */
      }
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Poll which event-stream clusters have their consumers paused (registry-only, so
  // it's cheap) every few seconds, so the diagram can badge them. The flag is a pure
  // streams.json write toggled from the Topics tab — no rebuild involved.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
        const res = await fetch(`/api/consumer-pause?system=${SYSTEM_ID}`);
        const data = (await res.json()) as { ok?: boolean; paused?: string[] };
        if (!cancelled && data.ok)
          setPausedConsumers(new Set(data.paused || []));
      } catch {
        /* keep the last good state */
      }
    };
    tick();
    const id = setInterval(tick, 3000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // Poll every registered custom service type's runtime endpoint (~1s) and merge the
  // results into one node-keyed map the diagram + edit tabs read. No registered runtime
  // → this is a no-op. Each endpoint returns { ok, nodes: { [id]: state } }.
  useEffect(() => {
    if (CUSTOM_RUNTIMES.length === 0) return;
    let cancelled = false;
    const tick = async () => {
      const maps = await Promise.all(
        CUSTOM_RUNTIMES.map(async (rt): Promise<CustomStateMap> => {
          try {
            const res = await fetch(rt.url(SYSTEM_ID));
            const data = (await res.json()) as { ok?: boolean; nodes?: CustomStateMap };
            return data.ok ? data.nodes || {} : {};
          } catch {
            return {};
          }
        }),
      );
      if (!cancelled) setCustomState(Object.assign({}, ...maps));
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  // ─── Edit queue ─────────────────────────────────────────────────────────────
  // Submissions used to launch a Claude session immediately, so a second edit
  // clobbered the first. Now each is ENQUEUED and run one at a time; the queue
  // auto-advances ~10s after a session prints its completion sentinel (wired via
  // terminal.js → Terminal.jsx's onSessionDone).
  const queueRef = useRef(queue);
  useEffect(() => {
    queueRef.current = queue;
  }, [queue]);

  const clearAdvanceTimers = () => {
    if (advanceTimerRef.current) {
      clearTimeout(advanceTimerRef.current);
      advanceTimerRef.current = null;
    }
    if (countdownTimerRef.current) {
      clearInterval(countdownTimerRef.current);
      countdownTimerRef.current = null;
    }
    setDoneCountdown(null);
  };
  useEffect(() => () => clearAdvanceTimers(), []);

  // The seam every modal calls (passed as onLaunch). `meta` ({ kind, target, title })
  // only renders the queue row; missing meta degrades to a generic label.
  const enqueueSession = (cfg: SessionLaunch, meta: SessionMeta = {}) => {
    if (!cfg || !cfg.sessionId) return;
    // "Resume" launches (re-opening an existing endpoint/function session) bypass the
    // queue — they're a direct "show me this session" action, not a new edit to run in turn.
    if (cfg.mode === "resume") {
      setTerminalSession(cfg);
      setShowTerminal(true);
      return;
    }
    setQueue((q) => [
      ...q,
      {
        id: cfg.sessionId,
        sessionId: cfg.sessionId,
        prompt: cfg.prompt || "",
        meta,
        status: "pending",
      },
    ]);
    setShowQueue(true);
  };

  // Runner: when nothing is running (and nothing is in its post-completion hold),
  // promote the first pending item and open its session in the terminal.
  useEffect(() => {
    if (queue.some((it) => it.status === "running" || it.status === "done"))
      return;
    const next = queue.find((it) => it.status === "pending");
    if (!next) return;
    setQueue((q) =>
      q.map((it) => (it.id === next.id ? { ...it, status: "running" } : it)),
    );
    setTerminalSession({
      sessionId: next.sessionId!,
      mode: "new",
      prompt: next.prompt,
    });
    setShowTerminal(true);
  }, [queue]);

  // Drop the finished (done) item; the runner then starts the next pending one.
  const advanceQueue = () => {
    clearAdvanceTimers();
    setQueue((q) => q.filter((it) => it.status !== "done"));
  };

  // Called by Terminal when the running session prints the completion sentinel.
  const handleSessionDone = (sessionId: string | null) => {
    const running = queueRef.current.find((it) => it.status === "running");
    if (!running || running.sessionId !== sessionId) return; // ignore ad-hoc / stale signals
    setQueue((q) =>
      q.map((it) => (it.status === "running" ? { ...it, status: "done" } : it)),
    );
    // Hold ~10s (with a visible countdown) before starting the next edit.
    clearAdvanceTimers();
    let n = 10;
    setDoneCountdown(n);
    countdownTimerRef.current = setInterval(() => {
      n -= 1;
      setDoneCountdown(Math.max(0, n));
    }, 1000);
    advanceTimerRef.current = setTimeout(advanceQueue, 10000);
  };

  // Remove a queued item. For the running/done item this also stops it: removing it
  // lets the runner start the next pending session (whose terminal reconnect kills the
  // old PTY), or — if none remain — hides the terminal so the session is torn down.
  const cancelItem = (id: string) => {
    const item = queueRef.current.find((it) => it.id === id);
    if (!item) return;
    if (item.status === "running" || item.status === "done") {
      clearAdvanceTimers();
      const hasNext = queueRef.current.some(
        (it) => it.id !== id && it.status === "pending",
      );
      if (!hasNext) {
        setTerminalSession(null);
        setShowTerminal(false);
      }
    }
    setQueue((q) => q.filter((it) => it.id !== id));
  };

  // Once a 'new' session is live its id exists, so demote it to 'resume' and drop
  // the prompt — otherwise re-mounting the terminal would re-run --session-id on
  // an id that now exists (an error).
  const onSessionLaunched = () => {
    setTerminalSession((s) =>
      s && s.mode === "new" ? { ...s, mode: "resume", prompt: "" } : s,
    );
  };

  // Add the Prometheus node to the diagram (only offered when none exists). No modal —
  // there's nothing to configure — just a POST; the 3s manifest poll shows the new node.
  const addPrometheus = async () => {
    try {
      const res = await fetch("/api/prom-node", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ system: SYSTEM_ID }),
      });
      const data = (await res.json().catch(() => ({}))) as { ok?: boolean; error?: string };
      if (!res.ok || !data.ok)
        throw new Error(data.error || `HTTP ${res.status}`);
    } catch (err) {
      setError(`Failed to add Prometheus: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  if (error) {
    return (
      <div className="app">
        <h1>Distributed Systems Sandbox</h1>
        <p className="error">{error}</p>
      </div>
    );
  }

  if (!manifest) {
    return (
      <div className="app">
        <h1>Distributed Systems Sandbox</h1>
        <p>
          Loading <code>{SYSTEM_ID}</code>…
        </p>
      </div>
    );
  }

  // Group the live functions registry by owner client into the function objects (with steps)
  // the diagram renders as rows + traces. Only clients own functions (external services don't).
  // A websocket client's rows are its pool script's BUILTIN ws methods (from /api/websockets,
  // synthesized here and traced along the tier path) PLUS — like any client — its own
  // authorable HTTP functions (from `scenarios`, traced client → LB → services). The builtin
  // methods are NOT injected into `scenarios`, so the end-to-end modal's client_list method
  // dropdown (built from `scenarios`) lists only a client's real HTTP functions.
  const clientFunctions: Record<string, DiagramClientFunction[]> = {};
  for (const n of manifest.nodes || []) {
    if (n.type !== "client") continue;
    const own = scenarios.filter((f) => f.client === n.id);
    if (n.origin === "create-websockets") {
      const builtin = (wsInfo?.clientMethods || []).map((m) => ({
        client: n.id,
        name: m.name,
        args: m.args || [],
        wsBuiltin: true,
      }));
      clientFunctions[n.id] = [...builtin, ...own];
      continue;
    }
    clientFunctions[n.id] = own;
  }

  // processId -> 'PASS' | 'FAIL' for the diagram's requirement-box status dots.
  const interviewTestVerdicts: Record<string, string> = {};
  for (const p of interviewProcesses) {
    if (p.lastRun?.verdict) interviewTestVerdicts[p.id] = p.lastRun.verdict;
  }

  // The ws pool client's last-run delivery stats, keyed by its node id for the diagram.
  const wsClientNode = (manifest.nodes || []).find(
    (n) => n.origin === "create-websockets" && n.wsRole === "client",
  );
  const wsStats =
    wsClientNode && wsInfo?.stats ? { [wsClientNode.id]: wsInfo.stats } : {};

  // Group the live consumer-function registry by owner service into the CONS rows the diagram
  // renders on each service node. Only internal services own consumer functions.
  const consumerFunctions: Record<string, DiagramConsumerFn[]> = {};
  for (const n of manifest.nodes || []) {
    if (n.type !== "service") continue;
    consumerFunctions[n.id] = consumers.filter((c) => c.service === n.id);
  }
  // Persistence reader groups render the same PULL row — pulling announced runs from
  // their stream redis instead of a Kafka topic. The consumerTrace shape is
  // transport-agnostic (cluster = the stream node, topic = the announce stream), so
  // the row is synthesized straight from the node's manifest persistence block.
  for (const n of manifest.nodes || []) {
    // A persistence_reader's block is always the reader shape (the redis RDB/AOF
    // shape shares the key only on redis nodes — see types/manifest.ts).
    const p = n.persistence as PersistenceBlock | undefined;
    if (n.service_type !== "persistence_reader" || n.instanceOf || !p) continue;
    consumerFunctions[n.id] = [
      ...(consumerFunctions[n.id] || []),
      {
        name: p.fn || "readLlmStream",
        cluster: p.stream,
        topic: p.announce,
        downstream: p.db ? [p.db] : [],
        downstreamDescriptions: p.db
          ? {
              [p.db]: `persists each finished run's output to ${p.table}.${p.field}`,
            }
          : {},
      },
    ];
  }

  return (
    <div className="app">
      <header>
        <h1>{manifest.name}</h1>
        <span className="system-id">{manifest.system_id}</span>
        <button
          className={`header-btn drag-toggle ${dragMode ? "active" : ""}`}
          onClick={() => setDragMode((v) => !v)}
          title="Drag mode — move nodes and the system boundary"
        >
          <MoveIcon /> Edit
        </button>
        <button
          className="header-btn no-auto"
          onClick={() => setShowEndToEnd(true)}
        >
          🔁 End-to-End
        </button>
        <button
          className={`header-btn no-auto ${showInterview ? "active" : ""}`}
          onClick={() => setShowInterview((v) => !v)}
          title="Interview mode — a mock system-design interview that designs into this canvas"
        >
          🎙 Interview
        </button>
        <button
          className="header-btn no-auto"
          onClick={() => setShowSkills(true)}
        >
          📖 Skills
        </button>
        <button
          className="header-btn no-auto icon-btn"
          onClick={() => setShowSettings(true)}
        >
          <GearIcon /> Settings
        </button>
        <AddMenu
          groups={[
            {
              label: "Nodes",
              items: [
                { label: "Service", onClick: () => setShowCreateSvc(true) },
                {
                  label: "External service",
                  onClick: () => setShowCreateExternal(true),
                },
                { label: "Client", onClick: () => setShowCreateClient(true) },
                { label: "Database", onClick: () => setShowCreateDb(true) },
                {
                  label: "Event stream",
                  onClick: () => setShowCreateEventStream(true),
                },
                // Only one etcd cluster may exist — hide the option while one is on the
                // diagram (the backend also 409s a second add), like Prometheus below.
                ...(manifest.nodes.some((n) => n.type === "etcd")
                  ? []
                  : [
                      { label: "etcd", onClick: () => setShowCreateEtcd(true) },
                    ]),
                {
                  label: "WebSockets",
                  onClick: () => setShowCreateWebsockets(true),
                },
                // Only one Prometheus node may exist — hide the option while one is on the
                // diagram (the backend also 409s a second add).
                ...(manifest.nodes.some((n) => n.type === "prometheus")
                  ? []
                  : [{ label: "Prometheus", onClick: addPrometheus }]),
              ],
            },
            {
              label: "Contracts & schemas",
              items: [
                {
                  label: "gRPC contract",
                  onClick: () => setShowGrpcContracts(true),
                },
                { label: "Models", onClick: () => setShowModels(true) },
              ],
            },
          ]}
        />
        <button
          className={`header-btn no-auto ${queue.length ? "has-queue" : ""}`}
          onClick={() => setShowQueue((v) => !v)}
          title="Edit queue — pending Claude sessions run one at a time"
        >
          🗒 Queue{queue.length ? ` (${queue.length})` : ""}
        </button>
        <button
          className="term-toggle"
          onClick={() => setShowTerminal((v) => !v)}
        >
          {showTerminal ? "Hide terminal" : "Edit with Claude ▸"}
        </button>
      </header>
      <div className="canvas">
        {showQueue && queue.length > 0 && (
          <EditQueuePanel
            items={queue}
            countdown={doneCountdown}
            onRemove={cancelItem}
            onNext={advanceQueue}
            onClose={() => setShowQueue(false)}
          />
        )}
        <SystemDiagram
          manifest={manifest}
          nodeData={nodeData}
          endpoints={endpoints}
          systemId={SYSTEM_ID}
          colors={settings.prefixColors}
          nodeColors={settings.nodeColors}
          dragMode={dragMode}
          onRequestEdit={setEditTarget}
          onRequestConnectionResilience={setConnectionTarget}
          resilienceState={resilienceState}
          poolState={poolState}
          outages={outages}
          pausedConsumers={pausedConsumers}
          customState={customState}
          methodTrace={methodTrace}
          onSelectMethod={(ep) => {
            setFunctionTrace(null);
            setConsumerTrace(null);
            setKeyspaceTrace(null);
            setRpcTrace(null);
            setRedisTrace(null);
            setCdcTrace(null);
            setMethodTrace(ep);
          }}
          onClearMethodTrace={() => setMethodTrace(null)}
          clientFunctions={clientFunctions}
          wsStats={wsStats}
          wsMethods={wsInfo?.tier?.methods || null}
          wsAlgorithm={wsInfo?.tier?.algorithm || "leastconn"}
          onRequestWsMethods={setWsMethodsTier}
          functionTrace={functionTrace}
          onSelectFunction={(fn, clientId) => {
            setMethodTrace(null);
            setConsumerTrace(null);
            setKeyspaceTrace(null);
            setRpcTrace(null);
            setRedisTrace(null);
            setCdcTrace(null);
            // A ws builtin has no authored steps — the diagram traces the tier path itself.
            setFunctionTrace(
              fn.wsBuiltin
                ? {
                    client: clientId,
                    name: fn.name,
                    wsBuiltin: true,
                    methods: [],
                  }
                : deriveFunctionTrace(fn, endpoints, clientId),
            );
          }}
          onClearFunctionTrace={() => setFunctionTrace(null)}
          consumerFunctions={consumerFunctions}
          consumerTrace={consumerTrace}
          onSelectConsumer={(c, serviceId) => {
            setMethodTrace(null);
            setFunctionTrace(null);
            setKeyspaceTrace(null);
            setRpcTrace(null);
            setRedisTrace(null);
            setCdcTrace(null);
            setConsumerTrace({
              cluster: c.cluster,
              service: serviceId,
              topic: c.topic,
              name: c.name,
              downstream: c.downstream || [],
              downstreamDescriptions: c.downstreamDescriptions || {},
            });
          }}
          onClearConsumerTrace={() => setConsumerTrace(null)}
          etcdKeyspaces={etcdKeyspaces}
          keyspaceTrace={keyspaceTrace}
          onSelectKeyspace={(ks, etcdId) => {
            setMethodTrace(null);
            setFunctionTrace(null);
            setConsumerTrace(null);
            setRpcTrace(null);
            setRedisTrace(null);
            setCdcTrace(null);
            setKeyspaceTrace({
              etcd: etcdId,
              type: ks.type || "discovery",
              service: ks.service,
              name: ks.name,
              prefix: ks.prefix,
              listeners: (ks.listeners || []).map((l) => l.service),
            });
          }}
          onClearKeyspaceTrace={() => setKeyspaceTrace(null)}
          onSelectSubscription={(ks, etcdId, listenerId) => {
            setMethodTrace(null);
            setFunctionTrace(null);
            setConsumerTrace(null);
            setRpcTrace(null);
            setRedisTrace(null);
            setCdcTrace(null);
            // Same keyspaceTrace shape as onSelectKeyspace, but focused on ONE listener: the
            // `kt` branch draws registrant → etcd → this listener only (config: etcd → listener).
            // `focus` lets the diagram mark the service's SUB row active without lighting the
            // etcd node's whole KEY row.
            setKeyspaceTrace({
              etcd: etcdId,
              type: ks.type || "discovery",
              service: ks.service,
              name: ks.name,
              prefix: ks.prefix,
              listeners: [listenerId],
              focus: listenerId,
            });
          }}
          grpcContracts={grpcContracts}
          rpcTrace={rpcTrace}
          onSelectRpc={(r, serviceId) => {
            setMethodTrace(null);
            setFunctionTrace(null);
            setConsumerTrace(null);
            setKeyspaceTrace(null);
            setRedisTrace(null);
            setCdcTrace(null);
            setRpcTrace({
              service: serviceId,
              contract: r.contract,
              method: r.method,
            });
          }}
          onClearRpcTrace={() => setRpcTrace(null)}
          redisTrace={redisTrace}
          onSelectRedisKeyspace={(ks, redisId) => {
            setMethodTrace(null);
            setFunctionTrace(null);
            setConsumerTrace(null);
            setKeyspaceTrace(null);
            setRpcTrace(null);
            setCdcTrace(null);
            setRedisTrace({
              redis: redisId,
              name: ks.name,
              match: ks.match,
              type: ks.type,
              shorthand: ks.shorthand,
              writers: ks.writers || [],
              readers: ks.readers || [],
              writeModes: ks.writeModes || {},
            });
          }}
          onClearRedisTrace={() => setRedisTrace(null)}
          cdcRules={cdcRules}
          cdcTrace={cdcTrace}
          onSelectCdcRule={(rule, cdcId) => {
            setMethodTrace(null);
            setFunctionTrace(null);
            setConsumerTrace(null);
            setKeyspaceTrace(null);
            setRpcTrace(null);
            setRedisTrace(null);
            setCdcTrace({
              cdc: cdcId,
              table: rule.table,
              operations: rule.operations || [],
              stream: rule.stream!,
              topic: rule.topic!,
            });
          }}
          onClearCdcTrace={() => setCdcTrace(null)}
          interview={interviewInfo.interview}
          interviewTestVerdicts={interviewTestVerdicts}
        />
      </div>
      {showInterview && (
        <InterviewPanel
          systemId={SYSTEM_ID}
          interview={interviewInfo.interview}
          turnInFlight={interviewInfo.turnInFlight}
          skipPermissions={interviewInfo.skipPermissions}
          onRefresh={refreshInterview}
          onLaunch={enqueueSession}
          onClose={() => setShowInterview(false)}
        />
      )}
      {showTerminal && (
        <div className="terminal-panel">
          <Terminal
            systemId={SYSTEM_ID}
            session={terminalSession}
            onLaunched={onSessionLaunched}
            onSessionDone={handleSessionDone}
          />
        </div>
      )}
      {showCreateDb && (
        <CreateDatabase
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateDb(false)}
          onLaunch={enqueueSession}
        />
      )}
      {showCreateSvc && (
        <CreateService
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateSvc(false)}
        />
      )}
      {showCreateExternal && (
        <CreateExternalService
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateExternal(false)}
        />
      )}
      {showCreateClient && (
        <CreateClient
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateClient(false)}
        />
      )}
      {showCreateEventStream && (
        <CreateEventStream
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateEventStream(false)}
        />
      )}
      {showCreateEtcd && (
        <CreateEtcd
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateEtcd(false)}
        />
      )}
      {showCreateWebsockets && (
        <CreateWebsockets
          systemId={SYSTEM_ID}
          onClose={() => setShowCreateWebsockets(false)}
        />
      )}
      {showEndToEnd && (
        <EndToEndModal
          systemId={SYSTEM_ID}
          manifest={manifest}
          scenarios={scenarios}
          onLaunch={enqueueSession}
          onClose={() => setShowEndToEnd(false)}
        />
      )}
      {showSkills && <SkillsModal onClose={() => setShowSkills(false)} />}
      {showSettings && (
        <SettingsModal
          settings={settings}
          onSave={setSettings}
          onClose={() => setShowSettings(false)}
        />
      )}
      {showGrpcContracts && (
        <GrpcContractsModal
          systemId={SYSTEM_ID}
          onClose={() => setShowGrpcContracts(false)}
          onLaunch={enqueueSession}
        />
      )}
      {showModels && (
        <ModelsModal
          systemId={SYSTEM_ID}
          manifest={manifest}
          onLaunch={enqueueSession}
          onClose={() => setShowModels(false)}
        />
      )}
      {connectionTarget && (
        <ConnectionResilienceModal
          key={`${connectionTarget.from}->${connectionTarget.to}`}
          systemId={SYSTEM_ID}
          from={connectionTarget.from}
          to={connectionTarget.to}
          initial={
            (manifest.edges || []).find(
              (e) =>
                e.from === connectionTarget.from &&
                e.to === connectionTarget.to,
            )?.resilience || null
          }
          initialPool={
            (manifest.edges || []).find(
              (e) =>
                e.from === connectionTarget.from &&
                e.to === connectionTarget.to,
            )?.connection_pool || null
          }
          poolEligible={
            !(manifest.nodes || []).find((n) => n.id === connectionTarget.to)
              ?.external
          }
          onClose={() => setConnectionTarget(null)}
          onLaunch={enqueueSession}
        />
      )}
      {wsMethodsTier && wsInfo?.tier && (
        <WsSharedMethodsModal
          systemId={SYSTEM_ID}
          tier={wsInfo.tier}
          manifest={manifest}
          outages={outages}
          onClose={() => setWsMethodsTier(null)}
          onLaunch={enqueueSession}
        />
      )}
      {editTarget && (
        <NodeEditModal
          systemId={SYSTEM_ID}
          node={editTarget}
          manifest={manifest}
          current={outages[editTarget.id] || null}
          onClose={() => setEditTarget(null)}
          onLaunch={enqueueSession}
          onTraceMethod={(ep) => {
            // Trace the picked method on the main diagram and close the modal so it's
            // visible behind the (now-dismissed) overlay.
            setFunctionTrace(null);
            setConsumerTrace(null);
            setKeyspaceTrace(null);
            setMethodTrace(ep);
            setEditTarget(null);
          }}
        />
      )}
    </div>
  );
}

import type { PromInstantResponse, VectorSample } from './types/registries'

/**
 * Run a single instant PromQL query against the Prometheus HTTP API.
 *
 * `base` is the manifest's `prometheus_base` (e.g. "/api/prometheus"), which the
 * Vite dev proxy forwards to the Prometheus container. Returns the scalar value
 * of the first result series, or null if there is no data / the value is NaN.
 */
export async function queryInstant(base: string, query: string): Promise<number | null> {
  const url = `${base}/api/v1/query?query=${encodeURIComponent(query)}`
  const res = await fetch(url)
  if (!res.ok) throw new Error(`Prometheus HTTP ${res.status}`)

  const body = (await res.json()) as PromInstantResponse
  if (body.status !== 'success') {
    throw new Error(`Prometheus error: ${body.error || 'unknown'}`)
  }

  const result = body.data?.result
  if (!result || result.length === 0) return null

  // Instant vector / scalar: value is [timestamp, "stringified-number"].
  const raw = Number(result[0].value?.[1])
  return Number.isNaN(raw) ? null : raw
}

/**
 * Like queryInstant, but returns EVERY series of the instant vector:
 *   [{ labels: { instance, job, … }, value }]
 * Needed when the per-series identity matters — e.g. the etcd node's member
 * strip reads all N `up{job="etcd"}` series (one per member) where
 * queryInstant would collapse them to the first.
 */
export async function queryVector(base: string, query: string): Promise<VectorSample[]> {
  const url = `${base}/api/v1/query?query=${encodeURIComponent(query)}`
  const res = await fetch(url)
  if (!res.ok) throw new Error(`Prometheus HTTP ${res.status}`)

  const body = (await res.json()) as PromInstantResponse
  if (body.status !== 'success') {
    throw new Error(`Prometheus error: ${body.error || 'unknown'}`)
  }

  return (body.data?.result || []).map((r) => ({
    labels: r.metric || {},
    value: Number(r.value?.[1]),
  }))
}

/**
 * Run a single instant PromQL query against the Prometheus HTTP API.
 *
 * `base` is the manifest's `prometheus_base` (e.g. "/api/prometheus"), which the
 * Vite dev proxy forwards to the Prometheus container. Returns the scalar value
 * of the first result series, or null if there is no data / the value is NaN.
 */
export async function queryInstant(base, query) {
  const url = `${base}/api/v1/query?query=${encodeURIComponent(query)}`
  const res = await fetch(url)
  if (!res.ok) throw new Error(`Prometheus HTTP ${res.status}`)

  const body = await res.json()
  if (body.status !== 'success') {
    throw new Error(`Prometheus error: ${body.error || 'unknown'}`)
  }

  const result = body.data?.result
  if (!result || result.length === 0) return null

  // Instant vector / scalar: value is [timestamp, "stringified-number"].
  const raw = Number(result[0].value?.[1])
  return Number.isNaN(raw) ? null : raw
}

async function request(path, options = {}) {
  const res = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...options })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}
export const createJob = (params = {}) => request('/api/jobs', { method: 'POST', body: JSON.stringify({ params }) })
export const listJobs = () => request('/api/jobs')
export const getJob = (id) => request(`/api/jobs/${id}`)
export const retryJob = (id) => request(`/api/jobs/${id}/retry`, { method: 'POST' })
export const getResult = (id) => request(`/api/results/${id}`)
export const resultDownloadUrl = (id) => `/api/results/${id}/download`
export const listResults = () => request('/api/results')
export const getCacheStatus = () => request('/api/cache/status')
export const compareReports = (leftId, rightId) => request('/api/compare', {
  method: 'POST',
  body: JSON.stringify({ left_run_id: leftId, right_run_id: rightId })
})

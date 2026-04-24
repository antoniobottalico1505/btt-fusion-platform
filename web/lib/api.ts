const RAW_API_BASE =
  process.env.NODE_ENV === 'development'
    ? (
        process.env.NEXT_PUBLIC_API_BASE_URL ||
        process.env.API_BASE_URL ||
        'http://localhost:8000'
      )
    : ''

export const API_BASE = RAW_API_BASE.replace(/\/+$/, '')

export function getToken() {
  if (typeof window === 'undefined') return ''
  return localStorage.getItem('btt_token') || ''
}

export function setToken(token: string) {
  if (typeof window === 'undefined') return
  localStorage.setItem('btt_token', token)
}

export async function apiFetch<T>(path: string, init?: RequestInit, auth = false): Promise<T> {
  const headers = new Headers(init?.headers || {})

  const hasBody = init?.body !== undefined && init?.body !== null
  const isFormData = typeof FormData !== 'undefined' && init?.body instanceof FormData

  if (hasBody && !isFormData && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }

  if (auth) {
    const token = getToken()
    if (token) headers.set('Authorization', `Bearer ${token}`)
  }

  const url = path.startsWith('http') ? path : `${API_BASE}${path}`

  let res: Response
  try {
    res = await fetch(url, {
      ...init,
      headers,
      cache: 'no-store',
    })
  } catch (err: any) {
    throw new Error(`Network error su ${url}: ${err?.message || 'fetch failed'}`)
  }

  const contentType = res.headers.get('content-type') || ''

  if (!res.ok) {
    let message = `HTTP ${res.status}`

    try {
      if (contentType.includes('application/json')) {
        const data = await res.json()
        message = data.detail || data.message || message
      } else {
        const text = (await res.text()).trim()
        if (text) message = text.slice(0, 400)
      }
    } catch {}

    throw new Error(message)
  }

  if (res.status === 204) {
    return undefined as T
  }

  if (contentType.includes('application/json')) {
    return res.json()
  }

  return (await res.text()) as T
}
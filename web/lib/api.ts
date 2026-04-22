const RAW_API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  (process.env.NODE_ENV === 'development' ? 'http://localhost:8000' : '')

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

  if (!API_BASE && !path.startsWith('http')) {
    throw new Error('NEXT_PUBLIC_API_BASE_URL mancante o non esposta a build-time')
  }

  const url = path.startsWith('http') ? path : `${API_BASE}${path}`

  const res = await fetch(url, {
    ...init,
    headers,
    cache: 'no-store',
    mode: 'cors',
  })

  if (!res.ok) {
    let message = `HTTP ${res.status}`
    try {
      const data = await res.json()
      message = data.detail || data.message || message
    } catch {}
    throw new Error(message)
  }

  return res.json()
}

export const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000'

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
  headers.set('Content-Type', 'application/json')
  if (auth) {
    const token = getToken()
    if (token) headers.set('Authorization', `Bearer ${token}`)
  }
  const res = await fetch(`${API_BASE}${path}`, { ...init, headers, cache: 'no-store' })
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

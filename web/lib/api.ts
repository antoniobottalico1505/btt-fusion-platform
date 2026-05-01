let memoryToken = ''

export function getToken(): string {
  if (typeof window !== 'undefined') {
    const stored = window.localStorage.getItem('access_token') || ''
    if (stored) {
      memoryToken = stored
      return stored
    }
  }
  return memoryToken
}

export function setToken(token: string) {
  memoryToken = token || ''
  if (typeof window !== 'undefined') {
    if (token) {
      window.localStorage.setItem('access_token', token)
    } else {
      window.localStorage.removeItem('access_token')
    }
  }
}

export function clearToken() {
  setToken('')
}

export function getLocalVerifiedFlag(): boolean {
  if (typeof window === 'undefined') return false
  return window.localStorage.getItem('btt_email_verified') === '1'
}

export function setLocalVerifiedFlag(value: boolean) {
  if (typeof window === 'undefined') return
  if (value) {
    window.localStorage.setItem('btt_email_verified', '1')
  } else {
    window.localStorage.removeItem('btt_email_verified')
  }
}

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  'https://btt-fusion-backend.onrender.com'

export async function apiFetch<T = any>(
  path: string,
  init?: RequestInit,
  auth: boolean = false
): Promise<T> {
  const headers = new Headers(init?.headers || {})
  headers.set('Content-Type', 'application/json')

  if (auth) {
    const token = getToken()
    if (!token) {
      throw new Error('missing token')
    }
    headers.set('Authorization', `Bearer ${token}`)
  }

  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers,
    cache: 'no-store',
  })

  const raw = await res.text()
  let data: any = null

  try {
    data = raw ? JSON.parse(raw) : null
  } catch {
    data = raw
  }

  if (!res.ok) {
    const detail =
      (data && typeof data === 'object' && (data.detail || data.message)) ||
      (typeof data === 'string' ? data : '') ||
      `HTTP ${res.status}`

    throw new Error(String(detail))
  }

  return data as T
}
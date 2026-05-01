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

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  process.env.NEXT_PUBLIC_API_URL ||
  ''

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
  })

  let data: any = null
  const text = await res.text()

  try {
    data = text ? JSON.parse(text) : null
  } catch {
    data = text
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
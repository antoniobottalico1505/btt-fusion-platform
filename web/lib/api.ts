let memoryToken = ''

const DEFAULT_API_BASE = 'https://btt-fusion-backend.onrender.com'

function normalizeBase(value: string | undefined | null): string {
  return String(value || '').trim().replace(/\/+$/, '')
}

function isFrontendOrigin(base: string): boolean {
  if (typeof window === 'undefined') return false

  try {
    const target = new URL(base)
    const current = new URL(window.location.origin)
    return target.origin === current.origin
  } catch {
    return false
  }
}

function getApiBases(): string[] {
  const configured = normalizeBase(process.env.NEXT_PUBLIC_API_BASE_URL)
  const fallback = normalizeBase(DEFAULT_API_BASE)
  const bases: string[] = []

  // Se per errore Vercel punta al frontend bttcapital.cc, lo ignora.
  if (configured && !isFrontendOrigin(configured)) {
    bases.push(configured)
  }

  bases.push(fallback)

  return Array.from(new Set(bases.filter(Boolean)))
}

function makeApiError(message: string, status?: number, baseUrl?: string) {
  const err = new Error(message)
  ;(err as any).status = status
  ;(err as any).baseUrl = baseUrl
  return err
}

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

async function apiFetchFromBase<T>(
  baseUrl: string,
  path: string,
  init: RequestInit | undefined,
  headers: Headers
): Promise<T> {
  const res = await fetch(`${baseUrl}${path}`, {
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

    throw makeApiError(String(detail), res.status, baseUrl)
  }

  return data as T
}

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
      throw makeApiError('missing token', 401)
    }
    headers.set('Authorization', `Bearer ${token}`)
  }

  const bases = getApiBases()
  let lastError: any = null

  for (const baseUrl of bases) {
    try {
      return await apiFetchFromBase<T>(baseUrl, path, init, headers)
    } catch (e: any) {
      lastError = e

      const status = Number(e?.status || 0)
      const canRetryFallback =
        baseUrl !== DEFAULT_API_BASE &&
        (status === 0 || status === 404 || status === 405 || e instanceof TypeError)

      if (canRetryFallback) {
        continue
      }

      throw e
    }
  }

  throw lastError || makeApiError('API non disponibile')
}
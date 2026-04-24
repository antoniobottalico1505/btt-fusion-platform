import { NextRequest, NextResponse } from 'next/server'

export const dynamic = 'force-dynamic'

const RAW_BACKEND =
  process.env.API_BASE_URL ||
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  'http://localhost:8000'

const BACKEND = RAW_BACKEND.replace(/\/+$/, '')

async function proxy(req: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params
  const query = req.nextUrl.search || ''
  const target = `${BACKEND}/api/${path.join('/')}${query}`

  const headers = new Headers(req.headers)
  headers.delete('host')
  headers.delete('connection')
  headers.delete('content-length')

  const init: RequestInit = {
    method: req.method,
    headers,
    redirect: 'manual',
    cache: 'no-store',
  }

  if (req.method !== 'GET' && req.method !== 'HEAD') {
    init.body = await req.arrayBuffer()
  }

  let upstream: Response
  try {
    upstream = await fetch(target, init)
  } catch (err: any) {
    return NextResponse.json(
      {
        detail: `Proxy error verso backend (${target}): ${err?.message || 'fetch failed'}`,
      },
      { status: 502 }
    )
  }

  const outHeaders = new Headers(upstream.headers)
  outHeaders.delete('content-length')
  outHeaders.delete('content-encoding')

  return new NextResponse(upstream.body, {
    status: upstream.status,
    headers: outHeaders,
  })
}

export const GET = proxy
export const POST = proxy
export const PUT = proxy
export const PATCH = proxy
export const DELETE = proxy
export const OPTIONS = proxy
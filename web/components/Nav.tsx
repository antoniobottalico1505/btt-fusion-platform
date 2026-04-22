'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'

export function Nav() {
  const [me, setMe] = useState<any>(null)

  useEffect(() => {
    apiFetch('/api/auth/me', undefined, true).then(setMe).catch(() => setMe(null))
  }, [])

  return (
    <div className="topbar">
      <div className="shell topbar-inner">
        <div className="brand">
          <div className="brand-mark" />
          <div>
            <div>BTT Fusion</div>
            <div className="muted" style={{ fontSize: 12 }}>BTT Capital + Microcap Bot</div>
          </div>
        </div>
        <div className="nav">
          <Link href="/">Home</Link>
          <Link href="/dashboard">Dashboard</Link>
          <Link href="/microcap">Microcap</Link>
          <Link href="/btt">BTT Capital</Link>
          <Link href="/pricing">Pricing</Link>
          {me?.is_admin ? <Link href="/admin">Admin</Link> : null}
          {me ? <span className="pill">{me.email}</span> : <Link href="/login">Login</Link>}
        </div>
      </div>
    </div>
  )
}

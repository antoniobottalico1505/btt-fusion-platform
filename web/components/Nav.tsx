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
          <img
            src="/logo-mark.svg"
            alt="BTTcapital"
            style={{ width: 36, height: 36, borderRadius: 8 }}
          />
          <div>
            <div>BTTcapital</div>
            <div className="muted" style={{ fontSize: 12 }}>
              Private market intelligence platform
            </div>
          </div>
        </div>

        <div className="nav">
          <Link href="/">Home</Link>
          <Link href="/dashboard">Dashboard</Link>
          <Link href="/bttcrypto">BTTcrypto</Link>
          <Link href="/bttstock">BTTstock</Link>
          <Link href="/pricing">Accesso</Link>
          <Link href="/wallet">Wallet</Link>
          <Link href="/partners">For Partners</Link>
          <Link href="/methodology">Methodology</Link>
          <Link href="/risk">Risk</Link>
          <Link href="/faq">FAQ</Link>
          <Link href="/policy">Policy</Link>
          <Link href="/terms">Termini</Link>
          {me?.is_admin ? <Link href="/admin">Admin</Link> : null}
          {me ? <span className="pill">{me.email}</span> : <Link href="/login">Login</Link>}
        </div>
      </div>
    </div>
  )
}
'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function PricingPage() {
  const [me, setMe] = useState<any>(null)
  const [accepted, setAccepted] = useState(false)
  const [error, setError] = useState('')
  const [msg, setMsg] = useState('')

  useEffect(() => {
    apiFetch('/api/auth/me', undefined, true).then(setMe).catch(() => null)
  }, [])

  async function acceptTerms() {
    setError('')
    setMsg('')
    try {
      await apiFetch('/api/user/accept-terms', {
        method: 'POST',
        body: JSON.stringify({ accepted: true }),
      }, true)
      setAccepted(true)
      setMsg('Termini accettati')
    } catch (e: any) {
      setError(e.message)
    }
  }

  async function checkout(plan: string) {
    setError('')
    try {
      const res = await apiFetch<{ url: string }>(
        '/api/billing/checkout',
        { method: 'POST', body: JSON.stringify({ plan }) },
        true
      )
      window.location.href = res.url
    } catch (e: any) {
      setError(e.message)
    }
  }

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Abbonamenti BTTcapital</h1>
        <p className="section-sub">Scegli BTTcrypto, BTTstock oppure il bundle completo.</p>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {error ? <div className="bad">{error}</div> : null}

      <div className="card stack">
        <label style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <input type="checkbox" checked={accepted} onChange={(e) => setAccepted(e.target.checked)} />
          <span>Confermo di aver letto e accettato <Link href="/terms">Termini</Link> e <Link href="/policy">Policy</Link>.</span>
        </label>
        <button onClick={acceptTerms}>Salva accettazione termini</button>
      </div>

      <div className="grid-3">
        <div className="card stack">
          <h2 className="section-title">BTTcrypto</h2>
          <p>Mensile o annuale per il modulo crypto.</p>
          <button onClick={() => checkout('crypto_monthly')}>Checkout crypto mensile</button>
          <button className="secondary" onClick={() => checkout('crypto_yearly')}>Checkout crypto annuale</button>
        </div>

        <div className="card stack">
          <h2 className="section-title">BTTstock</h2>
          <p>Mensile o annuale per il modulo stock.</p>
          <button onClick={() => checkout('stock_monthly')}>Checkout stock mensile</button>
          <button className="secondary" onClick={() => checkout('stock_yearly')}>Checkout stock annuale</button>
        </div>

        <div className="card stack">
          <h2 className="section-title">BTTcapital Bundle</h2>
          <p>Accesso completo a BTTcrypto e BTTstock.</p>
          <button onClick={() => checkout('bundle_monthly')}>Checkout bundle mensile</button>
          <button className="secondary" onClick={() => checkout('bundle_yearly')}>Checkout bundle annuale</button>
        </div>
      </div>
    </div>
  )
}
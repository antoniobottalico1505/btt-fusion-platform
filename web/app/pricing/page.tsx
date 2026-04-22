'use client'

import { useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function PricingPage() {
  const [error, setError] = useState('')

  async function checkout(plan: 'monthly' | 'yearly') {
    setError('')
    try {
      const res = await apiFetch<{ url: string }>('/api/billing/checkout', { method: 'POST', body: JSON.stringify({ plan }) }, true)
      window.location.href = res.url
    } catch (e: any) {
      setError(e.message)
    }
  }

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Pricing</h1>
        <p className="section-sub">Trial 24 ore. Poi accesso premium mensile o annuale.</p>
      </div>
      {error ? <div className="bad">{error}</div> : null}
      <div className="grid-2">
        <div className="card"><h2 className="section-title">Mensile</h2><p className="section-sub">Accesso continuativo a dashboard, run e modalità premium.</p><button onClick={() => checkout('monthly')}>Checkout mensile</button></div>
        <div className="card"><h2 className="section-title">Annuale</h2><p className="section-sub">Stessa piattaforma, minor costo medio mensile.</p><button onClick={() => checkout('yearly')}>Checkout annuale</button></div>
      </div>
    </div>
  )
}

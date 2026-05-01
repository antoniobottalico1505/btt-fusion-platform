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
      await apiFetch(
        '/api/user/accept-terms',
        {
          method: 'POST',
          body: JSON.stringify({ accepted: true }),
        },
        true
      )
      setAccepted(true)
      setMsg('Termini accettati')
    } catch (e: any) {
      setError(e.message)
    }
  }

  async function checkout(plan: 'monthly' | 'yearly') {
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
        <h1 className="section-title">Accesso BTTcapital</h1>
        <p className="section-sub">
          Un solo accesso premium a BTTcrypto e BTTstock, in formula mensile o annuale.
        </p>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {error ? <div className="bad">{error}</div> : null}

      <div className="card stack">
        <label style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
          <input
            type="checkbox"
            checked={accepted}
            onChange={(e) => setAccepted(e.target.checked)}
          />
          <span>
            Confermo di aver letto e accettato <Link href="/terms">Termini</Link> e{' '}
            <Link href="/policy">Policy</Link>.
          </span>
        </label>
        <button onClick={acceptTerms}>Salva accettazione termini</button>
      </div>

      <div className="grid-2">
        <div className="card stack">
          <h2 className="section-title">BTTcapital Mensile</h2>
          <p>
            Accesso completo alla piattaforma privata, alle performance aggregate, alle dashboard e ai moduli BTTcrypto + BTTstock.
          </p>
          <p className="lead">€99 / mese</p>
          <button onClick={() => checkout('monthly')}>Checkout mensile</button>
        </div>

        <div className="card stack">
          <h2 className="section-title">BTTcapital Annuale</h2>
          <p>
            Accesso completo con struttura annuale ottimizzata per utilizzo continuativo e monitoraggio di lungo periodo.
          </p>
          <p className="lead">€990 / anno</p>
          <button className="secondary" onClick={() => checkout('yearly')}>
            Checkout annuale
          </button>
        </div>
      </div>

      <div className="card stack">
        <h2 className="section-title">Cosa include</h2>
        <div className="stack muted">
          <span>BTTcrypto con risultati aggregati reali, storico e metriche di qualità.</span>
          <span>BTTstock con ranking, performance aggregate e visualizzazione istituzionale.</span>
          <span>Dashboard combinata con media dei due settori e parametri aggiuntivi.</span>
          <span>Nessuna distribuzione del motore, del codice o della logica proprietaria.</span>
        </div>
      </div>
    </div>
  )
}
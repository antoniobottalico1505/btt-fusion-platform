'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import {
  apiFetch,
  getLocalVerifiedFlag,
  getToken,
  setLocalVerifiedFlag,
} from '@/lib/api'

type MeResponse = {
  email_verified: boolean
  accepted_terms_version: string
  subscription_status: string
  terms_ok?: boolean
}

export default function PricingPage() {
  const router = useRouter()
  const [me, setMe] = useState<MeResponse | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [msg, setMsg] = useState('')
  const [acceptChecked, setAcceptChecked] = useState(false)
  const [localVerified, setLocalVerified] = useState(false)

  async function refreshMe() {
    const token = getToken()
    if (!token) {
      setMe(null)
      return
    }

    try {
      const fresh = await apiFetch<MeResponse>('/api/auth/me', undefined, true)
      setMe(fresh)
      if (fresh?.email_verified) {
        setLocalVerifiedFlag(true)
        setLocalVerified(true)
      }
    } catch (e: any) {
      setError((prev) => prev || e.message || 'Errore caricamento utente')
    }
  }

  useEffect(() => {
    const lv = getLocalVerifiedFlag()
    setLocalVerified(lv)
    refreshMe()
  }, [])

  async function ensureLoggedIn() {
    const token = getToken()
    if (!token) {
      router.push('/login')
      return false
    }
    return true
  }

  async function acceptTerms() {
    setBusy(true)
    setError('')
    setMsg('')

    try {
      const ok = await ensureLoggedIn()
      if (!ok) return

      if (!acceptChecked) {
        setError('Devi spuntare il quadratino prima di confermare i termini')
        return
      }

      await apiFetch(
        '/api/user/accept-terms',
        {
          method: 'POST',
          body: JSON.stringify({ accepted: true }),
        },
        true
      )

      await refreshMe()
      setMsg('Termini accettati con successo')
    } catch (e: any) {
      const msgText = String(e.message || '')
      if (msgText.toLowerCase().includes('missing token')) {
        router.push('/login')
        return
      }
      if (msgText.toLowerCase().includes('not found')) {
        setError('Endpoint termini non trovato sul backend. Devi ridistribuire il backend Render corretto.')
        return
      }
      setError(msgText || 'Errore accettazione termini')
    } finally {
      setBusy(false)
    }
  }

  async function startCheckout(plan: string) {
    setBusy(true)
    setError('')
    setMsg('')

    try {
      const ok = await ensureLoggedIn()
      if (!ok) return

      const fresh = await apiFetch<MeResponse>('/api/auth/me', undefined, true)
      setMe(fresh)

      const verifiedNow = !!fresh.email_verified || localVerified
      if (fresh?.email_verified) {
        setLocalVerifiedFlag(true)
        setLocalVerified(true)
      }

      if (!verifiedNow) {
        setError('La tua email non risulta ancora verificata')
        return
      }

      if (!fresh.terms_ok && !fresh.accepted_terms_version) {
        setError('Devi prima accettare termini e policy')
        return
      }

      const res = await apiFetch<{ url: string }>(
        '/api/billing/checkout',
        {
          method: 'POST',
          body: JSON.stringify({ plan }),
        },
        true
      )

      if (!res?.url) {
        setError('Checkout non disponibile')
        return
      }

      window.location.href = res.url
    } catch (e: any) {
      const msgText = String(e.message || '')
      if (msgText.toLowerCase().includes('missing token')) {
        router.push('/login')
        return
      }
      if (msgText.toLowerCase().includes('not found')) {
        setError('Endpoint abbonamenti non trovato sul backend. Devi ridistribuire il backend Render corretto.')
        return
      }
      setError(msgText || 'Errore avvio checkout')
    } finally {
      setBusy(false)
    }
  }

  const effectiveVerified = !!me?.email_verified || localVerified
  const effectiveTerms = !!me?.terms_ok || !!me?.accepted_terms_version

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Accesso completo BTTcapital</h1>
        <p className="section-sub">
          Per attivare l’abbonamento devi essere loggato, email verificata e termini accettati.
        </p>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {error ? <div className="bad">{error}</div> : null}

      <div className="card stack">
        <div className="muted">Email verificata: {effectiveVerified ? 'Sì' : 'No'}</div>
        <div className="muted">Termini accettati: {effectiveTerms ? 'Sì' : 'No'}</div>
        <div className="muted">Abbonamento: {me?.subscription_status || 'inactive'}</div>
      </div>

      <div className="card stack">
        <label style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <input
            type="checkbox"
            checked={acceptChecked}
            onChange={(e) => setAcceptChecked(e.target.checked)}
          />
          <span>Ho letto e accetto termini e policy</span>
        </label>

        <button onClick={acceptTerms} disabled={busy}>
          Conferma accettazione termini
        </button>
      </div>

      <div className="grid-2">
        <div className="card stack">
          <h2 className="section-title">Mensile</h2>
          <button onClick={() => startCheckout('monthly')} disabled={busy}>
            Attiva abbonamento mensile
          </button>
        </div>

        <div className="card stack">
          <h2 className="section-title">Annuale</h2>
          <button onClick={() => startCheckout('yearly')} disabled={busy}>
            Attiva abbonamento annuale
          </button>
        </div>
      </div>
    </div>
  )
}
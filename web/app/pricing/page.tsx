'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import {
  apiFetch,
  clearToken,
  getLocalVerifiedFlag,
  getToken,
  setLocalVerifiedFlag,
} from '@/lib/api'

type MeResponse = {
  email?: string
  email_verified: boolean
  accepted_terms_version: string
  accepted_terms_at?: string | null
  subscription_status: string
  subscription_plan?: string
  terms_version?: string
  terms_ok?: boolean
}

function isAuthExpiredError(e: any): boolean {
  const status = Number(e?.status || 0)
  const msg = String(e?.message || '').toLowerCase()

  return (
    status === 401 ||
    msg.includes('missing token') ||
    msg.includes('invalid token') ||
    msg.includes('user not found')
  )
}

function isMissingEndpointError(e: any): boolean {
  const status = Number(e?.status || 0)
  const msg = String(e?.message || '').toLowerCase()

  return (
    status === 404 ||
    status === 405 ||
    msg === 'not found' ||
    msg.includes('not found')
  )
}

export default function PricingPage() {
  const router = useRouter()
  const [me, setMe] = useState<MeResponse | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [msg, setMsg] = useState('')
  const [acceptChecked, setAcceptChecked] = useState(false)
  const [localVerified, setLocalVerified] = useState(false)

  async function refreshMe(silent = true): Promise<MeResponse | null> {
    const token = getToken()

    if (!token) {
      setMe(null)
      return null
    }

    try {
      const fresh = await apiFetch<MeResponse>('/api/auth/me', undefined, true)
      setMe(fresh)

      if (fresh?.email_verified) {
        setLocalVerifiedFlag(true)
        setLocalVerified(true)
      } else {
        setLocalVerifiedFlag(false)
        setLocalVerified(false)
      }

      return fresh
    } catch (e: any) {
      if (isAuthExpiredError(e)) {
        clearToken()
        setMe(null)
        return null
      }

      if (isMissingEndpointError(e)) {
        if (!silent) {
          setError(
            'Backend Render non aggiornato: manca /api/auth/me. Devi deployare il backend aggiornato.'
          )
        }
        return null
      }

      if (!silent) {
        setError(e?.message || 'Errore caricamento utente')
      }

      return null
    }
  }

  useEffect(() => {
    const lv = getLocalVerifiedFlag()
    setLocalVerified(lv)
    refreshMe(true)
  }, [])

  async function ensureLoggedIn() {
    const token = getToken()

    if (!token) {
      router.push('/login?next=/pricing')
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

      await refreshMe(false)
      setMsg('Termini accettati con successo. Ora puoi attivare l’abbonamento.')
    } catch (e: any) {
      if (isAuthExpiredError(e)) {
        clearToken()
        router.push('/login?next=/pricing')
        return
      }

      if (isMissingEndpointError(e)) {
        setError(
          'Backend Render non aggiornato: manca /api/user/accept-terms. Devi deployare il backend corretto.'
        )
        return
      }

      setError(e?.message || 'Errore accettazione termini')
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

      if (!fresh?.email_verified) {
        setLocalVerifiedFlag(false)
        setLocalVerified(false)
        setError(
          'Il backend non vede questa email come verificata. Rientra dal link di verifica o fai logout/login dopo la verifica.'
        )
        return
      }

      setLocalVerifiedFlag(true)
      setLocalVerified(true)

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
      if (isAuthExpiredError(e)) {
        clearToken()
        router.push('/login?next=/pricing')
        return
      }

      if (isMissingEndpointError(e)) {
        setError(
          'Checkout non raggiungibile: Vercel sta puntando al backend sbagliato oppure Render non ha ancora ridistribuito il backend aggiornato.'
        )
        return
      }

      setError(e?.message || 'Errore avvio checkout')
    } finally {
      setBusy(false)
    }
  }

  const serverVerified = !!me?.email_verified
  const effectiveTerms = !!me?.terms_ok || !!me?.accepted_terms_version
  const hasToken = !!getToken()

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

      {!hasToken && localVerified ? (
        <div className="good">
          Email verificata. Ora devi solo fare login per accettare termini e abbonarti.
        </div>
      ) : null}

      <div className="card stack">
        <div className="muted">Login attivo: {hasToken ? 'Sì' : 'No'}</div>
        <div className="muted">Email verificata lato server: {serverVerified ? 'Sì' : 'No'}</div>
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
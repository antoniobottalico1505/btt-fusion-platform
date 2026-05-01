'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { apiFetch, setLocalVerifiedFlag } from '@/lib/api'

export default function VerifyEmailPage() {
  const [loading, setLoading] = useState(true)
  const [success, setSuccess] = useState(false)
  const [message, setMessage] = useState('')

  useEffect(() => {
    let token = ''

    try {
      const params = new URLSearchParams(window.location.search)
      token = params.get('token') || ''
    } catch {
      token = ''
    }

    if (!token) {
      setLoading(false)
      setSuccess(false)
      setMessage('Token di verifica mancante')
      return
    }

    apiFetch<{ message: string; email_verified?: boolean }>(
      `/api/auth/verify-email?token=${encodeURIComponent(token)}`
    )
      .then((res) => {
        setLocalVerifiedFlag(true)
        setSuccess(true)
        setMessage(
          res?.message ||
            'Email verificata con successo. Da ora puoi entrare sempre senza rifare la verifica.'
        )
      })
      .catch((e: any) => {
        setSuccess(false)
        setMessage(e.message || 'Errore verifica email')
      })
      .finally(() => {
        setLoading(false)
      })
  }, [])

  return (
    <div className="auth-card panel">
      <h1 className="section-title">Verifica email</h1>

      {loading ? (
        <div className="muted">Verifica in corso...</div>
      ) : success ? (
        <div className="good">{message}</div>
      ) : (
        <div className="bad">{message}</div>
      )}

      <div className="stack" style={{ marginTop: 16 }}>
        <Link href="/login">
          <button>Vai al login</button>
        </Link>
        <Link href="/pricing">
          <button className="secondary">Vai agli abbonamenti</button>
        </Link>
      </div>
    </div>
  )
}
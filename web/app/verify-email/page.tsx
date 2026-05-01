'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import { useSearchParams } from 'next/navigation'
import { apiFetch } from '@/lib/api'

export default function VerifyEmailPage() {
  const searchParams = useSearchParams()
  const [loading, setLoading] = useState(true)
  const [success, setSuccess] = useState(false)
  const [message, setMessage] = useState('')

  useEffect(() => {
    const token = searchParams.get('token')

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
        setSuccess(true)
        setMessage(
          res?.message ||
            'Email verificata con successo. Da ora puoi entrare sempre senza ripetere la verifica.'
        )
      })
      .catch((e: any) => {
        setSuccess(false)
        setMessage(e.message || 'Errore verifica email')
      })
      .finally(() => {
        setLoading(false)
      })
  }, [searchParams])

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
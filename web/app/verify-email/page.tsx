'use client'

import { useEffect, useState } from 'react'

export default function VerifyEmailPage() {
  const [message, setMessage] = useState('Verifica in corso...')

  useEffect(() => {
    const token = new URLSearchParams(window.location.search).get('token')
    if (!token) {
      setMessage('Token mancante')
      return
    }

    fetch(`/api/auth/verify-email?token=${encodeURIComponent(token)}`)
      .then(async (r) => {
        const data = await r.json()
        setMessage(data.message || 'Email verificata')
      })
      .catch(() => setMessage('Errore verifica email'))
  }, [])

  return (
    <div className="shell section">
      <div className="card"><h1 className="section-title">{message}</h1></div>
    </div>
  )
}
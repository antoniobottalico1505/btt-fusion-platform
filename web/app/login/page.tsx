'use client'

import Link from 'next/link'
import { useRouter, useSearchParams } from 'next/navigation'
import { useState } from 'react'
import { apiFetch, setToken } from '@/lib/api'

export default function LoginPage() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')

  const verifyMsg = searchParams.get('verify')

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    try {
      const res = await apiFetch<{ access_token: string }>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ email, password }),
      })
      setToken(res.access_token)
      router.push('/dashboard')
    } catch (err: any) {
      setError(err.message)
    }
  }

  return (
    <div className="auth-card panel">
      <h1 className="section-title">Login</h1>
      <p className="section-sub">Accedi dopo aver verificato la tua email.</p>

      {verifyMsg ? (
        <div className="good">Controlla la tua email e verifica l’account prima del login.</div>
      ) : null}

      <form className="stack" onSubmit={submit}>
        <input placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} />
        <input placeholder="Password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
        {error ? <div className="bad">{error}</div> : null}
        <button type="submit">Entra</button>
      </form>

      <p className="muted">Nessun account? <Link href="/register">Registrati</Link></p>
      <p className="muted"><Link href="/forgot-password">Password dimenticata?</Link></p>
    </div>
  )
}
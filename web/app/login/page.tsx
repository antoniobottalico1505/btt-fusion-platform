'use client'

import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { useEffect, useState } from 'react'
import { apiFetch, setLocalVerifiedFlag, setToken } from '@/lib/api'

export default function LoginPage() {
  const router = useRouter()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [verifyMsg, setVerifyMsg] = useState(false)
  const [nextPath, setNextPath] = useState('/dashboard')

  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search)
      setVerifyMsg(params.get('verify') === '1')

      const rawNext = params.get('next') || '/dashboard'
      if (rawNext.startsWith('/') && !rawNext.startsWith('//')) {
        setNextPath(rawNext)
      } else {
        setNextPath('/dashboard')
      }
    } catch {
      setVerifyMsg(false)
      setNextPath('/dashboard')
    }
  }, [])

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setError('')

    try {
      const res = await apiFetch<{ access_token: string }>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ email, password }),
      })

      if (!res?.access_token) {
        setError('Token login non ricevuto')
        return
      }

      setToken(res.access_token)
      setLocalVerifiedFlag(true)
      router.push(nextPath)
    } catch (err: any) {
      setError(err.message || 'Errore login')
    }
  }

  return (
    <div className="auth-card panel">
      <h1 className="section-title">Login</h1>
      <p className="section-sub">Accedi dopo aver verificato la tua email.</p>

      {verifyMsg ? (
        <div className="good">
          Email verificata. Ora fai login: poi tornerai automaticamente alla pagina corretta.
        </div>
      ) : null}

      <form className="stack" onSubmit={submit}>
        <input
          placeholder="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
        <input
          placeholder="Password"
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
        {error ? <div className="bad">{error}</div> : null}
        <button type="submit">Entra</button>
      </form>

      <p className="muted">
        Nessun account? <Link href="/register">Registrati</Link>
      </p>
      <p className="muted">
        <Link href="/forgot-password">Password dimenticata?</Link>
      </p>
    </div>
  )
}
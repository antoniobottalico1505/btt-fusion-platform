'use client'

import { useEffect, useMemo, useState } from 'react'
import { apiFetch, getToken, goToLogin, isAuthMissingOrExpired } from '@/lib/api'
import {
  requestWalletConnection,
  sendErc20Approval,
  sendWalletTransaction,
} from '@/lib/wallet'

type WalletState = {
  wallet_connected: boolean
  wallet_address: string
  wallet_chain_id: number
  subscription_status: string
  email_verified: boolean
  terms_ok: boolean
  non_custodial_ready: boolean
}

const BASE_CHAIN_ID = 8453
const BASE_USDC = '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
const BASE_WETH = '0x4200000000000000000000000000000000000006'

function usdcToRaw(value: string): string {
  const clean = String(value || '0').replace(',', '.').trim()

  if (!/^\d+(\.\d{0,6})?$/.test(clean)) {
    throw new Error('Importo USDC non valido')
  }

  const [whole, frac = ''] = clean.split('.')
  const raw = `${whole}${frac.padEnd(6, '0')}`.replace(/^0+(?=\d)/, '')

  return raw || '0'
}

function shortHash(hash: string): string {
  if (!hash) return ''
  return hash.length > 18 ? `${hash.slice(0, 10)}...${hash.slice(-8)}` : hash
}

export default function WalletPage() {
  const [wallet, setWallet] = useState<WalletState | null>(null)
  const [amountUsdc, setAmountUsdc] = useState('1')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')
  const [step, setStep] = useState('')
  const [lastApprovalHash, setLastApprovalHash] = useState('')
  const [lastSwapHash, setLastSwapHash] = useState('')

  const ready = !!wallet?.non_custodial_ready
  const walletConnected = !!wallet?.wallet_connected

  const amountPreview = useMemo(() => {
    try {
      return usdcToRaw(amountUsdc)
    } catch {
      return '0'
    }
  }, [amountUsdc])

  async function loadWallet() {
    try {
      if (!getToken()) {
        setWallet(null)
        return
      }

      const res = await apiFetch<WalletState>('/api/wallet/me', undefined, true)
      setWallet(res)
      setErr('')
    } catch (e: any) {
      if (isAuthMissingOrExpired(e)) {
        goToLogin('/wallet')
        return
      }

      setErr(e.message || 'Errore caricamento wallet')
    }
  }

  useEffect(() => {
    loadWallet()
  }, [])

  async function connectWallet(): Promise<WalletState | null> {
    setBusy(true)
    setErr('')
    setMsg('')
    setStep('Collegamento wallet in corso...')

    try {
      if (!getToken()) {
        goToLogin('/wallet')
        return null
      }

      const payload = await requestWalletConnection()

      const res = await apiFetch<WalletState>(
        '/api/wallet/connect',
        {
          method: 'POST',
          body: JSON.stringify(payload),
        },
        true
      )

      setWallet(res)
      setMsg('Wallet collegato correttamente.')
      setStep('')
      await loadWallet()
      return res
    } catch (e: any) {
      if (isAuthMissingOrExpired(e)) {
        goToLogin('/wallet')
        return null
      }

      setErr(e.message || 'Collegamento wallet non riuscito')
      setStep('')
      return null
    } finally {
      setBusy(false)
    }
  }

  async function disconnectWallet() {
    setBusy(true)
    setErr('')
    setMsg('')
    setStep('Scollegamento wallet...')

    try {
      if (!getToken()) {
        goToLogin('/wallet')
        return
      }

      await apiFetch('/api/wallet/disconnect', { method: 'DELETE' }, true)

      setLastApprovalHash('')
      setLastSwapHash('')
      setMsg('Wallet scollegato.')
      setStep('')
      await loadWallet()
    } catch (e: any) {
      if (isAuthMissingOrExpired(e)) {
        goToLogin('/wallet')
        return
      }

      setErr(e.message || 'Errore scollegamento wallet')
      setStep('')
    } finally {
      setBusy(false)
    }
  }

  async function authorizeOperation() {
    setBusy(true)
    setErr('')
    setMsg('')
    setStep('')
    setLastApprovalHash('')
    setLastSwapHash('')

    try {
      if (!getToken()) {
        goToLogin('/wallet')
        return
      }

      let currentWallet = wallet

      if (!currentWallet?.wallet_connected) {
        currentWallet = await connectWallet()
      }

      if (!currentWallet?.wallet_connected) {
        setErr('Wallet non collegato.')
        return
      }

      if (!currentWallet.email_verified) {
        setErr('Completa prima la verifica email.')
        return
      }

      if (!currentWallet.terms_ok) {
        setErr('Accetta prima termini e policy.')
        return
      }

      if (currentWallet.subscription_status !== 'active') {
        setErr('Serve un abbonamento attivo.')
        return
      }

      const raw = usdcToRaw(amountUsdc)

      if (BigInt(raw) <= 0n) {
        setErr('Inserisci un importo maggiore di zero.')
        return
      }

      setStep('Preparazione operazione...')

      const firstQuote = await apiFetch<any>(
        '/api/wallet/zeroex/quote',
        {
          method: 'POST',
          body: JSON.stringify({
            chain_id: BASE_CHAIN_ID,
            sell_token: BASE_USDC,
            buy_token: BASE_WETH,
            sell_amount: raw,
          }),
        },
        true
      )

      const allowanceIssue = firstQuote?.quote?.issues?.allowance
      const spender = allowanceIssue?.spender || firstQuote?.allowance_target || ''

      if (allowanceIssue && spender) {
        setStep('Conferma autorizzazione nel wallet.')

        const approvalHash = await sendErc20Approval({
          token: BASE_USDC,
          spender,
          amountRaw: raw,
          from: currentWallet.wallet_address,
        })

        setLastApprovalHash(approvalHash)
        setStep('Autorizzazione inviata. Preparazione finale...')

        await new Promise((resolve) => setTimeout(resolve, 7000))
      }

      setStep('Conferma operazione nel wallet.')

      const finalQuote = await apiFetch<any>(
        '/api/wallet/zeroex/quote',
        {
          method: 'POST',
          body: JSON.stringify({
            chain_id: BASE_CHAIN_ID,
            sell_token: BASE_USDC,
            buy_token: BASE_WETH,
            sell_amount: raw,
          }),
        },
        true
      )

      if (!finalQuote?.transaction) {
        setErr('Operazione non disponibile in questo momento.')
        return
      }

      const txHash = await sendWalletTransaction(finalQuote.transaction)

      setLastSwapHash(txHash)
      setMsg('Operazione inviata.')
      setStep('')
    } catch (e: any) {
      if (isAuthMissingOrExpired(e)) {
        goToLogin('/wallet')
        return
      }

      setErr(e.message || 'Operazione non riuscita')
      setStep('')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Wallet</h1>
        <p className="section-sub">
          Collega il wallet e gestisci le operazioni live in modalità non-custodial.
        </p>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {err ? <div className="bad">{err}</div> : null}
      {step ? <div className="good">{step}</div> : null}

      <div className="card stack">
        <h2 className="section-title">Stato wallet</h2>

        <div className="muted">Wallet collegato: {walletConnected ? 'Sì' : 'No'}</div>
        <div className="muted">Address: {wallet?.wallet_address || 'N/D'}</div>
        <div className="muted">Rete: Base</div>
        <div className="muted">Email verificata: {wallet?.email_verified ? 'Sì' : 'No'}</div>
        <div className="muted">Termini accettati: {wallet?.terms_ok ? 'Sì' : 'No'}</div>
        <div className="muted">Abbonamento: {wallet?.subscription_status || 'inactive'}</div>
        <div className="muted">Operatività live: {ready ? 'Attiva' : 'Non attiva'}</div>

        <div className="actions">
          <button onClick={() => connectWallet()} disabled={busy}>
            {walletConnected ? 'Ricollega wallet' : 'Collega wallet'}
          </button>

          <button className="ghost" onClick={disconnectWallet} disabled={busy || !walletConnected}>
            Scollega wallet
          </button>
        </div>
      </div>

      <div className="card stack">
        <h2 className="section-title">Operazione live</h2>

        <label className="stack">
          <span className="muted">Capitale operativo in USDC</span>
          <input
            value={amountUsdc}
            inputMode="decimal"
            placeholder="1"
            onChange={(e) => setAmountUsdc(e.target.value)}
          />
        </label>

        <button onClick={authorizeOperation} disabled={busy}>
          Autorizza dal wallet
        </button>

        <div className="muted">
          Importo selezionato: {amountUsdc || '0'} USDC
        </div>
      </div>

      {(lastApprovalHash || lastSwapHash) ? (
        <div className="card stack">
          <h2 className="section-title">Ultima operazione</h2>

          {lastApprovalHash ? (
            <div className="muted">Autorizzazione: {shortHash(lastApprovalHash)}</div>
          ) : null}

          {lastSwapHash ? (
            <div className="muted">Transazione: {shortHash(lastSwapHash)}</div>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}
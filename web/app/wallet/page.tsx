'use client'

import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'
import { requestWalletConnection, sendWalletTransaction } from '@/lib/wallet'

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

export default function WalletPage() {
  const [wallet, setWallet] = useState<WalletState | null>(null)
  const [sellToken, setSellToken] = useState(BASE_USDC)
  const [buyToken, setBuyToken] = useState(BASE_WETH)
  const [sellAmount, setSellAmount] = useState('1000000')
  const [chainId, setChainId] = useState(BASE_CHAIN_ID)
  const [quote, setQuote] = useState<any>(null)
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')

  async function loadWallet() {
    try {
      const res = await apiFetch<WalletState>('/api/wallet/me', undefined, true)
      setWallet(res)
    } catch (e: any) {
      setErr(e.message || 'Errore caricamento wallet')
    }
  }

  useEffect(() => {
    loadWallet()
  }, [])

  async function connectWallet() {
    setBusy(true)
    setErr('')
    setMsg('')

    try {
      const payload = await requestWalletConnection()
      const res = await apiFetch<WalletState>(
        '/api/wallet/connect',
        {
          method: 'POST',
          body: JSON.stringify(payload),
        },
        true
      )

      setWallet(res as any)
      setMsg('Wallet collegato correttamente')
      await loadWallet()
    } catch (e: any) {
      setErr(e.message || 'Errore collegamento wallet')
    } finally {
      setBusy(false)
    }
  }

  async function disconnectWallet() {
    setBusy(true)
    setErr('')
    setMsg('')

    try {
      await apiFetch('/api/wallet/disconnect', { method: 'DELETE' }, true)
      setQuote(null)
      setMsg('Wallet scollegato')
      await loadWallet()
    } catch (e: any) {
      setErr(e.message || 'Errore scollegamento wallet')
    } finally {
      setBusy(false)
    }
  }

  async function getQuote() {
    setBusy(true)
    setErr('')
    setMsg('')
    setQuote(null)

    try {
      const res = await apiFetch<any>(
        '/api/wallet/zeroex/quote',
        {
          method: 'POST',
          body: JSON.stringify({
            chain_id: chainId,
            sell_token: sellToken,
            buy_token: buyToken,
            sell_amount: sellAmount,
          }),
        },
        true
      )

      setQuote(res)
      setMsg('Quote generata. Controlla e firma dal wallet solo se vuoi eseguire.')
    } catch (e: any) {
      setErr(e.message || 'Errore generazione quote')
    } finally {
      setBusy(false)
    }
  }

  async function executeQuote() {
    setBusy(true)
    setErr('')
    setMsg('')

    try {
      if (!quote?.transaction) {
        setErr('Prima genera una quote')
        return
      }

      const txHash = await sendWalletTransaction(quote.transaction)
      setMsg(`Transazione inviata: ${txHash}`)
    } catch (e: any) {
      setErr(e.message || 'Errore invio transazione')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Wallet non-custodial</h1>
        <p className="section-sub">
          Collega il wallet: BTTcapital prepara segnali e transazioni, ma il tuo wallet firma. Il server non conserva la tua private key.
        </p>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {err ? <div className="bad">{err}</div> : null}

      <div className="card stack">
        <h2 className="section-title">Stato wallet</h2>
        <div className="muted">Wallet collegato: {wallet?.wallet_connected ? 'Sì' : 'No'}</div>
        <div className="muted">Address: {wallet?.wallet_address || 'N/D'}</div>
        <div className="muted">Chain ID: {wallet?.wallet_chain_id || 'N/D'}</div>
        <div className="muted">Email verificata: {wallet?.email_verified ? 'Sì' : 'No'}</div>
        <div className="muted">Termini accettati: {wallet?.terms_ok ? 'Sì' : 'No'}</div>
        <div className="muted">Abbonamento: {wallet?.subscription_status || 'inactive'}</div>
        <div className="muted">Live non-custodial pronto: {wallet?.non_custodial_ready ? 'Sì' : 'No'}</div>

        <div className="actions">
          <button onClick={connectWallet} disabled={busy}>Collega wallet</button>
          <button className="ghost" onClick={disconnectWallet} disabled={busy}>Scollega wallet</button>
        </div>
      </div>

      <div className="card stack">
        <h2 className="section-title">Esecuzione non-custodial via wallet</h2>
        <p className="section-sub">
          Questo blocco serve per testare l’esecuzione: il backend prepara la quote 0x, poi il wallet firma e invia.
        </p>

        <label className="stack">
          <span className="muted">Chain ID</span>
          <input value={chainId} onChange={(e) => setChainId(Number(e.target.value || BASE_CHAIN_ID))} />
        </label>

        <label className="stack">
          <span className="muted">Sell token</span>
          <input value={sellToken} onChange={(e) => setSellToken(e.target.value)} />
        </label>

        <label className="stack">
          <span className="muted">Buy token</span>
          <input value={buyToken} onChange={(e) => setBuyToken(e.target.value)} />
        </label>

        <label className="stack">
          <span className="muted">Sell amount raw units</span>
          <input value={sellAmount} onChange={(e) => setSellAmount(e.target.value)} />
        </label>

        <div className="actions">
          <button onClick={getQuote} disabled={busy || !wallet?.non_custodial_ready}>
            Genera quote
          </button>
          <button className="secondary" onClick={executeQuote} disabled={busy || !quote?.transaction}>
            Firma e invia dal wallet
          </button>
        </div>
      </div>

      {quote ? (
        <div className="card stack">
          <h2 className="section-title">Quote generata</h2>
          <div className="muted">Allowance target: {quote.allowance_target || 'N/D'}</div>
          <pre className="log">{JSON.stringify(quote.transaction, null, 2)}</pre>
        </div>
      ) : null}
    </div>
  )
}
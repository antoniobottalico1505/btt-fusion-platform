'use client'

import { useEffect, useMemo, useState } from 'react'
import { apiFetch } from '@/lib/api'
import {
  LineChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

type ChartPoint = {
  x: number
  label?: string
  profit_pct: number
  profit_money: number
}

type PublicMetrics = {
  metric_key: string | null
  point_count: number
  avg_pct: number | null
  best_pct: number | null
  worst_pct: number | null
  last_pct: number | null
  positives: number
  negatives: number
  chart: ChartPoint[]
}

type CombinedPoint = {
  x: number
  crypto_profit_pct: number
  stock_profit_pct: number
  combined_profit_pct: number
}

function n(v: unknown): number {
  const x = Number(v)
  return Number.isFinite(x) ? x : 0
}

function pctSigned(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
}

function maybePct(v: unknown): string {
  const x = Number(v)
  if (!Number.isFinite(x)) return 'N/D'
  return pctSigned(x)
}

function shortError(value: unknown): string {
  const s = String(value || '').trim()
  if (!s) return ''
  return s.length > 700 ? `${s.slice(0, 700)}...` : s
}

export default function DashboardPage() {
  const [me, setMe] = useState<any>(null)
  const [crypto, setCrypto] = useState<any>(null)
  const [stock, setStock] = useState<any>(null)
  const [error, setError] = useState('')
  const [msg, setMsg] = useState('')
  const [busy, setBusy] = useState(false)

  async function loadAll() {
    setError('')

    let meRes: any = null
    try {
      meRes = await apiFetch('/api/auth/me', undefined, true)
      setMe(meRes)
    } catch {
      setMe(null)
    }

    const stockPromise = apiFetch('/api/public/btt/latest')
      .then(setStock)
      .catch(() => setStock(null))

    if (meRes) {
      try {
        const cryptoRes = await apiFetch('/api/user/microcap/status', undefined, true)
        setCrypto(cryptoRes)
      } catch (e: any) {
        setError(e.message || 'Errore caricamento sessione microcap')
      }
    } else {
      try {
        const fallbackCrypto = await apiFetch('/api/public/microcap')
        setCrypto(fallbackCrypto)
      } catch (e: any) {
        setError(e.message || 'Errore caricamento BTTcrypto')
      }
    }

    await stockPromise
  }

  useEffect(() => {
    loadAll()
    const t = setInterval(loadAll, 5000)
    return () => clearInterval(t)
  }, [])

  async function startPaper() {
    setBusy(true)
    setError('')
    setMsg('')

    try {
      const res = await apiFetch<any>('/api/user/microcap/start-paper', { method: 'POST' }, true)
      const status = res?.status || res

      if (!status?.running) {
        setError(shortError(status?.last_error || 'Avvio paper fallito'))
      } else {
        setMsg('Istanza personale paper attivata')
      }

      setCrypto((prev: any) => ({
        ...(prev || {}),
        process: status,
        session_scope: 'user',
      }))

      await loadAll()
    } catch (e: any) {
      setError(e.message || 'Errore avvio paper')
    } finally {
      setBusy(false)
    }
  }

  async function startLive() {
    setBusy(true)
    setError('')
    setMsg('')

    try {
      const res = await apiFetch<any>('/api/user/microcap/start-live', { method: 'POST' }, true)
      const status = res?.status || res

      if (!status?.running) {
        setError(shortError(status?.last_error || 'Avvio live fallito'))
      } else {
        setMsg('Istanza personale live attivata')
      }

      setCrypto((prev: any) => ({
        ...(prev || {}),
        process: status,
        session_scope: 'user',
      }))

      await loadAll()
    } catch (e: any) {
      setError(e.message || 'Errore avvio live')
    } finally {
      setBusy(false)
    }
  }

  async function stopSession() {
    setBusy(true)
    setError('')
    setMsg('')

    try {
      const res = await apiFetch<any>('/api/user/microcap/stop', { method: 'POST' }, true)
      const status = res?.status || res

      setMsg('Istanza personale fermata')
      setCrypto((prev: any) => ({
        ...(prev || {}),
        process: status,
        session_scope: 'user',
      }))

      await loadAll()
    } catch (e: any) {
      setError(e.message || 'Errore stop sessione')
    } finally {
      setBusy(false)
    }
  }

  const cryptoSummary = crypto?.summary || crypto?.dashboard?.summary || {}
  const cryptoChart: ChartPoint[] = cryptoSummary?.chart || []

  const stockSummary = stock?.summary_metrics || {}
  const stockPublic: PublicMetrics = stockSummary?.public_metrics || {
    metric_key: null,
    point_count: 0,
    avg_pct: null,
    best_pct: null,
    worst_pct: null,
    last_pct: null,
    positives: 0,
    negatives: 0,
    chart: [],
  }

  const stockChart: ChartPoint[] = Array.isArray(stockPublic.chart) ? stockPublic.chart : []

  const combinedChart = useMemo<CombinedPoint[]>(() => {
    const maxLen = Math.max(cryptoChart.length, stockChart.length)

    return Array.from({ length: maxLen }).map((_, idx: number) => {
      const c = cryptoChart[idx]
      const s = stockChart[idx]

      const cryptoPct = c ? n(c.profit_pct) : 0
      const stockPct = s ? n(s.profit_pct) : 0

      return {
        x: idx + 1,
        crypto_profit_pct: cryptoPct,
        stock_profit_pct: stockPct,
        combined_profit_pct: Number((((cryptoPct + stockPct) / 2)).toFixed(2)),
      }
    })
  }, [cryptoChart, stockChart])

  const combinedNow =
    combinedChart.length > 0
      ? combinedChart[combinedChart.length - 1]
      : {
          x: 0,
          crypto_profit_pct: 0,
          stock_profit_pct: 0,
          combined_profit_pct: 0,
        }

  const spreadPct = n(combinedNow.crypto_profit_pct) - n(combinedNow.stock_profit_pct)

  const sectorLeader =
    n(combinedNow.crypto_profit_pct) > n(combinedNow.stock_profit_pct)
      ? 'BTTcrypto'
      : n(combinedNow.crypto_profit_pct) < n(combinedNow.stock_profit_pct)
        ? 'BTTstock'
        : 'Parità'

  const positiveCombinedPoints = combinedChart.filter(
    (p: CombinedPoint) => n(p.combined_profit_pct) > 0
  ).length

  const consistencyPct = combinedChart.length
    ? (positiveCombinedPoints / combinedChart.length) * 100
    : 0

  const liveUnlocked = !!crypto?.live_unlocked
  const liveAvailable = !!crypto?.live_available
  const currentMode = crypto?.process?.mode || crypto?.public_mode || 'paper'
  const sessionScope = crypto?.session_scope || crypto?.process?.scope || 'public'
  const processRunning = !!crypto?.process?.running
  const processError = shortError(crypto?.process?.last_error)
  const processExitCode = crypto?.process?.exit_code

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Dashboard BTTcapital</h1>
        <p className="section-sub">
          Vista media aggregata di BTTcrypto e BTTstock senza esposizione pubblica di token, stock, ticker o numeri asset-level.
        </p>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {error ? <div className="bad">{error}</div> : null}

      <div className="card">
        <h2 className="section-title">Istanza personale microcap</h2>
        <div className="stack muted">
          <span>Email verificata: {me?.email_verified ? 'Sì' : 'No'}</span>
          <span>Abbonamento: {me?.subscription_status || 'inactive'}</span>
          <span>Live disponibile lato server: {liveAvailable ? 'Sì' : 'No'}</span>
          <span>Live sbloccato per questo account: {liveUnlocked ? 'Sì' : 'No'}</span>
          <span>Modalità attuale: {currentMode}</span>
          <span>Session scope: {sessionScope}</span>
          <span>Motore attivo: {processRunning ? 'Sì' : 'No'}</span>
          <span>Exit code: {processExitCode ?? 'N/D'}</span>
        </div>

        <div className="actions" style={{ marginTop: 16 }}>
          <button onClick={startPaper} disabled={busy}>
            Avvia paper personale
          </button>
          <button
            className="secondary"
            onClick={startLive}
            disabled={busy || !liveUnlocked}
          >
            Avvia live personale
          </button>
          <button
            className="ghost"
            onClick={stopSession}
            disabled={busy}
          >
            Ferma sessione personale
          </button>
        </div>

        {processError ? (
          <div className="bad" style={{ marginTop: 16 }}>
            {processError}
          </div>
        ) : null}
      </div>

      <div className="card">
        <h2 className="section-title">Base temporale dei risultati</h2>
        <p className="section-sub">
          I risultati pubblici mostrati in dashboard partono dal <strong>29 aprile 2016 alle 9.30 p.m.</strong>
        </p>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Media rendimento aggregato</span>
          <strong>{pctSigned(n(combinedNow.combined_profit_pct))}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Differenziale settori %</span>
          <strong>{pctSigned(spreadPct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Settore leader</span>
          <strong>{sectorLeader}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Consistency rate</span>
          <strong>{pctSigned(consistencyPct)}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Rendimento BTTcrypto</span>
          <strong>{maybePct(cryptoSummary?.profit_pct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento medio BTTstock</span>
          <strong>{maybePct(stockPublic?.avg_pct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Migliore % BTTstock</span>
          <strong>{maybePct(stockPublic?.best_pct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Peggiore % BTTstock</span>
          <strong>{maybePct(stockPublic?.worst_pct)}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Titoli stock rilevati</span>
          <strong>{stockPublic?.point_count ?? 0}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Titoli positivi</span>
          <strong>{stockPublic?.positives ?? 0}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Titoli negativi</span>
          <strong>{stockPublic?.negatives ?? 0}</strong>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Curva percentuale aggregata</h2>
        <div style={{ width: '100%', height: 360 }}>
          <ResponsiveContainer>
            <LineChart data={combinedChart}>
              <XAxis dataKey="x" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="crypto_profit_pct" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="stock_profit_pct" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="combined_profit_pct" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h2 className="section-title">Parametri avanzati</h2>
          <pre className="log">
            {JSON.stringify(
              {
                consistency_pct: Number(consistencyPct.toFixed(2)),
                positive_combined_points: positiveCombinedPoints,
                total_combined_points: combinedChart.length,
                sector_leader: sectorLeader,
                current_spread_pct: Number(spreadPct.toFixed(2)),
                stock_points: stockPublic?.point_count ?? 0,
                stock_metric_key: stockPublic?.metric_key ?? null,
                live_unlocked: liveUnlocked,
                live_available: liveAvailable,
                current_mode: currentMode,
                session_scope: sessionScope,
                process_running: processRunning,
                process_exit_code: processExitCode,
              },
              null,
              2
            )}
          </pre>
        </div>

        <div className="card">
          <h2 className="section-title">Policy di esposizione pubblica</h2>
          <div className="stack muted">
            <span>Nessun nome token pubblico.</span>
            <span>Nessun nome stock pubblico.</span>
            <span>Nessuna tabella asset-level pubblica.</span>
            <span>Solo percentuali reali per titolo e sintesi aggregata.</span>
          </div>
        </div>
      </div>
    </div>
  )
}
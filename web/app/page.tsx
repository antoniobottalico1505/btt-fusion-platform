'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'

function signedMoney(v: number) {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}$${Math.abs(v).toFixed(2)}`
}

function signedPct(v: number) {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
}

export default function HomePage() {
  const [site, setSite] = useState<any>(null)
  const [crypto, setCrypto] = useState<any>(null)
  const [stock, setStock] = useState<any>(null)

  useEffect(() => {
    apiFetch('/api/public/site').then(setSite).catch(() => null)
    apiFetch('/api/public/microcap').then(setCrypto).catch(() => null)
    apiFetch('/api/public/btt/latest').then(setStock).catch(() => null)
  }, [])

  const cryptoOverview = crypto?.dashboard?.overview || {}
  const cryptoSummary = crypto?.dashboard?.summary || crypto?.summary || {}
  const stockLatest = stock?.latest || null

  return (
    <div className="shell">
      <section className="hero">
        <div className="panel hero-copy">
          <span className="eyebrow">Private analytics • hosted-only • no source access</span>
          <h1 className="h1">{site?.copy?.hero_title || 'BTTcapital'}</h1>
          <p className="lead">
            {site?.copy?.hero_subtitle ||
              'BTTcapital è una piattaforma privata di market intelligence che unisce BTTcrypto e BTTstock in un ambiente premium orientato a risultati aggregati, controllo del rischio e presentazione istituzionale.'}
          </p>

          <div className="actions">
            <Link href="/dashboard"><button>Apri dashboard</button></Link>
            <Link href="/pricing"><button className="secondary">Accesso completo</button></Link>
            <Link href="/partners"><button className="ghost">For Partners</button></Link>
          </div>
        </div>

        <div className="hero-stats">
          <div className="metric">
            <div className="metric-label">BTTcrypto mode</div>
            <div className="metric-value">{crypto?.public_mode || 'paper'}</div>
          </div>

          <div className="metric">
            <div className="metric-label">Cash stimata</div>
            <div className="metric-value">${Number(cryptoOverview?.cash || 0).toFixed(2)}</div>
          </div>

          <div className="metric">
            <div className="metric-label">Profitto totale BTTcrypto</div>
            <div className="metric-value">{signedMoney(Number(cryptoSummary?.profit_money || 0))}</div>
          </div>

          <div className="metric">
            <div className="metric-label">Rendimento totale BTTcrypto</div>
            <div className="metric-value">{signedPct(Number(cryptoSummary?.profit_pct || 0))}</div>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="card">
          <h2 className="section-title">Track record pubblico</h2>
          <p className="section-sub">
            I risultati pubblici mostrati nel sito partono dal <strong>29 aprile 2016 alle 9.30 p.m.</strong>
          </p>
        </div>
      </section>

      <section className="section grid-3">
        <div className="card">
          <h3 className="section-title">BTTcrypto</h3>
          <p className="section-sub">
            Motore crypto osservabile esclusivamente tramite risultati aggregati e reporting sintetico.
          </p>
          <div className="stack muted">
            <span>Profitto/perdita totale e ultima operazione</span>
            <span>Rendimento totale e ultimo rendimento</span>
            <span>Win rate, drawdown, efficienza operativa</span>
          </div>
        </div>

        <div className="card">
          <h3 className="section-title">BTTstock</h3>
          <p className="section-sub">
            Motore stock server-side con performance aggregate e presentazione premium senza esposizione asset-level.
          </p>
          <div className="stack muted">
            <span>Performance media aggregata</span>
            <span>Distribuzione risultati, best/worst range, hit rate</span>
            <span>Visualizzazione istituzionale senza ticker pubblici</span>
          </div>
        </div>

        <div className="card">
          <h3 className="section-title">Accesso unico</h3>
          <p className="section-sub">
            Un solo abbonamento per tutto BTTcapital, mensile o annuale.
          </p>
          <div className="stack muted">
            <span>Mensile: €99</span>
            <span>Annuale: €990</span>
            <span>Accesso completo a BTTcrypto e BTTstock</span>
          </div>
        </div>
      </section>

      <section className="section grid-2">
        <div className="card">
          <h2 className="section-title">Struttura privata</h2>
          <div className="stack muted">
            <span>Hosted-only: il motore resta privato e non distribuito.</span>
            <span>Nessun accesso pubblico a codice, logica proprietaria o infrastruttura.</span>
            <span>Risultati e metriche aggregate come interfaccia esterna.</span>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Posizionamento premium</h2>
          <div className="stack muted">
            <span>Design e copy orientati a una percezione fund-tech / institutional style.</span>
            <span>Output pubblico focalizzato su risultati, percentuali, qualità operativa e rischio.</span>
            <span>Nessuna esposizione diretta di token, stock, ticker o numeri asset-specifici.</span>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="card">
          <h2 className="section-title">Ultimo stato BTTstock</h2>
          <p className="section-sub">
            {stockLatest ? `Ultimo report disponibile: ${stockLatest.status}` : 'Nessun report disponibile'}
          </p>
        </div>
      </section>
    </div>
  )
}
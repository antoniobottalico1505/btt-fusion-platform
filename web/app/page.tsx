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

      <section className="section grid-3">
        <div className="card">
          <h3 className="section-title">BTTcrypto</h3>
          <p className="section-sub">
            Motore crypto osservabile tramite risultati aggregati, metriche reali e storico operativo.
          </p>
          <div className="stack muted">
            <span>Performance aggregate</span>
            <span>Cash, rendimento, win/loss, andamento</span>
            <span>Nessun token pubblico in chiaro in homepage</span>
          </div>
        </div>

        <div className="card">
          <h3 className="section-title">BTTstock</h3>
          <p className="section-sub">
            Motore stock server-side con ranking, reportistica e resa “institutional style”.
          </p>
          <div className="stack muted">
            <span>Top opportunities aggregate</span>
            <span>Grafico performance e portafoglio suggerito</span>
            <span>Output pubblico senza esposizione completa del motore</span>
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
          <h2 className="section-title">Perché il prodotto è diverso</h2>
          <div className="stack muted">
            <span>Hosted-only: il motore resta privato e non distribuito.</span>
            <span>Output pubblici aggregati, non esposizione del codice o della logica proprietaria.</span>
            <span>Impostazione “fund-tech” / decision-intelligence, non dashboard amatoriale.</span>
            <span>Metriche pubbliche focalizzate su equity, rendimento, drawdown e qualità dei risultati.</span>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Struttura partner-ready</h2>
          <div className="stack muted">
            <span>BTTcapital può operare come piattaforma privata per partner autorizzati.</span>
            <span>Tu mantieni algoritmo, infrastruttura, API e proprietà intellettuale.</span>
            <span>Il partner può usare dashboard, risultati aggregati e interfaccia senza toccare il motore.</span>
            <span>Approccio adatto a licensing, recurring B2B e strutture white-label.</span>
          </div>
        </div>
      </section>
    </div>
  )
}
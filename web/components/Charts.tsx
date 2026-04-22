'use client'

import { Area, AreaChart, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

export function EquityChart({ data }: { data: Array<{ ts: number; equity: number }> }) {
  const safe = data?.map((d) => ({ ...d, label: new Date(d.ts * 1000).toLocaleTimeString() })) || []
  return (
    <div style={{ width: '100%', height: 280 }}>
      <ResponsiveContainer>
        <AreaChart data={safe}>
          <XAxis dataKey="label" hide />
          <YAxis hide domain={['auto', 'auto']} />
          <Tooltip />
          <Area type="monotone" dataKey="equity" stroke="#53d1ff" fill="#53d1ff33" strokeWidth={2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

export function ScoreChart({ data }: { data: Array<{ token: string; score?: number }> }) {
  const safe = (data || []).slice(0, 10).map((d) => ({ name: (d.token || '').slice(0, 8), score: Number(d.score || 0) }))
  return (
    <div style={{ width: '100%', height: 280 }}>
      <ResponsiveContainer>
        <LineChart data={safe}>
          <XAxis dataKey="name" />
          <YAxis domain={[0, 'auto']} />
          <Tooltip />
          <Line type="monotone" dataKey="score" stroke="#7c5cff" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

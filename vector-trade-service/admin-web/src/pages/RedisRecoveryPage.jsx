import { useEffect, useState } from 'react'
import api from '../api.js'

const c = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center', marginBottom: 16 },
  btn: (color = '#2563eb') => ({
    padding: '8px 14px', border: 0, borderRadius: 6, background: color,
    color: '#fff', cursor: 'pointer', fontSize: 12, fontWeight: 700,
  }),
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, padding: 18, marginBottom: 16 },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 12, marginBottom: 16 },
  metric: { background: '#10131c', border: '1px solid #252b44', borderRadius: 8, padding: 14 },
  metricLabel: { color: '#7c8db5', fontSize: 11, textTransform: 'uppercase', letterSpacing: .7 },
  metricValue: { color: '#f8fafc', fontSize: 20, fontWeight: 800, marginTop: 6 },
  badge: (ok) => ({
    display: 'inline-block', padding: '3px 9px', borderRadius: 12, fontSize: 11, fontWeight: 800,
    background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171',
  }),
  msg: (ok) => ({
    fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14,
    background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171',
  }),
  th: { padding: '9px 12px', fontSize: 11, color: '#7c8db5', borderBottom: '1px solid #232840', textAlign: 'left' },
  td: { padding: '8px 12px', fontSize: 12, borderBottom: '1px solid #1a1d27' },
  mono: { fontFamily: 'monospace', color: '#cbd5e1' },
  warn: { color: '#fbbf24', fontSize: 12, lineHeight: 1.5 },
}

function fmtDate(value) {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString('es-CL', { dateStyle: 'short', timeStyle: 'medium' })
}

export default function RedisRecoveryPage() {
  const [status, setStatus] = useState(null)
  const [msg, setMsg] = useState(null)
  const [loading, setLoading] = useState(false)

  const load = async () => {
    const res = await api.get('/redis')
    setStatus(res.data)
  }

  useEffect(() => {
    load().catch(error => setMsg({ ok: false, text: error.response?.data?.error || error.message }))
  }, [])

  const reconnect = async () => {
    setLoading(true)
    setMsg(null)
    try {
      const res = await api.post('/redis/reconnect')
      setStatus(res.data)
      setMsg({ ok: !!res.data.ok, text: res.data.message || 'Redis reconectado.' })
    } catch (error) {
      setMsg({ ok: false, text: error.response?.data?.error || error.message })
    } finally {
      setLoading(false)
    }
  }

  const maps = status?.maps ? Object.entries(status.maps) : []

  return (
    <div>
      <div style={c.h1}>Redis Recovery</div>
      <div style={c.sub}>Estado y reconexión controlada de Redis sin reiniciar el core.</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      <div style={c.row}>
        <button style={c.btn('#374151')} onClick={() => load().catch(console.error)}>↺ Refrescar estado</button>
        <button style={c.btn(status?.connected ? '#0f766e' : '#dc2626')} onClick={reconnect} disabled={loading}>
          {loading ? 'Reconectando...' : 'Reconectar Redis'}
        </button>
      </div>

      <div style={c.grid}>
        <div style={c.metric}>
          <div style={c.metricLabel}>Estado</div>
          <div style={c.metricValue}><span style={c.badge(status?.connected)}>{status?.connected ? 'OK' : 'FALLA'}</span></div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Host</div>
          <div style={{ ...c.metricValue, fontSize: 16 }}>{status?.host || '-'}:{status?.port || '-'}</div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Portafolios</div>
          <div style={c.metricValue}>{status?.portfolioUsers ?? '-'}</div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Multibooks</div>
          <div style={c.metricValue}>{status?.multibookUsers ?? '-'}</div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Ultimo OK</div>
          <div style={{ ...c.metricValue, fontSize: 13 }}>{fmtDate(status?.lastSuccessAt)}</div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Retry Redis</div>
          <div style={c.metricValue}>{status?.retryActive ? `Activo (${status?.retryAttempts || 0})` : 'Inactivo'}</div>
        </div>
      </div>

      {status?.lastError && (
        <div style={c.card}>
          <div style={c.metricLabel}>Último error</div>
          <div style={{ ...c.mono, marginTop: 8, color: '#fca5a5' }}>{status.lastError}</div>
        </div>
      )}

      <div style={c.card}>
        <div style={{ ...c.warn, marginBottom: 12 }}>
          Reconectar Redis vuelve a enlazar mapas y configuración persistente. No fuerza restauración de posiciones intradía sobre actores vivos.
        </div>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr><th style={c.th}>Mapa</th><th style={c.th}>Inicializado</th></tr>
          </thead>
          <tbody>
            {maps.map(([name, ok]) => (
              <tr key={name}>
                <td style={c.td}><span style={c.mono}>{name}</span></td>
                <td style={c.td}><span style={c.badge(ok)}>{ok ? 'OK' : 'NULL'}</span></td>
              </tr>
            ))}
            {maps.length === 0 && (
              <tr><td style={{ ...c.td, textAlign: 'center', color: '#6b7280' }} colSpan={2}>Sin estado disponible</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

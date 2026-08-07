import { useState, useEffect } from 'react'
import api from '../api.js'

const EXCHANGES = ['XSGO', 'NUAM', 'IB_SMART', 'ALPACA']

const c = {
  h1:    { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  h2:    { fontSize: 15, fontWeight: 700, marginBottom: 10, marginTop: 24 },
  sub:   { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row:   { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap' },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, flex: 1, minWidth: 200 },
  numIn: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, width: 160, textAlign: 'right' },
  btn:   (color = '#ef4444') => ({ padding: '8px 16px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  card:  { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th:    { padding: '10px 14px', fontSize: 12, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td:    { padding: '9px 14px', fontSize: 13, borderBottom: '1px solid #1a1d27' },
  msg:   (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  empty: { padding: '9px 14px', fontSize: 13, color: '#6b7280', textAlign: 'center' },
}

const fmt = (n) => n >= 1e9 ? (n / 1e6).toLocaleString() + 'M'
  : n === Number.MAX_VALUE ? '∞'
  : n?.toLocaleString('es-CL') ?? '—'

export default function RiskPage() {
  const [blocked,  setBlocked]  = useState([])
  const [limits,   setLimits]   = useState({})
  const [editing,  setEditing]  = useState({})  // { XSGO: "25000000", ... }
  const [symbol,   setSymbol]   = useState('')
  const [msg,      setMsg]      = useState(null)
  const [loading,  setLoading]  = useState(false)

  const loadBlocked = () => api.get('/risk/blocked-symbols').then(r => setBlocked(r.data)).catch(console.error)
  const loadLimits  = () => api.get('/risk/notional-limits').then(r => {
    const map = {}
    r.data.forEach(e => { map[e.exchange] = e.limit })
    setLimits(map)
    setEditing(Object.fromEntries(Object.entries(map).map(([k, v]) => [k, String(v)])))
  }).catch(console.error)

  useEffect(() => { loadBlocked(); loadLimits() }, [])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 4000) }

  const block = async () => {
    const sym = symbol.trim().toUpperCase()
    if (!sym) return
    setLoading(true)
    try {
      await api.post('/risk/blocked-symbols/block', { symbol: sym })
      flash(true, `🔒 Símbolo bloqueado: ${sym}`)
      setSymbol('')
      setTimeout(loadBlocked, 300)
    } catch (e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const unblock = async (sym) => {
    try {
      await api.post('/risk/blocked-symbols/unblock', { symbol: sym })
      flash(true, `🔓 Símbolo desbloqueado: ${sym}`)
      setTimeout(loadBlocked, 300)
    } catch (e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
  }

  const saveLimit = async (exchange) => {
    const val = parseFloat(editing[exchange])
    if (isNaN(val) || val <= 0) { flash(false, '❌ Valor inválido'); return }
    setLoading(true)
    try {
      await api.post('/risk/notional-limits/set', { exchange, limit: val })
      flash(true, `✅ Límite actualizado: ${exchange} → ${fmt(val)}`)
      setTimeout(loadLimits, 300)
    } catch (e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  return (
    <div>
      <div style={c.h1}>🛡️ Control de Riesgo</div>
      <div style={c.sub}>
        Controles aplicados en tiempo real a todas las órdenes nuevas y replace. Persisten en Redis tras reinicios.
      </div>

      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      {/* ── Límites de monto nominal ── */}
      <div style={c.h2}>💰 Límites de monto nominal (precio × cantidad)</div>
      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={c.th}>Destino</th>
              <th style={c.th}>Límite actual</th>
              <th style={c.th}>Nuevo valor</th>
              <th style={c.th}>Acción</th>
            </tr>
          </thead>
          <tbody>
            {EXCHANGES.map(ex => (
              <tr key={ex}>
                <td style={{ ...c.td, fontWeight: 700 }}>{ex}</td>
                <td style={{ ...c.td, color: '#fbbf24' }}>{fmt(limits[ex])}</td>
                <td style={c.td}>
                  <input
                    style={c.numIn}
                    type="number"
                    min="1"
                    value={editing[ex] ?? ''}
                    onChange={e => setEditing(prev => ({ ...prev, [ex]: e.target.value }))}
                    onKeyDown={e => e.key === 'Enter' && saveLimit(ex)}
                  />
                </td>
                <td style={c.td}>
                  <button
                    style={{ ...c.btn('#1d4ed8'), fontSize: 11, padding: '4px 10px' }}
                    onClick={() => saveLimit(ex)}
                    disabled={loading}
                  >
                    💾 Guardar
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ── Símbolos bloqueados ── */}
      <div style={c.h2}>🔒 Símbolos bloqueados</div>
      <div style={c.sub}>
        Las órdenes sobre estos símbolos son rechazadas con "Orden rechazada por riesgo".
      </div>

      <div style={c.row}>
        <input
          style={c.input}
          placeholder="Símbolo a bloquear (ej: SQM-B)"
          value={symbol}
          onChange={e => setSymbol(e.target.value.toUpperCase())}
          onKeyDown={e => e.key === 'Enter' && block()}
        />
        <button style={c.btn()} onClick={block} disabled={loading}>
          {loading ? '...' : '🔒 Bloquear'}
        </button>
        <button style={c.btn('#374151')} onClick={() => { loadBlocked(); loadLimits() }}>↺ Actualizar</button>
      </div>

      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={c.th}>Símbolo bloqueado</th>
              <th style={c.th}>Acción</th>
            </tr>
          </thead>
          <tbody>
            {blocked.length === 0
              ? <tr><td colSpan={2} style={c.empty}>✅ No hay símbolos bloqueados</td></tr>
              : blocked.map(sym => (
                <tr key={sym}>
                  <td style={{ ...c.td, fontWeight: 700, color: '#f87171' }}>🔒 {sym}</td>
                  <td style={c.td}>
                    <button
                      style={{ ...c.btn('#16a34a'), fontSize: 11, padding: '4px 10px' }}
                      onClick={() => unblock(sym)}
                    >
                      🔓 Desbloquear
                    </button>
                  </td>
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>
    </div>
  )
}

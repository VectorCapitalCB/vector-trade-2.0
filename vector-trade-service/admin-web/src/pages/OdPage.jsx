import { useEffect, useMemo, useState } from 'react'
import api from '../api.js'

const c = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#7c8db5', marginBottom: 18 },
  row: { display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center', marginBottom: 14 },
  btn: (color = '#1d4ed8') => ({ padding: '8px 14px', background: color, border: 'none', borderRadius: 7, color: '#fff', fontSize: 13, fontWeight: 700, cursor: 'pointer' }),
  input: { padding: '8px 12px', background: '#0f1117', border: '1px solid #334155', borderRadius: 7, color: '#e5e7eb', fontSize: 13, minWidth: 260, flex: 1 },
  cards: { display: 'grid', gridTemplateColumns: 'repeat(4, minmax(160px, 1fr))', gap: 12, marginBottom: 16 },
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, padding: '14px 16px' },
  num: { fontSize: 26, fontWeight: 800, color: '#f8fafc' },
  label: { fontSize: 11, color: '#7c8db5', letterSpacing: 1, textTransform: 'uppercase', marginTop: 4 },
  tableWrap: { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th: { padding: '10px 12px', fontSize: 12, color: '#8b95a7', borderBottom: '1px solid #232840', textAlign: 'left', whiteSpace: 'nowrap' },
  td: { padding: '10px 12px', fontSize: 13, borderBottom: '1px solid #1a1d27', verticalAlign: 'top' },
  pill: (color) => ({ display: 'inline-block', background: color, color: '#fff', borderRadius: 999, padding: '2px 8px', fontSize: 11, fontWeight: 800 }),
  empty: { padding: 18, textAlign: 'center', color: '#7c8db5', fontSize: 13 },
  msg: (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
}

const fmtTime = (ts) => ts ? new Date(ts).toLocaleString('es-CL') : '—'
const fmtNum = (n) => Number.isFinite(Number(n)) ? Number(n).toLocaleString('es-CL') : '—'

export default function OdPage() {
  const [attempts, setAttempts] = useState([])
  const [query, setQuery] = useState('')
  const [msg, setMsg] = useState(null)
  const [loading, setLoading] = useState(false)
  const [enabled, setEnabled] = useState(true)

  const load = async () => {
    setLoading(true)
    try {
      const { data } = await api.get('/od')
      setAttempts(data.attempts || [])
      setEnabled(data.enabled !== false)
    } catch (e) {
      setMsg({ ok: false, text: e.response?.data?.error || e.message })
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return attempts
    return attempts.filter(a => [
      a.username, a.account, a.symbol, a.attemptedSide, a.conflictingSide, a.reason,
    ].some(v => String(v || '').toLowerCase().includes(q)))
  }, [attempts, query])

  const users = new Set(attempts.map(a => a.username).filter(Boolean)).size
  const accounts = new Set(attempts.map(a => a.account).filter(Boolean)).size

  const clear = async () => {
    try {
      await api.post('/od/clear', {})
      setAttempts([])
      setMsg({ ok: true, text: 'Intentos OD limpiados' })
      setTimeout(() => setMsg(null), 3000)
    } catch (e) {
      setMsg({ ok: false, text: e.response?.data?.error || e.message })
    }
  }

  const toggleProtection = async () => {
    const nextEnabled = !enabled
    try {
      const { data } = await api.post(nextEnabled ? '/od/enable' : '/od/disable', {})
      setEnabled(data.enabled !== false)
      setMsg({
        ok: true,
        text: data.enabled === false
          ? 'Reglas OD desactivadas en caliente. Las próximas órdenes/replaces no serán bloqueados por OD.'
          : 'Reglas OD activadas nuevamente.',
      })
      setTimeout(() => setMsg(null), 5000)
    } catch (e) {
      setMsg({ ok: false, text: e.response?.data?.error || e.message })
    }
  }

  return (
    <div>
      <div style={c.h1}>Control OD</div>
      <div style={c.sub}>
        Auditoría de órdenes rechazadas por intentar dejar compra y venta activas con la misma cuenta y el mismo papel.
      </div>

      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      <div style={c.cards}>
        <div style={c.card}>
          <div style={{ ...c.num, color: enabled ? '#4ade80' : '#f87171' }}>{enabled ? 'ON' : 'OFF'}</div>
          <div style={c.label}>Protección OD</div>
        </div>
        <div style={c.card}><div style={c.num}>{attempts.length}</div><div style={c.label}>Intentos registrados</div></div>
        <div style={c.card}><div style={c.num}>{users}</div><div style={c.label}>Usuarios</div></div>
        <div style={c.card}><div style={c.num}>{accounts}</div><div style={c.label}>Cuentas</div></div>
      </div>

      <div style={c.row}>
        <input
          style={c.input}
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Buscar usuario, cuenta o papel..."
        />
        <button style={c.btn('#1d4ed8')} onClick={load} disabled={loading}>
          {loading ? 'Cargando...' : 'Actualizar'}
        </button>
        <button style={c.btn(enabled ? '#b91c1c' : '#16a34a')} onClick={toggleProtection}>
          {enabled ? 'Desactivar reglas OD' : 'Activar reglas OD'}
        </button>
        <button style={c.btn('#991b1b')} onClick={clear}>Limpiar</button>
      </div>

      <div style={c.tableWrap}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={c.th}>Hora</th>
              <th style={c.th}>Usuario</th>
              <th style={c.th}>Cuenta</th>
              <th style={c.th}>Papel</th>
              <th style={c.th}>Intentó enviar</th>
              <th style={c.th}>Punta activa encontrada</th>
              <th style={c.th}>Motivo</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr><td colSpan={7} style={c.empty}>Sin intentos OD registrados</td></tr>
            ) : filtered.map((a, idx) => (
              <tr key={`${a.timestamp}-${a.attemptedOrderId}-${idx}`}>
                <td style={c.td}>{fmtTime(a.timestamp)}</td>
                <td style={{ ...c.td, fontWeight: 700 }}>{a.username || '—'}</td>
                <td style={{ ...c.td, fontWeight: 700 }}>{a.account}</td>
                <td style={{ ...c.td, color: '#fbbf24', fontWeight: 800 }}>{a.symbol}</td>
                <td style={c.td}>
                  <span style={c.pill(a.attemptedSide === 'BUY' ? '#16a34a' : '#dc2626')}>{a.attemptedSide}</span>
                  <div>{fmtNum(a.attemptedQuantity)} @ {fmtNum(a.attemptedPrice)}</div>
                  <div style={{ color: '#7c8db5', fontSize: 11 }}>{a.attemptedExchange} · {a.attemptedOrderId}</div>
                </td>
                <td style={c.td}>
                  <span style={c.pill(a.conflictingSide === 'BUY' ? '#16a34a' : '#dc2626')}>{a.conflictingSide}</span>
                  <div>{fmtNum(a.conflictingQuantity)} @ {fmtNum(a.conflictingPrice)}</div>
                  <div style={{ color: '#7c8db5', fontSize: 11 }}>{a.conflictingStatus} · {a.conflictingOrderId}</div>
                </td>
                <td style={c.td}>{a.reason}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

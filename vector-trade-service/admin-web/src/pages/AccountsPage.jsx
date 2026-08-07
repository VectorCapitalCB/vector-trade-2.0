import { useState, useEffect } from 'react'
import api from '../api.js'

const c = {
  h1:  { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap' },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, flex: 1, minWidth: 200 },
  btn: (color = '#3b82f6') => ({ padding: '8px 16px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th:   { padding: '10px 14px', fontSize: 12, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td:   { padding: '9px 14px', fontSize: 13, borderBottom: '1px solid #1a1d27' },
  msg:  (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  badge: (ok) => ({ display: 'inline-block', padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600, background: ok ? '#052e16' : '#1c1917', color: ok ? '#4ade80' : '#9ca3af' }),
  box: { background: '#0f1117', border: '1px solid #3b82f6', borderRadius: 8, padding: '14px 16px', marginBottom: 20 },
}

export default function AccountsPage() {
  const [accounts, setAccounts] = useState([])
  const [filter,   setFilter]   = useState('')
  const [msg,      setMsg]      = useState(null)
  const [loading,  setLoading]  = useState({})
  const [rut,      setRut]      = useState('')
  const [rutLoading, setRutLoading] = useState(false)
  const [onlyEmpty, setOnlyEmpty] = useState(false)

  const reload = () => api.get('/accounts').then(r => setAccounts(r.data)).catch(console.error)
  useEffect(() => { reload() }, [])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 6000) }

  const recalculate = async (acct) => {
    setLoading(prev => ({ ...prev, [acct]: true }))
    try {
      const r = await api.post(`/accounts/${encodeURIComponent(acct)}/recalculate`)
      flash(true, `✅ ${r.data.message || 'Recálculo enviado: ' + acct}`)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(prev => ({ ...prev, [acct]: false })) }
  }

  const recalcByRut = async () => {
    const val = rut.trim()
    if (!val) return
    setRutLoading(true)
    try {
      const r = await api.post(`/accounts/${encodeURIComponent(val)}/recalculate`)
      flash(true, `✅ ${r.data.message}`)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setRutLoading(false) }
  }

  const matchesText = (a) => !filter.trim() || a.account.includes(filter) || a.user?.toLowerCase().includes(filter.toLowerCase())
  const isEmptyCustody = (a) => a.custodyKnown && a.custodyEmpty
  const filtered = () => accounts.filter(a => matchesText(a) && (!onlyEmpty || isEmptyCustody(a)))
  const emptyCount = accounts.filter(isEmptyCustody).length

  const custodyCell = (a) => {
    if (!a.custodyKnown) return <span style={c.badge(false)}>— sin info</span>
    if (a.custodyEmpty)  return <span style={{ ...c.badge(false), background: '#450a0a', color: '#f87171' }}>⚠️ Sin custodia (null/vacío)</span>
    const saldo = a.saldoDisponible != null ? Number(a.saldoDisponible).toLocaleString('es-CL', { maximumFractionDigits: 0 }) : '?'
    return <span style={c.badge(true)}>✅ {a.positionsCount} pos · saldo ${saldo}</span>
  }

  return (
    <div>
      <div style={c.h1}>⚡ Cuadratura Prioritaria</div>
      <div style={c.sub}>Valida qué cuentas tienen su custodia (posiciones/saldo desde SQL) y cuáles están vacías/null (no pueden rutear). Usá "⚡ Re-buscar SQL" para volver a cargar los datos de esa cuenta.</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      {/* ── RUT directo ── */}
      <div style={c.box}>
        <div style={{ fontSize: 12, color: '#93c5fd', fontWeight: 700, marginBottom: 8 }}>⚡ Recálculo directo por RUT / cuenta</div>
        <div style={{ display: 'flex', gap: 8 }}>
          <input
            style={{ ...c.input, flex: 1, minWidth: 0 }}
            placeholder="Ej: 12345678 o 12345678/0"
            value={rut}
            onChange={e => setRut(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && recalcByRut()}
          />
          <button style={c.btn('#7c3aed')} disabled={rutLoading || !rut.trim()} onClick={recalcByRut}>
            {rutLoading ? '...' : '⚡ Calcular'}
          </button>
        </div>
        <div style={{ fontSize: 11, color: '#4b5563', marginTop: 6 }}>
          Busca todas las sub-cuentas que coincidan con el RUT ingresado y lanza recálculo para cada una.
        </div>
      </div>

      <div style={c.row}>
        <input style={c.input} placeholder="Filtrar cuenta o usuario..." value={filter} onChange={e => setFilter(e.target.value)} />
        <button style={c.btn('#374151')} onClick={reload}>↺ Actualizar</button>
        <button style={c.btn(onlyEmpty ? '#dc2626' : '#374151')} onClick={() => setOnlyEmpty(v => !v)}>
          {onlyEmpty ? '✓ ' : ''}⚠️ Solo sin custodia ({emptyCount})
        </button>
        <button style={c.btn('#b45309')} onClick={() => filtered().forEach(a => recalculate(a.account))}>⚡ Re-buscar SQL (visibles)</button>
      </div>
      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead><tr>
            <th style={c.th}>Cuenta</th><th style={c.th}>Custodia (SQL)</th><th style={c.th}>Estado actor</th><th style={c.th}>Acción</th>
          </tr></thead>
          <tbody>
            {filtered().map(a => (
              <tr key={a.account} style={isEmptyCustody(a) ? { background: '#1a0a0a' } : {}}>
                <td style={c.td}><b>{a.account}</b></td>
                <td style={c.td}>{custodyCell(a)}</td>
                <td style={c.td}><span style={c.badge(a.hasOrders)}>{a.hasOrders ? '✓ Con órdenes' : '⊘ Sin órdenes'}</span></td>
                <td style={c.td}>
                  <button style={{...c.btn(isEmptyCustody(a) ? '#dc2626' : '#7c3aed'), fontSize:12, padding:'5px 12px'}} disabled={loading[a.account]} onClick={() => recalculate(a.account)}>
                    {loading[a.account] ? '...' : '⚡ Re-buscar SQL'}
                  </button>
                </td>
              </tr>
            ))}
            {filtered().length === 0 && <tr><td colSpan={4} style={{...c.td, textAlign:'center', color:'#6b7280'}}>Sin cuentas</td></tr>}
          </tbody>
        </table>
      </div>
      <div style={{ fontSize: 11, color: '#4b5563', marginTop: 8 }}>Total: {accounts.length} — visibles: {filtered().length}</div>
    </div>
  )
}

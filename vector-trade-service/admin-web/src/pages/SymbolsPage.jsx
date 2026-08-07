import { useState, useEffect } from 'react'
import api from '../api.js'

const c = {
  h1:     { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub:    { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row:    { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap' },
  input:  { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, flex: 1, minWidth: 180 },
  select: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13 },
  btn: (color = '#3b82f6') => ({ padding: '8px 16px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  card:   { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th:     { padding: '10px 14px', fontSize: 12, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td:     { padding: '9px 14px', fontSize: 13, borderBottom: '1px solid #1a1d27' },
  badge:  (ok) => ({ display: 'inline-block', padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600, background: ok ? '#052e16' : '#3b0764', color: ok ? '#4ade80' : '#c084fc' }),
  msg:    (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
}

const EXCHANGES = ['BCS','FH_IBKR','ALPACA_MKD','DATATEC_XBCL','NUAM_MKD']
const SETTL_TYPES = [
  { value: 'T2', label: 'T2' },
  { value: 'CASH', label: 'CASH' },
  { value: 'NEXT_DAY', label: 'T1 / NEXT_DAY' },
  { value: 'REGULAR', label: 'REGULAR' },
  { value: 'T3', label: 'T3' },
  { value: 'T5', label: 'T5' },
]

export default function SymbolsPage() {
  const [subs,     setSubs]     = useState([])
  const [symbols,  setSymbols]  = useState([])
  const [symbol,   setSymbol]   = useState('')
  const [exchange, setExchange] = useState(EXCHANGES[0])
  const [settlType, setSettlType] = useState('T2')
  const [msg,      setMsg]      = useState(null)
  const [loading,  setLoading]  = useState(false)
  const [tab,      setTab]      = useState('subs')

  const loadSubs    = () => api.get('/symbols/subscriptions').then(r => setSubs(r.data)).catch(console.error)
  const loadSymbols = () => api.get('/symbols').then(r => setSymbols(r.data)).catch(console.error)
  useEffect(() => { loadSubs(); loadSymbols() }, [])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 4000) }

  const subscribe = async () => {
    if (!symbol.trim()) return
    setLoading(true)
    try {
      await api.post('/symbols/subscribe', { symbol: symbol.trim(), exchange, settlType })
      flash(true, `✅ Re-suscripción enviada: ${symbol.trim()} @ ${exchange} ${settlType}`)
      setTimeout(loadSubs, 600)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const unsubscribe = async (row) => {
    try {
      await api.post('/symbols/unsubscribe', row)
      flash(true, `✅ Desuscrito: ${row.symbol} @ ${row.exchange} ${row.settlType || ''}`)
      setTimeout(loadSubs, 400)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
  }

  const resubscribe = async (row) => {
    setLoading(true)
    try {
      await api.post('/symbols/subscribe', row)
      flash(true, `✅ Re-suscripción forzada: ${row.symbol} @ ${row.exchange} ${row.settlType || 'T2'}`)
      setTimeout(loadSubs, 600)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const tabStyle = (active) => ({ padding: '7px 16px', cursor: 'pointer', border: 'none', background: active ? '#1e2540' : 'transparent', color: active ? '#60a5fa' : '#6b7280', fontSize: 13, borderRadius: '6px 6px 0 0' })

  return (
    <div>
      <div style={c.h1}>📡 Suscripciones Market Data</div>
      <div style={c.sub}>Fuerza re-suscripción de papeles en caliente cuando la MKD deja de responder.</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}
      <div style={c.row}>
        <input style={c.input} placeholder="Símbolo (ej: SQM-B)" value={symbol}
          onChange={e => setSymbol(e.target.value.toUpperCase())}
          onKeyDown={e => e.key === 'Enter' && subscribe()} />
        <select style={c.select} value={exchange} onChange={e => setExchange(e.target.value)}>
          {EXCHANGES.map(ex => <option key={ex}>{ex}</option>)}
        </select>
        <select style={c.select} value={settlType} onChange={e => setSettlType(e.target.value)}>
          {SETTL_TYPES.map(st => <option key={st.value} value={st.value}>{st.label}</option>)}
        </select>
        <button style={c.btn()} onClick={subscribe} disabled={loading}>{loading ? '...' : '🔄 Re-suscribir'}</button>
        <button style={c.btn('#374151')} onClick={() => { loadSubs(); loadSymbols() }}>↺ Actualizar</button>
      </div>
      <div>
        <button style={tabStyle(tab === 'subs')} onClick={() => setTab('subs')}>Activas ({subs.length})</button>
        <button style={tabStyle(tab === 'all')} onClick={() => setTab('all')}>Todos los símbolos ({symbols.length})</button>
      </div>
      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead><tr>
            {tab === 'subs' ? <><th style={c.th}>ID</th><th style={c.th}>Símbolo</th><th style={c.th}>Exchange</th><th style={c.th}>Liquidación</th><th style={c.th}>Snapshot</th><th style={c.th}>Acciones</th></>
              : <><th style={c.th}>Símbolo</th><th style={c.th}>Exchange</th><th style={c.th}>Moneda</th><th style={c.th}>Tipo</th></>}
          </tr></thead>
          <tbody>
            {tab === 'subs' ? subs.map(s => (
              <tr key={s.id}>
                <td style={{...c.td, color:'#6b7280', fontSize:11}}>{s.id}</td>
                <td style={c.td}><b>{s.symbol}</b></td>
                <td style={c.td}>{s.exchange}</td>
                <td style={c.td}><b>{s.settlType || '—'}</b></td>
                <td style={c.td}><span style={c.badge(s.hasSnapshot)}>{s.hasSnapshot ? '✓ OK' : '✗ Sin datos'}</span></td>
                <td style={c.td}>
                  <button style={{...c.btn(), fontSize:11, padding:'4px 10px', marginRight:6}} onClick={() => resubscribe(s)}>Re-suscribir</button>
                  <button style={{...c.btn('#7f1d1d'), fontSize:11, padding:'4px 10px'}} onClick={() => unsubscribe(s)}>Eliminar</button>
                </td>
              </tr>
            )) : symbols.map((s,i) => (
              <tr key={i}>
                <td style={c.td}><b>{s.symbol}</b></td><td style={c.td}>{s.exchange}</td>
                <td style={c.td}>{s.currency}</td><td style={c.td}>{s.securityType}</td>
              </tr>
            ))}
            {((tab==='subs'&&subs.length===0)||(tab==='all'&&symbols.length===0)) && (
              <tr><td colSpan={5} style={{...c.td, textAlign:'center', color:'#6b7280'}}>Sin datos</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

import { useState, useEffect, useCallback } from 'react'
import api from '../api.js'

const c = {
  h1:  { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' },
  select: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13 },
  btn: (color = '#3b82f6') => ({ padding: '8px 14px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 12, fontWeight: 600, cursor: 'pointer' }),
  card:  { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th:    { padding: '10px 12px', fontSize: 11, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td:    { padding: '8px 12px', fontSize: 12, borderBottom: '1px solid #1a1d27' },
  cb:    { width: 14, height: 14, cursor: 'pointer', accentColor: '#3b82f6' },
  msg:   (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  badge: (s) => {
    const map = { NEW:{bg:'#1e3a5f',c:'#93c5fd'}, PARTIAL:{bg:'#365314',c:'#a3e635'}, FILLED:{bg:'#052e16',c:'#4ade80'}, CANCELLED:{bg:'#1c1917',c:'#9ca3af'}, REJECTED:{bg:'#450a0a',c:'#f87171'}, PENDING:{bg:'#2d1d6e',c:'#c084fc'} }
    const m = map[s] || {bg:'#1a1d27',c:'#e2e8f0'}
    return { display:'inline-block', padding:'2px 8px', borderRadius:10, fontSize:11, fontWeight:600, background:m.bg, color:m.c }
  },
  box: { background: '#0f1117', border: '1px solid #3b82f6', borderRadius: 8, padding: '14px 16px', marginBottom: 20 },
  textarea: { width: '100%', minHeight: 120, padding: '8px 10px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 11, fontFamily: 'monospace', resize: 'vertical', boxSizing: 'border-box' },
}

// ── Sección: pegar líneas de log ─────────────────────────────────────────────
function LogResendSection() {
  const [lines,   setLines]   = useState('')
  const [loading, setLoading] = useState(false)
  const [msg,     setMsg]     = useState(null)

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 7000) }

  const send = async () => {
    if (!lines.trim()) return
    setLoading(true)
    // Quitar prefijo [timestamp] de cada línea antes de enviar
    const cleaned = lines
      .split('\n')
      .map(l => l.replace(/^\[[\d\-,. :]+\]\s*/, '').trim())
      .filter(l => l.length > 0)
      .join('\n')
    try {
      const r = await api.post('/orders/resend-log', { lines: cleaned })
      flash(true, `✅ ${r.data.message}` + (r.data.errorList?.length ? `\nErrores: ${r.data.errorList.join(' | ')}` : ''))
    } catch(e) {
      const msg = e.response?.data?.error || e.response?.data || e.message
      flash(false, '❌ ' + (typeof msg === 'string' ? msg : JSON.stringify(msg)))
    }
    finally { setLoading(false) }
  }

  // preview: cuenta cada tipo de mensaje
  const countNew     = (lines.match(/NewOrderRequest\s*:/g) || []).length
  const countReplace = (lines.match(/OrderReplaceRequest\s*:/g) || []).length
  const countCancel  = (lines.match(/OrderCancelRequest\s*:/g) || []).length
  const countOrder   = (lines.match(/(?<![A-Za-z])Order\s*:/g) || []).length
  const total = countNew + countReplace + countCancel + countOrder

  const typeLabel = {bg:'#1e3a5f', color:'#93c5fd', padding:'2px 8px', borderRadius:4, fontSize:10, fontWeight:700, marginRight:4}

  return (
    <div style={c.box}>
      <div style={{ fontSize: 12, color: '#93c5fd', fontWeight: 700, marginBottom: 8 }}>
        📋 Reinyectar desde líneas de log
        <span style={{ fontWeight: 400, color: '#6b7280', marginLeft: 8 }}>
          — Buyside: NewOrderRequest / OrderReplaceRequest / OrderCancelRequest &nbsp;|&nbsp; Sellside: Order
        </span>
      </div>
      {msg && <div style={{...c.msg(msg.ok), whiteSpace:'pre-wrap', marginBottom:10}}>{msg.text}</div>}
      <textarea
        style={c.textarea}
        placeholder={'Pega una o varias líneas de log (buyside o sellside):\n[timestamp] NewOrderRequest : {"order":{...}}\n[timestamp] OrderReplaceRequest : {"id":"...","price":83000}\n[timestamp] OrderCancelRequest : {"id":"..."}\n[timestamp] Order : {"account":"47024924/0","id":"...",...}'}
        value={lines}
        onChange={e => setLines(e.target.value)}
      />
      <div style={{ display: 'flex', gap: 8, marginTop: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        <button style={c.btn('#7c3aed')} disabled={loading || total === 0} onClick={send}>
          {loading ? '...' : `📤 Reinyectar ${total > 0 ? total + ' mensaje' + (total > 1 ? 's' : '') : ''}`}
        </button>
        <button style={c.btn('#374151')} onClick={() => { setLines(''); setMsg(null) }}>✕ Limpiar</button>
        {total > 0 && <span style={{ fontSize: 11, color: '#9ca3af' }}>
          {countNew > 0 && <span style={{...typeLabel, background:'#1e3a5f', color:'#93c5fd'}}>New×{countNew}</span>}
          {countReplace > 0 && <span style={{...typeLabel, background:'#365314', color:'#a3e635'}}>Replace×{countReplace}</span>}
          {countCancel > 0 && <span style={{...typeLabel, background:'#450a0a', color:'#f87171'}}>Cancel×{countCancel}</span>}
          {countOrder > 0 && <span style={{...typeLabel, background:'#2d1d6e', color:'#c084fc'}}>Order(sell)×{countOrder}</span>}
        </span>}
        {total === 0 && <span style={{ fontSize: 11, color: '#4b5563' }}>Pega líneas de log</span>}
      </div>
    </div>
  )
}

// ── Página principal ─────────────────────────────────────────────────────────
export default function OrdersPage() {
  const [accounts, setAccounts] = useState([])
  const [account,  setAccount]  = useState('')
  const [orders,   setOrders]   = useState([])
  const [selected, setSelected] = useState(new Set())
  const [msg,      setMsg]      = useState(null)
  const [loading,  setLoading]  = useState(false)

  useEffect(() => { api.get('/accounts').then(r => setAccounts(r.data)).catch(console.error) }, [])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 5000) }

  const loadOrders = useCallback(async () => {
    if (!account) return
    setLoading(true)
    try { const r = await api.get('/orders', { params: { account } }); setOrders(r.data); setSelected(new Set()) }
    catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }, [account])

  useEffect(() => { loadOrders() }, [loadOrders])

  const toggleSel = (id) => setSelected(prev => { const n = new Set(prev); n.has(id) ? n.delete(id) : n.add(id); return n })
  const toggleAll = () => selected.size === orders.length ? setSelected(new Set()) : setSelected(new Set(orders.map(o => o.id)))

  const resend = async (ids) => {
    if (!account) return
    setLoading(true)
    try { const r = await api.post('/orders/resend', { account, orderIds: ids || [...selected] }); flash(true, `✅ ${r.data.message}`) }
    catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  return (
    <div>
      <div style={c.h1}>📋 Reenvío de Órdenes</div>
      <div style={c.sub}>Reenvía órdenes al frontend vía MessageEventBus — desde Redis o pegando líneas de log.</div>

      {/* ── Sección log ── */}
      <LogResendSection />

      {/* ── Sección Redis ── */}
      <div style={{ fontSize: 12, color: '#9ca3af', fontWeight: 700, marginBottom: 10 }}>📦 Desde Redis</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}
      <div style={c.row}>
        <select style={c.select} value={account} onChange={e => setAccount(e.target.value)}>
          <option value="">-- Selecciona cuenta --</option>
          {accounts.map(a => <option key={a.account} value={a.account}>{a.account}</option>)}
        </select>
        <button style={c.btn('#374151')} onClick={loadOrders}>↺ Cargar</button>
        {orders.length > 0 && <>
          <button style={c.btn()} disabled={selected.size === 0 || loading} onClick={() => resend()}>📤 Reenviar sel. ({selected.size})</button>
          <button style={c.btn('#d97706')} disabled={loading} onClick={() => resend(orders.map(o => o.id))}>📤 Todas ({orders.length})</button>
        </>}
      </div>
      {orders.length > 0 && (
        <div style={c.card}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead><tr>
              <th style={c.th}><input type="checkbox" style={c.cb} checked={selected.size === orders.length && orders.length > 0} onChange={toggleAll} /></th>
              <th style={c.th}>ID</th><th style={c.th}>Símbolo</th><th style={c.th}>Lado</th>
              <th style={c.th}>Qty</th><th style={c.th}>Precio</th><th style={c.th}>Estado</th><th style={c.th}>Estrategia</th>
            </tr></thead>
            <tbody>
              {orders.map(o => (
                <tr key={o.id} style={{ opacity: loading ? 0.6 : 1 }}>
                  <td style={c.td}><input type="checkbox" style={c.cb} checked={selected.has(o.id)} onChange={() => toggleSel(o.id)} /></td>
                  <td style={{...c.td, fontSize:10, color:'#6b7280'}}>{o.id}</td>
                  <td style={c.td}><b>{o.symbol}</b></td>
                  <td style={{...c.td, color: o.side==='BUY'?'#4ade80':'#f87171'}}>{o.side}</td>
                  <td style={c.td}>{o.quantity}</td>
                  <td style={c.td}>{o.price}</td>
                  <td style={c.td}><span style={c.badge(o.status)}>{o.status}</span></td>
                  <td style={{...c.td, color:'#9ca3af', fontSize:11}}>{o.strategy}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {account && orders.length === 0 && !loading && <div style={{ color: '#6b7280', fontSize: 13 }}>No hay órdenes en Redis para esta cuenta.</div>}
    </div>
  )
}

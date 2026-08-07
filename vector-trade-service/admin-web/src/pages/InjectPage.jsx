import { useState } from 'react'
import api from '../api.js'

const TEMPLATE_ORDER = `{
  "clOrdId": "ADMIN-001",
  "symbol": "SQM-B",
  "exchange": "XSGO",
  "side": "BUY",
  "orderQty": 100,
  "price": 12500.0,
  "ordType": "LIMIT",
  "account": "123456/7",
  "currency": "CLP"
}`

const TEMPLATE_CANCEL = `{
  "clOrdId": "ADMIN-001",
  "origClOrdId": "ORD-ORIG-001",
  "symbol": "SQM-B",
  "exchange": "XSGO",
  "side": "BUY",
  "account": "123456/7"
}`

const EXCHANGES_ROUTING = ['IB_SMART','ALPACA','XSGO','NUAM']

const c = {
  h1:   { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub:  { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row:  { display: 'flex', gap: 10, marginBottom: 14, flexWrap: 'wrap', alignItems: 'center' },
  label:{ fontSize: 12, color: '#9ca3af', marginBottom: 4, display: 'block' },
  select: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13 },
  textarea: { width: '100%', minHeight: 280, padding: '12px 14px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 8, color: '#e2e8f0', fontSize: 12, fontFamily: "'Cascadia Code', 'Fira Code', monospace", lineHeight: 1.6, resize: 'vertical', outline: 'none' },
  btn: (color = '#3b82f6') => ({ padding: '10px 20px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  msg: (ok) => ({ fontSize: 13, padding: '12px 16px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  warn: { fontSize: 12, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: '#431407', color: '#fb923c', border: '1px solid #7c2d12' },
  pre: { background: '#0f1117', border: '1px solid #232840', borderRadius: 6, padding: '12px 14px', fontSize: 11, color: '#a3e635', overflowX: 'auto', fontFamily: 'monospace', marginTop: 12, maxHeight: 200, overflowY: 'auto' },
}

export default function InjectPage() {
  const [type,     setType]     = useState('ORDER')
  const [exchange, setExchange] = useState(EXCHANGES_ROUTING[2])
  const [payload,  setPayload]  = useState(TEMPLATE_ORDER)
  const [msg,      setMsg]      = useState(null)
  const [response, setResponse] = useState(null)
  const [loading,  setLoading]  = useState(false)

  const changeType = (t) => { setType(t); setPayload(t === 'ORDER' ? TEMPLATE_ORDER : TEMPLATE_CANCEL) }
  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 8000) }

  const inject = async () => {
    let parsed
    try { parsed = JSON.parse(payload) } catch(e) { flash(false, '❌ JSON inválido: ' + e.message); return }
    setLoading(true); setResponse(null)
    try {
      const r = await api.post('/inject', { type, exchange, payload: parsed })
      flash(true, `✅ ${r.data.message || 'Mensaje inyectado correctamente'}`); setResponse(r.data)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)); if (e.response?.data) setResponse(e.response.data) }
    finally { setLoading(false) }
  }

  const validateJson = () => {
    try { JSON.parse(payload); flash(true, '✅ JSON válido') } catch(e) { flash(false, '❌ JSON inválido: ' + e.message) }
  }

  return (
    <div>
      <div style={c.h1}>💉 Inyección de Mensajes</div>
      <div style={c.sub}>Inyecta mensajes al bus como si vinieran del sellside — útil para recuperar órdenes perdidas.</div>
      <div style={c.warn}>⚠️ <b>Atención:</b> Este panel afecta directamente el estado interno de órdenes en producción. Úsalo solo cuando el mensaje original se perdió o no llegó al front.</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}
      <div style={c.row}>
        <div>
          <label style={c.label}>Tipo de mensaje</label>
          <select style={c.select} value={type} onChange={e => changeType(e.target.value)}>
            <option value="ORDER">ORDER (ExecutionReport)</option>
            <option value="CANCEL_REJECT">CANCEL_REJECT (OrderCancelReject)</option>
          </select>
        </div>
        <div>
          <label style={c.label}>Exchange destino</label>
          <select style={c.select} value={exchange} onChange={e => setExchange(e.target.value)}>
            {EXCHANGES_ROUTING.map(ex => <option key={ex}>{ex}</option>)}
          </select>
        </div>
      </div>
      <div style={{ marginBottom: 10 }}>
        <label style={c.label}>Payload JSON</label>
        <textarea style={c.textarea} value={payload} onChange={e => setPayload(e.target.value)} spellCheck={false} />
      </div>
      <div style={c.row}>
        <button style={c.btn('#374151')} onClick={validateJson}>✔ Validar JSON</button>
        <button style={c.btn('#059669')} onClick={() => setPayload(type === 'ORDER' ? TEMPLATE_ORDER : TEMPLATE_CANCEL)}>📋 Template</button>
        <button style={c.btn('#dc2626')} onClick={inject} disabled={loading}>{loading ? '⏳ Inyectando...' : '💉 Inyectar'}</button>
      </div>
      {response && <><div style={{ fontSize: 12, color: '#6b7280', marginBottom: 4 }}>Respuesta:</div><pre style={c.pre}>{JSON.stringify(response, null, 2)}</pre></>}
    </div>
  )
}

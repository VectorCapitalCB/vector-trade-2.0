import { useState, useEffect } from 'react'
import api from '../api.js'

const EXCHANGES = [
  'BCS', 'NUAM_MKD', 'BVC', 'BVL',
  'NYSE', 'AMEX', 'NASDAQ',
  'BINANCE_MKD', 'CRYPTO_MARKET_MKD',
  'NONE_MKD',
]

const c = {
  h1:   { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub:  { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, padding: '18px 20px', marginBottom: 20 },
  label:{ display: 'block', fontSize: 11, color: '#6b7280', marginBottom: 4, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 },
  input:{ width: '100%', padding: '9px 12px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, boxSizing: 'border-box' },
  textarea: { width: '100%', padding: '9px 12px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, fontFamily: 'inherit', resize: 'vertical', minHeight: 80, boxSizing: 'border-box' },
  select: { width: '100%', padding: '9px 12px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13 },
  btn:  (color = '#3b82f6') => ({ padding: '9px 20px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  msg:  (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  row:  { display: 'flex', gap: 10, marginBottom: 16 },
  sep:  { borderTop: '1px solid #232840', margin: '20px 0' },
  th:   { padding: '10px 14px', fontSize: 11, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td:   { padding: '8px 14px', fontSize: 12, borderBottom: '1px solid #1a1d27', color: '#e2e8f0' },
  badge:{ display: 'inline-block', padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600, background: '#1e3a5f', color: '#93c5fd' },
}

export default function NewsPage() {
  const [texto,      setTexto]      = useState('')
  const [lineoftext, setLineoftext] = useState('')
  const [exchange,   setExchange]   = useState('BCS')
  const [loading,    setLoading]    = useState(false)
  const [msg,        setMsg]        = useState(null)
  const [history,    setHistory]    = useState([])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 6000) }

  const loadHistory = () => {
    api.get('/news').then(r => setHistory(r.data)).catch(() => {})
  }

  useEffect(() => { loadHistory() }, [])

  const inject = async () => {
    if (!texto.trim()) return
    setLoading(true)
    try {
      const r = await api.post('/news/inject', { texto, lineoftext, securityExchange: exchange })
      flash(true, `✅ ${r.data.message}`)
      setTexto('')
      setLineoftext('')
      loadHistory()
    } catch(e) {
      const err = e.response?.data?.error || e.message
      flash(false, '❌ ' + err)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <div style={c.h1}>📰 Inyección de Noticias</div>
      <div style={c.sub}>
        Publica una noticia como si viniera del mercado — se envía a todos los usuarios conectados vía <code>MessageEventBus["news"]</code>.
      </div>

      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      <div style={c.card}>
        <div style={{ marginBottom: 14 }}>
          <label style={c.label}>Exchange</label>
          <select style={c.select} value={exchange} onChange={e => setExchange(e.target.value)}>
            {EXCHANGES.map(ex => <option key={ex} value={ex}>{ex}</option>)}
          </select>
        </div>

        <div style={{ marginBottom: 14 }}>
          <label style={c.label}>Texto (requerido)</label>
          <textarea
            style={c.textarea}
            placeholder="Ej: SUSPENCION DE NEGOCIACION INSTRUMENTO SQM-B"
            value={texto}
            onChange={e => setTexto(e.target.value)}
          />
        </div>

        <div style={{ marginBottom: 18 }}>
          <label style={c.label}>Line of text (opcional)</label>
          <input
            style={c.input}
            placeholder="Línea adicional de la noticia..."
            value={lineoftext}
            onChange={e => setLineoftext(e.target.value)}
          />
        </div>

        <div style={{ display: 'flex', gap: 10 }}>
          <button
            style={c.btn(texto.trim() ? '#7c3aed' : '#374151')}
            disabled={loading || !texto.trim()}
            onClick={inject}
          >
            {loading ? '...' : '📤 Inyectar noticia'}
          </button>
          <button style={c.btn('#374151')} onClick={() => { setTexto(''); setLineoftext(''); setMsg(null) }}>
            ✕ Limpiar
          </button>
        </div>
      </div>

      {/* Historial en memoria */}
      <div style={{ fontSize: 13, fontWeight: 600, color: '#9ca3af', marginBottom: 8 }}>
        Historial en memoria ({history.length})
        <button style={{ ...c.btn('#374151'), padding: '4px 10px', fontSize: 11, marginLeft: 10 }} onClick={loadHistory}>↺ Refrescar</button>
      </div>

      {history.length > 0 ? (
        <div style={{ background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th style={c.th}>Exchange</th>
                <th style={c.th}>Texto</th>
                <th style={c.th}>Line of text</th>
              </tr>
            </thead>
            <tbody>
              {[...history].reverse().map((n, i) => (
                <tr key={i}>
                  <td style={c.td}><span style={c.badge}>{n.securityExchange}</span></td>
                  <td style={c.td}>{n.texto}</td>
                  <td style={{ ...c.td, color: '#6b7280' }}>{n.lineoftext || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div style={{ fontSize: 12, color: '#4b5563' }}>Sin noticias en memoria todavía.</div>
      )}
    </div>
  )
}

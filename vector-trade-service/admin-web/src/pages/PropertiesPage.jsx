import { useState, useEffect } from 'react'
import api from '../api.js'

const c = {
  h1:   { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub:  { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row:  { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' },
  input:{ padding: '6px 10px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 5, color: '#e2e8f0', fontSize: 12, width: '100%' },
  btn:  (color = '#3b82f6') => ({ padding: '8px 16px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 12, fontWeight: 600, cursor: 'pointer' }),
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th:   { padding: '9px 12px', fontSize: 11, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left', userSelect: 'none' },
  td:   { padding: '6px 12px', fontSize: 12, borderBottom: '1px solid #1a1d27', verticalAlign: 'middle' },
  msg:  (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  warn: { fontSize: 11, padding: '6px 10px', borderRadius: 4, background: '#431407', color: '#fb923c', display: 'inline-block' },
  info: { fontSize: 11, padding: '6px 10px', borderRadius: 4, background: '#0c4a6e', color: '#7dd3fc', display: 'inline-block' },
  restart: { fontSize: 10, padding: '2px 6px', borderRadius: 10, background: '#7c2d12', color: '#fed7aa', fontWeight: 600 },
}

export default function PropertiesPage() {
  const [props,   setProps]   = useState([])
  const [filter,  setFilter]  = useState('')
  const [editing, setEditing] = useState({})   // key → newValue
  const [msg,     setMsg]     = useState(null)
  const [loading, setLoading] = useState(false)

  const load = () => api.get('/properties').then(r => setProps(r.data)).catch(console.error)
  useEffect(() => { load() }, [])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 6000) }

  const startEdit = (p) => {
    setEditing(prev => ({ ...prev, [p.key]: p.sensitive ? '' : p.value }))
  }
  const cancelEdit = (key) => setEditing(prev => { const n = { ...prev }; delete n[key]; return n })

  const save = async (p) => {
    const value = editing[p.key]
    if (value === undefined) return
    setLoading(true)
    try {
      const r = await api.post('/properties', { key: p.key, value })
      flash(true, r.data.message + (r.data.requiresRestart ? ' ⚠️ Requiere reinicio.' : ''))
      cancelEdit(p.key)
      setTimeout(load, 300)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const saveToDisk = async () => {
    setLoading(true)
    try { const r = await api.post('/properties/save'); flash(true, '💾 ' + r.data.message) }
    catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const reloadFromDisk = async () => {
    setLoading(true)
    try {
      const r = await api.post('/properties/reload')
      flash(true, '🔄 ' + r.data.message)
      setTimeout(load, 400)
    } catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const visible = filter.trim()
    ? props.filter(p => p.key.toLowerCase().includes(filter.toLowerCase()) ||
                        (!p.sensitive && p.value.toLowerCase().includes(filter.toLowerCase())))
    : props

  return (
    <div>
      <div style={c.h1}>⚙️ Propiedades</div>
      <div style={c.sub}>Edita parámetros en caliente. Cada cambio se aplica en memoria <strong>y se guarda al disco automáticamente</strong>. Las propiedades marcadas con <span style={c.restart}>reinicio</span> requieren reiniciar el proceso.</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      <div style={c.row}>
        <input style={{ ...c.input, width: 280 }} placeholder="Filtrar por clave o valor..." value={filter}
          onChange={e => setFilter(e.target.value)} />
        <button style={c.btn('#374151')} onClick={load}>↺ Refrescar</button>
        <button style={c.btn('#059669')} onClick={saveToDisk} disabled={loading}>💾 Guardar en disco</button>
        <button style={c.btn('#7c3aed')} onClick={reloadFromDisk} disabled={loading}>🔄 Recargar desde disco</button>
      </div>

      <div style={{ ...c.info, marginBottom: 14 }}>
        ℹ️ Los valores marcados con <span style={c.restart}>reinicio</span> requieren reiniciar el proceso para tomar efecto.
        Los marcados con 🔒 son sensibles — debes ingresar el nuevo valor al editar.
      </div>

      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={{ ...c.th, width: '35%' }}>Clave</th>
              <th style={c.th}>Valor actual</th>
              <th style={{ ...c.th, width: 120 }}>Acción</th>
            </tr>
          </thead>
          <tbody>
            {visible.map(p => {
              const isEditing = editing[p.key] !== undefined
              return (
                <tr key={p.key}>
                  <td style={c.td}>
                    <span style={{ fontFamily: 'monospace', fontSize: 11, color: '#93c5fd' }}>{p.key}</span>
                    {p.requiresRestart && <span style={{ ...c.restart, marginLeft: 6 }}>reinicio</span>}
                    {p.sensitive && <span style={{ marginLeft: 6, fontSize: 11, color: '#6b7280' }}>🔒</span>}
                  </td>
                  <td style={c.td}>
                    {isEditing ? (
                      <input
                        style={c.input}
                        value={editing[p.key]}
                        placeholder={p.sensitive ? 'Nuevo valor (sensible)…' : ''}
                        onChange={e => setEditing(prev => ({ ...prev, [p.key]: e.target.value }))}
                        onKeyDown={e => { if (e.key === 'Enter') save(p); if (e.key === 'Escape') cancelEdit(p.key) }}
                        autoFocus
                      />
                    ) : (
                      <span style={{ fontFamily: 'monospace', fontSize: 12, color: p.sensitive ? '#6b7280' : '#e2e8f0' }}>
                        {p.value}
                      </span>
                    )}
                  </td>
                  <td style={c.td}>
                    {isEditing ? (
                      <div style={{ display: 'flex', gap: 4 }}>
                        <button style={{ ...c.btn('#16a34a'), fontSize: 11, padding: '4px 10px' }}
                          onClick={() => save(p)} disabled={loading}>✓ OK</button>
                        <button style={{ ...c.btn('#374151'), fontSize: 11, padding: '4px 10px' }}
                          onClick={() => cancelEdit(p.key)}>✕</button>
                      </div>
                    ) : (
                      <button style={{ ...c.btn('#1e40af'), fontSize: 11, padding: '4px 10px' }}
                        onClick={() => startEdit(p)}>✎ Editar</button>
                    )}
                  </td>
                </tr>
              )
            })}
            {visible.length === 0 && (
              <tr><td colSpan={3} style={{ ...c.td, textAlign: 'center', color: '#6b7280' }}>Sin resultados</td></tr>
            )}
          </tbody>
        </table>
      </div>
      <div style={{ fontSize: 11, color: '#4b5563', marginTop: 8 }}>
        Total: {props.length} propiedades — mostrando: {visible.length}
      </div>
    </div>
  )
}

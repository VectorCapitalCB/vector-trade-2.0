import { useState, useEffect } from 'react'
import api from '../api.js'

const c = {
  h1:  { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, width: 170 },
  btn: (color = '#3b82f6') => ({ padding: '8px 14px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 12, fontWeight: 600, cursor: 'pointer' }),
  card:  { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden', marginBottom: 20 },
  th:    { padding: '10px 12px', fontSize: 11, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td:    { padding: '8px 12px', fontSize: 12, borderBottom: '1px solid #1a1d27', verticalAlign: 'middle' },
  badge: (ok) => ({ display: 'inline-block', padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  msg: (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
}

function ConnTable({ title, data, onReconnect, onEdit, loading }) {
  const [editing, setEditing] = useState({})
  const startEdit  = (key, conn) => setEditing(e => ({ ...e, [key]: { host: conn.host, port: conn.port } }))
  const cancelEdit = (key) => setEditing(e => { const n = {...e}; delete n[key]; return n })
  const saveEdit   = (key, conn) => { onEdit(conn.type, conn.exchange, editing[key].host, editing[key].port); cancelEdit(key) }

  return (
    <div>
      <div style={{ fontSize: 13, fontWeight: 700, color: '#9ca3af', marginBottom: 8 }}>{title}</div>
      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead><tr>
            <th style={c.th}>Exchange</th><th style={c.th}>Host</th><th style={c.th}>Port</th><th style={c.th}>Estado</th><th style={c.th}>Acciones</th>
          </tr></thead>
          <tbody>
            {data.map(conn => {
              const key = conn.type + '_' + conn.exchange
              const ed = editing[key]
              return (
                <tr key={key}>
                  <td style={c.td}><b>{conn.exchange}</b></td>
                  <td style={c.td}>{ed ? <input style={{...c.input, width:180}} value={ed.host} onChange={e => setEditing(p => ({...p, [key]:{...p[key], host:e.target.value}}))} /> : conn.host}</td>
                  <td style={c.td}>{ed ? <input style={{...c.input, width:70}} type="number" value={ed.port} onChange={e => setEditing(p => ({...p, [key]:{...p[key], port:e.target.value}}))} /> : conn.port}</td>
                  <td style={c.td}><span style={c.badge(conn.connected)}>{conn.connected ? '✓ Conectado' : '✗ Desconectado'}</span></td>
                  <td style={c.td}>
                    {ed ? <>
                      <button style={{...c.btn('#16a34a'), fontSize:11, padding:'4px 10px', marginRight:4}} onClick={() => saveEdit(key, conn)}>💾 Guardar</button>
                      <button style={{...c.btn('#374151'), fontSize:11, padding:'4px 10px'}} onClick={() => cancelEdit(key)}>✕</button>
                    </> : <>
                      <button style={{...c.btn(), fontSize:11, padding:'4px 10px', marginRight:4}} disabled={loading[key]} onClick={() => onReconnect(conn.type, conn.exchange)}>{loading[key] ? '...' : '🔄 Reconectar'}</button>
                      <button style={{...c.btn('#374151'), fontSize:11, padding:'4px 10px'}} onClick={() => startEdit(key, conn)}>✎ Editar</button>
                    </>}
                  </td>
                </tr>
              )
            })}
            {data.length === 0 && <tr><td colSpan={5} style={{...c.td, textAlign:'center', color:'#6b7280'}}>Sin conexiones</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default function ConnectionsPage() {
  const [conns,   setConns]   = useState({ routing: [], mkd: [] })
  const [msg,     setMsg]     = useState(null)
  const [loading, setLoading] = useState({})

  const load = () => api.get('/connections').then(r => setConns(r.data)).catch(console.error)
  useEffect(() => { load() }, [])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 5000) }

  const reconnect = async (type, exchange) => {
    const key = type + '_' + exchange
    setLoading(p => ({...p, [key]: true}))
    try { const r = await api.put(`/connections/${type}/${exchange}`); flash(true, `✅ ${r.data.message}`); setTimeout(load, 800) }
    catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(p => ({...p, [key]: false})) }
  }

  const editHost = async (type, exchange, host, port) => {
    try { const r = await api.put(`/connections/${type}/${exchange}`, { host, port: parseInt(port) }); flash(true, `✅ ${r.data.message}`); setTimeout(load, 800) }
    catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
  }

  const reloadAll = async () => {
    try { const r = await api.post('/connections/reload'); flash(true, `✅ ${r.data.message}`); setTimeout(load, 1000) }
    catch(e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
  }

  return (
    <div>
      <div style={c.h1}>🔌 Conexiones Netty</div>
      <div style={c.sub}>Reconecta o cambia host/puerto de cualquier conexión TCP en caliente, sin reiniciar el proceso.</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}
      <div style={c.row}>
        <button style={c.btn('#374151')} onClick={load}>↺ Actualizar estado</button>
        <button style={c.btn('#7c3aed')} onClick={reloadAll}>🔄 Recargar todo desde JSON</button>
      </div>
      <ConnTable title="🔵 Routing" data={conns.routing || []} onReconnect={reconnect} onEdit={editHost} loading={loading} />
      <ConnTable title="🟢 Market Data (MKD)" data={conns.mkd || []} onReconnect={reconnect} onEdit={editHost} loading={loading} />
    </div>
  )
}

import { useState, useEffect } from 'react'
import api from '../api.js'

const c = {
  h1:      { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  h2:      { fontSize: 15, fontWeight: 700, marginBottom: 10, marginTop: 24 },
  sub:     { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row:     { display: 'flex', gap: 10, marginBottom: 16 },
  input:   { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, flex: 1 },
  btn:     (color = '#3b82f6') => ({ padding: '8px 16px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  card:    { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th:      { padding: '10px 14px', fontSize: 12, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td:      { padding: '9px 14px', fontSize: 13, borderBottom: '1px solid #1a1d27' },
  badge:   (open) => ({ display: 'inline-block', padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600, background: open ? '#052e16' : '#450a0a', color: open ? '#4ade80' : '#f87171' }),
  blocked: { display: 'inline-block', padding: '2px 8px', borderRadius: 10, fontSize: 11, fontWeight: 600, background: '#1c1917', color: '#f97316' },
  msg:     (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  empty:   { padding: '9px 14px', fontSize: 13, color: '#6b7280', textAlign: 'center' },
}

export default function SessionsAdminPage() {
  const [sessions,      setSessions]      = useState([])
  const [blockedUsers,  setBlockedUsers]  = useState([])
  const [search,        setSearch]        = useState('')
  const [manualUser,    setManualUser]    = useState('')
  const [msg,           setMsg]           = useState(null)
  const [loading,       setLoading]       = useState(false)

  const load = async () => {
    const [s, b] = await Promise.all([
      api.get('/sessions').catch(() => ({ data: [] })),
      api.get('/sessions/blocked').catch(() => ({ data: [] })),
    ])
    setSessions(s.data)
    setBlockedUsers(Array.isArray(b.data) ? b.data : [])
  }

  useEffect(() => { load() }, [])

  const flash = (ok, text) => { setMsg({ ok, text }); setTimeout(() => setMsg(null), 5000) }

  const disconnect = async (username) => {
    setLoading(true)
    try {
      await api.post('/sessions/disconnect', { username })
      flash(true, `✅ Usuario desconectado: ${username}`)
      setTimeout(load, 400)
    } catch (e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const blockUser = async (username) => {
    setLoading(true)
    try {
      await api.post('/sessions/block', { username })
      flash(true, `🔒 Usuario bloqueado: ${username}`)
      setTimeout(load, 400)
    } catch (e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const unblockUser = async (username) => {
    setLoading(true)
    try {
      await api.post('/sessions/unblock', { username })
      flash(true, `🔓 Usuario desbloqueado: ${username}`)
      setTimeout(load, 400)
    } catch (e) { flash(false, '❌ ' + (e.response?.data?.error || e.message)) }
    finally { setLoading(false) }
  }

  const filtered = sessions.filter(s =>
    s.username.toLowerCase().includes(search.toLowerCase()) ||
    s.remote.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div>
      <div style={c.h1}>👤 Sesiones y Bloqueos</div>
      <div style={c.sub}>Gestiona usuarios conectados y bloquea accesos en caliente. Los bloqueos persisten tras reinicio.</div>

      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      {/* ── Sesiones activas ── */}
      <div style={c.h2}>🟢 Sesiones activas</div>
      <div style={c.row}>
        <input
          style={c.input}
          placeholder="Buscar por usuario o IP..."
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
        <button style={c.btn('#374151')} onClick={load}>↺ Actualizar</button>
      </div>

      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={c.th}>Usuario</th>
              <th style={c.th}>IP / Remote</th>
              <th style={c.th}>Conectado desde</th>
              <th style={c.th}>Estado</th>
              <th style={c.th}>Acciones</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0
              ? <tr><td colSpan={5} style={c.empty}>Sin sesiones activas</td></tr>
              : filtered.map(s => (
                <tr key={s.username}>
                  <td style={{ ...c.td, fontWeight: 700 }}>{s.username}</td>
                  <td style={{ ...c.td, color: '#6b7280', fontSize: 12 }}>{s.remote}</td>
                  <td style={{ ...c.td, fontSize: 12 }}>{s.connectedAt}</td>
                  <td style={c.td}><span style={c.badge(s.open)}>{s.open ? '● Conectado' : '○ Cerrado'}</span></td>
                  <td style={{ ...c.td, display: 'flex', gap: 6 }}>
                    <button
                      style={{ ...c.btn('#7f1d1d'), fontSize: 11, padding: '4px 10px' }}
                      onClick={() => disconnect(s.username)}
                      disabled={loading || !s.open}
                    >
                      ⏏ Desconectar
                    </button>
                    <button
                      style={{ ...c.btn('#78350f'), fontSize: 11, padding: '4px 10px' }}
                      onClick={() => blockUser(s.username)}
                      disabled={loading}
                    >
                      🔒 Bloquear
                    </button>
                  </td>
                </tr>
              ))
            }
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: 8, fontSize: 11, color: '#4b5563', marginBottom: 4 }}>
        Total: {sessions.length} sesión(es) · Mostrando: {filtered.length}
      </div>

      {/* ── Bloquear usuario manualmente ── */}
      <div style={c.h2}>🚫 Bloquear usuario por nombre</div>
      <div style={c.row}>
        <input
          style={c.input}
          placeholder="Nombre de usuario (ej: jperez)"
          value={manualUser}
          onChange={e => setManualUser(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && manualUser.trim() && blockUser(manualUser.trim())}
        />
        <button
          style={c.btn('#78350f')}
          disabled={loading || !manualUser.trim()}
          onClick={() => blockUser(manualUser.trim())}
        >
          🔒 Bloquear
        </button>
      </div>

      {/* ── Usuarios bloqueados ── */}
      <div style={c.h2}>🔒 Usuarios bloqueados ({blockedUsers.length})</div>
      <div style={c.card}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={c.th}>Usuario</th>
              <th style={c.th}>Estado</th>
              <th style={c.th}>Acción</th>
            </tr>
          </thead>
          <tbody>
            {blockedUsers.length === 0
              ? <tr><td colSpan={3} style={c.empty}>No hay usuarios bloqueados</td></tr>
              : blockedUsers.map(u => (
                <tr key={u}>
                  <td style={{ ...c.td, fontWeight: 700 }}>{u}</td>
                  <td style={c.td}><span style={c.blocked}>🔒 Bloqueado</span></td>
                  <td style={c.td}>
                    <button
                      style={{ ...c.btn('#14532d'), fontSize: 11, padding: '4px 10px' }}
                      onClick={() => unblockUser(u)}
                      disabled={loading}
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

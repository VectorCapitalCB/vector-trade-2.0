import { useEffect, useMemo, useState } from 'react'
import { Trash2 } from 'lucide-react'
import api from '../api.js'

const c = {
  h1: { fontSize: 18, fontWeight: 700, margin: '0 0 4px' },
  sub: { fontSize: 12, color: '#94a3b8', marginBottom: 18 },
  toolbar: { display: 'flex', alignItems: 'end', gap: 10, flexWrap: 'wrap', padding: 14, background: '#141720', border: '1px solid #273244', borderRadius: 7, marginBottom: 14 },
  field: { display: 'grid', gap: 5, minWidth: 180 },
  label: { fontSize: 11, color: '#94a3b8', fontWeight: 600 },
  select: { height: 34, padding: '0 10px', background: '#0f1720', border: '1px solid #334155', borderRadius: 5, color: '#e2e8f0', fontSize: 13 },
  btn: (color = '#2563eb') => ({ height: 34, padding: '0 14px', background: color, border: '1px solid rgba(255,255,255,.08)', borderRadius: 5, color: '#fff', fontWeight: 700, fontSize: 12, cursor: 'pointer' }),
  card: { background: '#141720', border: '1px solid #273244', borderRadius: 7, overflow: 'hidden' },
  tableScroll: { width: '100%', overflowX: 'auto', overflowY: 'hidden', scrollbarGutter: 'stable' },
  th: { padding: '9px 11px', color: '#94a3b8', fontSize: 11, textAlign: 'left', borderBottom: '1px solid #273244', whiteSpace: 'nowrap' },
  td: { padding: '9px 11px', fontSize: 12, borderBottom: '1px solid #1f2937', verticalAlign: 'middle' },
  msg: ok => ({ padding: '9px 12px', marginBottom: 12, borderRadius: 5, fontSize: 12, background: ok ? '#052e16' : '#450a0a', color: ok ? '#86efac' : '#fca5a5' }),
  pre: { margin: 0, padding: 14, minHeight: 260, maxHeight: '52vh', overflow: 'auto', background: '#090d13', color: '#cbd5e1', fontSize: 11, lineHeight: 1.45, whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' },
}

const statusStyle = status => {
  const colors = {
    RECEIVED: ['#052e16', '#4ade80'],
    PENDING: ['#172554', '#93c5fd'],
    ERROR: ['#450a0a', '#f87171'],
    REJECTED: ['#3f1d2e', '#f9a8d4'],
    TIMEOUT: ['#422006', '#facc15'],
  }
  const [background, color] = colors[status] || ['#1f2937', '#cbd5e1']
  return { display: 'inline-block', padding: '3px 7px', borderRadius: 4, background, color, fontWeight: 700, fontSize: 10 }
}

const formatDate = value => value ? new Date(value).toLocaleString('es-CL') : '-'
const formatSize = value => value ? `${(value / 1024).toFixed(1)} KB` : '-'

export default function ClientLogsPage() {
  const [sessions, setSessions] = useState([])
  const [items, setItems] = useState([])
  const [username, setUsername] = useState('')
  const [minutes, setMinutes] = useState(30)
  const [selected, setSelected] = useState(null)
  const [message, setMessage] = useState(null)
  const [loading, setLoading] = useState(false)

  const load = async () => {
    const [sessionResult, logResult] = await Promise.all([
      api.get('/sessions').catch(() => ({ data: [] })),
      api.get('/client-logs').catch(() => ({ data: [] })),
    ])
    const active = (Array.isArray(sessionResult.data) ? sessionResult.data : []).filter(session => session.open)
    setSessions(active)
    setItems(Array.isArray(logResult.data) ? logResult.data : [])
    setUsername(current => current || active[0]?.username || '')
  }

  useEffect(() => {
    load()
    const timer = setInterval(load, 3000)
    return () => clearInterval(timer)
  }, [])

  const pendingCount = useMemo(() => items.filter(item => item.status === 'PENDING').length, [items])

  const requestLogs = async () => {
    if (!username) return
    setLoading(true)
    try {
      const { data } = await api.post('/client-logs/request', { username, minutes: Number(minutes) })
      setMessage({ ok: true, text: `Solicitud enviada a ${username}. Esperando autorización en el front.` })
      setItems(current => [data, ...current.filter(item => item.requestId !== data.requestId)])
    } catch (error) {
      setMessage({ ok: false, text: error.response?.data?.error || error.message })
    } finally {
      setLoading(false)
      setTimeout(() => setMessage(null), 6000)
    }
  }

  const view = async requestId => {
    try {
      const { data } = await api.get(`/client-logs/${requestId}`)
      setSelected(data)
    } catch (error) {
      setMessage({ ok: false, text: error.response?.data?.error || error.message })
    }
  }

  const download = async item => {
    try {
      const response = await api.get(`/client-logs/${item.requestId}/download`, { responseType: 'blob' })
      const url = URL.createObjectURL(response.data)
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = `${item.username}-${item.requestId}.log`
      anchor.click()
      URL.revokeObjectURL(url)
    } catch (error) {
      setMessage({ ok: false, text: error.response?.data?.error || error.message })
    }
  }

  const remove = async item => {
    if (!window.confirm(`¿Eliminar el diagnóstico de ${item.username} solicitado el ${formatDate(item.requestedAt)}?`)) return
    try {
      await api.delete(`/client-logs/${item.requestId}`)
      setItems(current => current.filter(currentItem => currentItem.requestId !== item.requestId))
      if (selected?.requestId === item.requestId) setSelected(null)
      setMessage({ ok: true, text: 'Diagnóstico eliminado del core.' })
    } catch (error) {
      setMessage({ ok: false, text: error.response?.data?.error || error.message })
    } finally {
      setTimeout(() => setMessage(null), 4000)
    }
  }

  const removeAll = async () => {
    if (!items.length || !window.confirm(`¿Eliminar definitivamente los ${items.length} registros de diagnóstico?`)) return
    try {
      const { data } = await api.delete('/client-logs')
      setItems([])
      setSelected(null)
      setMessage({ ok: true, text: `${data.deleted || 0} diagnóstico(s) eliminados del core.` })
    } catch (error) {
      setMessage({ ok: false, text: error.response?.data?.error || error.message })
    } finally {
      setTimeout(() => setMessage(null), 4000)
    }
  }

  return (
    <div>
      <h1 style={c.h1}>Diagnóstico de fronts</h1>
      <div style={c.sub}>Solicita un extracto acotado y sanitizado del log de un usuario conectado. Retención en el core: 15 días.</div>

      {message && <div style={c.msg(message.ok)}>{message.text}</div>}

      <div style={c.toolbar}>
        <label style={c.field}>
          <span style={c.label}>Usuario conectado</span>
          <select style={c.select} value={username} onChange={event => setUsername(event.target.value)}>
            {sessions.length === 0 && <option value="">Sin usuarios conectados</option>}
            {sessions.map(session => <option key={session.username} value={session.username}>{session.username}</option>)}
          </select>
        </label>
        <label style={{ ...c.field, minWidth: 130 }}>
          <span style={c.label}>Rango del log</span>
          <select style={c.select} value={minutes} onChange={event => setMinutes(event.target.value)}>
            <option value={15}>Últimos 15 min</option>
            <option value={30}>Últimos 30 min</option>
            <option value={60}>Última hora</option>
            <option value={120}>Últimas 2 horas</option>
            <option value={240}>Últimas 4 horas</option>
          </select>
        </label>
        <button style={c.btn()} disabled={loading || !username} onClick={requestLogs}>
          {loading ? 'Solicitando...' : 'Solicitar diagnóstico'}
        </button>
        <button style={c.btn('#334155')} onClick={load}>Actualizar</button>
        <button
          style={{ ...c.btn('#4c1d24'), borderColor: '#b4535a', display: 'inline-flex', alignItems: 'center', gap: 7 }}
          disabled={!items.length}
          onClick={removeAll}
          title="Eliminar todos los diagnósticos"
        >
          <Trash2 size={14} /> Limpiar registros
        </button>
        <span style={{ marginLeft: 'auto', fontSize: 11, color: pendingCount ? '#93c5fd' : '#64748b' }}>
          {pendingCount ? `${pendingCount} solicitud(es) pendiente(s)` : 'Sin solicitudes pendientes'}
        </span>
      </div>

      <div style={c.card}>
        <div style={c.tableScroll}>
        <table style={{ width: '100%', minWidth: 1840, borderCollapse: 'collapse', tableLayout: 'auto' }}>
          <thead><tr>
            <th style={c.th}>Solicitado</th><th style={c.th}>Usuario</th><th style={c.th}>Equipo</th>
            <th style={c.th}>Versión</th><th style={c.th}>Sistema</th><th style={c.th}>Rango</th>
            <th style={c.th}>Especificaciones</th><th style={c.th}>Tamaño</th><th style={c.th}>Estado</th><th style={c.th}>Acciones</th>
          </tr></thead>
          <tbody>
            {items.length === 0 && <tr><td colSpan={10} style={{ ...c.td, color: '#64748b', textAlign: 'center' }}>Aún no hay diagnósticos solicitados</td></tr>}
            {items.map(item => <tr key={item.requestId}>
              <td style={c.td}>{formatDate(item.requestedAt)}</td>
              <td style={{ ...c.td, fontWeight: 700 }}>{item.username}</td>
              <td style={c.td}>{item.deviceId || '-'}</td>
              <td style={c.td}>{item.appVersion || '-'}</td>
              <td style={{ ...c.td, minWidth: 190, whiteSpace: 'nowrap' }}>{item.os || '-'}</td>
              <td style={c.td}>{item.minutes} min</td>
              <td style={{ ...c.td, minWidth: 470, maxWidth: 560, lineHeight: 1.4, whiteSpace: 'normal' }}>{item.hardware || '-'}</td>
              <td style={c.td}>{formatSize(item.sizeBytes)}{item.truncated ? ' (recortado)' : ''}</td>
              <td style={c.td}><span style={statusStyle(item.status)}>{item.status}</span></td>
              <td style={{ ...c.td, whiteSpace: 'nowrap' }}>
                <button style={{ ...c.btn('#334155'), height: 27, padding: '0 9px', marginRight: 5 }} disabled={item.status === 'PENDING'} onClick={() => view(item.requestId)}>Ver</button>
                <button style={{ ...c.btn('#14532d'), height: 27, padding: '0 9px', marginRight: 5 }} disabled={!item.sizeBytes} onClick={() => download(item)}>Descargar</button>
                <button
                  style={{ ...c.btn('#4c1d24'), height: 27, width: 29, padding: 0, borderColor: '#b4535a', display: 'inline-grid', placeItems: 'center' }}
                  onClick={() => remove(item)}
                  title="Eliminar diagnóstico"
                  aria-label="Eliminar diagnóstico"
                ><Trash2 size={13} /></button>
              </td>
            </tr>)}
          </tbody>
        </table>
        </div>
      </div>

      {selected && <div style={{ ...c.card, marginTop: 14 }}>
        <div style={{ padding: '10px 14px', display: 'flex', alignItems: 'center', gap: 12, borderBottom: '1px solid #273244' }}>
          <strong style={{ fontSize: 13 }}>{selected.username} · {selected.deviceId || 'equipo sin identificar'}</strong>
          <span style={{ fontSize: 11, color: '#94a3b8' }}>{formatDate(selected.generatedAt)} · {selected.minutes} min · {formatSize(selected.sizeBytes)}</span>
          {selected.error && <span style={{ fontSize: 11, color: '#fca5a5' }}>{selected.error}</span>}
          <button style={{ ...c.btn('#334155'), height: 28, marginLeft: 'auto' }} onClick={() => setSelected(null)}>Cerrar</button>
        </div>
        <div style={{ padding: '9px 14px', display: 'flex', gap: 18, flexWrap: 'wrap', color: '#cbd5e1', fontSize: 11, borderBottom: '1px solid #273244', background: '#101722' }}>
          <span><strong style={{ color: '#94a3b8' }}>Sistema:</strong> {selected.os || 'No informado'}</span>
          <span><strong style={{ color: '#94a3b8' }}>Equipo:</strong> {selected.hardware || 'No informado por este front; reinícialo y solicita un diagnóstico nuevo'}</span>
        </div>
        <pre style={c.pre}>{selected.content || selected.error || 'El diagnóstico no contiene líneas de log.'}</pre>
      </div>}
    </div>
  )
}

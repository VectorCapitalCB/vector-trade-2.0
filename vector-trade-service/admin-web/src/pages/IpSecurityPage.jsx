import { useState, useEffect, useCallback } from 'react'
import api from '../api.js'

const API = '/security'

const errText = (e) => e.response?.data?.error || e.message

const s = {
  title: { fontSize: 20, fontWeight: 700, color: '#f1f5f9', marginBottom: 4 },
  sub:   { color: '#64748b', fontSize: 13, marginBottom: 24 },
  card:  { background: '#1e2540', borderRadius: 10, padding: 20, marginBottom: 20 },
  cardTitle: { fontSize: 14, fontWeight: 700, color: '#93c5fd', marginBottom: 14, display: 'flex', alignItems: 'center', gap: 8 },
  row:   { display: 'flex', gap: 10, alignItems: 'center', marginBottom: 10, flexWrap: 'wrap' },
  input: { background: '#0f1117', border: '1px solid #334155', borderRadius: 6, padding: '7px 12px', color: '#f1f5f9', fontSize: 13, flex: 1, minWidth: 180 },
  btn:   (c) => ({ background: c || '#3b82f6', border: 'none', borderRadius: 6, padding: '7px 16px', color: '#fff', fontSize: 13, cursor: 'pointer', fontWeight: 600 }),
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 13 },
  th:    { textAlign: 'left', padding: '8px 12px', background: '#141720', color: '#64748b', fontWeight: 600, fontSize: 12 },
  td:    { padding: '9px 12px', borderBottom: '1px solid #232840', color: '#e2e8f0' },
  badge: (ok) => ({ display: 'inline-block', padding: '2px 10px', borderRadius: 12, fontSize: 11, fontWeight: 700,
    background: ok ? '#14532d' : '#7f1d1d', color: ok ? '#4ade80' : '#f87171' }),
  statBox: { background: '#141720', borderRadius: 8, padding: '14px 18px', textAlign: 'center' },
  statVal: { fontSize: 26, fontWeight: 700, color: '#60a5fa' },
  statLbl: { fontSize: 11, color: '#64748b', marginTop: 2 },
  alert:   { background: '#7f1d1d', border: '1px solid #991b1b', borderRadius: 8, padding: '12px 16px', color: '#fca5a5', fontSize: 13, marginBottom: 16 },
  safe:    { background: '#14532d', border: '1px solid #166534', borderRadius: 8, padding: '12px 16px', color: '#86efac', fontSize: 13, marginBottom: 16 },
  msg:     { fontSize: 12, marginTop: 8, color: '#94a3b8' },
}

function StatusCard({ status, onResetProtection }) {
  if (!status) return null
  const pm = status.protectionMode
  return (
    <div style={s.card}>
      <div style={s.cardTitle}>🛡️ Estado del Sistema</div>
      {pm
        ? <div style={s.alert}>⚠️ <strong>MODO PROTECCIÓN ACTIVO</strong> — Se detectaron {status.autoBlockEvents} auto-bloqueos en los últimos 60s. Los umbrales se han reducido a la mitad.</div>
        : <div style={s.safe}>✅ Sistema operando normalmente — sin amenazas activas detectadas.</div>
      }
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', marginBottom: pm ? 16 : 0 }}>
        <div style={s.statBox}>
          <div style={s.statVal}>{status.blockedIpsCount}</div>
          <div style={s.statLbl}>IPs Bloqueadas</div>
        </div>
        <div style={s.statBox}>
          <div style={s.statVal}>{status.autoBlockEvents}</div>
          <div style={s.statLbl}>Auto-bloqueos (60s)</div>
        </div>
        <div style={s.statBox}>
          <div style={s.statVal}>{status.trackedIps}</div>
          <div style={s.statLbl}>IPs en seguimiento</div>
        </div>
        <div style={s.statBox}>
          <div style={{ ...s.statVal, fontSize: 18 }}>{status.maxMessagesPerSec}{pm ? `→${Math.max(1,Math.floor(status.maxMessagesPerSec/2))}` : ''}</div>
          <div style={s.statLbl}>Máx. msg/s por IP</div>
        </div>
        <div style={s.statBox}>
          <div style={{ ...s.statVal, fontSize: 18 }}>{status.maxOrdersPerSec}{pm ? `→${Math.max(1,Math.floor(status.maxOrdersPerSec/2))}` : ''}</div>
          <div style={s.statLbl}>Máx. órdenes/s por IP</div>
        </div>
      </div>
      {pm && (
        <button style={s.btn('#166534')} onClick={onResetProtection}>
          ✅ Desactivar Modo Protección
        </button>
      )}
    </div>
  )
}

export default function IpSecurityPage() {
  const [status, setStatus]       = useState(null)
  const [blockedIps, setBlockedIps] = useState([])
  const [newIp, setNewIp]         = useState('')
  const [msg, setMsg]             = useState('')
  const [loading, setLoading]     = useState(false)

  const flash = (text) => { setMsg(text); setTimeout(() => setMsg(''), 3500) }

  const load = useCallback(async () => {
    try {
      const [stRes, ipRes] = await Promise.all([
        api.get(`${API}/status`),
        api.get(`${API}/blocked-ips`),
      ])
      setStatus(stRes.data)
      // Guarda: si la respuesta no es un array (error/401), no romper el render con .map
      setBlockedIps(Array.isArray(ipRes.data) ? ipRes.data : [])
    } catch (e) {
      setBlockedIps([])
      flash('Error cargando datos: ' + errText(e))
    }
  }, [])

  useEffect(() => { load() }, [load])

  // Auto-refresh every 10 seconds
  useEffect(() => {
    const id = setInterval(load, 10000)
    return () => clearInterval(id)
  }, [load])

  const blockIp = async () => {
    const ip = newIp.trim()
    if (!ip) return
    setLoading(true)
    try {
      const d = (await api.post(`${API}/block-ip`, { ip })).data
      if (d.ok) { flash(`✅ IP ${ip} bloqueada`); setNewIp(''); load() }
      else flash('Error: ' + (d.error || JSON.stringify(d)))
    } catch (e) { flash('Error: ' + errText(e)) }
    finally { setLoading(false) }
  }

  const unblockIp = async (ip) => {
    setLoading(true)
    try {
      const d = (await api.post(`${API}/unblock-ip`, { ip })).data
      if (d.ok) { flash(`✅ IP ${ip} desbloqueada`); load() }
      else flash('Error: ' + (d.error || JSON.stringify(d)))
    } catch (e) { flash('Error: ' + errText(e)) }
    finally { setLoading(false) }
  }

  const resetProtection = async () => {
    setLoading(true)
    try {
      const d = (await api.post(`${API}/reset-protection`)).data
      if (d.ok) { flash('✅ Modo protección reseteado'); load() }
      else flash('Error: ' + (d.error || JSON.stringify(d)))
    } catch (e) { flash('Error: ' + errText(e)) }
    finally { setLoading(false) }
  }

  return (
    <div>
      <div style={s.title}>🔒 Seguridad por IP</div>
      <div style={s.sub}>Rate limiting automático, bloqueo de IPs y modo protección contra ataques DDoS</div>

      <StatusCard status={status} onResetProtection={resetProtection} />

      {/* Manual block */}
      <div style={s.card}>
        <div style={s.cardTitle}>🚫 Bloquear IP manualmente</div>
        <div style={s.row}>
          <input
            style={s.input}
            placeholder="Ej: 192.168.1.100"
            value={newIp}
            onChange={e => setNewIp(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && blockIp()}
          />
          <button style={s.btn('#dc2626')} onClick={blockIp} disabled={loading || !newIp.trim()}>
            🚫 Bloquear
          </button>
        </div>
        {msg && <div style={s.msg}>{msg}</div>}
      </div>

      {/* Blocked IPs list */}
      <div style={s.card}>
        <div style={s.cardTitle}>
          📋 IPs Bloqueadas
          <span style={{ marginLeft: 'auto', fontSize: 12, fontWeight: 400, color: '#64748b' }}>
            {blockedIps.length} total · auto-refresh 10s
          </span>
          <button style={{ ...s.btn('#1e40af'), fontSize: 12, padding: '4px 10px' }} onClick={load}>↻ Actualizar</button>
        </div>
        {blockedIps.length === 0 ? (
          <div style={{ color: '#64748b', fontSize: 13 }}>No hay IPs bloqueadas.</div>
        ) : (
          <table style={s.table}>
            <thead>
              <tr>
                <th style={s.th}>IP</th>
                <th style={s.th}>Estado</th>
                <th style={s.th}>Acción</th>
              </tr>
            </thead>
            <tbody>
              {blockedIps.map(ip => (
                <tr key={ip}>
                  <td style={s.td}><code style={{ color: '#f87171' }}>{ip}</code></td>
                  <td style={s.td}><span style={s.badge(false)}>BLOQUEADA</span></td>
                  <td style={s.td}>
                    <button style={s.btn('#166534')} onClick={() => unblockIp(ip)} disabled={loading}>
                      ✅ Desbloquear
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Info */}
      <div style={s.card}>
        <div style={s.cardTitle}>ℹ️ Cómo funciona</div>
        <ul style={{ color: '#94a3b8', fontSize: 13, lineHeight: 1.8, paddingLeft: 20 }}>
          <li><strong style={{ color: '#93c5fd' }}>Rate limiting:</strong> Cada IP tiene una ventana deslizante de 1 segundo. Si supera el umbral de mensajes ({status?.maxMessagesPerSec ?? '...'}/s) u órdenes ({status?.maxOrdersPerSec ?? '...'}/s), es bloqueada automáticamente.</li>
          <li><strong style={{ color: '#93c5fd' }}>Modo Protección:</strong> Se activa cuando hay {status?.autoBlockThreshold ?? '...'} o más auto-bloqueos en 60 segundos. Los umbrales se reducen a la mitad para cortar tráfico borderline.</li>
          <li><strong style={{ color: '#93c5fd' }}>Persistencia:</strong> Las IPs bloqueadas se guardan en Redis sin expiración diaria (sobreviven reinicios).</li>
          <li><strong style={{ color: '#93c5fd' }}>Clientes legítimos:</strong> Los usuarios ya autenticados y conectados <em>no</em> se desconectan cuando se activa el modo protección.</li>
        </ul>
      </div>
    </div>
  )
}

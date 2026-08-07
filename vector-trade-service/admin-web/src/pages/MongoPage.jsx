import { useEffect, useState } from 'react'
import api from '../api.js'

const c = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center', marginBottom: 16 },
  btn: (color = '#2563eb') => ({
    padding: '8px 14px', border: 0, borderRadius: 6, background: color,
    color: '#fff', cursor: 'pointer', fontSize: 12, fontWeight: 700,
  }),
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, padding: 18, marginBottom: 16 },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(190px, 1fr))', gap: 12, marginBottom: 16 },
  metric: { background: '#10131c', border: '1px solid #252b44', borderRadius: 8, padding: 14 },
  metricLabel: { color: '#7c8db5', fontSize: 11, textTransform: 'uppercase', letterSpacing: .7 },
  metricValue: { color: '#f8fafc', fontSize: 20, fontWeight: 800, marginTop: 6 },
  badge: (ok) => ({
    display: 'inline-block', padding: '3px 9px', borderRadius: 12, fontSize: 11, fontWeight: 800,
    background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171',
  }),
  msg: (ok) => ({
    fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14,
    background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171',
  }),
  label: { fontSize: 11, color: '#94a3b8', marginBottom: 6 },
  input: {
    width: '100%', boxSizing: 'border-box', padding: '8px 12px', background: '#0f1117',
    border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13,
  },
  field: { marginBottom: 12 },
  mono: { fontFamily: 'monospace', color: '#cbd5e1' },
  warn: { color: '#fbbf24', fontSize: 12, lineHeight: 1.5 },
}

export default function MongoPage() {
  const [status, setStatus] = useState(null)
  const [form, setForm] = useState({ connection: '', db: '', collection: '', enabled: false })
  const [msg, setMsg] = useState(null)
  const [loading, setLoading] = useState(false)

  const load = async () => {
    const res = await api.get('/mongo')
    setStatus(res.data)
    setForm({
      connection: '', // el uri viene enmascarado: vacio = mantener el actual
      db: res.data.db || '',
      collection: res.data.collection || '',
      enabled: !!res.data.enabled,
    })
  }

  useEffect(() => {
    load().catch(error => setMsg({ ok: false, text: error.response?.data?.error || error.message }))
  }, [])

  const reconnect = async () => {
    setLoading(true); setMsg(null)
    try {
      const res = await api.post('/mongo/reconnect')
      setStatus(res.data)
      setMsg({ ok: !!res.data.ok, text: res.data.message || 'Mongo reconectado.' })
    } catch (error) {
      setMsg({ ok: false, text: error.response?.data?.error || error.message })
    } finally { setLoading(false) }
  }

  const saveAndReconnect = async () => {
    setLoading(true); setMsg(null)
    try {
      if (form.connection.trim()) {
        await api.post('/properties', { key: 'mongo.connection', value: form.connection.trim() })
      }
      await api.post('/properties', { key: 'mongo.db', value: form.db.trim() })
      await api.post('/properties', { key: 'collection', value: form.collection.trim() })
      await api.post('/properties', { key: 'mongo.isconnected', value: String(form.enabled) })
      const res = await api.post('/mongo/reconnect')
      setStatus(res.data)
      setForm(f => ({ ...f, connection: '' }))
      setMsg({ ok: !!res.data.ok, text: (res.data.message || 'Config aplicada.') + ' Guardada en disco.' })
    } catch (error) {
      setMsg({ ok: false, text: error.response?.data?.error || error.message })
    } finally { setLoading(false) }
  }

  return (
    <div>
      <div style={c.h1}>🍃 Mongo (Previous Close)</div>
      <div style={c.sub}>
        Conexión a Mongo para el cierre de ayer y el cálculo de variación %. Cambia la URL y la
        config en caliente sin reiniciar el core.
      </div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      <div style={c.grid}>
        <div style={c.metric}>
          <div style={c.metricLabel}>Conexión</div>
          <div style={c.metricValue}><span style={c.badge(status?.connected)}>{status?.connected ? 'OK' : 'FALLA'}</span></div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Feature</div>
          <div style={c.metricValue}><span style={c.badge(status?.enabled)}>{status?.enabled ? 'ON' : 'OFF'}</span></div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>DB / Colección</div>
          <div style={{ ...c.metricValue, fontSize: 14 }}>{status?.db || '-'} / {status?.collection || '-'}</div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Símbolos cacheados</div>
          <div style={c.metricValue}>{status?.cacheSize ?? '-'}</div>
        </div>
        <div style={c.metric}>
          <div style={c.metricLabel}>Lectura de cierres</div>
          <div style={{ ...c.metricValue, fontSize: 14 }}>
            {status?.reading
              ? <span style={c.badge(false)}>LEYENDO MONGO…</span>
              : status?.warmed
                ? <span style={c.badge(true)}>CARGADO</span>
                : <span style={c.badge(false)}>SIN CARGAR</span>}
          </div>
        </div>
      </div>

      <div style={c.card}>
        <div style={{ ...c.metricLabel, marginBottom: 8 }}>URI actual (enmascarada)</div>
        <div style={{ ...c.mono, marginBottom: 16, wordBreak: 'break-all' }}>{status?.uri || '(sin configurar)'}</div>

        <div style={c.field}>
          <div style={c.label}>Connection string (dejar vacío = mantener la actual)</div>
          <input
            style={c.input} type="password" autoComplete="new-password"
            placeholder="mongodb://user:pass@host:27017/db?authSource=admin"
            value={form.connection}
            onChange={e => setForm(f => ({ ...f, connection: e.target.value }))}
          />
        </div>
        <div style={c.field}>
          <div style={c.label}>Base de datos (mongo.db)</div>
          <input style={c.input} value={form.db} onChange={e => setForm(f => ({ ...f, db: e.target.value }))} />
        </div>
        <div style={c.field}>
          <div style={c.label}>Colección (collection)</div>
          <input style={c.input} value={form.collection} onChange={e => setForm(f => ({ ...f, collection: e.target.value }))} />
        </div>
        <div style={c.field}>
          <label style={{ ...c.label, display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer' }}>
            <input type="checkbox" checked={form.enabled} onChange={e => setForm(f => ({ ...f, enabled: e.target.checked }))} />
            Feature habilitado (mongo.isconnected)
          </label>
        </div>

        <div style={c.row}>
          <button style={c.btn('#2563eb')} onClick={saveAndReconnect} disabled={loading}>
            {loading ? 'Aplicando...' : '💾 Guardar y reconectar'}
          </button>
          <button style={c.btn(status?.connected ? '#0f766e' : '#dc2626')} onClick={reconnect} disabled={loading}>
            {loading ? '...' : '↻ Reconectar (config actual)'}
          </button>
          <button style={c.btn('#374151')} onClick={() => load().catch(console.error)} disabled={loading}>↺ Refrescar</button>
        </div>
      </div>

      {status?.lastError && (
        <div style={c.card}>
          <div style={c.metricLabel}>Último error</div>
          <div style={{ ...c.mono, marginTop: 8, color: '#fca5a5' }}>{status.lastError}</div>
        </div>
      )}

      <div style={c.card}>
        <div style={c.warn}>
          Al guardar, la config queda persistida en application.properties del servidor (el password nunca
          se muestra: se enmascara). Los símbolos ya suscritos toman el nuevo cierre al re-suscribirse;
          las suscripciones nuevas lo toman de inmediato.
        </div>
      </div>
    </div>
  )
}

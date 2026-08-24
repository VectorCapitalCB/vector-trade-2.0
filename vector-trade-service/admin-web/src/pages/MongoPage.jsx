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
  progress: { height: 8, background: '#0f1117', border: '1px solid #252b44', borderRadius: 6, overflow: 'hidden', marginTop: 10 },
  progressBar: (pct) => ({ height: '100%', width: `${pct}%`, background: 'linear-gradient(90deg, #2563eb, #38bdf8)', transition: 'width .3s' }),
  inline: { width: 110, boxSizing: 'border-box', padding: '8px 10px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13 },
}

const fmtDur = (s) => {
  if (!s || s < 0) return '0s'
  const m = Math.floor(s / 60)
  return m > 0 ? `${m}m ${s % 60}s` : `${s}s`
}

export default function MongoPage() {
  const [status, setStatus] = useState(null)
  const [form, setForm] = useState({ connection: '', db: '', collection: '', enabled: false })
  const [msg, setMsg] = useState(null)
  const [loading, setLoading] = useState(false)
  // actioning se separa de loading: los botones de recarga conviven con los de config.
  const [actioning, setActioning] = useState(false)
  // Arranca vacio y se prellena con los defaults efectivos del servidor (properties o constante):
  // asi los numeros no quedan duplicados entre el JSX y el Java.
  const [reloadForm, setReloadForm] = useState(null)
  const [symbol, setSymbol] = useState('')

  const load = async () => {
    const res = await api.get('/mongo')
    setStatus(res.data)
    setReloadForm(f => f || {
      types: (res.data.reloadTypesDefault || []).join(','),
      priority: (res.data.reloadPriorityDefault || []).join(','),
      batchSize: res.data.reloadBatchDefault ?? 100,
      ratePerSecond: res.data.reloadRateDefault ?? 10,
    })
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

  // Polling solo mientras el backend dice que la recarga corre (mismo patron que SqlRecoveryPage).
  useEffect(() => {
    if (!status?.reloadRunning) return undefined
    const id = setInterval(() => { load().catch(() => {}) }, 2000)
    return () => clearInterval(id)
  }, [status?.reloadRunning])

  const startReload = async () => {
    if (!reloadForm) return
    setActioning(true); setMsg(null)
    try {
      const split = v => v.split(',').map(x => x.trim().toUpperCase()).filter(Boolean)
      const res = await api.post('/mongo/reload', {
        types: split(reloadForm.types),
        prioritySymbols: split(reloadForm.priority),
        batchSize: Number(reloadForm.batchSize) || undefined,
        ratePerSecond: Number(reloadForm.ratePerSecond) || undefined,
      })
      setStatus(res.data)
      setMsg({ ok: !!res.data.ok, text: res.data.message || 'Recarga iniciada.' })
    } catch (error) {
      // el 409 trae el status para poder pintar el job que ya esta corriendo
      if (error.response?.data?.reload) setStatus(error.response.data)
      setMsg({ ok: false, text: error.response?.data?.message || error.response?.data?.error || error.message })
    } finally { setActioning(false) }
  }

  const stopReload = async () => {
    setActioning(true); setMsg(null)
    try {
      const res = await api.post('/mongo/reload/stop')
      setStatus(res.data)
      setMsg({ ok: !!res.data.ok, text: res.data.message || 'Detención solicitada.' })
    } catch (error) {
      setMsg({ ok: false, text: error.response?.data?.error || error.message })
    } finally { setActioning(false) }
  }

  const refreshSymbol = async () => {
    const sym = symbol.trim()
    if (!sym) return
    setActioning(true); setMsg(null)
    try {
      const res = await api.post('/mongo/symbol', { symbol: sym })
      setMsg({ ok: !!res.data.ok, text: res.data.message || 'Símbolo recargado.' })
      await load().catch(() => {})
    } catch (error) {
      setMsg({ ok: false, text: error.response?.data?.error || error.message })
    } finally { setActioning(false) }
  }

  const run = status?.reload || status?.lastReload || null
  const pct = run && run.symbolsTotal > 0
    ? Math.min(100, Math.round((run.symbolsProcessed / run.symbolsTotal) * 100))
    : 0

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

      <div style={c.card}>
        <div style={{ ...c.h1, fontSize: 15 }}>Recarga de cierres en caliente</div>
        <div style={{ ...c.sub, marginBottom: 14 }}>
          Para cuando el cierre llega a Mongo más tarde de lo normal. Relee al ritmo que indiques, primero
          los símbolos de prioridad y después por SecurityType, sin reiniciar el core y sin vaciar la
          caché (la variación % nunca cae al valor del feed mientras se repuebla). Al cerrar cada grupo
          avisa a los actores suscritos, así el cliente ve la var% corregida sin esperar el próximo tick.
          El universo sale de Mongo, no de la SecurityList: los símbolos sin tipo conocido se leen al
          final en el grupo OTROS en vez de quedar fuera.
        </div>

        {reloadForm && <div style={c.row}>
          <div>
            <div style={c.label}>Orden por SecurityType</div>
            <input
              style={{ ...c.input, width: 200 }} value={reloadForm.types}
              onChange={e => setReloadForm(f => ({ ...f, types: e.target.value }))}
            />
          </div>
          <div>
            <div style={c.label}>Leer primero (sí o sí)</div>
            <input
              style={{ ...c.input, width: 200 }} value={reloadForm.priority}
              onChange={e => setReloadForm(f => ({ ...f, priority: e.target.value }))}
            />
          </div>
          <div>
            <div style={c.label}>Símbolos por segundo</div>
            <input
              style={c.inline} type="number" min="0.1" step="0.1" value={reloadForm.ratePerSecond}
              onChange={e => setReloadForm(f => ({ ...f, ratePerSecond: e.target.value }))}
            />
          </div>
          <div>
            <div style={c.label}>Símbolos por lote</div>
            <input
              style={c.inline} type="number" min="1" max="1000" value={reloadForm.batchSize}
              onChange={e => setReloadForm(f => ({ ...f, batchSize: e.target.value }))}
            />
          </div>
        </div>}

        <div style={c.row}>
          <button
            style={c.btn('#2563eb')} onClick={startReload}
            disabled={actioning || !reloadForm || status?.reloadRunning || !status?.connected}
          >
            {actioning ? '...' : '▶ Iniciar recarga secuencial'}
          </button>
          <button
            style={c.btn('#dc2626')} onClick={stopReload}
            disabled={actioning || !status?.reloadRunning}
          >
            ■ Detener
          </button>
        </div>

        {run && (
          <div>
            <div style={c.grid}>
              <div style={c.metric}>
                <div style={c.metricLabel}>Estado</div>
                <div style={{ ...c.metricValue, fontSize: 14 }}>
                  {run.running
                    ? <span style={c.badge(false)}>{run.currentType ? `LEYENDO ${run.currentType}` : 'INICIANDO'}</span>
                    : <span style={c.badge(!run.error)}>{run.error ? 'CON ERROR' : run.stopRequested ? 'DETENIDA' : 'TERMINADA'}</span>}
                </div>
              </div>
              <div style={c.metric}>
                <div style={c.metricLabel}>Símbolos</div>
                <div style={c.metricValue}>{run.symbolsProcessed} / {run.symbolsTotal}</div>
              </div>
              <div style={c.metric}>
                <div style={c.metricLabel}>Cierres actualizados</div>
                <div style={c.metricValue}>{run.symbolsUpdated}</div>
              </div>
              <div style={c.metric}>
                <div style={c.metricLabel}>Lotes</div>
                <div style={c.metricValue}>{run.batchesDone} / {run.batchesTotal}</div>
              </div>
              <div style={c.metric}>
                <div style={c.metricLabel}>Duración</div>
                <div style={c.metricValue}>{fmtDur(Math.round((run.elapsedMs || 0) / 1000))}</div>
              </div>
              <div style={c.metric}>
                <div style={c.metricLabel}>Falta (est.)</div>
                <div style={c.metricValue}>{run.running ? fmtDur(run.etaSeconds || 0) : '-'}</div>
              </div>
            </div>
            <div style={c.progress}><div style={c.progressBar(pct)} /></div>
            {run.error && <div style={{ ...c.mono, marginTop: 10, color: '#fca5a5' }}>{run.error}</div>}
          </div>
        )}
      </div>

      <div style={c.card}>
        <div style={{ ...c.h1, fontSize: 15 }}>Recargar un símbolo</div>
        <div style={{ ...c.sub, marginBottom: 14 }}>
          Una sola consulta acotada: actualiza la caché y notifica al instante a los actores suscritos
          a ese papel.
        </div>
        <div style={c.row}>
          <input
            style={{ ...c.input, width: 200 }} placeholder="SQM-B" value={symbol}
            onChange={e => setSymbol(e.target.value.toUpperCase())}
            onKeyDown={e => { if (e.key === 'Enter') refreshSymbol() }}
          />
          <button
            style={c.btn('#0f766e')} onClick={refreshSymbol}
            disabled={actioning || !symbol.trim() || !status?.connected}
          >
            {actioning ? '...' : '↻ Recargar símbolo'}
          </button>
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
          se muestra: se enmascara). La recarga clasifica los símbolos con la SecurityList que mandó el
          sellside — <b>si esa lista no llegó, no hay símbolos que recargar</b> (close_prices no trae el
          tipo de instrumento). CS/CFI/ETF son valores de SecurityType, no de SettlType.
        </div>
      </div>
    </div>
  )
}

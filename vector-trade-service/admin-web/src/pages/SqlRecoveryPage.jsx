import { useEffect, useMemo, useState } from 'react'
import api from '../api.js'

const s = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 18 },
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden', marginBottom: 16 },
  cardTitle: { padding: '10px 12px', borderBottom: '1px solid #232840', fontSize: 14, fontWeight: 700 },
  cardBody: { padding: 14 },
  row: { display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center', marginBottom: 12 },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, minWidth: 120 },
  textarea: { width: '100%', minHeight: 110, padding: '10px 12px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, resize: 'vertical' },
  btn: (color = '#2563eb') => ({ padding: '8px 14px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  msg: ok => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 12, marginBottom: 12 },
  stat: { background: '#0f1117', border: '1px solid #232840', borderRadius: 8, padding: '12px 14px' },
  label: { fontSize: 11, color: '#94a3b8', marginBottom: 6 },
  value: { fontSize: 16, fontWeight: 700 },
  small: { fontSize: 11, color: '#94a3b8' },
  badge: status => ({
    display: 'inline-block',
    padding: '3px 8px',
    borderRadius: 999,
    fontSize: 11,
    fontWeight: 700,
    background: status === 'ok' ? '#052e16' : status === 'error' ? '#450a0a' : '#1e293b',
    color: status === 'ok' ? '#4ade80' : status === 'error' ? '#fca5a5' : '#cbd5e1',
  }),
  tableWrap: { overflowX: 'auto', border: '1px solid #232840', borderRadius: 8 },
  th: { padding: '10px 12px', fontSize: 12, color: '#94a3b8', textAlign: 'left', borderBottom: '1px solid #232840' },
  td: { padding: '9px 12px', fontSize: 13, borderBottom: '1px solid #1a1d27', verticalAlign: 'top' },
}

const defaultForm = { mode: 'actors', delayMs: '400', askTimeoutSec: '45', accounts: '' }

export default function SqlRecoveryPage() {
  const [form, setForm] = useState(defaultForm)
  const [status, setStatus] = useState(null)
  const [loading, setLoading] = useState(false)
  const [actioning, setActioning] = useState(false)
  const [msg, setMsg] = useState(null)

  const flash = (ok, text) => {
    setMsg({ ok, text })
    setTimeout(() => setMsg(null), 7000)
  }

  const loadStatus = async () => {
    setLoading(true)
    try {
      const res = await api.get('/sql-recovery')
      setStatus(res.data)
    } catch (err) {
      flash(false, err.response?.data?.error || err.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadStatus()
  }, [])

  useEffect(() => {
    if (!status?.running) return undefined
    const id = setInterval(loadStatus, 2000)
    return () => clearInterval(id)
  }, [status?.running])

  const activeRun = status?.activeRun
  const lastRun = !status?.running ? status?.lastRun : null
  const visibleRun = activeRun || lastRun

  const requestedCount = useMemo(() => {
    if (form.mode === 'actors') return status?.activeActors || 0
    return form.accounts
      .split(/[\s,;]+/)
      .map(v => v.trim())
      .filter(Boolean).length
  }, [form, status?.activeActors])

  const start = async () => {
    setActioning(true)
    try {
      const payload = {
        mode: form.mode,
        delayMs: Number(form.delayMs || 0),
        askTimeoutSec: Number(form.askTimeoutSec || 45),
      }
      if (form.mode === 'accounts') {
        payload.accounts = form.accounts
      }
      const res = await api.post('/sql-recovery/start', payload)
      setStatus(res.data.status)
      flash(true, res.data.message || 'Recarga SQL iniciada')
    } catch (err) {
      flash(false, err.response?.data?.error || err.message)
      if (err.response?.data?.status) {
        setStatus(err.response.data.status)
      }
    } finally {
      setActioning(false)
    }
  }

  const stop = async () => {
    setActioning(true)
    try {
      const res = await api.post('/sql-recovery/stop', {})
      setStatus(res.data.status)
      flash(true, res.data.message || 'Stop solicitado')
    } catch (err) {
      flash(false, err.response?.data?.error || err.message)
    } finally {
      setActioning(false)
    }
  }

  return (
    <div>
      <div style={s.h1}>🗄️ SQL Recovery</div>
      <div style={s.sub}>
        Reinyecta secuencialmente la base SQL de una cuenta usando el mismo cálculo de arranque
        (`saldo`, `custodias`, `simultáneas`, `préstamos`). Procesa una cuenta a la vez, con pausa entre cuentas,
        para no pegarle en ráfaga al core durante mercado.
      </div>
      {msg && <div style={s.msg(msg.ok)}>{msg.text}</div>}

      <section style={s.card}>
        <div style={s.cardTitle}>Lanzar recarga</div>
        <div style={s.cardBody}>
          <div style={s.row}>
            <label style={s.small}>
              <input
                type="radio"
                checked={form.mode === 'actors'}
                onChange={() => setForm(current => ({ ...current, mode: 'actors' }))}
                style={{ marginRight: 6 }}
              />
              Actores vivos
            </label>
            <label style={s.small}>
              <input
                type="radio"
                checked={form.mode === 'accounts'}
                onChange={() => setForm(current => ({ ...current, mode: 'accounts' }))}
                style={{ marginRight: 6 }}
              />
              Cuentas pegadas
            </label>
          </div>

          {form.mode === 'accounts' && (
            <div style={{ marginBottom: 12 }}>
              <textarea
                style={s.textarea}
                placeholder={'Una cuenta por línea o separadas por coma\nEj:\n12345678/0\n87654321/9'}
                value={form.accounts}
                onChange={(e) => setForm(current => ({ ...current, accounts: e.target.value }))}
              />
            </div>
          )}

          <div style={s.row}>
            <div>
              <div style={s.small}>Pausa entre cuentas (ms)</div>
              <input
                style={s.input}
                type="number"
                min="0"
                step="50"
                value={form.delayMs}
                onChange={(e) => setForm(current => ({ ...current, delayMs: e.target.value }))}
              />
            </div>
            <div>
              <div style={s.small}>Timeout por cuenta (s)</div>
              <input
                style={s.input}
                type="number"
                min="5"
                step="5"
                value={form.askTimeoutSec}
                onChange={(e) => setForm(current => ({ ...current, askTimeoutSec: e.target.value }))}
              />
            </div>
            <div style={{ alignSelf: 'end' }}>
              <button
                style={s.btn('#7c3aed')}
                disabled={actioning || status?.running || (form.mode === 'accounts' && !form.accounts.trim())}
                onClick={start}
              >
                {actioning ? '...' : 'Iniciar recarga secuencial'}
              </button>
            </div>
            <div style={{ alignSelf: 'end' }}>
              <button
                style={s.btn('#b91c1c')}
                disabled={actioning || !status?.running}
                onClick={stop}
              >
                {actioning ? '...' : 'Detener'}
              </button>
            </div>
            <div style={{ alignSelf: 'end' }}>
              <button style={s.btn('#374151')} disabled={loading} onClick={loadStatus}>
                {loading ? '...' : 'Actualizar'}
              </button>
            </div>
          </div>

          <div style={s.small}>
            Alcance estimado: <b>{requestedCount}</b> cuenta(s). Modo recomendado en mercado: actores vivos + pausa de 300 a 700 ms.
          </div>
        </div>
      </section>

      <section style={s.card}>
        <div style={s.cardTitle}>Estado</div>
        <div style={s.cardBody}>
          <div style={s.grid}>
            <div style={s.stat}>
              <div style={s.label}>Actores vivos</div>
              <div style={s.value}>{status?.activeActors ?? 0}</div>
            </div>
            <div style={s.stat}>
              <div style={s.label}>SQL requerido</div>
              <div style={s.value}>{status?.requiresSql ? 'Sí' : 'No'}</div>
            </div>
            <div style={s.stat}>
              <div style={s.label}>Run activo</div>
              <div style={s.value}>{status?.running ? `#${activeRun?.runId ?? '...'}` : 'No'}</div>
            </div>
            <div style={s.stat}>
              <div style={s.label}>Cuenta actual</div>
              <div style={s.value}>{visibleRun?.currentAccount || '—'}</div>
            </div>
            <div style={s.stat}>
              <div style={s.label}>Completadas</div>
              <div style={s.value}>{visibleRun ? `${visibleRun.completed}/${visibleRun.total}` : '0/0'}</div>
            </div>
            <div style={s.stat}>
              <div style={s.label}>Resultado</div>
              <div style={s.value}>{visibleRun?.lastMessage || '—'}</div>
            </div>
          </div>

          {visibleRun && (
            <div style={s.row}>
              <span style={s.badge('ok')}>ok {visibleRun.succeeded}</span>
              <span style={s.badge('error')}>failed {visibleRun.failed}</span>
              <span style={s.badge('skipped')}>skipped {visibleRun.skipped}</span>
              <span style={s.small}>delay={visibleRun.delayMs}ms</span>
              <span style={s.small}>timeout={visibleRun.askTimeoutSec}s</span>
              <span style={s.small}>modo={visibleRun.mode}</span>
            </div>
          )}
        </div>
      </section>

      <section style={s.card}>
        <div style={s.cardTitle}>Últimos resultados</div>
        <div style={s.cardBody}>
          <div style={s.tableWrap}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr>
                  <th style={s.th}>Cuenta</th>
                  <th style={s.th}>Estado</th>
                  <th style={s.th}>Duración</th>
                  <th style={s.th}>Mensaje</th>
                </tr>
              </thead>
              <tbody>
                {(visibleRun?.results || []).slice().reverse().map((row, idx) => (
                  <tr key={`${row.account}-${idx}`}>
                    <td style={s.td}><b>{row.account}</b></td>
                    <td style={s.td}><span style={s.badge(row.status)}>{row.status}</span></td>
                    <td style={s.td}>{row.durationMs} ms</td>
                    <td style={s.td}>{row.message || '—'}</td>
                  </tr>
                ))}
                {(!visibleRun?.results || visibleRun.results.length === 0) && (
                  <tr>
                    <td colSpan={4} style={{ ...s.td, textAlign: 'center', color: '#6b7280' }}>
                      Sin ejecución registrada todavía.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </section>
    </div>
  )
}

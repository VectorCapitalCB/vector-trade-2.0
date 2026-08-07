import { useState } from 'react'
import api from '../api.js'

const s = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, minWidth: 180 },
  btn: (color = '#2563eb') => ({ padding: '8px 16px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 10, padding: 18, marginBottom: 18 },
  msg: (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 8, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#86efac' : '#fca5a5' }),
  small: { fontSize: 12, color: '#94a3b8' },
  label: { fontSize: 12, color: '#94a3b8', marginBottom: 6 },
  value: { fontSize: 14, fontWeight: 600 },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12, marginBottom: 16 },
  stat: { background: '#0f1117', border: '1px solid #232840', borderRadius: 8, padding: '12px 14px' },
  badge: (ok) => ({ display: 'inline-block', padding: '2px 8px', borderRadius: 999, fontSize: 11, fontWeight: 700, background: ok ? '#052e16' : '#1e293b', color: ok ? '#4ade80' : '#cbd5e1' }),
  tableWrap: { overflowX: 'auto', border: '1px solid #232840', borderRadius: 8 },
  th: { padding: '10px 12px', fontSize: 12, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td: { padding: '9px 12px', fontSize: 13, borderBottom: '1px solid #1a1d27' },
}

const fmt = (value) => {
  if (value == null || value === '') return '—'
  return String(value)
}

export default function KeycloakRiskPage() {
  const [account, setAccount] = useState('')
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [activating, setActivating] = useState(false)
  const [msg, setMsg] = useState(null)
  const [data, setData] = useState(null)
  const [form, setForm] = useState({ margin: '', leverage: '' })
  const [selectedUser, setSelectedUser] = useState('')

  const flash = (ok, text) => {
    setMsg({ ok, text })
    setTimeout(() => setMsg(null), 6000)
  }

  const load = async () => {
    const value = account.trim()
    if (!value) return
    setLoading(true)
    try {
      const res = await api.get('/accounts/keycloak-config', { params: { account: value } })
      setData(res.data)
      setForm({
        margin: res.data?.keycloakMargin != null ? String(res.data.keycloakMargin) : '',
        leverage: res.data?.keycloakLeverage != null ? String(res.data.keycloakLeverage) : '',
      })
      setSelectedUser(res.data?.editableOwner || res.data?.matches?.[0]?.username || '')
      flash(true, `Configuración cargada para ${value}`)
    } catch (err) {
      setData(null)
      setSelectedUser('')
      flash(false, err.response?.data?.error || err.message)
    } finally {
      setLoading(false)
    }
  }

  const apply = async () => {
    const value = account.trim()
    if (!value || !selectedUser) return
    setSaving(true)
    try {
      const payload = { account: value, username: selectedUser }
      if (String(form.margin).trim() !== '') payload.margin = Number(form.margin)
      if (String(form.leverage).trim() !== '') payload.leverage = Number(form.leverage)
      if (payload.margin == null && payload.leverage == null) {
        flash(false, 'Debes informar margin o palanca')
        return
      }
      const res = await api.post('/accounts/keycloak-config/apply', payload)
      setData(res.data)
      setForm({
        margin: res.data?.keycloakMargin != null ? String(res.data.keycloakMargin) : '',
        leverage: res.data?.keycloakLeverage != null ? String(res.data.keycloakLeverage) : '',
      })
      setSelectedUser(res.data?.editableOwner || selectedUser)
      flash(true, res.data?.message || `Configuración aplicada para ${value}`)
    } catch (err) {
      const details = err.response?.data?.details
      if (details) {
        setData(details)
        setSelectedUser(details?.editableOwner || details?.matches?.[0]?.username || selectedUser)
      }
      flash(false, err.response?.data?.error || err.message)
    } finally {
      setSaving(false)
    }
  }

  const activate = async () => {
    const value = account.trim()
    if (!value || !selectedUser) return
    setActivating(true)
    try {
      const res = await api.post('/accounts/keycloak-config/activate', { account: value, username: selectedUser })
      setData(res.data)
      setForm({
        margin: res.data?.keycloakMargin != null ? String(res.data.keycloakMargin) : '',
        leverage: res.data?.keycloakLeverage != null ? String(res.data.keycloakLeverage) : '',
      })
      setSelectedUser(res.data?.editableOwner || selectedUser)
      flash(true, res.data?.message || `Cuenta ${value} dada de alta`)
    } catch (err) {
      const details = err.response?.data?.details
      if (details) {
        setData(details)
        setSelectedUser(details?.editableOwner || details?.matches?.[0]?.username || selectedUser)
      }
      flash(false, err.response?.data?.error || err.message)
    } finally {
      setActivating(false)
    }
  }

  return (
    <div>
      <div style={s.h1}>🗝️ Keycloak</div>
      <div style={s.sub}>Lee una cuenta exacta desde Keycloak, actualiza `palanca/margin` y dale alta inmediata en el core sin depender del barrido automático.</div>
      {msg && <div style={s.msg(msg.ok)}>{msg.text}</div>}

      <div style={s.card}>
        <div style={s.row}>
          <input
            style={{ ...s.input, flex: 1 }}
            placeholder="Cuenta exacta, ej: 10666945/0"
            value={account}
            onChange={(e) => setAccount(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && load()}
          />
          <button style={s.btn('#374151')} onClick={load} disabled={loading || !account.trim()}>
            {loading ? '...' : 'Leer desde Keycloak'}
          </button>
        </div>

        <div style={s.grid}>
          <div style={s.stat}>
            <div style={s.label}>Owner editable</div>
            <div style={s.value}>{selectedUser || data?.editableOwner || '—'}</div>
          </div>
          <div style={s.stat}>
            <div style={s.label}>Actor en core</div>
            <div style={s.value}><span style={s.badge(!!data?.hasActor)}>{data?.hasActor ? 'Sí' : 'No'}</span></div>
          </div>
          <div style={s.stat}>
            <div style={s.label}>Margin actual en Keycloak</div>
            <div style={s.value}>{fmt(data?.keycloakMargin)}</div>
          </div>
          <div style={s.stat}>
            <div style={s.label}>Palanca actual en Keycloak</div>
            <div style={s.value}>{fmt(data?.keycloakLeverage)}</div>
          </div>
          <div style={s.stat}>
            <div style={s.label}>Margin cacheado en core</div>
            <div style={s.value}>{fmt(data?.cachedMargin)}</div>
          </div>
          <div style={s.stat}>
            <div style={s.label}>Palanca cacheada en core</div>
            <div style={s.value}>{fmt(data?.cachedLeverage)}</div>
          </div>
        </div>

        {data?.issue && (
          <div style={{ ...s.msg(false), marginBottom: 16 }}>
            {data.issue}
          </div>
        )}

        <div style={{ ...s.row, alignItems: 'flex-end' }}>
          <div>
            <div style={s.label}>Usuario a editar</div>
            <select
              style={s.input}
              value={selectedUser}
              onChange={(e) => setSelectedUser(e.target.value)}
            >
              <option value="">Selecciona usuario</option>
              {(data?.matches || []).map((row) => (
                <option key={`${row.username}-${row.userId || 'na'}`} value={row.username}>
                  {row.username}
                </option>
              ))}
            </select>
          </div>
          <div>
            <div style={s.label}>Nuevo margin</div>
            <input
              style={s.input}
              type="number"
              step="any"
              placeholder="Vacío = no tocar"
              value={form.margin}
              onChange={(e) => setForm((current) => ({ ...current, margin: e.target.value }))}
            />
          </div>
          <div>
            <div style={s.label}>Nueva palanca</div>
            <input
              style={s.input}
              type="number"
              step="any"
              placeholder="Vacío = default x3 / no tocar"
              value={form.leverage}
              onChange={(e) => setForm((current) => ({ ...current, leverage: e.target.value }))}
            />
          </div>
          <button
            style={s.btn('#7c3aed')}
            onClick={apply}
            disabled={saving || !data?.canEdit || !account.trim() || !selectedUser || (String(form.margin).trim() === '' && String(form.leverage).trim() === '')}
          >
            {saving ? 'Aplicando...' : 'Guardar en Keycloak y refrescar core'}
          </button>
          <button
            style={s.btn('#0f766e')}
            onClick={activate}
            disabled={activating || !account.trim() || !selectedUser}
          >
            {activating ? 'Dando alta...' : 'Dar alta cuenta'}
          </button>
        </div>
        <div style={s.small}>
          `Guardar` cambia sólo los campos informados del usuario seleccionado y reprocesa esa cuenta. `Dar alta cuenta` usa lo que ya existe en Keycloak; si no hay palanca, el core ocupa su default.
        </div>
      </div>

      <div style={s.card}>
        <div style={{ fontSize: 14, fontWeight: 700, marginBottom: 12 }}>Coincidencias encontradas</div>
        <div style={s.tableWrap}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th style={s.th}>Usar</th>
                <th style={s.th}>Usuario</th>
                <th style={s.th}>Cuenta declarada</th>
                <th style={s.th}>Margin</th>
                <th style={s.th}>Palanca</th>
                <th style={s.th}>Attr margin</th>
                <th style={s.th}>Attr palanca</th>
              </tr>
            </thead>
            <tbody>
              {(data?.matches || []).map((row) => (
                <tr
                  key={`${row.username}-${row.userId || 'na'}`}
                  style={{ background: selectedUser === row.username ? '#111827' : 'transparent' }}
                >
                  <td style={s.td}>
                    <input
                      type="radio"
                      name="keycloak-user"
                      checked={selectedUser === row.username}
                      onChange={() => setSelectedUser(row.username)}
                    />
                  </td>
                  <td style={s.td}><b>{row.username}</b></td>
                  <td style={s.td}>{row.accountDeclared ? 'Sí' : 'No'}</td>
                  <td style={s.td}>{fmt(row.margin)}</td>
                  <td style={s.td}>{fmt(row.leverage)}</td>
                  <td style={s.td}>{fmt(row.marginAttributeKey)}</td>
                  <td style={s.td}>{fmt(row.leverageAttributeKey)}</td>
                </tr>
              ))}
              {(!data?.matches || data.matches.length === 0) && (
                <tr>
                  <td colSpan={7} style={{ ...s.td, textAlign: 'center', color: '#6b7280' }}>
                    Sin coincidencias cargadas todavía.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

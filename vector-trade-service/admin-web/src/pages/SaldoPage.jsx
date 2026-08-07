import { useMemo, useState } from 'react'
import api from '../api.js'

const ZERO_FIELDS = {
  caja: 0,
  cuentaTransitorias: 0,
  garantiaEfectivo: 0,
  garantiasConstituidas: 0,
  garantiasExigidas: 0,
  garantiasReservadas: 0,
  limiteFinanciero: 0,
  garantiasDisponible: 0,
  ordenesActivasCompras: 0,
  ordenesActivasVentas: 0,
  ordenesCalzadasCompras: 0,
  ordenesCalzadasVentas: 0,
  ordenesCestaCompras: 0,
  ordenesCestaVentas: 0,
  rendimiento: 0,
  total: 0,
  cartera: 0,
  cupo: 0,
  saldoDisponible: 0,
  activos: 0,
  activosPct: 0,
  liquidez: 0,
  liquidezPct: 0,
  rentaFija: 0,
  rentaFijaPct: 0,
  rentaVariable: 0,
  rentaVariablePct: 0,
  accionesNacionales: 0,
  accionesNacionalesPct: 0,
  simultaneasPatrimonio: 0,
  simultaneasPct: 0,
  prestamosPatrimonio: 0,
  prestamosPct: 0,
  accionesExtranjeras: 0,
  accionesExtranjerasPct: 0,
  fondosMutuos: 0,
  fondosMutuosPct: 0,
  rvNacional: 0,
  rvNacionalPct: 0,
  rvExtranjeros: 0,
  rvExtranjerosPct: 0,
  fondoInversionRentaVariable: 0,
  fondoInversionRentaVariablePct: 0,
  activosInmobiliarios: 0,
  activosInmobiliariosPct: 0,
  eftsRentaVariable: 0,
  eftsRentaVariablePct: 0,
  inversionesAlternativas: 0,
  inversionesAlternativasPct: 0,
  derivados: 0,
  derivadosPct: 0,
}

const MANUAL_FIELDS = [
  'caja', 'cuentaTransitorias', 'garantiaEfectivo',
  'garantiasConstituidas', 'garantiasExigidas', 'garantiasReservadas', 'limiteFinanciero', 'garantiasDisponible',
  'ordenesActivasCompras', 'ordenesActivasVentas', 'ordenesCalzadasCompras', 'ordenesCalzadasVentas',
  'ordenesCestaCompras', 'ordenesCestaVentas', 'rendimiento', 'total',
]

const s = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' },
  topGrid: { display: 'grid', gridTemplateColumns: '1.15fr 1fr', gap: 14, alignItems: 'start' },
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  cardTitle: { padding: '10px 12px', borderBottom: '1px solid #232840', fontSize: 14, fontWeight: 700 },
  cardBody: { padding: 14 },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, minWidth: 180 },
  select: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, minWidth: 110 },
  btn: (color = '#3b82f6') => ({ padding: '8px 14px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  msg: ok => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  badge: ok => ({ display: 'inline-block', padding: '3px 8px', borderRadius: 999, fontSize: 11, fontWeight: 700, background: ok ? '#052e16' : '#1e293b', color: ok ? '#4ade80' : '#cbd5e1' }),
  note: { fontSize: 11, color: '#94a3b8' },
  twoCol: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 18, alignItems: 'start' },
  blockTitle: { textAlign: 'center', fontSize: 14, fontWeight: 700, margin: '10px 0 12px' },
  fieldRow: { display: 'grid', gridTemplateColumns: '130px 1fr', gap: 10, alignItems: 'center', marginBottom: 10 },
  fieldLabel: { fontSize: 12, color: '#e2e8f0' },
  fieldInput: { width: '100%', padding: '7px 10px', background: '#0f1117', border: '1px solid #3a4254', borderRadius: 6, color: '#e2e8f0', fontSize: 13 },
  disabledInput: { width: '100%', padding: '7px 10px', background: '#11161f', border: '1px solid #30384a', borderRadius: 6, color: '#cbd5e1', fontSize: 13 },
  patrimonyTable: { width: '100%', borderCollapse: 'collapse' },
  th: { padding: '8px 10px', fontSize: 12, color: '#e2e8f0', textAlign: 'left', borderBottom: '1px solid #2d3748', background: '#11161f' },
  td: { padding: '7px 10px', fontSize: 12, borderBottom: '1px solid #1f2430', verticalAlign: 'middle' },
  amountInput: { width: '100%', padding: '6px 8px', background: '#0f1117', border: '1px solid #3a4254', borderRadius: 6, color: '#e2e8f0', fontSize: 12 },
  treeLabel: level => ({ paddingLeft: `${level * 18}px`, fontWeight: level === 0 ? 700 : 400, color: level === 0 ? '#f8fafc' : '#dbe2ee' }),
}

const LEFT_SECTIONS = [
  {
    title: 'SALDO',
    rows: [
      { key: 'account', label: 'Cuenta', readOnly: true, type: 'text' },
      { key: 'cartera', label: 'Cartera', readOnly: true },
      { key: 'cupo', label: 'Cupo', readOnly: true },
      { key: 'saldoDisponible', label: 'Disponible', readOnly: true, strong: true },
    ],
  },
  {
    title: 'GARANTÍAS',
    rows: [
      { key: 'garantiasConstituidas', label: 'Garantías constituidas' },
      { key: 'garantiasExigidas', label: 'Garantías exigidas' },
      { key: 'garantiasReservadas', label: 'Garantías reservadas' },
      { key: 'limiteFinanciero', label: 'Límite financiero' },
      { key: 'garantiasDisponible', label: 'Disponible' },
    ],
  },
]

const RIGHT_SECTIONS = [
  {
    title: 'ÓRDENES ACTIVAS',
    rows: [
      { key: 'ordenesActivasCompras', label: 'Compras' },
      { key: 'ordenesActivasVentas', label: 'Ventas' },
    ],
  },
  {
    title: 'ÓRDENES CALZADAS',
    rows: [
      { key: 'ordenesCalzadasCompras', label: 'Compras' },
      { key: 'ordenesCalzadasVentas', label: 'Ventas' },
    ],
  },
  {
    title: 'ÓRDENES CESTA O ALGORITMOS',
    rows: [
      { key: 'ordenesCestaCompras', label: 'Compras' },
      { key: 'ordenesCestaVentas', label: 'Ventas' },
      { key: 'rendimiento', label: 'Rendimiento' },
      { key: 'total', label: 'Total', strong: true },
    ],
  },
]

function formatMoney(value) {
  return Number(value || 0).toLocaleString('es-CL', { maximumFractionDigits: 2 })
}

function formatPercent(value) {
  return `${Number(value || 0).toLocaleString('es-CL', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`
}

export default function SaldoPage() {
  const [account, setAccount] = useState('')
  const [currency, setCurrency] = useState('CLP')
  const [data, setData] = useState(ZERO_FIELDS)
  const [msg, setMsg] = useState(null)
  const [loading, setLoading] = useState(false)
  const [applying, setApplying] = useState(false)
  const [source, setSource] = useState('')
  const [hasManualOverride, setHasManualOverride] = useState(false)
  const [dirty, setDirty] = useState(false)

  const flash = (ok, text) => {
    setMsg({ ok, text })
    setTimeout(() => setMsg(null), 6000)
  }

  const patrimonyRows = useMemo(() => ([
    { label: 'Activos', pct: data.activosPct, value: data.activos, level: 0, editable: false },
    { label: 'LIQUIDEZ', pct: data.liquidezPct, value: data.liquidez, level: 1, editable: false },
    { label: 'Cajas', pct: data.activos ? (Number(data.caja || 0) / Number(data.activos || 1)) * 100 : 0, value: data.caja, level: 2, editable: true, key: 'caja' },
    { label: 'Cuentas transitorias por cobrar/pagar', pct: data.activos ? (Number(data.cuentaTransitorias || 0) / Number(data.activos || 1)) * 100 : 0, value: data.cuentaTransitorias, level: 2, editable: true, key: 'cuentaTransitorias' },
    { label: 'Garantías en efectivo', pct: data.activos ? (Number(data.garantiaEfectivo || 0) / Number(data.activos || 1)) * 100 : 0, value: data.garantiaEfectivo, level: 2, editable: true, key: 'garantiaEfectivo' },
    { label: 'RENTA FIJA', pct: data.rentaFijaPct, value: data.rentaFija, level: 1, editable: false },
    { label: 'RENTA VARIABLE', pct: data.rentaVariablePct, value: data.rentaVariable, level: 1, editable: false },
    { label: 'Acciones nacionales', pct: data.accionesNacionalesPct, value: data.accionesNacionales, level: 2, editable: false },
    { label: 'Simultáneas', pct: data.simultaneasPct, value: data.simultaneasPatrimonio, level: 2, editable: false, note: 'Se mueve en Simultáneas Manuales' },
    { label: 'Préstamos', pct: data.prestamosPct, value: data.prestamosPatrimonio, level: 2, editable: false, note: 'Se mueve en Préstamos Manuales' },
    { label: 'Acciones Extranjeras', pct: data.accionesExtranjerasPct, value: data.accionesExtranjeras, level: 2, editable: false },
    { label: 'Fondos Mutuos RV Extranjeros', pct: data.fondosMutuosPct, value: data.fondosMutuos, level: 2, editable: false },
    { label: 'Fondos Mutuos RV Nacional', pct: data.rvNacionalPct, value: data.rvNacional, level: 2, editable: false },
    { label: 'Activos Inmobiliarios', pct: data.activosInmobiliariosPct, value: data.activosInmobiliarios, level: 2, editable: false },
    { label: 'Fondos Inversión Renta Variable', pct: data.fondoInversionRentaVariablePct, value: data.fondoInversionRentaVariable, level: 2, editable: false },
    { label: 'ETFs Renta Variable', pct: data.eftsRentaVariablePct, value: data.eftsRentaVariable, level: 2, editable: false },
    { label: 'Derivados', pct: data.derivadosPct, value: data.derivados, level: 2, editable: false },
  ]), [data])

  const loadData = async nextSource => {
    const acct = account.trim()
    if (!acct) {
      flash(false, '❌ Debes ingresar una cuenta antes de consultar')
      return
    }

    setLoading(true)
    try {
      const response = await api.get('/saldo', { params: { account: acct, source: nextSource } })
      setData({ ...ZERO_FIELDS, ...response.data })
      setSource(nextSource)
      setHasManualOverride(Boolean(response.data.hasManualOverride))
      setDirty(false)
      flash(true, `✅ Saldo cargado desde ${nextSource.toUpperCase()}`)
    } catch (error) {
      flash(false, '❌ ' + (error.response?.data?.error || error.message))
    } finally {
      setLoading(false)
    }
  }

  const applyManual = async () => {
    const acct = account.trim()
    if (!acct) {
      flash(false, '❌ Debes ingresar una cuenta')
      return
    }

    const payload = { account: acct }
    for (const field of MANUAL_FIELDS) {
      payload[field] = Number(data[field] || 0)
    }

    setApplying(true)
    try {
      const response = await api.post('/saldo/apply', payload)
      setData({ ...ZERO_FIELDS, ...response.data })
      setSource('effective')
      setHasManualOverride(Boolean(response.data.hasManualOverride))
      setDirty(false)
      flash(true, '✅ Saldo manual aplicado y reflejado en el front.')
    } catch (error) {
      flash(false, '❌ ' + (error.response?.data?.error || error.message))
    } finally {
      setApplying(false)
    }
  }

  const clearManual = async () => {
    const acct = account.trim()
    if (!acct) {
      flash(false, '❌ Debes ingresar una cuenta')
      return
    }

    setApplying(true)
    try {
      const response = await api.post('/saldo/clear', { account: acct })
      setData({ ...ZERO_FIELDS, ...response.data })
      setSource('effective')
      setHasManualOverride(Boolean(response.data.hasManualOverride))
      setDirty(false)
      flash(true, `✅ ${response.data.message}`)
    } catch (error) {
      flash(false, '❌ ' + (error.response?.data?.error || error.message))
    } finally {
      setApplying(false)
    }
  }

  const updateField = (key, value) => {
    setData(prev => ({ ...prev, [key]: value }))
    setDirty(true)
  }

  const renderField = row => {
    if (row.readOnly) {
      return (
        <input
          style={s.disabledInput}
          value={row.key === 'account' ? account : formatMoney(data[row.key])}
          disabled
          readOnly
        />
      )
    }

    return (
      <input
        type="number"
        step="any"
        style={s.fieldInput}
        value={data[row.key] ?? 0}
        onChange={e => updateField(row.key, e.target.value)}
      />
    )
  }

  return (
    <div>
      <div style={s.h1}>💰 Saldo Manual</div>
      <div style={s.sub}>Lo dejamos parecido al saldo del front viejo. En esta pantalla podemos mover manualmente la parte de `Saldo de cartera` que existe en el código y ver al lado el `Patrimonio` recalculado. `Cartera`, `Cupo` y `Disponible` siguen siendo derivados.</div>
      {msg && <div style={s.msg(msg.ok)}>{msg.text}</div>}

      <div style={s.row}>
        <input
          style={s.input}
          placeholder="Cuenta exacta (ej: 15070264/9)"
          value={account}
          onChange={e => setAccount(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && loadData('effective')}
        />
        <select style={s.select} value={currency} onChange={e => setCurrency(e.target.value)}>
          <option value="CLP">CLP</option>
        </select>
        <button style={s.btn('#2563eb')} disabled={loading} onClick={() => loadData('effective')}>
          {loading && source === 'effective' ? '...' : '📥 Cargar actual'}
        </button>
        <button style={s.btn('#374151')} disabled={loading} onClick={() => loadData('sql')}>
          {loading && source === 'sql' ? '...' : '🗄 Cargar SQL'}
        </button>
        <button style={s.btn('#4f46e5')} disabled={loading} onClick={() => loadData('manual')}>
          {loading && source === 'manual' ? '...' : '📝 Ver manual'}
        </button>
        <button style={s.btn('#7c3aed')} disabled={applying} onClick={applyManual}>
          {applying ? '...' : '💾 Aplicar al front'}
        </button>
        <button style={s.btn('#b91c1c')} disabled={applying} onClick={clearManual}>
          {applying ? '...' : '🧹 Limpiar manual'}
        </button>
      </div>

      <div style={{ ...s.row, marginTop: -4, marginBottom: 14 }}>
        <span style={s.badge(hasManualOverride)}>override manual {hasManualOverride ? 'activo' : 'inactivo'}</span>
        <span style={s.note}>Fuente visible: <b>{source || 'ninguna'}</b></span>
        {dirty && <span style={{ ...s.note, color: '#fbbf24' }}>hay cambios sin aplicar</span>}
      </div>

      <div style={s.topGrid}>
        <section style={s.card}>
          <div style={s.cardTitle}>Saldo de cartera</div>
          <div style={s.cardBody}>
            <div style={s.twoCol}>
              <div>
                {LEFT_SECTIONS.map(section => (
                  <div key={section.title}>
                    <div style={s.blockTitle}>{section.title}</div>
                    {section.rows.map(row => (
                      <div key={row.key} style={s.fieldRow}>
                        <div style={{ ...s.fieldLabel, fontWeight: row.strong ? 700 : 400 }}>{row.label}</div>
                        {renderField(row)}
                      </div>
                    ))}
                  </div>
                ))}
              </div>

              <div>
                {RIGHT_SECTIONS.map(section => (
                  <div key={section.title}>
                    <div style={s.blockTitle}>{section.title}</div>
                    {section.rows.map(row => (
                      <div key={row.key} style={s.fieldRow}>
                        <div style={{ ...s.fieldLabel, fontWeight: row.strong ? 700 : 400 }}>{row.label}</div>
                        <input
                          type="number"
                          step="any"
                          style={s.fieldInput}
                          value={data[row.key] ?? 0}
                          onChange={e => updateField(row.key, e.target.value)}
                        />
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section style={s.card}>
          <div style={s.cardTitle}>Patrimonio</div>
          <div style={s.cardBody}>
            <table style={s.patrimonyTable}>
              <thead>
                <tr>
                  <th style={s.th}>Descripción</th>
                  <th style={{ ...s.th, width: 120 }}>Porcentaje</th>
                  <th style={{ ...s.th, width: 180 }}>Monto</th>
                </tr>
              </thead>
              <tbody>
                {patrimonyRows.map(row => (
                  <tr key={row.label}>
                    <td style={s.td}>
                      <div style={s.treeLabel(row.level)}>{row.label}</div>
                      {row.note && <div style={{ ...s.note, paddingLeft: `${row.level * 18}px` }}>{row.note}</div>}
                    </td>
                    <td style={s.td}>{formatPercent(row.pct)}</td>
                    <td style={s.td}>
                      {row.editable ? (
                        <input
                          type="number"
                          step="any"
                          style={s.amountInput}
                          value={data[row.key] ?? 0}
                          onChange={e => updateField(row.key, e.target.value)}
                        />
                      ) : (
                        <span>{formatMoney(row.value)}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </div>
  )
}

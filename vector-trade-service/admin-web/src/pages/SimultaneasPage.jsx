import { useMemo, useState } from 'react'
import api from '../api.js'

const COLUMNS = [
  { key: 'identCliente', label: 'Id Clien.', width: 110 },
  { key: 'numCuenta', label: 'NC', width: 110, readOnly: true },
  { key: 'fechaOperacion', label: 'F. Ope', width: 120 },
  { key: 'fechaVcto', label: 'F. Ven', width: 120 },
  { key: 'nombreCliente', label: 'N Cli', width: 190 },
  { key: 'plazo', label: 'Plazo', width: 80 },
  { key: 'plazoRem', label: 'Plazo Rem', width: 90 },
  { key: 'nemotecnico', label: 'Nemo', width: 120 },
  { key: 'cantidad', label: 'Qty', width: 110 },
  { key: 'tasa', label: 'Tasa', width: 90 },
  { key: 'precioPH', label: 'PX PH', width: 95 },
  { key: 'precioPlazo', label: 'PX Plazo', width: 95 },
  { key: 'precioMercado', label: 'PX Mercado', width: 95 },
  { key: 'montoContado', label: 'Monto Contado', width: 120 },
  { key: 'costoDiario2', label: 'Costo Diario', width: 105 },
  { key: 'montoPresente', label: 'M Present', width: 105 },
  { key: 'montoPlazo', label: 'M Plazo', width: 105 },
  { key: 'cantidadOrig', label: 'Qty Ori', width: 100 },
]

const c = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  row: { display: 'flex', gap: 10, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, minWidth: 220 },
  btn: (color = '#3b82f6') => ({ padding: '8px 14px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }),
  btnSm: (color = '#3b82f6') => ({ padding: '5px 10px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 12, fontWeight: 600, cursor: 'pointer' }),
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th: { padding: '10px 10px', fontSize: 12, color: '#94a3b8', borderBottom: '1px solid #232840', textAlign: 'left', position: 'sticky', top: 0, background: '#141720', whiteSpace: 'nowrap' },
  td: { padding: '8px 10px', fontSize: 12, borderBottom: '1px solid #1a1d27', verticalAlign: 'top' },
  cellInput: { width: '100%', minWidth: 0, padding: '6px 8px', background: '#0f1117', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 12 },
  msg: (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  badge: (ok) => ({ display: 'inline-block', padding: '3px 8px', borderRadius: 999, fontSize: 11, fontWeight: 700, background: ok ? '#052e16' : '#1e293b', color: ok ? '#4ade80' : '#cbd5e1' }),
  note: { fontSize: 11, color: '#94a3b8' },
}

function emptyRow(account) {
  const today = new Date().toISOString().slice(0, 10)
  return {
    id: '',
    tipoSimul: 'Simultánea Manual',
    detalleSimultanea: '',
    identCliente: account.split('/')[0] || '',
    numCuenta: account,
    fechaOperacion: today,
    fechaVcto: today,
    nombreCliente: '',
    plazo: '0',
    plazoRem: '0',
    nemotecnico: '',
    cantidad: '0',
    tasa: '0',
    precioPH: '0',
    precioPlazo: '0',
    precioMercado: '0',
    montoContado: '0',
    costoDiario2: '0',
    montoPresente: '0',
    montoPlazo: '0',
    codInst: '',
    cantidadOrig: '0',
    corredorVenta: '',
    corredorCompra: '',
    folioFactPH: '',
    folioFactTP: '',
  }
}

export default function SimultaneasPage() {
  const [account, setAccount] = useState('')
  const [rows, setRows] = useState([])
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

  const loadRows = async (nextSource) => {
    const acct = account.trim()
    if (!acct) {
      flash(false, '❌ Debes ingresar una cuenta antes de consultar')
      return
    }

    setLoading(true)
    try {
      const response = await api.get('/simultaneas', { params: { account: acct, source: nextSource } })
      setRows(response.data.rows || [])
      setSource(nextSource)
      setHasManualOverride(Boolean(response.data.hasManualOverride))
      setDirty(false)
      flash(true, `✅ ${response.data.count || 0} simultánea(s) cargadas desde ${nextSource.toUpperCase()}`)
    } catch (error) {
      flash(false, '❌ ' + (error.response?.data?.error || error.message))
    } finally {
      setLoading(false)
    }
  }

  const applyRows = async () => {
    await applyRowsPayload(rows, 'Simultáneas manuales aplicadas')
  }

  const applyRowsPayload = async (payloadRows, successPrefix = 'Simultáneas manuales aplicadas') => {
    const acct = account.trim()
    if (!acct) {
      flash(false, '❌ Debes ingresar una cuenta')
      return
    }

    setApplying(true)
    try {
      const response = await api.post('/simultaneas/apply', { account: acct, rows: payloadRows })
      flash(true, `✅ ${successPrefix}. Se recalculó patrimonio/disponible.`)
      setHasManualOverride(true)
      setDirty(false)
      await loadRows('effective')
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
      const response = await api.post('/simultaneas/clear', { account: acct })
      flash(true, `✅ ${response.data.message}`)
      setHasManualOverride(false)
      setDirty(false)
      await loadRows('effective')
    } catch (error) {
      flash(false, '❌ ' + (error.response?.data?.error || error.message))
    } finally {
      setApplying(false)
    }
  }

  const addRow = () => {
    const acct = account.trim()
    if (!acct) {
      flash(false, '❌ Debes ingresar una cuenta antes de agregar filas')
      return
    }
    setRows(prev => [...prev, emptyRow(acct)])
    setDirty(true)
  }

  const removeRow = (index) => {
    const nextRows = rows.filter((_, current) => current !== index)
    setRows(nextRows)
    setDirty(false)
    applyRowsPayload(nextRows, 'Fila eliminada y aplicada al front')
  }

  const updateRow = (index, key, value) => {
    setRows(prev => prev.map((row, current) => current === index ? { ...row, [key]: value } : row))
    setDirty(true)
  }

  const totalMontoPresente = useMemo(() => rows.reduce((sum, row) => sum + Number(row.montoPresente || 0), 0), [rows])
  const totalCostoDiario = useMemo(() => rows.reduce((sum, row) => sum + Number(row.costoDiario2 || 0), 0), [rows])

  return (
    <div>
      <div style={c.h1}>🧮 Simultáneas Manuales</div>
      <div style={c.sub}>Consulta la foto SQL por cuenta, ajústala y aplícala al actor vivo. Al aplicar, la cuenta recalcula simultáneas, patrimonio y disponible.</div>
      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      <div style={c.row}>
        <input
          style={c.input}
          placeholder="Cuenta exacta (ej: 18024083/0)"
          value={account}
          onChange={e => setAccount(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && loadRows('effective')}
        />
        <button style={c.btn('#2563eb')} disabled={loading} onClick={() => loadRows('sql')}>
          {loading && source === 'sql' ? '...' : '📥 Consultar SQL'}
        </button>
        <button style={c.btn('#374151')} disabled={loading} onClick={() => loadRows('effective')}>
          {loading && source === 'effective' ? '...' : '👁 Ver vigentes'}
        </button>
        <button style={c.btn('#4f46e5')} disabled={loading} onClick={() => loadRows('manual')}>
          {loading && source === 'manual' ? '...' : '📝 Ver manual'}
        </button>
        <button style={c.btn('#0f766e')} onClick={addRow}>➕ Agregar fila</button>
        <button style={c.btn('#7c3aed')} disabled={applying} onClick={applyRows}>
          {applying ? '...' : '💾 Aplicar al front'}
        </button>
        <button style={c.btn('#b91c1c')} disabled={applying} onClick={clearManual}>
          {applying ? '...' : '🧹 Limpiar manual'}
        </button>
      </div>

      <div style={{ ...c.row, marginTop: -4, marginBottom: 14 }}>
        <span style={c.badge(hasManualOverride)}>override manual {hasManualOverride ? 'activo' : 'inactivo'}</span>
        <span style={c.note}>Fuente visible: <b>{source || 'ninguna'}</b></span>
        <span style={c.note}>Filas: <b>{rows.length}</b></span>
        <span style={c.note}>M presente total: <b>{totalMontoPresente.toLocaleString('es-CL')}</b></span>
        <span style={c.note}>Costo diario total: <b>{totalCostoDiario.toLocaleString('es-CL')}</b></span>
        {dirty && <span style={{ ...c.note, color: '#fbbf24' }}>hay cambios sin aplicar</span>}
      </div>

      <div style={c.card}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', minWidth: 2200, borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                {COLUMNS.map(col => (
                  <th key={col.key} style={{ ...c.th, minWidth: col.width }}>{col.label}</th>
                ))}
                <th style={{ ...c.th, minWidth: 110 }}>Acción</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={row.id || `row-${index}`}>
                  {COLUMNS.map(col => (
                    <td key={col.key} style={{ ...c.td, minWidth: col.width }}>
                      <input
                        style={c.cellInput}
                        value={row[col.key] ?? ''}
                        readOnly={Boolean(col.readOnly)}
                        onChange={e => updateRow(index, col.key, e.target.value)}
                      />
                    </td>
                  ))}
                  <td style={c.td}>
                    <button style={c.btnSm('#991b1b')} onClick={() => removeRow(index)}>Eliminar</button>
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td colSpan={COLUMNS.length + 1} style={{ ...c.td, textAlign: 'center', color: '#6b7280', padding: '24px 12px' }}>
                    Sin simultáneas cargadas aún. Usa <b>Consultar SQL</b> o <b>Agregar fila</b>.
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

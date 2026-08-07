import { useEffect, useMemo, useState } from 'react'
import api from '../api.js'

const EXCHANGES = ['BCS', 'FH_IBKR', 'ALPACA_MKD', 'DATATEC_XBCL', 'NUAM_MKD', 'NASDAQ', 'NYSE', 'AMEX', 'BVC', 'BVL']
const SETTL_TYPES = ['T2', 'CASH', 'NEXT_DAY', 'REGULAR', 'T3', 'T5']
const SECURITY_TYPES = ['CS', 'CFI', 'MON', 'FUT', 'OPT', 'PAXOS']
const DEPTHS = ['FULL_BOOK']

const c = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 18 },
  row: { display: 'flex', gap: 10, marginBottom: 14, flexWrap: 'wrap', alignItems: 'center' },
  input: { padding: '8px 12px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 13, minWidth: 220, flex: 1 },
  select: { padding: '8px 10px', background: '#141720', border: '1px solid #2d3748', borderRadius: 6, color: '#e2e8f0', fontSize: 12 },
  btn: (color = '#3b82f6') => ({ padding: '8px 14px', background: color, border: 'none', borderRadius: 6, color: '#fff', fontSize: 12, fontWeight: 700, cursor: 'pointer' }),
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 8, overflow: 'hidden' },
  th: { padding: '9px 10px', fontSize: 11, color: '#8b95a7', borderBottom: '1px solid #232840', textAlign: 'left', whiteSpace: 'nowrap' },
  td: { padding: '8px 10px', fontSize: 12, borderBottom: '1px solid #1a1d27', verticalAlign: 'top' },
  msg: (ok) => ({ fontSize: 13, padding: '10px 14px', borderRadius: 6, marginBottom: 14, background: ok ? '#052e16' : '#450a0a', color: ok ? '#4ade80' : '#f87171' }),
  badge: { display: 'inline-block', padding: '3px 8px', borderRadius: 999, fontSize: 11, fontWeight: 700, background: '#1e293b', color: '#cbd5e1' },
  code: { fontFamily: 'monospace', fontSize: 11, color: '#93c5fd' },
  helper: { fontSize: 11, color: '#64748b' },
  suggestBox: { marginTop: -6, marginBottom: 14, display: 'flex', gap: 8, flexWrap: 'wrap' },
  suggestBtn: { padding: '5px 10px', background: '#1e2540', border: '1px solid #334155', borderRadius: 999, color: '#93c5fd', cursor: 'pointer', fontSize: 11 },
  tableWrap: { overflowX: 'auto' },
  check: { width: 15, height: 15, accentColor: '#3b82f6' },
}

let nextRowId = 1
const newRowId = () => `mb-${Date.now()}-${nextRowId++}`

const toEditableRow = (row) => ({
  ...row,
  rowId: newRowId(),
})

const blankRow = (rows) => ({
  rowId: newRowId(),
  originalPositions: null,
  positions: rows.length ? Math.max(...rows.map(r => Number(r.positions) || 0)) + 1 : 1,
  symbol: '',
  exchange: 'BCS',
  settlType: 'T2',
  securityType: 'CS',
  trade: true,
  statistic: true,
  book: true,
  depth: 'FULL_BOOK',
  tradeQty: 0,
  bookQty: 0,
  occurrences: 0,
  lastIndex: -1,
  id: '',
})

export default function MultibookPage() {
  const [query, setQuery] = useState('')
  const [suggestions, setSuggestions] = useState([])
  const [username, setUsername] = useState('')
  const [rows, setRows] = useState([])
  const [removedPositions, setRemovedPositions] = useState([])
  const [meta, setMeta] = useState({ rawCount: 0, effectiveCount: 0, exists: false })
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [msg, setMsg] = useState(null)
  const [dirty, setDirty] = useState(false)

  const flash = (ok, text) => {
    setMsg({ ok, text })
    setTimeout(() => setMsg(null), 5000)
  }

  useEffect(() => {
    const q = query.trim()
    if (q.length < 2) {
      setSuggestions([])
      return
    }

    let cancelled = false
    api.get('/multibook/users', { params: { q } })
      .then(r => !cancelled && setSuggestions(Array.isArray(r.data) ? r.data : []))
      .catch(() => !cancelled && setSuggestions([]))

    return () => { cancelled = true }
  }, [query])

  const loadUser = async (user) => {
    const target = (user || query).trim()
    if (!target) return
    setLoading(true)
    try {
      const { data } = await api.get(`/multibook/${encodeURIComponent(target)}`)
      setUsername(data.username || target)
      setQuery(data.username || target)
      setRows((data.rows || []).map(toEditableRow))
      setRemovedPositions([])
      setMeta({
        rawCount: data.rawCount || 0,
        effectiveCount: data.effectiveCount || 0,
        exists: !!data.exists,
      })
      setDirty(false)
      flash(true, `✅ MultiBook cargado para ${data.username || target}`)
    } catch (e) {
      flash(false, '❌ ' + (e.response?.data?.error || e.message))
    } finally {
      setLoading(false)
    }
  }

  const save = async () => {
    if (!username) return

    const positions = rows.map(r => Number(r.positions))
    const invalid = positions.some(p => !Number.isInteger(p) || p < 0)
    if (invalid) {
      flash(false, '❌ Todas las posiciones deben ser números enteros mayores o iguales a 0')
      return
    }

    const unique = new Set(positions)
    if (unique.size !== positions.length) {
      flash(false, '❌ No se pueden guardar posiciones duplicadas')
      return
    }

    setSaving(true)
    try {
      const payload = {
        rows: rows.map(r => ({
          originalPositions: r.originalPositions,
          positions: Number(r.positions),
          symbol: String(r.symbol || '').trim().toUpperCase(),
          exchange: r.exchange,
          settlType: r.settlType,
          securityType: r.securityType,
          trade: !!r.trade,
          statistic: !!r.statistic,
          book: !!r.book,
          depth: r.depth,
          tradeQty: Number(r.tradeQty || 0),
          bookQty: Number(r.bookQty || 0),
        })),
        removePositions: removedPositions,
      }

      const { data } = await api.put(`/multibook/${encodeURIComponent(username)}`, payload)
      setRows((data.rows || []).map(toEditableRow))
      setRemovedPositions([])
      setMeta({
        rawCount: data.rawCount || 0,
        effectiveCount: data.effectiveCount || 0,
        exists: !!data.exists,
      })
      setDirty(false)
      flash(true, `✅ MultiBook guardado para ${username}`)
    } catch (e) {
      flash(false, '❌ ' + (e.response?.data?.error || e.message))
    } finally {
      setSaving(false)
    }
  }

  const updateRow = (rowId, patch) => {
    setRows(curr => curr.map(r => r.rowId === rowId ? { ...r, ...patch } : r))
    setDirty(true)
  }

  const removeRow = (row) => {
    setRows(curr => curr.filter(r => r.rowId !== row.rowId))
    if (row.originalPositions !== null && row.originalPositions !== undefined) {
      setRemovedPositions(curr => Array.from(new Set([...curr, Number(row.originalPositions)])))
    }
    setDirty(true)
  }

  const duplicateRow = (row) => {
    setRows(curr => [
      ...curr,
      {
        ...row,
        rowId: newRowId(),
        originalPositions: null,
        positions: curr.length ? Math.max(...curr.map(r => Number(r.positions) || 0)) + 1 : 1,
        occurrences: 0,
        lastIndex: -1,
        id: '',
      },
    ])
    setDirty(true)
  }

  const addRow = () => {
    setRows(curr => [...curr, blankRow(curr)])
    setDirty(true)
  }

  const sortedRows = useMemo(
    () => [...rows].sort((a, b) => Number(a.positions) - Number(b.positions)),
    [rows]
  )

  return (
    <div>
      <div style={c.h1}>🧩 MultiBook por Usuario</div>
      <div style={c.sub}>
        Busca un usuario, carga su configuración efectiva del multibook y edítala directo sobre Redis.
        Esta vista trabaja con la última ocurrencia por posición, que es la que manda en la práctica.
      </div>

      {msg && <div style={c.msg(msg.ok)}>{msg.text}</div>}

      <div style={c.row}>
        <input
          style={c.input}
          placeholder="Usuario exacto o parte del nombre..."
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && loadUser()}
        />
        <button style={c.btn()} onClick={() => loadUser()} disabled={loading}>
          {loading ? '...' : 'Cargar'}
        </button>
        <button style={c.btn('#374151')} onClick={() => username && loadUser(username)} disabled={loading || !username}>
          Recargar
        </button>
        <button style={c.btn('#0f766e')} onClick={addRow} disabled={!username}>
          + Agregar fila
        </button>
        <button style={c.btn('#7c3aed')} onClick={save} disabled={saving || !username || !dirty}>
          {saving ? 'Guardando...' : 'Guardar en Redis'}
        </button>
      </div>

      {suggestions.length > 0 && (
        <div style={c.suggestBox}>
          {suggestions.map(user => (
            <button key={user} style={c.suggestBtn} onClick={() => loadUser(user)}>
              {user}
            </button>
          ))}
        </div>
      )}

      {username && (
        <>
          <div style={c.row}>
            <span style={c.badge}>Usuario: {username}</span>
            <span style={c.badge}>Filas efectivas: {meta.effectiveCount}</span>
            <span style={c.badge}>Filas raw en Redis: {meta.rawCount}</span>
            {removedPositions.length > 0 && <span style={c.badge}>Posiciones a borrar: {removedPositions.join(', ')}</span>}
            {dirty && <span style={{ ...c.badge, background: '#3b0764', color: '#c084fc' }}>cambios sin guardar</span>}
          </div>

          <div style={{ ...c.helper, marginBottom: 12 }}>
            `occ` indica cuántas veces apareció esa posición en Redis. `idx` es la última fila raw que hoy define la configuración efectiva.
          </div>

          <div style={c.card}>
            <div style={c.tableWrap}>
              <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                  <tr>
                    <th style={c.th}>Pos</th>
                    <th style={c.th}>Símbolo</th>
                    <th style={c.th}>Exchange</th>
                    <th style={c.th}>Liq.</th>
                    <th style={c.th}>Tipo</th>
                    <th style={c.th}>Trade</th>
                    <th style={c.th}>Stat</th>
                    <th style={c.th}>Book</th>
                    <th style={c.th}>Depth</th>
                    <th style={c.th}>TradeQty</th>
                    <th style={c.th}>BookQty</th>
                    <th style={c.th}>occ</th>
                    <th style={c.th}>idx</th>
                    <th style={c.th}>Topic ID</th>
                    <th style={c.th}>Acciones</th>
                  </tr>
                </thead>
                <tbody>
                  {sortedRows.length === 0 && (
                    <tr>
                      <td colSpan={15} style={{ ...c.td, textAlign: 'center', color: '#6b7280' }}>
                        Sin filas efectivas para este usuario.
                      </td>
                    </tr>
                  )}

                  {sortedRows.map(row => (
                    <tr key={row.rowId}>
                      <td style={c.td}>
                        <input
                          style={{ ...c.input, minWidth: 0, width: 72 }}
                          type="number"
                          value={row.positions}
                          onChange={e => updateRow(row.rowId, { positions: e.target.value })}
                        />
                      </td>
                      <td style={c.td}>
                        <input
                          style={{ ...c.input, minWidth: 0, width: 170 }}
                          value={row.symbol}
                          onChange={e => updateRow(row.rowId, { symbol: e.target.value.toUpperCase() })}
                        />
                      </td>
                      <td style={c.td}>
                        <select style={c.select} value={row.exchange} onChange={e => updateRow(row.rowId, { exchange: e.target.value })}>
                          {EXCHANGES.map(v => <option key={v} value={v}>{v}</option>)}
                        </select>
                      </td>
                      <td style={c.td}>
                        <select style={c.select} value={row.settlType} onChange={e => updateRow(row.rowId, { settlType: e.target.value })}>
                          {SETTL_TYPES.map(v => <option key={v} value={v}>{v}</option>)}
                        </select>
                      </td>
                      <td style={c.td}>
                        <select style={c.select} value={row.securityType} onChange={e => updateRow(row.rowId, { securityType: e.target.value })}>
                          {SECURITY_TYPES.map(v => <option key={v} value={v}>{v}</option>)}
                        </select>
                      </td>
                      <td style={c.td}>
                        <input style={c.check} type="checkbox" checked={!!row.trade} onChange={e => updateRow(row.rowId, { trade: e.target.checked })} />
                      </td>
                      <td style={c.td}>
                        <input style={c.check} type="checkbox" checked={!!row.statistic} onChange={e => updateRow(row.rowId, { statistic: e.target.checked })} />
                      </td>
                      <td style={c.td}>
                        <input style={c.check} type="checkbox" checked={!!row.book} onChange={e => updateRow(row.rowId, { book: e.target.checked })} />
                      </td>
                      <td style={c.td}>
                        <select style={c.select} value={row.depth} onChange={e => updateRow(row.rowId, { depth: e.target.value })}>
                          {DEPTHS.map(v => <option key={v} value={v}>{v}</option>)}
                        </select>
                      </td>
                      <td style={c.td}>
                        <input
                          style={{ ...c.input, minWidth: 0, width: 78 }}
                          type="number"
                          value={row.tradeQty}
                          onChange={e => updateRow(row.rowId, { tradeQty: e.target.value })}
                        />
                      </td>
                      <td style={c.td}>
                        <input
                          style={{ ...c.input, minWidth: 0, width: 78 }}
                          type="number"
                          value={row.bookQty}
                          onChange={e => updateRow(row.rowId, { bookQty: e.target.value })}
                        />
                      </td>
                      <td style={c.td}>{row.occurrences}</td>
                      <td style={c.td}>{row.lastIndex}</td>
                      <td style={{ ...c.td, ...c.code }}>{row.id || 'se recalcula al guardar'}</td>
                      <td style={{ ...c.td, display: 'flex', gap: 6 }}>
                        <button style={{ ...c.btn('#0f766e'), fontSize: 11, padding: '4px 8px' }} onClick={() => duplicateRow(row)}>
                          Duplicar
                        </button>
                        <button style={{ ...c.btn('#7f1d1d'), fontSize: 11, padding: '4px 8px' }} onClick={() => removeRow(row)}>
                          Quitar
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

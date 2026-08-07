import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import api from '../api.js'

const CACHE_KEY = 'vt.account-load.snapshot'
const PAGE_SIZE = 30

const readCachedData = () => {
  try {
    const raw = window.sessionStorage.getItem(CACHE_KEY)
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}

const writeCachedData = (value) => {
  try {
    window.sessionStorage.setItem(CACHE_KEY, JSON.stringify(value))
  } catch {
    // ignore storage errors in admin UI
  }
}

const s = {
  h1: { fontSize: 18, fontWeight: 700, marginBottom: 4 },
  sub: { fontSize: 12, color: '#6b7280', marginBottom: 20 },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 20 },
  stat: { background: '#141720', border: '1px solid #232840', borderRadius: 10, padding: '14px 16px' },
  statVal: { fontSize: 24, fontWeight: 700, color: '#f8fafc' },
  statLbl: { fontSize: 11, color: '#64748b', marginTop: 4, textTransform: 'uppercase', letterSpacing: '.04em' },
  card: { background: '#141720', border: '1px solid #232840', borderRadius: 10, padding: 18, marginBottom: 18 },
  h2: { fontSize: 14, fontWeight: 700, marginBottom: 12 },
  tableWrap: { background: '#141720', border: '1px solid #232840', borderRadius: 10, overflowX: 'auto', marginBottom: 18 },
  th: { padding: '10px 12px', fontSize: 12, color: '#6b7280', borderBottom: '1px solid #232840', textAlign: 'left' },
  td: { padding: '9px 12px', fontSize: 13, borderBottom: '1px solid #1a1d27', verticalAlign: 'top' },
  badge: (active) => ({ display: 'inline-block', padding: '2px 8px', borderRadius: 999, fontSize: 11, fontWeight: 700, background: active ? '#052e16' : '#1f2937', color: active ? '#4ade80' : '#cbd5e1' }),
  warn: { background: '#3f1d1d', border: '1px solid #7f1d1d', borderRadius: 8, padding: '10px 12px', color: '#fca5a5', fontSize: 13, marginBottom: 14 },
  btn: { padding: '8px 14px', background: '#1d4ed8', border: 'none', borderRadius: 8, color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' },
  pageBtn: (active) => ({ padding: '6px 10px', background: active ? '#1d4ed8' : '#0f1117', border: '1px solid #334155', borderRadius: 8, color: '#fff', fontSize: 12, fontWeight: 600, cursor: 'pointer', minWidth: 36 }),
  small: { fontSize: 11, color: '#64748b' },
  progress: { height: 10, background: '#0f1117', borderRadius: 999, overflow: 'hidden', border: '1px solid #232840' },
  progressBar: (pct) => ({ width: `${pct}%`, height: '100%', background: 'linear-gradient(90deg, #2563eb, #38bdf8)' }),
}

const fmtDate = (value) => value ? new Date(value).toLocaleString('es-CL') : '—'
const fmtMs = (value) => {
  if (value == null || value <= 0) return '—'
  if (value < 1000) return `${value} ms`
  const sec = Math.round(value / 1000)
  if (sec < 60) return `${sec} s`
  const min = Math.floor(sec / 60)
  const rem = sec % 60
  return `${min}m ${rem}s`
}

const listOrDash = (values) => values?.length ? values.join(', ') : '—'

const statusPill = (kind) => {
  const palette = {
    ok: { background: '#052e16', color: '#4ade80' },
    warn: { background: '#3f1d1d', color: '#fca5a5' },
    info: { background: '#1e293b', color: '#cbd5e1' },
  }
  const selected = palette[kind] || palette.info
  return {
    display: 'inline-block',
    padding: '2px 8px',
    borderRadius: 999,
    fontSize: 11,
    fontWeight: 700,
    background: selected.background,
    color: selected.color,
  }
}

const ownershipSummaryOf = (account) => {
  const marginOwners = account.marginOwners || []
  const leverageOwners = account.leverageOwners || []
  const hasMarginConflict = marginOwners.length > 1
  const hasLeverageConflict = leverageOwners.length > 1

  if (!hasMarginConflict && !hasLeverageConflict) {
    return null
  }

  if (hasMarginConflict && hasLeverageConflict) {
    return {
      kind: 'warn',
      label: 'CONFLICTO',
      type: 'Margin + Palanca',
      note: `Margin duplicado en ${marginOwners.join(', ')}. Palanca duplicada en ${leverageOwners.join(', ')}.`,
    }
  }

  if (hasMarginConflict) {
    return {
      kind: 'warn',
      label: 'CONFLICTO',
      type: 'Margin',
      note: `Margin duplicado en más de un usuario: ${marginOwners.join(', ')}.`,
    }
  }

  return {
    kind: 'warn',
    label: 'CONFLICTO',
    type: 'Palanca',
    note: `Palanca duplicada en más de un usuario: ${leverageOwners.join(', ')}.`,
  }
}

export default function AccountLoadPage() {
  const [data, setData] = useState(() => readCachedData())
  const [error, setError] = useState('')
  const [accountFilter, setAccountFilter] = useState('')
  const [currentPage, setCurrentPage] = useState(1)
  const [recalcLoading, setRecalcLoading] = useState(false)
  const [validateLoading, setValidateLoading] = useState({})
  const [recalcMsg, setRecalcMsg] = useState(null)
  const inFlightRef = useRef(false)
  const timerRef = useRef(null)
  const mountedRef = useRef(false)

  const load = useCallback(async ({ silent = false } = {}) => {
    if (inFlightRef.current) return
    inFlightRef.current = true
    try {
      const res = await api.get('/accounts/progress', { timeout: 45000 })
      if (!mountedRef.current) return
      setData(res.data)
      writeCachedData(res.data)
      setError('')
    } catch (err) {
      if (!mountedRef.current || silent) return
      setError(err.response?.data?.error || err.message)
    } finally {
      inFlightRef.current = false
    }
  }, [])

  useEffect(() => {
    mountedRef.current = true

    const tick = async () => {
      await load({ silent: true })
      if (!mountedRef.current) return
      timerRef.current = setTimeout(tick, 5000)
    }

    load()
    timerRef.current = setTimeout(tick, 5000)

    return () => {
      mountedRef.current = false
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [load])

  const visibleCycle = useMemo(() => data?.currentCycle || data?.recentCycles?.[0] || null, [data])
  const postProcessTotal = useMemo(() => {
    if (!visibleCycle) return 0
    return (visibleCycle.completedBackgroundRefreshes || 0) + (visibleCycle.pendingBackgroundRefreshes || 0)
  }, [visibleCycle])
  const progressPct = useMemo(() => {
    if (!visibleCycle) return 0
    if (visibleCycle.phase === 'post_process') {
      if (!visibleCycle.running) return 100
      if (postProcessTotal <= 0) return 0
      return Math.min(99, Math.round(((visibleCycle.completedBackgroundRefreshes || 0) / postProcessTotal) * 100))
    }
    if (!visibleCycle.totalUsers) return 0
    if (!visibleCycle.running) return 100
    return Math.min(100, Math.round((visibleCycle.processedUsers / visibleCycle.totalUsers) * 100))
  }, [visibleCycle, postProcessTotal])
  const cycleTitle = useMemo(() => {
    if (!visibleCycle) return 'Último ciclo conocido'
    if (visibleCycle.running && visibleCycle.phase === 'post_process') return 'Post-proceso en curso'
    if (visibleCycle.running) return 'Ciclo en curso'
    return 'Último ciclo conocido'
  }, [visibleCycle])
  const cycleStatusText = useMemo(() => {
    if (!visibleCycle) return '—'
    if (visibleCycle.running && visibleCycle.phase === 'post_process') {
      return `${visibleCycle.completedBackgroundRefreshes || 0} / ${postProcessTotal} recálculos`
    }
    if (visibleCycle.running) return fmtMs(visibleCycle.etaMs)
    return 'Completo'
  }, [visibleCycle, postProcessTotal])
  const visibleAccounts = useMemo(() => {
    return (data?.accounts || []).filter(row => {
      const owners = [...(row.owners || []), ...(row.accountOwners || [])].map(owner => String(owner).toLowerCase())
      return !row.isVoultech && !owners.includes('voultech')
    })
  }, [data])
  const filteredAccounts = useMemo(() => {
    const rows = visibleAccounts
    const filter = accountFilter.trim().toLowerCase()
    if (!filter) return rows
    return rows.filter(row =>
      row.account?.toLowerCase().includes(filter) ||
      (row.lastUsername && String(row.lastUsername).toLowerCase().includes(filter)) ||
      (row.owners || []).some(owner => String(owner).toLowerCase().includes(filter)) ||
      (row.accountOwners || []).some(owner => String(owner).toLowerCase().includes(filter)) ||
      (row.marginOwners || []).some(owner => String(owner).toLowerCase().includes(filter)) ||
      (row.leverageOwners || []).some(owner => String(owner).toLowerCase().includes(filter)) ||
      String(row.configurationOwner || '').toLowerCase().includes(filter)
    )
  }, [visibleAccounts, accountFilter])
  const totalPages = useMemo(() => Math.max(1, Math.ceil(filteredAccounts.length / PAGE_SIZE)), [filteredAccounts])
  const pagedAccounts = useMemo(() => {
    const start = (currentPage - 1) * PAGE_SIZE
    return filteredAccounts.slice(start, start + PAGE_SIZE)
  }, [filteredAccounts, currentPage])
  const ownershipRows = useMemo(() => {
    return filteredAccounts
      .map(account => ({ ...account, ownership: ownershipSummaryOf(account) }))
      .filter(account => account.ownership)
      .sort((a, b) => {
        return (
          ((b.marginOwnersCount || 0) + (b.leverageOwnersCount || 0)) - ((a.marginOwnersCount || 0) + (a.leverageOwnersCount || 0)) ||
          a.account.localeCompare(b.account)
        )
      })
  }, [filteredAccounts])

  useEffect(() => {
    setCurrentPage(1)
  }, [accountFilter])

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages)
    }
  }, [currentPage, totalPages])

  const flash = (ok, text) => {
    setRecalcMsg({ ok, text })
    setTimeout(() => setRecalcMsg(null), 5000)
  }

  const recalculate = async (target) => {
    const value = (target || '').trim()
    if (!value) return
    setRecalcLoading(true)
    try {
      const res = await api.post(`/accounts/${encodeURIComponent(value)}/recalculate`)
      flash(true, res.data?.message || `Recálculo enviado para ${value}`)
      await load()
    } catch (err) {
      flash(false, err.response?.data?.error || err.message)
    } finally {
      setRecalcLoading(false)
    }
  }

  const revalidateKeycloak = async (target) => {
    const value = (target || '').trim()
    if (!value) return
    setValidateLoading((current) => ({ ...current, [value]: true }))
    try {
      const res = await api.post(`/accounts/${encodeURIComponent(value)}/validate-keycloak`)
      flash(true, res.data?.message || `Revalidación Keycloak OK para ${value}`)
      await load()
    } catch (err) {
      flash(false, err.response?.data?.error || err.message)
    } finally {
      setValidateLoading((current) => ({ ...current, [value]: false }))
    }
  }

  const statusOf = (account) => {
    if (account.initializationCount > 0 && account.lastInitializationSuccess) return { label: 'OK', active: true }
    if (account.initializationCount > 0) return { label: 'FAIL', active: false }
    if (account.hasActor) return { label: 'SIN INIT', active: false }
    return { label: 'DECL', active: false }
  }

  return (
    <div>
      <div style={s.h1}>📊 Carga de Clientes</div>
      <div style={s.sub}>Vista de todas las cuentas conocidas por el servicio, con búsqueda rápida y recálculo por fila.</div>
      {error && <div style={s.warn}>Error cargando progreso: {error}</div>}
      {recalcMsg && <div style={recalcMsg.ok ? { ...s.warn, background: '#052e16', border: '1px solid #14532d', color: '#86efac' } : s.warn}>{recalcMsg.text}</div>}

      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 16, flexWrap: 'wrap', alignItems: 'center' }}>
        <div style={s.small}>La vista se refresca sola cada 5 segundos. `Refrescar ahora` solo vuelve a pedir el estado actual; no recalcula cuentas.</div>
        <button style={s.btn} onClick={() => load()}>↺ Refrescar ahora</button>
      </div>

      <div style={s.grid}>
        <div style={s.stat}><div style={s.statVal}>{data?.processedUsersCount ?? '—'}</div><div style={s.statLbl}>Usuarios habilitados</div></div>
        <div style={s.stat}><div style={s.statVal}>{data?.priorityUsersConfiguredCount ?? '—'}</div><div style={s.statLbl}>Prioritarios configurados</div></div>
        <div style={s.stat}><div style={s.statVal}>{data?.activeActors ?? '—'}</div><div style={s.statLbl}>Actores de cuenta</div></div>
        <div style={s.stat}><div style={s.statVal}>{data?.cachedMargins ?? '—'}</div><div style={s.statLbl}>Cuentas con margen cacheado</div></div>
        <div style={s.stat}><div style={s.statVal}>{data?.cachedLeverages ?? '—'}</div><div style={s.statLbl}>Cuentas con palanca cacheada</div></div>
        <div style={s.stat}><div style={s.statVal}>{data?.accountsCount ?? '—'}</div><div style={s.statLbl}>Cuentas conocidas</div></div>
      </div>

      <div style={s.card}>
        <div style={s.h2}>{cycleTitle}</div>
        {!visibleCycle && <div style={s.small}>Aún no hay ciclos registrados para mostrar.</div>}
        {visibleCycle && (
          <>
            <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 8, flexWrap: 'wrap' }}>
              <div><span style={s.badge(visibleCycle.running || visibleCycle.success)}>{visibleCycle.trigger}</span></div>
              <div style={s.small}>
                Ciclo #{visibleCycle.cycleId}
                {visibleCycle.running
                  ? visibleCycle.phase === 'post_process'
                    ? ' · post-proceso'
                    : ' · ejecutándose'
                  : ' · finalizado'}
              </div>
            </div>
            <div style={s.progress}><div style={s.progressBar(progressPct)} /></div>
            <div style={{ marginTop: 8, marginBottom: 14, fontSize: 13 }}>
              {visibleCycle.phase === 'post_process'
                ? `${visibleCycle.processedUsers} / ${visibleCycle.totalUsers} usuarios procesados · ${visibleCycle.completedBackgroundRefreshes || 0} / ${postProcessTotal} recálculos completados · ${visibleCycle.pendingBackgroundRefreshes || 0} pendientes`
                : `${visibleCycle.processedUsers} / ${visibleCycle.totalUsers} usuarios procesados (${progressPct}%)`}
            </div>
            <div style={s.grid}>
              <div style={s.stat}><div style={s.statVal}>{visibleCycle.touchedAccounts}</div><div style={s.statLbl}>Cuentas vistas</div></div>
              <div style={s.stat}><div style={s.statVal}>{visibleCycle.actorCreations}</div><div style={s.statLbl}>Actores creados</div></div>
              <div style={s.stat}><div style={s.statVal}>{visibleCycle.initializedAccounts}</div><div style={s.statLbl}>Inicializaciones OK</div></div>
              <div style={s.stat}><div style={s.statVal}>{visibleCycle.failedAccountInitializations}</div><div style={s.statLbl}>Inicializaciones fallidas</div></div>
              <div style={s.stat}><div style={s.statVal}>{cycleStatusText}</div><div style={s.statLbl}>ETA / estado</div></div>
              <div style={s.stat}><div style={s.statVal}>{fmtMs(visibleCycle.durationMs)}</div><div style={s.statLbl}>Duración</div></div>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', gap: 12 }}>
              <div style={s.small}>Usuario actual: <strong>{visibleCycle.currentUsername || '—'}</strong></div>
              <div style={s.small}>Grupo actual: <strong>{visibleCycle.currentGroup || '—'}</strong></div>
              <div style={s.small}>Último usuario procesado: <strong>{visibleCycle.lastProcessedUsername || '—'}</strong></div>
              <div style={s.small}>
                Updates margen/palanca: <strong>{visibleCycle.marginUpdates} / {visibleCycle.leverageUpdates}</strong>
                {visibleCycle.phase === 'post_process' ? ` · completados ${visibleCycle.completedBackgroundRefreshes || 0}` : ''}
              </div>
            </div>
            {!!visibleCycle.error && <div style={{ ...s.warn, marginTop: 14, marginBottom: 0 }}>Último error: {visibleCycle.error}</div>}
          </>
        )}
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 12, flexWrap: 'wrap', alignItems: 'center' }}>
        <input
          style={{ minWidth: 320, flex: 1, padding: '9px 12px', background: '#0f1117', border: '1px solid #334155', borderRadius: 8, color: '#f8fafc', fontSize: 13 }}
          placeholder="Buscar cuenta o usuario dueño..."
          value={accountFilter}
          onChange={(e) => setAccountFilter(e.target.value)}
        />
        <div style={s.small}>
          Visibles {filteredAccounts.length} de {data?.visibleAccountsCount ?? visibleAccounts.length} · ocultas voultech: {data?.hiddenVoultechAccountsCount ?? 0}
        </div>
      </div>

      <div style={s.tableWrap}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th style={s.th}>Cuenta</th>
              <th style={s.th}>Actor</th>
              <th style={s.th}>Veces vista</th>
              <th style={s.th}>Inicializaciones</th>
              <th style={s.th}>Usuarios dueños</th>
              <th style={s.th}>Último usuario</th>
              <th style={s.th}>Última init</th>
              <th style={s.th}>Duración</th>
              <th style={s.th}>Estado</th>
              <th style={s.th}>Acción</th>
            </tr>
          </thead>
          <tbody>
            {pagedAccounts.map(account => {
              const status = statusOf(account)
              const owners = listOrDash(account.accountOwners?.length ? account.accountOwners : account.owners)
              return (
              <tr key={account.account}>
                <td style={s.td}><strong>{account.account}</strong></td>
                <td style={s.td}><span style={s.badge(account.hasActor)}>{account.hasActor ? 'Sí' : 'No'}</span></td>
                <td style={s.td}>{account.touchCount}</td>
                <td style={s.td}>{account.initializationCount}</td>
                <td style={s.td}>{owners}</td>
                <td style={s.td}>{account.lastUsername || '—'}</td>
                <td style={s.td}>{fmtDate(account.lastInitializationAt)}</td>
                <td style={s.td}>{fmtMs(account.lastInitializationDurationMs)}</td>
                <td style={s.td}><span style={s.badge(status.active)}>{status.label}</span></td>
                <td style={s.td}>
                  <button
                    style={{ ...s.btn, padding: '6px 10px', fontSize: 12 }}
                    disabled={recalcLoading}
                    onClick={() => recalculate(account.account)}
                  >
                    ⚡ Recalcular
                  </button>
                </td>
              </tr>
            )})}
            {pagedAccounts.length === 0 && (
              <tr><td colSpan={10} style={{ ...s.td, textAlign: 'center', color: '#6b7280' }}>Sin cuentas para mostrar</td></tr>
            )}
          </tbody>
        </table>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginBottom: 18, flexWrap: 'wrap', alignItems: 'center' }}>
        <div style={s.small}>Página {currentPage} de {totalPages} · {PAGE_SIZE} cuentas por página</div>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <button style={s.pageBtn(false)} disabled={currentPage === 1} onClick={() => setCurrentPage(page => Math.max(1, page - 1))}>Anterior</button>
          {Array.from({ length: totalPages }, (_, index) => index + 1).map(page => (
            <button key={page} style={s.pageBtn(page === currentPage)} onClick={() => setCurrentPage(page)}>
              {page}
            </button>
          ))}
          <button style={s.pageBtn(false)} disabled={currentPage === totalPages} onClick={() => setCurrentPage(page => Math.min(totalPages, page + 1))}>Siguiente</button>
        </div>
      </div>

      <div style={{ ...s.small, marginTop: -8, marginBottom: 18 }}>
        `OK` = inicializada correctamente. `FAIL` = inicialización fallida. `SIN INIT` = existe actor pero no hay inicialización registrada en este proceso. `DECL` = la cuenta fue declarada en `account`, pero no se creó actor ni se inicializó desde este barrido.
      </div>

      <div style={s.card}>
        <div style={s.h2}>Conflictos de configuración</div>
        <div style={{ ...s.small, marginBottom: 12 }}>
          Aquí solo se muestran cuentas con conflicto real: `marginaccount` repetido entre usuarios o `palanca` repetida entre usuarios.
        </div>
        {ownershipRows.length === 0 && <div style={s.small}>No hay conflictos de `marginaccount` o `palanca` para mostrar.</div>}
        {ownershipRows.length > 0 && (
          <div style={s.tableWrap}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr>
                  <th style={s.th}>Cuenta</th>
                  <th style={s.th}>Usuarios con account</th>
                  <th style={s.th}>Margin en</th>
                  <th style={s.th}>Palanca en</th>
                  <th style={s.th}>Tipo</th>
                  <th style={s.th}>Estado</th>
                  <th style={s.th}>Regla</th>
                  <th style={s.th}>Acción</th>
                </tr>
              </thead>
              <tbody>
                {ownershipRows.map(account => (
                  <tr key={`ownership-${account.account}`}>
                    <td style={s.td}><strong>{account.account}</strong></td>
                    <td style={s.td}>{listOrDash(account.accountOwners)}</td>
                    <td style={s.td}>{listOrDash(account.marginOwners)}</td>
                    <td style={s.td}>{listOrDash(account.leverageOwners)}</td>
                    <td style={s.td}>{account.ownership.type}</td>
                    <td style={s.td}><span style={statusPill(account.ownership.kind)}>{account.ownership.label}</span></td>
                    <td style={s.td}>{account.ownership.note}</td>
                    <td style={s.td}>
                      <button
                        style={{ ...s.btn, padding: '6px 10px', fontSize: 12 }}
                        disabled={!!validateLoading[account.account]}
                        onClick={() => revalidateKeycloak(account.account)}
                      >
                        {validateLoading[account.account] ? '…' : '🔎 Revalidar KC'}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

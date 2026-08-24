import { useEffect, useState } from 'react'
import {
  Activity, ArrowLeftRight, BarChart3, BookOpen, Boxes, Cable, CircleDollarSign,
  Database, FileClock, Gauge, KeyRound, LockKeyhole,
  LogOut, MessageSquareMore, Newspaper, RefreshCcw, Scale, Send, ServerCog,
  ShieldCheck, Users, WalletCards,
} from 'lucide-react'
import api, { clearAdminToken, getAdminToken, setAdminToken } from './api.js'
import SymbolsPage     from './pages/SymbolsPage.jsx'
import AccountsPage    from './pages/AccountsPage.jsx'
import ConnectionsPage from './pages/ConnectionsPage.jsx'
import OrdersPage      from './pages/OrdersPage.jsx'
import InjectPage      from './pages/InjectPage.jsx'
import PropertiesPage  from './pages/PropertiesPage.jsx'
import NewsPage        from './pages/NewsPage.jsx'
import RiskPage           from './pages/RiskPage.jsx'
import SessionsAdminPage  from './pages/SessionsAdminPage.jsx'
import IpSecurityPage     from './pages/IpSecurityPage.jsx'
import AccountLoadPage    from './pages/AccountLoadPage.jsx'
import SimultaneasPage    from './pages/SimultaneasPage.jsx'
import PrestamosPage      from './pages/PrestamosPage.jsx'
import SaldoPage          from './pages/SaldoPage.jsx'
import SqlRecoveryPage    from './pages/SqlRecoveryPage.jsx'
import OdPage             from './pages/OdPage.jsx'
import MultibookPage      from './pages/MultibookPage.jsx'
import KeycloakRiskPage   from './pages/KeycloakRiskPage.jsx'
import RedisRecoveryPage  from './pages/RedisRecoveryPage.jsx'
import MongoPage          from './pages/MongoPage.jsx'
import ClientLogsPage     from './pages/ClientLogsPage.jsx'

const PAGES = [
  { id: 'symbols',      group: 'Mercado',        label: 'Suscripciones MKD',      icon: Activity,          component: SymbolsPage },
  { id: 'accounts',     group: 'Mercado',        label: 'Cuadratura prioritaria', icon: Scale,             component: AccountsPage },
  { id: 'connections',  group: 'Mercado',        label: 'Conexiones',             icon: Cable,             component: ConnectionsPage },
  { id: 'redis',        group: 'Persistencia',   label: 'Redis recovery',         icon: RefreshCcw,        component: RedisRecoveryPage },
  { id: 'mongo',        group: 'Persistencia',   label: 'Mongo (Var %)',          icon: Database,          component: MongoPage },
  { id: 'multibook',    group: 'Persistencia',   label: 'MultiBook Redis',        icon: BookOpen,          component: MultibookPage },
  { id: 'sqlrecovery',  group: 'Persistencia',   label: 'SQL recovery',           icon: ServerCog,         component: SqlRecoveryPage },
  { id: 'orders',       group: 'Operaciones',    label: 'Reenvío de órdenes',     icon: Send,              component: OrdersPage },
  { id: 'inject',       group: 'Operaciones',    label: 'Inyección de mensajes',  icon: MessageSquareMore, component: InjectPage },
  { id: 'simultaneas',  group: 'Operaciones',    label: 'Simultáneas manuales',   icon: ArrowLeftRight,    component: SimultaneasPage },
  { id: 'prestamos',    group: 'Operaciones',    label: 'Préstamos manuales',     icon: WalletCards,       component: PrestamosPage },
  { id: 'saldo',        group: 'Operaciones',    label: 'Saldo manual',           icon: CircleDollarSign,  component: SaldoPage },
  { id: 'risk',         group: 'Control',        label: 'Control de riesgo',      icon: Gauge,             component: RiskPage },
  { id: 'od',           group: 'Control',        label: 'Control OD',             icon: ShieldCheck,       component: OdPage },
  { id: 'sessions',     group: 'Control',        label: 'Sesiones activas',       icon: Users,             component: SessionsAdminPage },
  { id: 'clientlogs',   group: 'Control',        label: 'Diagnóstico de fronts',  icon: FileClock,         component: ClientLogsPage },
  { id: 'ipsecurity',   group: 'Control',        label: 'Seguridad IP',           icon: LockKeyhole,       component: IpSecurityPage },
  { id: 'properties',   group: 'Administración', label: 'Propiedades',            icon: ServerCog,         component: PropertiesPage },
  { id: 'news',         group: 'Administración', label: 'Noticias',               icon: Newspaper,         component: NewsPage },
  { id: 'keycloakrisk', group: 'Administración', label: 'Keycloak',               icon: KeyRound,          component: KeycloakRiskPage },
  { id: 'accountload',  group: 'Administración', label: 'Carga de clientes',      icon: BarChart3,         component: AccountLoadPage },
]

const GROUPS = [...new Set(PAGES.map(page => page.group))]

const s = {
  app:     { display: 'flex', height: '100vh', overflow: 'hidden' },
  sidebar: {
    width: 248, height: '100vh', background: '#121722', borderRight: '1px solid #293246',
    display: 'flex', flexDirection: 'column', flexShrink: 0, overflow: 'hidden',
  },
  logo: {
    height: 58, padding: '0 17px', display: 'flex', alignItems: 'center', gap: 10,
    color: '#dbeafe', borderBottom: '1px solid #293246', flexShrink: 0,
  },
  logoMark: {
    width: 30, height: 30, display: 'grid', placeItems: 'center', borderRadius: 6,
    background: '#1e3a5f', color: '#7dd3fc', border: '1px solid #315579',
  },
  logoText: { display: 'grid', gap: 1 },
  logoTitle: { fontSize: 12, fontWeight: 800, letterSpacing: 1 },
  logoSub: { fontSize: 9, color: '#718096', letterSpacing: .5 },
  navScroll: {
    flex: 1, minHeight: 0, overflowY: 'auto', overflowX: 'hidden',
    padding: '9px 8px 12px', scrollbarGutter: 'stable',
  },
  group: { marginBottom: 8 },
  groupLabel: {
    padding: '8px 10px 5px', color: '#64748b', fontSize: 9, fontWeight: 800,
    letterSpacing: .9, textTransform: 'uppercase',
  },
  logout: {
    height: 34, margin: '10px 12px 12px', padding: '0 11px', borderRadius: 5,
    border: '1px solid #334155', background: '#182131', color: '#aebbd0',
    cursor: 'pointer', fontSize: 11, fontWeight: 700, flexShrink: 0,
    display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8,
  },
  navBtn: (active) => ({
    display: 'flex', alignItems: 'center', gap: 10, width: '100%', textAlign: 'left',
    height: 34, padding: '0 10px', marginBottom: 2, fontSize: 11.5, fontWeight: active ? 700 : 500,
    cursor: 'pointer', borderRadius: 5, transition: 'background .15s, color .15s',
    background: active ? '#1d3553' : 'transparent',
    color: active ? '#dbeafe' : '#9eabc0',
    border: active ? '1px solid #315579' : '1px solid transparent',
  }),
  navIcon: (active) => ({ color: active ? '#67c2f0' : '#718096', flexShrink: 0 }),
  main: { flex: 1, minWidth: 0, overflow: 'auto', padding: 24, background: '#0f1117' },
  authPage: {
    minHeight: '100vh', display: 'grid', placeItems: 'center', padding: 24,
    background: 'radial-gradient(circle at 20% 10%, #1d2b45 0, #0f1117 42%)',
  },
  authCard: {
    width: 'min(430px, 100%)', padding: 28, borderRadius: 12,
    border: '1px solid #2d3748', background: '#141720',
    boxShadow: '0 24px 70px rgba(0, 0, 0, .38)',
  },
  authTitle: { margin: '0 0 8px', fontSize: 22, color: '#f8fafc' },
  authText: { margin: '0 0 20px', fontSize: 13, lineHeight: 1.5, color: '#94a3b8' },
  authInput: {
    width: '100%', boxSizing: 'border-box', padding: '11px 12px', borderRadius: 7,
    border: '1px solid #334155', background: '#0f1117', color: '#e2e8f0', fontSize: 14,
  },
  authButton: {
    width: '100%', marginTop: 12, padding: '11px 14px', border: 0, borderRadius: 7,
    background: '#2563eb', color: '#fff', cursor: 'pointer', fontSize: 14, fontWeight: 700,
  },
  authError: {
    marginTop: 14, padding: '10px 12px', borderRadius: 7,
    background: '#450a0a', color: '#fca5a5', fontSize: 12, lineHeight: 1.45,
  },
}

export default function App() {
  const [page, setPage] = useState('symbols')
  const [authState, setAuthState] = useState(getAdminToken() ? 'checking' : 'login')
  const [tokenInput, setTokenInput] = useState('')
  const [authError, setAuthError] = useState('')
  const Active = PAGES.find(p => p.id === page)?.component || SymbolsPage

  const validateToken = async (token) => {
    setAuthError('')
    setAuthState('checking')
    setAdminToken(token)
    try {
      await api.get('/auth')
      setTokenInput('')
      setAuthState('ready')
    } catch (error) {
      clearAdminToken()
      const status = error.response?.status
      const message = error.response?.data?.error || error.message
      setAuthError(status === 503
        ? 'El servidor no tiene admin.token configurado. Agrégalo en application.properties y reinicia el core.'
        : status === 401
          ? 'Token incorrecto. Usa exactamente el valor configurado en admin.token.'
          : `No fue posible validar el token: ${message}`)
      setAuthState('login')
    }
  }

  useEffect(() => {
    const savedToken = getAdminToken()
    if (savedToken) validateToken(savedToken)
  }, [])

  const submitToken = event => {
    event.preventDefault()
    const token = tokenInput.trim()
    if (!token) {
      setAuthError('Ingresa el valor configurado en admin.token.')
      return
    }
    validateToken(token)
  }

  const logout = () => {
    clearAdminToken()
    setAuthError('')
    setAuthState('login')
  }

  if (authState !== 'ready') {
    return (
      <div style={s.authPage}>
        <form style={s.authCard} onSubmit={submitToken}>
          <h1 style={s.authTitle}>Acceso VT Admin</h1>
          <p style={s.authText}>
            Ingresa el mismo valor definido como <code>admin.token</code> en el properties del core.
            Se conservará solamente durante esta sesión del navegador.
          </p>
          <input
            autoFocus
            type="password"
            autoComplete="current-password"
            style={s.authInput}
            placeholder="Token de administración"
            value={tokenInput}
            onChange={event => setTokenInput(event.target.value)}
            disabled={authState === 'checking'}
          />
          <button style={s.authButton} type="submit" disabled={authState === 'checking'}>
            {authState === 'checking' ? 'Validando...' : 'Ingresar'}
          </button>
          {authError && <div style={s.authError}>{authError}</div>}
        </form>
      </div>
    )
  }

  return (
    <div style={s.app}>
      <nav style={s.sidebar}>
        <div style={s.logo}>
          <span style={s.logoMark}><Boxes size={17} strokeWidth={1.8} /></span>
          <span style={s.logoText}>
            <span style={s.logoTitle}>VT ADMIN</span>
            <span style={s.logoSub}>CONTROL DE SERVICIOS</span>
          </span>
        </div>
        <div style={s.navScroll}>
          {GROUPS.map(group => (
            <section key={group} style={s.group}>
              <div style={s.groupLabel}>{group}</div>
              {PAGES.filter(p => p.group === group).map(p => {
                const Icon = p.icon
                const active = page === p.id
                return (
                  <button key={p.id} style={s.navBtn(active)} onClick={() => setPage(p.id)} title={p.label}>
                    <Icon size={15} strokeWidth={1.8} style={s.navIcon(active)} />
                    <span>{p.label}</span>
                  </button>
                )
              })}
            </section>
          ))}
        </div>
        <button style={s.logout} onClick={logout}>
          <LogOut size={14} strokeWidth={1.8} />
          Cerrar sesión
        </button>
      </nav>
      <main style={s.main}>
        <Active />
      </main>
    </div>
  )
}

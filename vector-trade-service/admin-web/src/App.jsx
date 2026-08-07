import { useEffect, useState } from 'react'
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

const PAGES = [
  { id: 'symbols',     label: '📡 Suscripciones MKD',     component: SymbolsPage },
  { id: 'accounts',    label: '⚡ Cuadratura Prioritaria', component: AccountsPage },
  { id: 'connections', label: '🔌 Conexiones',             component: ConnectionsPage },
  { id: 'redis',       label: '🧯 Redis Recovery',         component: RedisRecoveryPage },
  { id: 'mongo',       label: '🍃 Mongo (Var %)',           component: MongoPage },
  { id: 'orders',      label: '📋 Reenvío Órdenes',        component: OrdersPage },
  { id: 'inject',      label: '💉 Inyección Mensajes',     component: InjectPage },
  { id: 'properties',  label: '⚙️ Propiedades',            component: PropertiesPage },
  { id: 'news',        label: '📰 Noticias',               component: NewsPage },
  { id: 'risk',        label: '🛡️ Control de Riesgo',      component: RiskPage },
  { id: 'od',          label: '🚫 Control OD',              component: OdPage },
  { id: 'multibook',   label: '🧩 MultiBook Redis',         component: MultibookPage },
  { id: 'sessions',    label: '👤 Sesiones Activas',       component: SessionsAdminPage },
  { id: 'ipsecurity',  label: '🔒 Seguridad IP',            component: IpSecurityPage },
  { id: 'simultaneas', label: '🧮 Simultáneas Manuales',    component: SimultaneasPage },
  { id: 'prestamos',   label: '💸 Préstamos Manuales',      component: PrestamosPage },
  { id: 'saldo',       label: '💰 Saldo Manual',            component: SaldoPage },
  { id: 'sqlrecovery', label: '🗄️ SQL Recovery',          component: SqlRecoveryPage },
  { id: 'keycloakrisk',label: '🗝️ Keycloak',               component: KeycloakRiskPage },
  { id: 'accountload', label: '📊 Carga Clientes',          component: AccountLoadPage },
]

const s = {
  app:     { display: 'flex', height: '100vh', overflow: 'hidden' },
  sidebar: {
    width: 230, background: '#141720', borderRight: '1px solid #232840',
    display: 'flex', flexDirection: 'column', padding: '0 0 16px 0', flexShrink: 0,
  },
  logo: {
    padding: '20px 18px 14px', fontSize: 13, fontWeight: 700,
    color: '#7c8db5', letterSpacing: 1, borderBottom: '1px solid #232840',
    marginBottom: 8,
  },
  logout: {
    margin: 'auto 14px 0', padding: '9px 12px', borderRadius: 6,
    border: '1px solid #334155', background: '#1e293b', color: '#cbd5e1',
    cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  navBtn: (active) => ({
    display: 'block', width: '100%', textAlign: 'left',
    padding: '10px 18px', fontSize: 13, cursor: 'pointer', border: 'none',
    borderRadius: 0, transition: 'all .15s',
    background: active ? '#1e2540' : 'transparent',
    color:      active ? '#60a5fa' : '#a0aec0',
    borderLeft: active ? '3px solid #3b82f6' : '3px solid transparent',
  }),
  main: { flex: 1, overflow: 'auto', padding: 24, background: '#0f1117' },
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
        <div style={s.logo}>VT ADMIN</div>
        {PAGES.map(p => (
          <button key={p.id} style={s.navBtn(page === p.id)} onClick={() => setPage(p.id)}>
            {p.label}
          </button>
        ))}
        <button style={s.logout} onClick={logout}>Cerrar sesión</button>
      </nav>
      <main style={s.main}>
        <Active />
      </main>
    </div>
  )
}

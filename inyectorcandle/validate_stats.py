"""
validate_stats.py
=================
Compara metricas del panel estadisticas del FRONTEND
contra MONGODB (trades + instrument_stats) y FIX LOG (35=X).

El frontend recibe BolsaStats via WebSocket desde vector-candle-service,
que agrega sobre la coleccion instrument_stats de MongoDB.

Uso: python validate_stats.py 20260309
"""

import sys, os, re
from collections import defaultdict

MONGO_URI      = "mongodb://admin:10wfyaxqxk2hq@68.211.112.146:27017"
MONGO_DATABASE = "inyectorcandle"
LOG_DIR        = r"E:\VC-GITHUB\vector-trade-2.0\inyectorcandle\logs\fix-logs"
LOG_PATTERN    = "FIX.4.4-inyectorcandle-BCSGATEWAY.messages_{date}.log"
DATE_STR       = sys.argv[1] if len(sys.argv) > 1 else "20260309"

# ─── VALORES DEL FRONTEND (ingresados manualmente por el usuario) ─────────────
FRONTEND = {
    "Volumen Total":           2_308_188_584.0,
    "Monto Total":             190_186_704_564.43,
    "Capitalizacion Total":    191_906_449_206.09,
    "N Total Trades":          102_441.0,
    "Volatilidad Promedio %":  0.62,
    "Sentimiento Positivo %":  24.52,
    "Sentimiento Negativo %":  24.52,
    "Rango Promedio":          157.25873871,
    "Indice Promedio":         83.14158147,
    "Indice Maximo":           83.99092449,
    "Indice Minimo":           80.03017584,
    "Liquidez Media":          0.0121,
    "Cap Promedio":            619_053_061.96,
    "Precio Prom Acum":        32_323.6,
    "Precio Max Acum":         10_036_885.7,
    "Tendencia Promedio %":    0.09,
}

SEP  = "=" * 100
SEP2 = "-" * 100

def fv(v, dec=2):
    if v is None: return "N/A"
    try:    return f"{float(v):>20,.{dec}f}"
    except: return str(v)

def diff_pct(got, ref):
    if ref is None or ref == 0: return "N/A"
    try:
        d = (float(got) - float(ref)) / abs(float(ref)) * 100
        return f"{d:+.4f}%"
    except: return "N/A"

def ok(got, ref, tol=1.0):
    if ref is None or got is None: return "?"
    try:
        return "OK" if abs((float(got)-float(ref))/abs(float(ref))*100) < tol else "DIFF"
    except: return "?"

# ══════════════════════════════════════════════════════════════════════════════
#  1. PARSEAR FIX LOG (solo 35=X, trades 269=2)
# ══════════════════════════════════════════════════════════════════════════════
SETTL_MAP = {"|||":"T2","PH|||":"CASH","PM|||":"NEXT_DAY",
             "0":"REGULAR","1":"CASH","2":"NEXT_DAY","3":"T2"}

class Sym:
    __slots__ = ('symbol','first','last','high','low','volume','turnover',
                 'count','first_t','last_t')
    def __init__(self,sym):
        self.symbol=sym; self.first=None; self.last=None
        self.high=None; self.low=None; self.volume=0.0; self.turnover=0.0
        self.count=0; self.first_t=None; self.last_t=None
    def update(self,px,qty,amt,t):
        if self.first is None or (t and (self.first_t is None or t<self.first_t)):
            self.first=px; self.first_t=t
        if self.last_t is None or (t and t>=self.last_t):
            self.last=px; self.last_t=t
        if self.high is None or px>self.high: self.high=px
        if self.low  is None or px<self.low:  self.low=px
        self.volume   += max(0, qty or 0)
        self.turnover += max(0, amt or 0)
        self.count    += 1
    def var_pct(self):
        if self.first and self.first>0 and self.last:
            return (self.last-self.first)/self.first*100
        return 0.0

def parse_fix(log_path):
    print(f"  Parseando {os.path.basename(log_path)} ...")
    by_sym = {}   # symbol (consolidado, sin settlement) -> Sym
    total_trades = total_vol = total_mnt = 0

    with open(log_path, 'rb') as f:
        for line in f:
            if b'\x0135=X\x01' not in line: continue
            if b'269=2'         not in line: continue

            pairs=[]
            for field in line.split(b'\x01'):
                if b'=' in field:
                    k,_,v=field.partition(b'=')
                    try: pairs.append((int(k),v.decode('utf-8','replace').strip()))
                    except: pass

            msg_sym  = next((v for t,v in pairs if t==55), None)
            msg_466  = next((v for t,v in pairs if t==466), None)

            cur={}
            def flush(c):
                nonlocal total_trades, total_vol, total_mnt
                if c.get('et')!='2': return
                px = c.get('p')
                if not px or float(px)<=0: return
                px=float(px); qty=float(c.get('q') or 0)
                amt_raw=c.get('a')
                amt=float(amt_raw) if amt_raw else px*qty
                sym=(c.get('s') or msg_sym or 'UNK').strip().upper()
                t=c.get('t','')
                total_trades+=1; total_vol+=max(0,qty); total_mnt+=max(0,amt)
                if sym not in by_sym: by_sym[sym]=Sym(sym)
                by_sym[sym].update(px,qty,amt,t)

            for tag,val in pairs:
                if tag==269:
                    flush(cur); cur={'et':val}
                elif tag==270: cur['p']=val
                elif tag==271: cur['q']=val
                elif tag==811: cur['a']=val
                elif tag==273: cur['t']=val
                elif tag==55:  cur['s']=val
            flush(cur)

    return by_sym, total_trades, total_vol, total_mnt


def agg_fix(by_sym, total_trades, total_vol, total_mnt):
    syms=[s for s in by_sym.values() if s.last and s.last>0 and s.count>=1]
    n=len(syms)
    if n==0: return {}
    vol_prom_pct = (sum(abs(s.var_pct()) for s in syms)/n/100)*100  # promedio |var%|
    sent_pos = sum(1 for s in syms if s.var_pct()>0)/n*100
    sent_neg = sum(1 for s in syms if s.var_pct()<0)/n*100
    rango    = sum((s.high-s.low) for s in syms if s.high and s.low)/n
    idx_prom = sum(s.last*s.volume for s in syms if s.last)/total_vol if total_vol>0 else 0
    idx_max  = sum((s.high or 0)*s.volume for s in syms)/total_vol   if total_vol>0 else 0
    idx_min  = sum((s.low  or 0)*s.volume for s in syms)/total_vol   if total_vol>0 else 0
    tend     = sum(s.var_pct() for s in syms)/n
    liq      = (total_vol/n)/(total_mnt/n) if total_mnt>0 else 0
    cap_tot  = total_mnt
    cap_prom = total_mnt/n
    px_prom  = sum(s.last for s in syms)/n
    px_max   = sum(s.high for s in syms if s.high)  # SUM(high por instrumento) = precioMaximoAcumulado
    return {
        "N instrumentos":        n,
        "Volumen Total":         total_vol,
        "Monto Total":           total_mnt,
        "Capitalizacion Total":  sum(s.last*s.volume for s in syms if s.last),
        "N Total Trades":        total_trades,
        "Volatilidad Promedio %":vol_prom_pct,
        "Sentimiento Positivo %":sent_pos,
        "Sentimiento Negativo %":sent_neg,
        "Rango Promedio":        rango,
        "Indice Promedio":       idx_prom,
        "Indice Maximo":         idx_max,
        "Indice Minimo":         idx_min,
        "Tendencia Promedio %":  tend,
        "Liquidez Media":        liq,
        "Cap Promedio":          cap_prom,
        "Precio Prom Acum":      px_prom,
        "Precio Max Acum":       px_max,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  2. MONGODB — trades collection (raw diario)
# ══════════════════════════════════════════════════════════════════════════════
def query_mongo_trades(db, day_prefix):
    """
    Agrupa por (symbol, destination, settlement) igual que Java TradeAgg/buildKey.
    Luego deduplica: si mismo (symbol, exchange, settl) aparece dos veces, queda el de mayor monto.
    Para los totales (vol/mnt/trades) suma todo sin deduplicar.
    """
    print("  Consultando mongo/trades ...")
    by_key={}; total_trades=total_vol=total_mnt=0
    for doc in db["trades"].find({"eventTime":{"$regex":f"^{day_prefix}"}}):
        sym   = str(doc.get("symbol","")).strip().upper()
        dest  = str(doc.get("destination","BCS")).strip().upper()
        settl = str(doc.get("settlement","T2")).strip().upper()
        price = float(doc.get("price") or 0)
        qty   = float(doc.get("quantity") or 0)
        amt_v = doc.get("amount")
        amt   = float(amt_v) if amt_v else price*qty
        t     = str(doc.get("eventTime",""))
        if not sym or price<=0: continue
        total_trades+=1; total_vol+=max(0,qty); total_mnt+=max(0,amt)
        key = f"{sym}|{dest}|{settl}"
        if key not in by_key: by_key[key]=Sym(key)
        by_key[key].update(price,qty,amt,t)
    # dedup: mismo (sym|exchange|settl) => queda uno (ya están así agrupados)
    # Para agg_fix usamos by_key directamente; convertimos a by_sym solo para totales de la tabla
    # pero necesitamos el dict de Sym para agg_fix (que itera .values())
    return by_key, total_trades, total_vol, total_mnt


# ══════════════════════════════════════════════════════════════════════════════
#  3. MONGODB — instrument_stats (fuente real del BolsaStats del frontend)
# ══════════════════════════════════════════════════════════════════════════════
def query_instrument_stats(db, day_prefix):
    """
    InstrumentActor acumula totalVolume/totalTurnover desde el inicio de la sesion.
    Como se hace purgeDay antes de la inyeccion, estos valores son del dia actual.
    variationPct = dailyVariationPct = (lastPrice - previousClose) / previousClose.
    NO tiene high/low guardado en instrument_stats.
    """
    print("  Consultando mongo/instrument_stats ...")
    rows=[]
    for doc in db["instrument_stats"].find():
        sym   = str(doc.get("symbol","")).strip().upper()
        vol   = float(doc.get("totalVolume")   or 0)
        mnt   = float(doc.get("totalTurnover") or 0)
        trd   = int(doc.get("totalTrades")     or 0)
        last  = float(doc.get("lastPrice")     or 0)
        var   = float(doc.get("variationPct")  or 0)
        dvar  = float(doc.get("dailyVariationPct") or 0)
        vwap  = float(doc.get("vwapIntraday")  or 0)
        if last<=0 or trd==0: continue   # instrumento sin actividad
        rows.append({'sym':sym,'vol':vol,'mnt':mnt,'trd':trd,
                     'last':last,'var':var,'dvar':dvar,'vwap':vwap})

    if not rows: return {}
    n          = len(rows)
    total_vol  = sum(r['vol'] for r in rows)
    total_mnt  = sum(r['mnt'] for r in rows)
    total_trd  = sum(r['trd'] for r in rows)

    # Volatilidad = mean(|dailyVariationPct|) / 100 * 100  (igual que MongoMarketRepository)
    vol_prom_pct = sum(abs(r['dvar']) for r in rows)/n/100*100

    sent_pos = sum(1 for r in rows if r['dvar']>0)/n*100
    sent_neg = sum(1 for r in rows if r['dvar']<0)/n*100

    # Indice ponderado por volumen (usando lastPrice ya que no hay high/low en inst_stats)
    idx_prom = sum(r['last']*r['vol'] for r in rows)/total_vol if total_vol>0 else 0

    # Capitalizacion = SUM(lastPrice * totalVolume)  — "market cap proxy"
    cap_tot  = sum(r['last']*r['vol'] for r in rows)

    # Cap promedio y precio promedio
    cap_prom = cap_tot/n
    px_prom  = sum(r['last'] for r in rows)/n
    px_max   = sum(r['last'] for r in rows)  # SUM(lastPrice por instrumento) = precioMaximoAcumulado

    tend     = sum(r['dvar'] for r in rows)/n
    liq      = (total_vol/n)/(total_mnt/n) if total_mnt>0 else 0

    return {
        "N instrumentos":        n,
        "Volumen Total":         total_vol,
        "Monto Total":           total_mnt,
        "Capitalizacion Total":  cap_tot,
        "N Total Trades":        total_trd,
        "Volatilidad Promedio %":vol_prom_pct,
        "Sentimiento Positivo %":sent_pos,
        "Sentimiento Negativo %":sent_neg,
        "Rango Promedio":        0.0,   # no disponible en instrument_stats
        "Indice Promedio":       idx_prom,
        "Indice Maximo":         0.0,   # no disponible
        "Indice Minimo":         0.0,   # no disponible
        "Tendencia Promedio %":  tend,
        "Liquidez Media":        liq,
        "Cap Promedio":          cap_prom,
        "Precio Prom Acum":      px_prom,
        "Precio Max Acum":       px_max,
        "_rows":                 rows,  # para debug
    }


# ══════════════════════════════════════════════════════════════════════════════
#  4. MONGODB — candles diarios (para high/low reales del dia)
# ══════════════════════════════════════════════════════════════════════════════
def query_candles_daily(db, day_prefix):
    print("  Consultando mongo/candles (P1D) ...")
    candles={}
    for tf in ("P1D","PT24H"):
        for doc in db["candles"].find({"timeframe":tf,"bucketStart":{"$regex":f"^{day_prefix}"}}):
            sym  = str(doc.get("symbol","")).strip().upper()
            sett = str(doc.get("settlement",""))
            k    = sym+'|'+sett
            if k not in candles:
                candles[k]={
                    'sym':sym,'open':doc.get("open"),'high':doc.get("high"),
                    'low':doc.get("low"),'close':doc.get("close"),
                    'vol':doc.get("volume"),'mnt':doc.get("turnover"),
                    'trd':doc.get("trades")
                }
    return candles


def agg_candles(candles, inst_stats_rows):
    """Combina candles diarios con variation% de instrument_stats para full metrics."""
    # Mapear inst_stats por symbol
    var_by_sym = {r['sym']: r['dvar'] for r in inst_stats_rows} if inst_stats_rows else {}

    by_sym={}
    for k,c in candles.items():
        sym=c['sym']
        hi=float(c['high'] or 0); lo=float(c['low'] or 0)
        cl=float(c['close'] or 0); vol=float(c['vol'] or 0)
        if cl<=0: continue
        if sym not in by_sym:
            by_sym[sym]={'high':hi,'low':lo,'close':cl,'vol':vol,
                         'var':var_by_sym.get(sym,0)}
        else:
            b=by_sym[sym]
            if hi>b['high']: b['high']=hi
            if lo and lo<b['low']: b['low']=lo
            b['vol']+=vol

    if not by_sym: return {}
    rows=list(by_sym.values())
    n=len(rows)
    total_vol=sum(r['vol'] for r in rows)

    rango    = sum(r['high']-r['low'] for r in rows if r['high'] and r['low'])/n
    idx_prom = sum(r['close']*r['vol'] for r in rows)/total_vol if total_vol>0 else 0
    idx_max  = sum(r['high']*r['vol']  for r in rows)/total_vol if total_vol>0 else 0
    idx_min  = sum(r['low']*r['vol']   for r in rows if r['low'])/total_vol if total_vol>0 else 0
    px_max   = sum(r['high'] for r in rows)  # SUM(high por instrumento) = precioMaximoAcumulado
    return {
        "N instrumentos candles": n,
        "Rango Promedio (candle)": rango,
        "Indice Promedio (candle)":idx_prom,
        "Indice Maximo (candle)":  idx_max,
        "Indice Minimo (candle)":  idx_min,
        "Precio Max Acum (candle)":px_max,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  REPORTE
# ══════════════════════════════════════════════════════════════════════════════
METRICS = [
    ("Volumen Total",           0,  1.0),
    ("Monto Total",             2,  1.0),
    ("Capitalizacion Total",    2,  2.0),
    ("N Total Trades",          0,  0.5),
    ("Volatilidad Promedio %",  4, 20.0),
    ("Sentimiento Positivo %",  4, 20.0),
    ("Sentimiento Negativo %",  4, 20.0),
    ("Rango Promedio",          6, 10.0),
    ("Indice Promedio",         6,  2.0),
    ("Indice Maximo",           6,  2.0),
    ("Indice Minimo",           6,  2.0),
    ("Tendencia Promedio %",    6, 50.0),
    ("Liquidez Media",          6, 10.0),
    ("Cap Promedio",            2, 10.0),
    ("Precio Prom Acum",        2, 10.0),
    ("Precio Max Acum",         2, 20.0),
]

def report(fix_m, trades_m, inst_m, candle_extra, out_path):
    lines=[]
    def w(s=""): lines.append(str(s))

    day_fmt = f"{DATE_STR[:4]}-{DATE_STR[4:6]}-{DATE_STR[6:8]}"

    w(SEP)
    w(f"  VALIDACION ESTADISTICAS  |  {day_fmt}")
    w(f"  Fuentes: FRONTEND (usuario) | MONGO/instrument_stats | MONGO/trades | FIX log 35=X")
    w(SEP)
    w()

    # Header
    H = f"  {'METRICA':<28} {'FRONTEND':>18} {'INST_STATS':>18} {'TRADES_DB':>18} {'FIX_35X':>18}"
    w(H)
    w(f"  {'':28} {'':18} {'diff':>10} {'ok':>6} {'diff':>10} {'ok':>6} {'diff':>10} {'ok':>6}")
    w(f"  {SEP2}")

    problems = []

    for key, dec, tol in METRICS:
        fe  = FRONTEND.get(key)
        im  = inst_m.get(key)
        tm  = trades_m.get(key)
        fx  = fix_m.get(key)

        def fmt(v): return f"{float(v):>18,.{dec}f}" if v is not None else f"{'N/A':>18}"

        ok_im = ok(im, fe, tol) if im is not None and fe else "N/A"
        ok_tm = ok(tm, fe, tol) if tm is not None and fe else "N/A"
        ok_fx = ok(fx, fe, tol) if fx is not None and fe else "N/A"

        d_im  = diff_pct(im, fe) if im is not None and fe else "N/A"
        d_tm  = diff_pct(tm, fe) if tm is not None and fe else "N/A"
        d_fx  = diff_pct(fx, fe) if fx is not None and fe else "N/A"

        w(f"  {key:<28} {fmt(fe)} {fmt(im)}")
        w(f"  {'':28} {'':18} {d_im:>10} {ok_im:>6} {d_tm:>10} {ok_tm:>6} {d_fx:>10} {ok_fx:>6}")
        w(f"  {'':28} {'':18} {'':18} {fmt(tm)} {fmt(fx)}")
        w()

        if ok_im == "DIFF": problems.append(f"{key}: inst_stats diff {d_im}")
        if ok_tm == "DIFF" and abs(float(d_tm.replace('%','').replace('+','') or 0)) > tol:
            problems.append(f"{key}: trades_db diff {d_tm}")

    # Candles extra
    w(SEP)
    w("  METRICAS CORREGIDAS USANDO CANDLES P1D (high/low reales del dia)")
    w(SEP)
    for k,v in candle_extra.items():
        fe_ref = FRONTEND.get(k.replace(" (candle)","").strip())
        d = diff_pct(v, fe_ref) if fe_ref else ""
        o = ok(v, fe_ref, 2.0) if fe_ref else ""
        w(f"  {k:<35} {float(v):>18,.6f}   frontend={float(fe_ref):>18,.6f}  diff={d}  {o}" if fe_ref else
          f"  {k:<35} {float(v):>18,.6f}")
    w()

    # Diagnostico
    w(SEP)
    w("  DIAGNOSTICO DETALLADO")
    w(SEP)

    fe_trd = FRONTEND["N Total Trades"]
    im_trd = inst_m.get("N Total Trades",0)
    tm_trd = trades_m.get("N Total Trades",0)
    fx_trd = fix_m.get("N Total Trades",0)

    fe_vol = FRONTEND["Volumen Total"]
    im_vol = inst_m.get("Volumen Total",0)
    tm_vol = trades_m.get("Volumen Total",0)
    fx_vol = fix_m.get("Volumen Total",0)

    fe_mnt = FRONTEND["Monto Total"]
    tm_mnt = trades_m.get("Monto Total",0)
    fx_mnt = fix_m.get("Monto Total",0)

    fe_cap = FRONTEND["Capitalizacion Total"]
    im_cap = inst_m.get("Capitalizacion Total",0)

    w()
    w("  [TRADES]")
    w(f"    Frontend     : {fe_trd:>12,.0f}")
    w(f"    instrument_stats : {im_trd:>12,.0f}   diff={im_trd-fe_trd:+,.0f}  {diff_pct(im_trd,fe_trd)}")
    w(f"    trades (db)  : {tm_trd:>12,.0f}   diff={tm_trd-fe_trd:+,.0f}  {diff_pct(tm_trd,fe_trd)}")
    w(f"    FIX 35=X     : {fx_trd:>12,.0f}   diff={fx_trd-fe_trd:+,.0f}  {diff_pct(fx_trd,fe_trd)}")
    extra = tm_trd - fx_trd
    if extra > 0:
        w(f"    => Mongo tiene {extra:,} trades extra vs FIX 35=X")
        w(f"       Causa: inyeccion previa sin purge O snapshot trades (35=W) procesados")
    w()
    w("  [VOLUMEN]")
    w(f"    Frontend         : {fe_vol:>20,.0f}")
    w(f"    instrument_stats : {im_vol:>20,.0f}   diff={diff_pct(im_vol,fe_vol)}")
    w(f"    trades (db)      : {tm_vol:>20,.0f}   diff={diff_pct(tm_vol,fe_vol)}")
    w(f"    FIX 35=X         : {fx_vol:>20,.0f}   diff={diff_pct(fx_vol,fe_vol)}")
    w()
    w("  [MONTO vs CAPITALIZACION]")
    w(f"    Monto Total (frontend)         : {fe_mnt:>24,.2f}  <- suma de price*qty de cada trade")
    w(f"    Capitalizacion (frontend)      : {fe_cap:>24,.2f}  <- diferencia de {fe_cap-fe_mnt:+,.2f}")
    w(f"    Capitalizacion calc inst_stats : {im_cap:>24,.2f}  <- SUM(lastPrice*totalVolume)")
    w(f"    Diff cap frontend vs inst_stats: {im_cap-fe_cap:+,.2f}  ({diff_pct(im_cap,fe_cap)})")
    w()
    w("  [SENTIMIENTO / VOLATILIDAD]")
    w(f"    Sentimiento Positivo frontend  : {FRONTEND['Sentimiento Positivo %']:.4f}%")
    w(f"    Sentimiento Positivo inst_stats: {inst_m.get('Sentimiento Positivo %',0):.4f}%  ({inst_m.get('N instrumentos',0)} instrumentos)")
    w(f"    Sentimiento Positivo trades_db : {trades_m.get('Sentimiento Positivo %',0):.4f}%  ({trades_m.get('N instrumentos',0)} instrumentos)")
    w(f"    Sentimiento Positivo FIX       : {fix_m.get('Sentimiento Positivo %',0):.4f}%  ({fix_m.get('N instrumentos',0)} instrumentos)")
    w()
    w(f"    Volatilidad Promedio frontend  : {FRONTEND['Volatilidad Promedio %']:.4f}%")
    w(f"    Volatilidad Promedio inst_stats: {inst_m.get('Volatilidad Promedio %',0):.4f}%")
    w(f"    Volatilidad Promedio trades_db : {trades_m.get('Volatilidad Promedio %',0):.4f}%")
    w(f"    Volatilidad Promedio FIX       : {fix_m.get('Volatilidad Promedio %',0):.4f}%")
    w()
    w("  [PRECIO MAX ACUM]")
    w(f"    Frontend         : {FRONTEND['Precio Max Acum']:>15,.2f}  <- SUM(precioMaximo por instrumento) segun CandleProtoMarketPublisher.java:322")
    w(f"    instrument_stats : {inst_m.get('Precio Max Acum',0):>15,.2f}  <- SUM(lastPrice) de inst_stats (no tiene dayHigh)")
    w(f"    candle P1D       : {candle_extra.get('Precio Max Acum (candle)',0):>15,.2f}  <- SUM(high) de candles P1D del dia")
    w(f"    FIX              : {fix_m.get('Precio Max Acum',0):>15,.2f}  <- SUM(high intradía) del log")
    w()
    w("  [PROBLEMAS IDENTIFICADOS]")
    if problems:
        for p in problems: w(f"    ! {p}")
    else:
        w("    Ninguno dentro de tolerancia.")
    w()
    w(SEP)

    txt = "\n".join(lines)
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    print(txt)
    with open(out_path,'w',encoding='utf-8') as f: f.write(txt+"\n")
    print(f"\n[OK] Guardado: {out_path}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    log_path = os.path.join(LOG_DIR, LOG_PATTERN.format(date=DATE_STR))
    out_path = os.path.join(LOG_DIR, f"validate_stats_{DATE_STR}.txt")
    day_pfx  = f"{DATE_STR[:4]}-{DATE_STR[4:6]}-{DATE_STR[6:8]}"

    print(f"[1/5] FIX log ...")
    fix_by_sym, fx_trd, fx_vol, fx_mnt = parse_fix(log_path)
    fix_m = agg_fix(fix_by_sym, fx_trd, fx_vol, fx_mnt)
    print(f"      {fx_trd:,} trades | {fix_m.get('N instrumentos',0)} simbolos | vol={fx_vol:,.0f}")

    from pymongo import MongoClient
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
    db = client[MONGO_DATABASE]

    print(f"[2/5] MongoDB trades ...")
    trades_by_sym, tm_trd, tm_vol, tm_mnt = query_mongo_trades(db, day_pfx)
    trades_m = agg_fix(trades_by_sym, tm_trd, tm_vol, tm_mnt)
    print(f"      {tm_trd:,} trades | {trades_m.get('N instrumentos',0)} simbolos | vol={tm_vol:,.0f}")

    print(f"[3/5] MongoDB instrument_stats ...")
    inst_m = query_instrument_stats(db, day_pfx)
    print(f"      {inst_m.get('N instrumentos',0)} instrumentos activos | vol={inst_m.get('Volumen Total',0):,.0f}")

    print(f"[4/5] MongoDB candles P1D ...")
    candles = query_candles_daily(db, day_pfx)
    inst_rows = inst_m.pop("_rows", [])
    candle_extra = agg_candles(candles, inst_rows)
    print(f"      {len(candles)} candles diarios")

    client.close()

    print(f"[5/5] Generando reporte ...")
    report(fix_m, trades_m, inst_m, candle_extra, out_path)

if __name__ == "__main__":
    main()

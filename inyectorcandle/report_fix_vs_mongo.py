#!/usr/bin/env python3
"""
report_fix_vs_mongo.py
======================
Genera un reporte completo del dia de mercado a partir del FIX log
y lo compara con los datos en MongoDB.

Uso:
  python report_fix_vs_mongo.py [fecha YYYYMMDD]
  Ejemplo: python report_fix_vs_mongo.py 20260309

Si no se especifica fecha, usa el dia de ayer (segun el sistema).
"""

import re
import sys
import os
from datetime import date, timedelta
from collections import defaultdict, Counter
from typing import Optional

# -------------------------------------------------------------
#  CONFIG  (editar si cambia el entorno)
# -------------------------------------------------------------
MONGO_URI       = "mongodb://admin:10wfyaxqxk2hq@68.211.112.146:27017"
MONGO_DATABASE  = "inyectorcandle"
LOG_DIR         = r"E:\VC-GITHUB\vector-trade-2.0\inyectorcandle\logs\fix-logs"
LOG_PATTERN     = "FIX.4.4-inyectorcandle-BCSGATEWAY.messages_{date}.log"
# Zona horaria Santiago: UTC-3 (en verano), UTC-4 (en invierno)
# El log FIX usa UTC en los campos 272/273; el timestamp de linea es local
# -------------------------------------------------------------

def usage():
    print(__doc__)
    sys.exit(0)

# --- FIX: normalizar settlement -------------------------------
SETTL_MAP = {
    "|||":   "T2",
    "PH|||": "CASH",
    "PM|||": "NEXT_DAY",
    "T+3|||":"T3",
    "T+5|||":"T5",
    "0":     "REGULAR",
    "1":     "CASH",
    "2":     "NEXT_DAY",
    "3":     "T2",
    "4":     "T3",
    "5":     "T4",
    "6":     "FUTURE",
    "7":     "WHEN_ISSUED",
    "8":     "SELLERS_OPTION",
    "9":     "T5",
    "B":     "BROKEN_DATE",
}

def normalize_settlement(raw: Optional[str]) -> str:
    if not raw:
        return "UNKNOWN"
    return SETTL_MAP.get(raw.strip().upper(), raw.strip().upper())

# --- FIX: parsear mensaje en campos ---------------------------
def parse_fix_fields(msg_bytes: bytes) -> dict:
    """Retorna dict tag(int) -> list[str] para manejar grupos repetidos."""
    result = defaultdict(list)
    for field in msg_bytes.split(b'\x01'):
        if b'=' not in field:
            continue
        k, _, v = field.partition(b'=')
        try:
            result[int(k)].append(v.decode('utf-8', errors='replace'))
        except ValueError:
            pass
    return result

def parse_fix_groups(fields: dict, group_delimiter_tag: int) -> list:
    """
    Divide los campos planos en grupos usando como separador
    el tag que indica inicio de grupo (ej: 269 = MDEntryType).
    """
    # reconstruir la lista ordenada de (tag, value) conservando orden de aparicion
    # necesitamos el raw para eso; se resuelve parseando en lista
    return []  # placeholder; se usa la logica inline mas abajo

def parse_fix_line(line: bytes):
    """
    Retorna (timestamp_str, msg_type, fields_list) donde fields_list
    es [(tag, value), ...] con TODOS los pares en orden de aparicion.
    Retorna None si la linea no es un mensaje FIX valido.
    """
    m = re.match(rb'^(\d{8} \d{2}:\d{2}:\d{2}\.\d+): (.+)$', line.strip())
    if not m:
        return None
    ts_raw, msg_bytes = m.group(1).decode(), m.group(2)

    fields_list = []
    msg_type = None
    for field in msg_bytes.split(b'\x01'):
        if b'=' not in field:
            continue
        k, _, v = field.partition(b'=')
        try:
            tag = int(k)
            val = v.decode('utf-8', errors='replace')
            fields_list.append((tag, val))
            if tag == 35:
                msg_type = val
        except ValueError:
            pass

    return ts_raw, msg_type, fields_list


def extract_trade_groups(fields_list: list) -> list:
    """
    Extrae grupos de trades (269=2) de la lista plana de campos.
    Devuelve lista de dicts con keys: price, qty, time, settlement, symbol, exchange,
    security_id, currency, update_action.
    """
    # Indice del 55 y 207 a nivel de mensaje
    msg_symbol   = None
    msg_exchange = None
    msg_currency = None
    msg_security_id = None
    for tag, val in fields_list:
        if tag == 55 and msg_symbol is None:     msg_symbol   = val
        if tag == 207 and msg_exchange is None:  msg_exchange = val
        if tag == 15 and msg_currency is None:   msg_currency = val
        if tag == 48 and msg_security_id is None:msg_security_id = val

    # Dividir en grupos usando 279 (incrementales) o 269 como primer tag del grupo
    # Strategy: cada vez que vemos 269 en la lista, iniciamos un nuevo grupo
    groups = []
    current = {}
    in_group = False

    for tag, val in fields_list:
        if tag == 269:
            if in_group and current.get('entry_type') == '2':
                groups.append(current)
            current = {'entry_type': val}
            in_group = True
        elif in_group:
            if tag == 270: current['price']       = val
            elif tag == 271: current['qty']        = val
            elif tag == 273: current['time']       = val
            elif tag == 272: current['date']       = val
            elif tag == 466: current['settl_466']  = val
            elif tag == 446: current['settl_446']  = val
            elif tag == 876: current['settl_876']  = val
            elif tag == 63:  current['settl_63']   = val
            elif tag == 55:  current['symbol']     = val
            elif tag == 207: current['exchange']   = val
            elif tag == 48:  current['security_id']= val
            elif tag == 15:  current['currency']   = val
            elif tag == 279: current['update_action'] = val
            elif tag == 58:  current['settl_date_text'] = val   # settlement date
            elif tag == 811: current['amount']     = val        # custom amount tag

    if in_group and current.get('entry_type') == '2':
        groups.append(current)

    # Completar campos faltantes con datos del mensaje
    trades = []
    for g in groups:
        try:
            price = float(g.get('price', 0) or 0)
            qty   = float(g.get('qty',   0) or 0)
            if price <= 0:
                continue
            amount_raw = g.get('amount')
            amount = float(amount_raw) if amount_raw else price * qty

            # settlement: prioridad 446 > 466 > 876 > 63
            raw_settl = g.get('settl_446') or g.get('settl_466') or g.get('settl_876') or g.get('settl_63')
            settlement = normalize_settlement(raw_settl)

            symbol   = g.get('symbol')   or msg_symbol   or 'UNKNOWN'
            exchange = g.get('exchange') or msg_exchange or 'UNKNOWN'
            currency = g.get('currency') or msg_currency or 'CLP'
            sec_id   = g.get('security_id') or msg_security_id or ''

            # OFS = operaciones en USD o bolsas extranjeras
            is_ofs = (currency == 'USD') or (exchange not in ('XSGO', 'UNKNOWN', ''))

            trades.append({
                'symbol':     symbol.strip(),
                'settlement': settlement,
                'exchange':   exchange,
                'currency':   currency,
                'security_id':sec_id,
                'is_ofs':     is_ofs,
                'price':      price,
                'qty':        qty,
                'amount':     amount,
                'time':       g.get('time', ''),
                'date':       g.get('date', ''),
            })
        except (ValueError, TypeError):
            continue
    return trades


# --- Aggregator -----------------------------------------------
class SymbolStats:
    __slots__ = ('symbol','settlement','currency','is_ofs',
                 'first_price','last_price','high','low',
                 'volume','turnover','count',
                 'first_time','last_time')

    def __init__(self, symbol, settlement, currency, is_ofs):
        self.symbol     = symbol
        self.settlement = settlement
        self.currency   = currency
        self.is_ofs     = is_ofs
        self.first_price = None
        self.last_price  = None
        self.high        = None
        self.low         = None
        self.volume      = 0.0
        self.turnover    = 0.0
        self.count       = 0
        self.first_time  = None
        self.last_time   = None

    def update(self, price, qty, amount, time_str):
        if self.first_price is None:
            self.first_price = price
            self.high = price
            self.low  = price
            self.first_time = time_str
        else:
            if price > self.high: self.high = price
            if price < self.low:  self.low  = price
        self.last_price = price
        self.last_time  = time_str
        self.volume   += qty
        self.turnover += amount
        self.count    += 1

    @property
    def variation_pct(self):
        if self.first_price and self.first_price > 0:
            return ((self.last_price - self.first_price) / self.first_price) * 100
        return 0.0

    @property
    def vwap(self):
        if self.volume > 0:
            return self.turnover / self.volume
        return 0.0

    def key(self):
        return f"{self.symbol}|{self.settlement}"


# --- PARSER PRINCIPAL ----------------------------------------
def parse_fix_log(log_path: str):
    """
    Parsea el log FIX y retorna dos conjuntos de stats:
    - 'stats_X':  solo trades de mensajes 35=X (incrementales) -> lo que inyecta la app
    - 'stats_all': todos los trades (W+X) -> referencia completa del dia
    """
    stats_all: dict[str, SymbolStats] = {}
    stats_X:   dict[str, SymbolStats] = {}
    total_lines = 0
    total_msgs  = 0
    trade_groups_all = 0
    trade_groups_X   = 0
    trade_groups_W   = 0
    msg_type_counts = Counter()
    first_time = None
    last_time  = None
    settl_counts_all = Counter()
    settl_counts_X   = Counter()

    with open(log_path, 'rb') as f:
        for line in f:
            total_lines += 1
            parsed = parse_fix_line(line)
            if parsed is None:
                continue
            ts, msg_type, fields_list = parsed
            total_msgs += 1
            msg_type_counts[msg_type] += 1

            if msg_type not in ('W', 'X'):
                continue

            trades = extract_trade_groups(fields_list)
            for t in trades:
                trade_groups_all += 1
                settl_counts_all[t['settlement']] += 1

                if first_time is None or t['time'] < first_time:
                    first_time = t['time']
                if last_time is None or t['time'] > last_time:
                    last_time = t['time']

                k = t['symbol'] + '|' + t['settlement']

                # --- ALL (W + X) ---
                if k not in stats_all:
                    stats_all[k] = SymbolStats(t['symbol'], t['settlement'], t['currency'], t['is_ofs'])
                stats_all[k].update(t['price'], t['qty'], t['amount'], t['time'])

                # --- Solo X (lo que el inyector procesa) ---
                if msg_type == 'X':
                    trade_groups_X += 1
                    settl_counts_X[t['settlement']] += 1
                    if k not in stats_X:
                        stats_X[k] = SymbolStats(t['symbol'], t['settlement'], t['currency'], t['is_ofs'])
                    stats_X[k].update(t['price'], t['qty'], t['amount'], t['time'])
                else:
                    trade_groups_W += 1

    return {
        'stats_all':       stats_all,
        'stats_X':         stats_X,
        'total_lines':     total_lines,
        'total_msgs':      total_msgs,
        'trade_groups_all':trade_groups_all,
        'trade_groups_X':  trade_groups_X,
        'trade_groups_W':  trade_groups_W,
        'msg_counts':      msg_type_counts,
        'first_time':      first_time,
        'last_time':       last_time,
        'settl_counts_all':settl_counts_all,
        'settl_counts_X':  settl_counts_X,
        # Backward compat
        'stats':           stats_X,       # <- comparacion principal vs mongo usa X
        'trade_groups':    trade_groups_X,
        'settl_counts':    settl_counts_X,
    }


# --- MONGO ----------------------------------------------------
def query_mongo(date_str: str):
    """
    Consulta MongoDB para el dia indicado (YYYYMMDD).
    Retorna dict con datos de trades, candles, stats, rankings.
    """
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()  # test connection
        db = client[MONGO_DATABASE]

        # Rango de fecha UTC (el log usa UTC en los timestamps de eventTime)
        day_str  = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        day_next = f"{date_str[:4]}-{date_str[4:6]}-{int(date_str[6:8])+1:02d}"
        # Para meses con 30/31 dias se necesita manejo mas robusto,
        # pero usaremos string prefix para simplicidad:
        day_prefix = day_str  # "2026-03-09"

        # - Trades -
        mongo_trades: dict[str, SymbolStats] = {}
        trades_coll = db["trades"]
        total_mongo_trades = 0

        for doc in trades_coll.find({
            "eventTime": {"$regex": f"^{day_prefix}"}
        }):
            symbol     = str(doc.get("symbol", "")).strip().upper()
            settlement = str(doc.get("settlement", ""))
            currency   = str(doc.get("currency", "CLP"))
            price      = float(doc.get("price")    or 0)
            qty        = float(doc.get("quantity") or 0)
            amount_v   = doc.get("amount")
            amount     = float(amount_v) if amount_v else price * qty
            event_time = str(doc.get("eventTime", ""))
            if not symbol or price <= 0:
                continue
            total_mongo_trades += 1
            k = symbol + '|' + settlement
            if k not in mongo_trades:
                is_ofs = (currency == 'USD')
                mongo_trades[k] = SymbolStats(symbol, settlement, currency, is_ofs)
            mongo_trades[k].update(price, qty, amount, event_time)

        # - Candles diarios -
        candles_daily = {}
        candles_coll = db["candles"]
        for doc in candles_coll.find({
            "timeframe": "PT24H",
            "bucketStart": {"$regex": f"^{day_prefix}"}
        }):
            sym  = str(doc.get("symbol","")).strip().upper()
            sett = str(doc.get("settlement",""))
            k    = sym + '|' + sett
            candles_daily[k] = {
                'open':     doc.get("open"),
                'high':     doc.get("high"),
                'low':      doc.get("low"),
                'close':    doc.get("close"),
                'volume':   doc.get("volume"),
                'turnover': doc.get("turnover"),
                'trades':   doc.get("trades"),
            }
        # Intentar tambien con P1D (ISO duration)
        if not candles_daily:
            for doc in candles_coll.find({
                "timeframe": "P1D",
                "bucketStart": {"$regex": f"^{day_prefix}"}
            }):
                sym  = str(doc.get("symbol","")).strip().upper()
                sett = str(doc.get("settlement",""))
                k    = sym + '|' + sett
                candles_daily[k] = {
                    'open':     doc.get("open"),
                    'high':     doc.get("high"),
                    'low':      doc.get("low"),
                    'close':    doc.get("close"),
                    'volume':   doc.get("volume"),
                    'turnover': doc.get("turnover"),
                    'trades':   doc.get("trades"),
                }

        # - instrument_stats -
        inst_stats = {}
        for doc in db["instrument_stats"].find():
            sym  = str(doc.get("symbol","")).strip().upper()
            sett = str(doc.get("settlement",""))
            k    = sym + '|' + sett
            inst_stats[k] = {
                'totalTrades':    doc.get("totalTrades",   0),
                'totalVolume':    doc.get("totalVolume",   0),
                'totalTurnover':  doc.get("totalTurnover", 0),
                'lastPrice':      doc.get("lastPrice",     0),
                'vwapIntraday':   doc.get("vwapIntraday",  0),
                'variationPct':   doc.get("variationPct",  0),
            }

        # - Rankings -
        ranking_doc = db["market_rankings"].find_one({"_id": "latest"})

        client.close()
        return {
            'ok':               True,
            'mongo_trades':     mongo_trades,
            'total_trades':     total_mongo_trades,
            'candles_daily':    candles_daily,
            'inst_stats':       inst_stats,
            'ranking_doc':      ranking_doc,
        }

    except Exception as e:
        return {'ok': False, 'error': str(e)}


# --- FORMATEO ------------------------------------------------
def fmt_num(v, decimals=0):
    if v is None: return "N/A"
    fmt = f"{{:,.{decimals}f}}"
    return fmt.format(v)

def fmt_pct(v):
    if v is None: return "N/A"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"

def bar(value, max_val, width=20):
    if max_val <= 0: return " " * width
    filled = int(round(value / max_val * width))
    return "#" * filled + "." * (width - filled)

SEP_DOUBLE = "=" * 80
SEP_SINGLE = "-" * 80
SEP_THIN   = "." * 80


# --- REPORTE -------------------------------------------------
def generate_report(date_str: str, log_result: dict, mongo_result: dict, output_file: str):

    lines_out = []
    def w(s=""): lines_out.append(str(s))

    fix_stats    = log_result['stats']
    mongo_ok     = mongo_result.get('ok', False)
    mongo_trades = mongo_result.get('mongo_trades', {})

    # --- Agrupar por simbolo (consolidado sin importar settlement) -
    # Para ranking global
    by_symbol: dict[str, SymbolStats] = {}
    for k, s in fix_stats.items():
        sym = s.symbol
        if sym not in by_symbol:
            by_symbol[sym] = SymbolStats(sym, s.settlement, s.currency, s.is_ofs)
        bs = by_symbol[sym]
        # merge
        if bs.first_price is None or (s.first_time and s.first_time < (bs.first_time or '')):
            bs.first_price = s.first_price
            bs.first_time  = s.first_time
        if bs.last_price is None or (s.last_time and s.last_time > (bs.last_time or '')):
            bs.last_price = s.last_price
            bs.last_time  = s.last_time
        if bs.high is None or (s.high and s.high > bs.high): bs.high = s.high
        if bs.low  is None or (s.low  and s.low  < bs.low ): bs.low  = s.low
        bs.volume   += s.volume
        bs.turnover += s.turnover
        bs.count    += s.count

    all_syms = sorted(by_symbol.values(), key=lambda x: x.turnover, reverse=True)
    all_rows = list(fix_stats.values())  # incluye por settlement

    total_fix_volume   = sum(s.volume   for s in all_rows)
    total_fix_turnover = sum(s.turnover for s in all_rows)
    total_fix_trades   = sum(s.count    for s in all_rows)
    n_instruments      = len(by_symbol)
    n_ofs              = sum(1 for s in all_syms if s.is_ofs)
    n_local            = n_instruments - n_ofs

    settl_counts = log_result['settl_counts']

    # --------------------------------------------------
    w(SEP_DOUBLE)
    w(f"  REPORTE DE MERCADO  {date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")
    w(f"  Generado a partir de: {os.path.basename(LOG_PATTERN.format(date=date_str))}")
    w(SEP_DOUBLE)
    w()

    # --- RESUMEN GENERAL FIX --------------------------
    stats_all = log_result.get('stats_all', fix_stats)
    all_rows_all = list(stats_all.values())
    total_all_vol = sum(s.volume   for s in all_rows_all)
    total_all_mnt = sum(s.turnover for s in all_rows_all)

    w("+-- RESUMEN FIX LOG " + "=" * 61)
    w(f"  Lineas en log          : {fmt_num(log_result['total_lines'])}")
    w(f"  Mensajes FIX totales   : {fmt_num(log_result['total_msgs'])}")
    w(f"  Subscripciones (35=V)  : {fmt_num(log_result['msg_counts'].get('V',0))}")
    w(f"  Snapshots    (35=W)    : {fmt_num(log_result['msg_counts'].get('W',0))}")
    w(f"  Incrementales(35=X)    : {fmt_num(log_result['msg_counts'].get('X',0))}")
    w(f"  Primer trade (UTC)     : {log_result['first_time']}")
    w(f"  Ultimo trade (UTC)     : {log_result['last_time']}")
    w()
    w("  NOTA: fix.process.snapshot.trades=false => inyector solo procesa 35=X")
    w(f"  {'':30} {'35=W snapshots':>16} {'35=X incrementales':>20} {'TOTAL':>12}")
    w(f"  {SEP_SINGLE[:82]}")
    w(f"  {'Trades 269=2':<30} {fmt_num(log_result.get('trade_groups_W',0)):>16} {fmt_num(log_result.get('trade_groups_X', log_result['trade_groups'])):>20} {fmt_num(log_result.get('trade_groups_all', log_result['trade_groups'])):>12}")
    w(f"  {'Instrumentos':<30} {'(historico)':>16} {fmt_num(n_instruments):>20}")
    w(f"  {'Volumen total':<30} {'':>16} {fmt_num(total_fix_volume):>20} {fmt_num(total_all_vol):>12}")
    w(f"  {'Monto total':<30} {'':>16} {fmt_num(total_fix_turnover):>20} {fmt_num(total_all_mnt):>12}")
    w()

    w("  Por liquidacion (solo 35=X - lo inyectado a Mongo):")
    for settl, cnt in sorted(log_result.get('settl_counts_X', log_result['settl_counts']).items()):
        rows_s = [s for s in all_rows if s.settlement == settl]
        vol_s  = sum(s.volume   for s in rows_s)
        mnt_s  = sum(s.turnover for s in rows_s)
        nsym   = len(set(s.symbol for s in rows_s))
        label  = f"  {settl:<12} : {cnt:>6} trades | {nsym:>4} instrumentos | vol={fmt_num(vol_s):>15} | monto={fmt_num(mnt_s):>20}"
        w(label)
    w()

    # --- RESUMEN MONGO -------------------------------
    w("+-- RESUMEN MONGODB " + "=" * 61)
    if not mongo_ok:
        w(f"  !  No se pudo conectar a MongoDB: {mongo_result.get('error','')}")
    else:
        total_mongo = mongo_result['total_trades']
        n_mongo_syms = len(set(s.symbol for s in mongo_trades.values()))
        total_mongo_vol = sum(s.volume   for s in mongo_trades.values())
        total_mongo_mnt = sum(s.turnover for s in mongo_trades.values())
        w(f"  Trades en coleccion 'trades': {fmt_num(total_mongo)}")
        w(f"  Instrumentos en trades      : {n_mongo_syms}")
        w(f"  Volumen total mongo         : {fmt_num(total_mongo_vol)}")
        w(f"  Monto total mongo           : {fmt_num(total_mongo_mnt)}")
        w(f"  Candles diarios en mongo    : {len(mongo_result['candles_daily'])}")
        w(f"  instrument_stats en mongo   : {len(mongo_result['inst_stats'])}")
    w()

    # --- COMPARACION FIX vs MONGO --------------------
    w("+-- COMPARACION FIX vs MONGO " + "=" * 52)
    if not mongo_ok:
        w("  (MongoDB no disponible - solo datos FIX)")
    else:
        total_mongo = mongo_result['total_trades']
        diff_trades = log_result['trade_groups'] - total_mongo
        diff_vol    = total_fix_volume - sum(s.volume   for s in mongo_trades.values())
        diff_mnt    = total_fix_turnover - sum(s.turnover for s in mongo_trades.values())

        w(f"  {'Metrica':<25} {'FIX LOG':>15} {'MONGODB':>15} {'DIFERENCIA':>15}")
        w(f"  {SEP_SINGLE[:70]}")
        w(f"  {'Trades totales':<25} {fmt_num(log_result['trade_groups']):>15} {fmt_num(total_mongo):>15} {fmt_num(diff_trades):>15}  {'! REVISAR' if abs(diff_trades) > 0 else 'OK OK'}")
        w(f"  {'Volumen total':<25} {fmt_num(total_fix_volume):>15} {fmt_num(sum(s.volume for s in mongo_trades.values())):>15} {fmt_num(diff_vol):>15}")
        w(f"  {'Monto total':<25} {fmt_num(total_fix_turnover):>15} {fmt_num(sum(s.turnover for s in mongo_trades.values())):>15} {fmt_num(diff_mnt):>15}")
    w()

    # --- TOP 10 MAS TRANZADOS (por monto) ------------
    w("+-- TOP 10 MAS TRANZADOS por MONTO " + "=" * 45)
    top10_monto = sorted(all_syms, key=lambda x: x.turnover, reverse=True)[:10]
    max_mnt = top10_monto[0].turnover if top10_monto else 1
    w(f"  {'#':<3} {'SIMBOLO':<14} {'SETTL':<12} {'CLOSE':>10} {'VAR%':>8} {'VOLUME':>12} {'MONTO':>18} {'TRADES':>7}")
    w(f"  {SEP_SINGLE[:80]}")
    for i, s in enumerate(top10_monto, 1):
        w(f"  {i:<3} {s.symbol:<14} {s.settlement:<12} {fmt_num(s.last_price,2):>10} "
          f"{fmt_pct(s.variation_pct):>8} {fmt_num(s.volume):>12} {fmt_num(s.turnover):>18} {fmt_num(s.count):>7}"
          f"  {bar(s.turnover, max_mnt, 15)}")
    w()

    # --- TOP 10 MENOS TRANZADOS ----------------------
    w("+-- TOP 10 MENOS TRANZADOS por MONTO " + "=" * 43)
    bottom10_monto = sorted(all_syms, key=lambda x: x.turnover)[:10]
    w(f"  {'#':<3} {'SIMBOLO':<14} {'SETTL':<12} {'CLOSE':>10} {'VAR%':>8} {'VOLUME':>12} {'MONTO':>18} {'TRADES':>7}")
    w(f"  {SEP_SINGLE[:80]}")
    for i, s in enumerate(bottom10_monto, 1):
        w(f"  {i:<3} {s.symbol:<14} {s.settlement:<12} {fmt_num(s.last_price,2):>10} "
          f"{fmt_pct(s.variation_pct):>8} {fmt_num(s.volume):>12} {fmt_num(s.turnover):>18} {fmt_num(s.count):>7}")
    w()

    # --- TOP 10 MEJORES ------------------------------
    w("+-- TOP 10 MEJORES (mayor variacion %) " + "=" * 41)
    gainers = sorted([s for s in all_syms if s.count >= 2], key=lambda x: x.variation_pct, reverse=True)[:10]
    w(f"  {'#':<3} {'SIMBOLO':<14} {'OPEN':>10} {'CLOSE':>10} {'HIGH':>10} {'LOW':>10} {'VAR%':>8} {'TRADES':>7}")
    w(f"  {SEP_SINGLE[:80]}")
    for i, s in enumerate(gainers, 1):
        w(f"  {i:<3} {s.symbol:<14} {fmt_num(s.first_price,2):>10} {fmt_num(s.last_price,2):>10} "
          f"{fmt_num(s.high,2):>10} {fmt_num(s.low,2):>10} {fmt_pct(s.variation_pct):>8} {fmt_num(s.count):>7}")
    w()

    # --- TOP 10 PEORES -------------------------------
    w("+-- TOP 10 PEORES (menor variacion %) " + "=" * 42)
    losers = sorted([s for s in all_syms if s.count >= 2], key=lambda x: x.variation_pct)[:10]
    w(f"  {'#':<3} {'SIMBOLO':<14} {'OPEN':>10} {'CLOSE':>10} {'HIGH':>10} {'LOW':>10} {'VAR%':>8} {'TRADES':>7}")
    w(f"  {SEP_SINGLE[:80]}")
    for i, s in enumerate(losers, 1):
        w(f"  {i:<3} {s.symbol:<14} {fmt_num(s.first_price,2):>10} {fmt_num(s.last_price,2):>10} "
          f"{fmt_num(s.high,2):>10} {fmt_num(s.low,2):>10} {fmt_pct(s.variation_pct):>8} {fmt_num(s.count):>7}")
    w()

    # --- TOP 10 MAS VOLATILES ------------------------
    w("+-- TOP 10 MAS VOLATILES (rango high-low / close) " + "=" * 30)
    def volatility(s): return (s.high - s.low) / s.last_price * 100 if s.last_price > 0 else 0
    volatiles = sorted([s for s in all_syms if s.count >= 2], key=volatility, reverse=True)[:10]
    w(f"  {'#':<3} {'SIMBOLO':<14} {'HIGH':>10} {'LOW':>10} {'RANGO':>10} {'VOLA%':>8} {'VWAP':>10}")
    w(f"  {SEP_SINGLE[:80]}")
    for i, s in enumerate(volatiles, 1):
        rango = s.high - s.low if s.high and s.low else 0
        w(f"  {i:<3} {s.symbol:<14} {fmt_num(s.high,2):>10} {fmt_num(s.low,2):>10} "
          f"{fmt_num(rango,2):>10} {fmt_pct(volatility(s)):>8} {fmt_num(s.vwap,2):>10}")
    w()

    # --- DETALLE COMPLETO POR INSTRUMENTO Y LIQUIDACION --
    w("+-- DETALLE COMPLETO POR INSTRUMENTO Y LIQUIDACION " + "=" * 29)
    w(f"  {'SIMBOLO':<14} {'SETTL':<12} {'OPEN':>10} {'HIGH':>10} {'LOW':>10} {'CLOSE':>10} "
      f"{'VOLUME':>12} {'MONTO':>18} {'TRD':>6} {'VAR%':>8}  FIX vs MONGO")
    w(f"  {SEP_SINGLE[:80]}")

    all_rows_sorted = sorted(all_rows, key=lambda s: s.turnover, reverse=True)
    discrepancies = []

    for s in all_rows_sorted:
        w(f"  {s.symbol:<14} {s.settlement:<12} "
          f"{fmt_num(s.first_price,2):>10} {fmt_num(s.high,2):>10} "
          f"{fmt_num(s.low,2):>10} {fmt_num(s.last_price,2):>10} "
          f"{fmt_num(s.volume):>12} {fmt_num(s.turnover):>18} "
          f"{s.count:>6} {fmt_pct(s.variation_pct):>8}")

        if mongo_ok:
            mk = s.symbol + '|' + s.settlement
            ms = mongo_trades.get(mk)
            if ms:
                vol_diff  = abs(s.volume  - ms.volume)
                mnt_diff  = abs(s.turnover - ms.turnover)
                trd_diff  = abs(s.count   - ms.count)
                cls_diff  = abs((s.last_price or 0) - (ms.last_price or 0))

                status = []
                if trd_diff > 0: status.append(f"trades FIX={s.count} MONGO={ms.count}")
                if vol_diff > 1: status.append(f"vol FIX={fmt_num(s.volume)} MONGO={fmt_num(ms.volume)}")
                if cls_diff > 0.01: status.append(f"close FIX={fmt_num(s.last_price,2)} MONGO={fmt_num(ms.last_price,2)}")

                if status:
                    msg = "    !  DISCREPANCIA: " + " | ".join(status)
                    w(msg)
                    discrepancies.append(f"{s.symbol}({s.settlement}): " + " | ".join(status))
                else:
                    w(f"    OK  MONGO OK  trades={ms.count} vol={fmt_num(ms.volume)} close={fmt_num(ms.last_price,2)}")
            else:
                w(f"    X  NO ENCONTRADO EN MONGO")
                discrepancies.append(f"{s.symbol}({s.settlement}): no existe en trades de MongoDB")
    w()

    # --- CIERRE DE MERCADO (ultimos precios) ---------
    w("+-- PRECIOS DE CIERRE (ultimo trade por instrumento) " + "=" * 27)
    w(f"  {'SIMBOLO':<14} {'SETTL':<12} {'CLOSE_FIX':>12} {'CLOSE_MONGO':>14} {'CLOSE_CANDLE':>14} {'MATCH':>8}")
    w(f"  {SEP_SINGLE[:80]}")

    candles_daily = mongo_result.get('candles_daily', {}) if mongo_ok else {}
    inst_stats    = mongo_result.get('inst_stats',    {}) if mongo_ok else {}

    for s in all_rows_sorted:
        mk = s.symbol + '|' + s.settlement
        ms = mongo_trades.get(mk)
        cd = candles_daily.get(mk)

        close_mongo  = fmt_num(ms.last_price,  2) if ms else "N/A"
        close_candle = fmt_num(float(cd['close']), 2) if cd and cd.get('close') else "N/A"
        close_fix    = fmt_num(s.last_price,   2)

        # Match check
        try:
            c_fx = s.last_price
            c_mn = ms.last_price if ms else None
            c_cd = float(cd['close']) if cd and cd.get('close') else None
            match_str = "OK" if (
                (c_mn is None or abs(c_fx - c_mn) < 0.01) and
                (c_cd is None or abs(c_fx - c_cd) < 0.01)
            ) else "! DIFF"
        except:
            match_str = "?"

        w(f"  {s.symbol:<14} {s.settlement:<12} {close_fix:>12} {close_mongo:>14} {close_candle:>14} {match_str:>8}")
    w()

    # --- RANKING MONGO --------------------------------
    if mongo_ok and mongo_result.get('ranking_doc'):
        rd = mongo_result['ranking_doc']
        w("+-- RANKING MONGODB (latest) " + "=" * 52)
        for category in ('topByTurnover','bottomByTurnover','topByVolume','bottomByVolume','topGainers','topLosers'):
            entries = rd.get(category, [])
            if not entries:
                continue
            w(f"  [{category}]")
            for e in entries[:10]:
                sym  = e.get('symbol','?')
                vol  = fmt_num(e.get('totalVolume',0))
                mnt  = fmt_num(e.get('totalTurnover',0))
                last = fmt_num(float(e.get('lastPrice') or 0), 2)
                var  = fmt_pct(float(e.get('variationPct') or 0))
                settl= e.get('settlement','')
                w(f"    {sym:<14} settl={settl:<10} last={last:>10} var={var:>8} vol={vol:>15} monto={mnt:>20}")
            w()
    w()

    # --- ALERTAS / DISCREPANCIAS ---------------------
    w("+-- ALERTAS Y DISCREPANCIAS " + "=" * 53)
    if discrepancies:
        for d in discrepancies:
            w(f"  !  {d}")
    else:
        w("  OK Sin discrepancias detectadas.")
    w()

    # --- RESUMEN PROPIEDADES (format key=value) --------
    w("+-- RESUMEN EN FORMATO PROPERTIES " + "=" * 46)
    w(f"report.date={date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")
    w(f"fix.total_lines={log_result['total_lines']}")
    w(f"fix.total_messages={log_result['total_msgs']}")
    w(f"fix.snapshots_W={log_result['msg_counts'].get('W',0)}")
    w(f"fix.incrementals_X={log_result['msg_counts'].get('X',0)}")
    w(f"fix.trade_groups={log_result['trade_groups']}")
    w(f"fix.instruments={n_instruments}")
    w(f"fix.total_volume={int(total_fix_volume)}")
    w(f"fix.total_turnover={int(total_fix_turnover)}")
    w(f"fix.total_trades={total_fix_trades}")
    for settl, cnt in sorted(settl_counts.items()):
        key = settl.lower().replace('_','-')
        w(f"fix.settlement.{key}.trades={cnt}")
    if mongo_ok:
        w(f"mongo.total_trades={mongo_result['total_trades']}")
        w(f"mongo.instruments={len(set(s.symbol for s in mongo_trades.values()))}")
        w(f"mongo.total_volume={int(sum(s.volume for s in mongo_trades.values()))}")
        w(f"mongo.total_turnover={int(sum(s.turnover for s in mongo_trades.values()))}")
        w(f"mongo.candles_daily_count={len(candles_daily)}")
        w(f"mongo.discrepancies_count={len(discrepancies)}")
    for i, s in enumerate(top10_monto, 1):
        pfx = f"top_monto.{i}"
        w(f"{pfx}.symbol={s.symbol}")
        w(f"{pfx}.settlement={s.settlement}")
        w(f"{pfx}.close={s.last_price:.4f}")
        w(f"{pfx}.variation_pct={s.variation_pct:.4f}")
        w(f"{pfx}.volume={int(s.volume)}")
        w(f"{pfx}.turnover={int(s.turnover)}")
        w(f"{pfx}.trades={s.count}")
    w()
    w(SEP_DOUBLE)

    # --- ESCRIBIR ARCHIVO ----------------------------
    report_text = "\n".join(lines_out)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report_text)

    # Imprimir en consola tambien (forzar UTF-8 en Windows)
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    print(report_text)
    sys.stdout.flush()
    print(f"\n[OK] Reporte guardado en: {output_file}")


# --- MAIN -----------------------------------------------------
def main():
    if len(sys.argv) > 1:
        date_str = sys.argv[1].strip()
    else:
        yesterday = date.today() - timedelta(days=1)
        date_str  = yesterday.strftime("%Y%m%d")

    log_file = os.path.join(LOG_DIR, LOG_PATTERN.format(date=date_str))
    if not os.path.exists(log_file):
        print(f"ERROR: No se encontro el log: {log_file}")
        sys.exit(1)

    output_file = os.path.join(
        os.path.dirname(log_file),
        f"report_fix_vs_mongo_{date_str}.txt"
    )

    print(f"[1/3] Parseando FIX log: {log_file}")
    log_result = parse_fix_log(log_file)
    print(f"      -> {log_result['trade_groups']} trades, {len(log_result['stats'])} claves instrumento-settlement")

    print(f"[2/3] Consultando MongoDB ({MONGO_DATABASE})...")
    mongo_result = query_mongo(date_str)
    if mongo_result['ok']:
        print(f"      -> {mongo_result['total_trades']} trades en mongo, {len(mongo_result['candles_daily'])} candles diarios")
    else:
        print(f"      -> ERROR: {mongo_result['error']}")

    print(f"[3/3] Generando reporte -> {output_file}")
    generate_report(date_str, log_result, mongo_result, output_file)


if __name__ == "__main__":
    main()

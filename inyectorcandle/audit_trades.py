"""
audit_trades.py
===============
Audita la coleccion `trades` de un dia: duplicados, cobertura contra `instrument_stats`
y `candles`, reinicios del proceso y estado congelado por throttle.

Sirve para medir antes/despues de un cambio en el inyector.

Uso:
    python audit_trades.py                      # hoy, config qa
    python audit_trades.py 2026-08-07
    python audit_trades.py 2026-08-07 --symbol SQM-B
    python audit_trades.py 2026-08-07 --config src/main/resources/application.properties

La URI de Mongo se lee del application*.properties (no se hardcodea aca).

LEER ANTES DE "DEDUPLICAR":
    `eventTime` NO es la hora de ejecucion: el consumidor ITCH estampa el lote completo con
    un mismo instante (hay lotes de 27 trades distintos con el mismo nanosegundo). Por eso
    agrupar por (symbol, eventTime, price, quantity) junta ejecuciones parciales legitimas.
    El bloque DUPLICADOS imprime el test que distingue ambos casos: si en un mismo lote
    conviven cantidades repetidas N veces y cantidades unicas, no hay duplicacion de
    transporte (un duplicador no elige a que trade duplicar) y borrarlos destruye volumen real.
"""

import sys
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from pymongo import MongoClient
from zoneinfo import ZoneInfo

MARKET_ZONE = ZoneInfo("America/Santiago")
DEFAULT_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "src", "main", "resources", "application.qa.properties")


def read_props(path):
    props = {}
    with open(path, encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            props[k.strip()] = v.strip()
    return props


def day_bounds(day):
    start = datetime.fromisoformat(day).replace(tzinfo=MARKET_ZONE)
    end = start + timedelta(days=1)
    fmt = lambda d: d.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    return fmt(start), fmt(end)


def num(v):
    return float(v) if isinstance(v, (int, float)) else 0.0


def parse_args(argv):
    """Acepta --opcion=valor y --opcion valor: la forma con espacio dejaba el valor en True
    y open(True) terminaba leyendo stdout ('Bad file descriptor')."""
    args, opts, i = [], {}, 0
    while i < len(argv):
        a = argv[i]
        if not a.startswith("--"):
            args.append(a)
        elif "=" in a:
            k, v = a.split("=", 1)
            opts[k] = v
        elif i + 1 < len(argv) and not argv[i + 1].startswith("--"):
            opts[a] = argv[i + 1]
            i += 1
        else:
            opts[a] = True
        i += 1
    return args, opts


def main():
    args, opts = parse_args(sys.argv[1:])
    day = args[0] if args else datetime.now(MARKET_ZONE).strftime("%Y-%m-%d")
    only = opts.get("--symbol")
    props = read_props(opts.get("--config", DEFAULT_CONFIG))
    db = MongoClient(props["mongo.uri"], serverSelectionTimeoutMS=10000)[props.get("mongo.database", "inyectorcandle")]

    lo, hi = day_bounds(day)
    q = {"eventTime": {"$gte": lo, "$lt": hi}}
    if only:
        q["symbol"] = only
    docs = list(db["trades"].find(q, {"instrumentId": 1, "symbol": 1, "eventTime": 1, "price": 1,
                                      "quantity": 1, "amount": 1, "updatedAt": 1, "mdEntryId": 1}))
    print(f"=== dia {day}  ventana {lo} .. {hi}  docs={len(docs)} ===")
    if not docs:
        return

    # ---- 1. reinicios del proceso -------------------------------------------------
    # cada arranque/reconexion reescribe securities completo con el SecurityList
    starts = sorted(Counter(d["updatedAt"][:19] for d in db["securities"].find({}, {"updatedAt": 1})).items())
    print("\n[REINICIOS] SecurityList recibida en (cada uno reinicia los contadores de instrument_stats):")
    for ts, n in starts:
        print(f"    {ts}  securities={n}")
    cut = starts[-1][0] if starts else lo

    # ---- 2. duplicados -------------------------------------------------------------
    groups = defaultdict(list)
    for d in docs:
        groups[(d["instrumentId"], d["eventTime"], d["price"], d["quantity"])].append(d)
    sizes = Counter(len(v) for v in groups.values())
    excess = len(docs) - len(groups)
    excess_vol = sum(num(v[0]["quantity"]) * (len(v) - 1) for v in groups.values() if len(v) > 1)
    total_vol = sum(num(d["quantity"]) for d in docs)
    print(f"\n[DUPLICADOS] clave (instrumentId,eventTime,price,quantity)")
    print(f"    grupos={len(groups)}  docs en exceso={excess} ({excess * 100.0 / len(docs):.2f}%)")
    print(f"    volumen en exceso={excess_vol:,.0f} de {total_vol:,.0f} ({excess_vol * 100.0 / max(total_vol, 1):.2f}%)")
    print(f"    tamano de grupo: {sorted(sizes.items())}")
    print(f"    _id repetidos (duplicado real de escritura): {len(docs) - len(set(d['_id'] for d in db['trades'].find(q, {'_id': 1})))}")

    lotes = defaultdict(Counter)
    for d in docs:
        lotes[(d["instrumentId"], d["eventTime"])][(d["price"], d["quantity"])] += 1
    selectivos = sum(1 for c in lotes.values() if max(c.values()) > 1 and any(x == 1 for x in c.values()))
    repetidos = sum(1 for c in lotes.values() if max(c.values()) > 1)
    print(f"    lotes con repeticion={repetidos}, de ellos con cantidades unicas en el MISMO lote={selectivos}")
    print(f"    -> multiplicidad selectiva: {selectivos * 100.0 / max(repetidos, 1):.1f}% "
          f"(alto = ejecuciones parciales reales, NO duplicacion de transporte)")

    top = sorted(((sum(len(v) - 1 for k, v in groups.items() if k[0] == iid), iid)
                  for iid in {k[0] for k in groups}), reverse=True)[:10]
    for n, iid in top:
        if n:
            print(f"      {iid:34s} docs en exceso={n}")

    # ---- 3. trades vs instrument_stats vs candles ----------------------------------
    by_inst = defaultdict(list)
    for d in docs:
        by_inst[d["instrumentId"]].append(d)
    stats = {d["_id"]: d for d in db["instrument_stats"].find({})}
    candles = defaultdict(lambda: [0, 0.0])
    for c in db["candles"].find({"timeframe": "PT1M", "bucketStart": {"$gte": lo, "$lt": hi}},
                                {"instrumentId": 1, "trades": 1, "volume": 1}):
        candles[c["instrumentId"]][0] += c.get("trades") or 0
        candles[c["instrumentId"]][1] += num(c.get("volume"))

    filas = []
    for iid, ds in by_inst.items():
        after = [d for d in ds if d["updatedAt"] >= cut]
        s = stats.get(iid) or {}
        filas.append((iid, len(ds), len(after), s.get("totalTrades", 0), candles[iid][0],
                      sum(num(d["quantity"]) for d in ds), num(s.get("totalVolume")), candles[iid][1]))
    t_docs = sum(f[1] for f in filas)
    t_after = sum(f[2] for f in filas)
    t_stats = sum(f[3] for f in filas)
    t_cand = sum(f[4] for f in filas)
    print(f"\n[COBERTURA] instrumentos={len(filas)}")
    print(f"    trades docs={t_docs}  desde ultimo arranque={t_after}  instrument_stats={t_stats}  candles PT1M={t_cand}")
    print(f"    stats/desde-arranque={t_stats * 100.0 / max(t_after, 1):.2f}%   candles/docs={t_cand * 100.0 / max(t_docs, 1):.2f}%")

    congelados = [f for f in filas if f[2] - f[3] > 2 or f[1] - f[4] > 2]
    congelados.sort(key=lambda f: -(f[2] - f[3] + f[1] - f[4]))
    print(f"\n[CONGELADOS POR THROTTLE] instrumentos cuyo estado persistido quedo atras: {len(congelados)}")
    print(f"    {'instrumento':34s} {'docs':>6s} {'desdeArr':>9s} {'stats':>7s} {'velaN':>7s} {'docVol':>16s} {'statsVol':>16s} {'velaVol':>16s}")
    for f in congelados[:15]:
        print(f"    {f[0]:34s} {f[1]:6d} {f[2]:9d} {f[3]:7d} {f[4]:7d} {f[5]:16,.0f} {f[6]:16,.0f} {f[7]:16,.0f}")


if __name__ == "__main__":
    main()

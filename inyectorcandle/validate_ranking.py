"""
validate_ranking.py
===================
Valida el ranking de instrumentos del frontend contra MongoDB/trades y FIX log 35=X.
Uso: python validate_ranking.py 20260309
"""
import sys, re, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from collections import defaultdict

MONGO_URI      = "mongodb://admin:10wfyaxqxk2hq@68.211.112.146:27017"
MONGO_DATABASE = "inyectorcandle"
LOG_DIR        = r"E:\VC-GITHUB\vector-trade-2.0\inyectorcandle\logs\fix-logs"
LOG_PATTERN    = "FIX.4.4-inyectorcandle-BCSGATEWAY.messages_{date}.log"
DATE_STR       = sys.argv[1] if len(sys.argv) > 1 else "20260309"

# ── Datos del frontend (masTranzado por Cap. Mercado) ─────────────────────────
FRONTEND = [
    # symbol,         price,      var_pct,  volume,         cap_mercado,          settl
    ("LTM",           21.7,        4.08,    1_658_871_615,  35_379_130_528.93,    "T2"),
    ("SQM-B",         66_990.0,    3.86,    294_316,        19_284_669_461.0,     "T2"),
    ("FALABELLA",     6_036.0,     2.31,    2_292_266,      13_772_355_576.2,     "T2"),
    ("CHILE",         178.89,      3.99,    77_540_013,     13_663_568_348.18,    "T2"),
    ("BSANTANDER",    73.19,       5.01,    127_660_825,    9_225_168_803.55,     "T2"),
    ("ENELAM",        77.1,        0.12,    109_462_760,    8_491_301_246.41,     "T2"),
    ("CENCOSUD",      2_600.0,     0.78,    2_679_214,      7_021_134_196.2,      "T2"),
    ("COPEC",         6_908.4,     3.89,    756_424,        5_128_984_062.1,      "T2"),
    ("PARAUCO",       3_660.0,     1.27,    1_385_129,      5_051_833_470.1,      "T2"),
    ("ITAUCL",        20_160.0,    0.20,    238_291,        4_816_831_502.0,      "T2"),
    ("MALLPLAZA",     3_939.9,     0.25,    1_156_709,      4_542_287_395.9,      "T2"),
    ("ENELCHILE",     71.69,       5.43,    57_740_954,     4_033_880_129.73,     "T2"),
    ("SMU",           147.5,      -1.34,    25_018_564,     3_692_822_214.2,      "T2"),
    ("CMPC",          1_275.0,    -1.08,    2_674_364,      3_443_183_195.3,      "T2"),
    ("ANDINA-B",      4_030.0,     1.03,    754_182,        3_003_304_835.8,      "T2"),
    ("SONDA",         292.1,      -4.23,    9_527_498,      2_858_203_179.17,     "T2"),
    ("CFIBTGPLAA",    15_116.69,   0.00,    155_178,        2_345_774_009.96,     "CASH"),
    ("ECL",           1_360.0,     0.37,    1_677_915,      2_257_793_017.3,      "T2"),
    ("COLBUN",        130.16,     -2.57,    16_377_991,     2_129_499_671.68,     "T2"),
]

SETTL_NORM = {
    "|||": "T2", "ph|||": "CASH", "pm|||": "NEXT_DAY",
    "0": "REGULAR", "1": "CASH", "2": "NEXT_DAY", "3": "T2",
    "t2": "T2", "t0": "CASH", "t1": "NEXT_DAY",
    "cash": "CASH", "next_day": "NEXT_DAY", "regular": "REGULAR",
}

def norm_settl(raw):
    if not raw: return "T2"
    return SETTL_NORM.get(raw.strip().lower(), raw.strip().upper())

def pct(got, ref):
    if not ref: return "N/A"
    return f"{(got-ref)/abs(ref)*100:+.2f}%"

def ok(got, ref, tol=1.0):
    if ref is None or ref == 0: return "?"
    return "OK" if abs((got-ref)/abs(ref)*100) < tol else "DIFF"

SEP  = "=" * 120
SEP2 = "-" * 120

# ══════════════════════════════════════════════════════════════════════════════
#  1. MONGODB trades
# ══════════════════════════════════════════════════════════════════════════════
class Agg:
    def __init__(self): self.first=self.last=self.high=self.low=None; self.vol=self.mnt=0.0; self.count=0; self.first_t=self.last_t=None
    def update(self, price, qty, amt, t):
        if price <= 0: return
        if self.first_t is None or (t and t < self.first_t): self.first=price; self.first_t=t
        if self.last_t  is None or (t and t >= self.last_t): self.last=price;  self.last_t=t
        if self.high is None or price > self.high: self.high=price
        if self.low  is None or price < self.low:  self.low=price
        self.vol += max(0, qty); self.mnt += max(0, amt); self.count += 1
    def var_pct(self):
        f = self.first or self.last
        if f and f > 0 and self.last: return (self.last - f) / f * 100
        return 0.0
    def cap(self): return (self.last or 0) * self.vol

def load_mongo(day_prefix):
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    by_key = {}
    for doc in db["trades"].find({"eventTime": {"$regex": f"^{day_prefix}"}}):
        sym   = str(doc.get("symbol","")).strip().upper()
        settl = norm_settl(str(doc.get("settlement","T2")))
        price = float(doc.get("price") or 0)
        qty   = float(doc.get("quantity") or 0)
        amt   = float(doc.get("amount") or 0) or price*qty
        t     = str(doc.get("eventTime",""))
        if not sym or price <= 0: continue
        k = f"{sym}|{settl}"
        if k not in by_key: by_key[k] = Agg()
        by_key[k].update(price, qty, amt, t)
    client.close()
    return by_key

# ══════════════════════════════════════════════════════════════════════════════
#  2. FIX LOG 35=X
# ══════════════════════════════════════════════════════════════════════════════
def load_fix(date_str):
    import os
    path = os.path.join(LOG_DIR, LOG_PATTERN.replace("{date}", date_str))
    by_key = {}
    with open(path, 'rb') as f:
        for line in f:
            if b'\x0135=X\x01' not in line: continue
            fields = {}
            for part in line.split(b'\x01'):
                if b'=' in part:
                    k, _, v = part.partition(b'=')
                    try: fields[int(k.strip())] = v.decode('utf-8','replace').strip()
                    except: pass
            # parse repeating groups
            current = {}
            for tag_raw, val in sorted(fields.items()):
                pass
            # parse properly iterating line parts in order
            groups = []
            cur = {}
            for part in line.split(b'\x01'):
                if b'=' not in part: continue
                k, _, v = part.partition(b'=')
                try: tag = int(k.strip())
                except: continue
                val = v.decode('utf-8','replace').strip()
                if tag == 279:  # MDUpdateAction → start of new group
                    if cur: groups.append(cur)
                    cur = {279: val}
                elif tag in (55,269,270,271,811,466,446,876,63,207,48,15):
                    cur[tag] = val
            if cur: groups.append(cur)

            for g in groups:
                if g.get(269) != '2': continue  # solo TRADE
                sym   = g.get(55,'').strip().upper()
                price = float(g.get(270,0) or 0)
                qty   = float(g.get(271,0) or 0)
                amt   = float(g.get(811,0) or 0) or price*qty
                raw_s = g.get(466) or g.get(446) or g.get(876) or g.get(63,'')
                settl = norm_settl(raw_s)
                t_tag = fields.get(52,'')  # SendingTime
                if not sym or price <= 0: continue
                k = f"{sym}|{settl}"
                if k not in by_key: by_key[k] = Agg()
                by_key[k].update(price, qty, amt, t_tag)
    return by_key

# ══════════════════════════════════════════════════════════════════════════════
#  REPORTE
# ══════════════════════════════════════════════════════════════════════════════
def main():
    day_prefix = f"{DATE_STR[:4]}-{DATE_STR[4:6]}-{DATE_STR[6:8]}"
    print(f"\nCargando MongoDB trades ({day_prefix})...")
    mongo = load_mongo(day_prefix)
    print(f"  {len(mongo)} claves (symbol|settl)")

    print(f"Cargando FIX log {DATE_STR}...")
    fix = load_fix(DATE_STR)
    print(f"  {len(fix)} claves (symbol|settl)")

    lines = []
    def w(s=""): lines.append(s)

    w(SEP)
    w(f"  VALIDACION RANKING INSTRUMENTOS  |  {day_prefix}")
    w(f"  Fuente: FRONTEND (usuario) vs MONGO/trades vs FIX log 35=X")
    w(SEP)

    HDR = f"  {'Instrumento':<14} {'Settl':<6}  {'CAMPO':<10}  {'FRONTEND':>20}  {'MONGO':>20}  {'DIFF':>10}  {'OK':>5}  {'FIX':>20}  {'DIFF':>10}  {'OK':>5}"
    w(HDR)
    w(SEP2)

    total_ok_mongo=0; total_ok_fix=0; total_rows=0

    for sym, price_f, var_f, vol_f, cap_f, settl_f in FRONTEND:
        k = f"{sym}|{settl_f}"
        m = mongo.get(k)
        fx = fix.get(k)

        rows = [
            ("Precio",    price_f, m.last if m else None,  fx.last if fx else None,  1.0),
            ("24h %",     var_f,   m.var_pct() if m else None, fx.var_pct() if fx else None, 5.0),
            ("Volumen",   vol_f,   m.vol if m else None,   fx.vol if fx else None,   1.0),
            ("Cap.Mcdo",  cap_f,   m.cap() if m else None, fx.cap() if fx else None, 1.0),
        ]

        first = True
        for campo, fval, mval, xval, tol in rows:
            lbl = f"  {sym:<14} {settl_f:<6}" if first else f"  {'':14} {'':6}"
            first = False

            m_str  = f"{mval:>20,.2f}" if mval is not None else f"{'N/A':>20}"
            x_str  = f"{xval:>20,.2f}" if xval is not None else f"{'N/A':>20}"
            f_str  = f"{fval:>20,.2f}"

            md = pct(mval, fval) if mval is not None else "N/A"
            xd = pct(xval, fval) if xval is not None else "N/A"
            mo = ok(mval, fval, tol) if mval is not None else "N/A"
            xo = ok(xval, fval, tol) if xval is not None else "N/A"

            total_rows += 1
            if mo == "OK": total_ok_mongo += 1
            if xo == "OK": total_ok_fix   += 1

            w(f"{lbl}  {campo:<10}  {f_str}  {m_str}  {md:>10}  {mo:>5}  {x_str}  {xd:>10}  {xo:>5}")

        w()

    w(SEP)
    w(f"  RESUMEN: MONGO OK={total_ok_mongo}/{total_rows}  |  FIX OK={total_ok_fix}/{total_rows}")
    w(SEP)

    # ── Instrumentos no encontrados ───────────────────────────────────────────
    missing_mongo = [f"{sym}|{s}" for sym,_,_,_,_,s in FRONTEND if f"{sym}|{s}" not in mongo]
    missing_fix   = [f"{sym}|{s}" for sym,_,_,_,_,s in FRONTEND if f"{sym}|{s}" not in fix]
    if missing_mongo:
        w(f"  No encontrados en MONGO: {', '.join(missing_mongo)}")
    if missing_fix:
        w(f"  No encontrados en FIX  : {', '.join(missing_fix)}")
    if missing_mongo or missing_fix:
        w()

    report = "\n".join(lines)
    print(report)

    import os
    out = os.path.join(LOG_DIR, f"validate_ranking_{DATE_STR}.txt")
    with open(out, 'w', encoding='utf-8') as f:
        f.write(report + "\n")
    print(f"\n[OK] Guardado: {out}")

if __name__ == "__main__":
    main()

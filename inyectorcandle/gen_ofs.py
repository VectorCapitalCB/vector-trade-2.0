import re

log_path = r'E:\VC-GITHUB\vector-trade-2.0\inyectorcandle\logs\fix-logs\FIX.4.4-inyectorcandle-BCSGATEWAY.messages_20260309.log'

with open(log_path, 'rb') as f:
    lines = f.readlines()

seclist_lines = [l for l in lines if b'\x0135=y\x01' in l]
print(f'Mensajes 35=y: {len(seclist_lines)}')

ofs_symbols = set()
for l in seclist_lines:
    pairs = []
    for field in l.split(b'\x01'):
        if b'=' in field:
            k, _, v = field.partition(b'=')
            try:
                pairs.append((int(k.strip()), v.decode('utf-8', 'replace').strip()))
            except Exception:
                pass

    current = {}
    for tag, val in pairs:
        if tag == 55:
            if current and current.get(207, '').upper() == 'OFS':
                sym = current.get(55, '').strip()
                if sym:
                    ofs_symbols.add(sym)
            current = {55: val}
        elif tag in (207, 167, 15, 48):
            current[tag] = val
    if current and current.get(207, '').upper() == 'OFS':
        sym = current.get(55, '').strip()
        if sym:
            ofs_symbols.add(sym)

# Filtrar solo simbolos "legibles" - excluir IDs internos numericos y prefijos BCS/BC/BZ/BW
SKIP_PREFIXES = ('BCS', 'BC4', 'BC8', 'B4', 'B43', 'B48', 'BZBWH', 'BWARAAU',
                 'PGIF', 'PBSAX', 'GAIFX', 'GAMGX', 'GASVX', 'WBAIX', 'CAGSX', 'PBSAX')

clean = sorted(
    [s for s in ofs_symbols
     if not s.isdigit()
     and not any(s.startswith(p) for p in SKIP_PREFIXES)],
    key=lambda x: x.upper()
)

print(f'Total simbolos OFS limpios: {len(clean)}')

PER_LINE = 10
chunks = [clean[i:i+PER_LINE] for i in range(0, len(clean), PER_LINE)]

lines_out = []
for i, chunk in enumerate(chunks):
    segment = ','.join(chunk)
    prefix = 'symbolooff_shore=' if i == 0 else '  '
    cont = ',' + chr(92) if i < len(chunks) - 1 else ''
    lines_out.append(prefix + segment + cont)

result = '\n'.join(lines_out) + '\n'

out_path = r'E:\VC-GITHUB\vector-trade-2.0\inyectorcandle\logs\fix-logs\ofs_symbols.txt'
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(result)

print('Guardado en:', out_path)
print()
print('Preview primeras 5 lineas:')
for line in lines_out[:5]:
    print(line)

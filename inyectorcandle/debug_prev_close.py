from pymongo import MongoClient

client = MongoClient('mongodb://admin:10wfyaxqxk2hq@68.211.112.146:27017')
db = client['inyectorcandle']

iid = 'LTM|T2|XSGO|UNKNOWN_CCY|CS'
day_start_09 = '2026-03-09T03:00:00Z'
day_start_10 = '2026-03-10T03:00:00Z'

# 1. Ver todas las candles PT24H de LTM
all_daily = list(db['candles'].find({'instrumentId': iid, 'timeframe': 'PT24H'}))
print(f'Candles PT24H para iid={iid}: {len(all_daily)}')
for d in all_daily:
    print(f"  bucketStart={repr(d['bucketStart'])}  close={d['close']}")

print()

# 2. Test $lt manual
print("Test $lt string comparison:")
for d in all_daily:
    bs = d['bucketStart']
    print(f"  Python: {repr(bs)} < {repr(day_start_09)} = {bs < day_start_09}")

print()

# 3. Query con $lt exacto
q = {'instrumentId': iid, 'timeframe': 'PT24H', 'bucketStart': {'$lt': day_start_09}}
print(f"Query: {q}")
r = list(db['candles'].find(q).sort('bucketStart', -1).limit(3))
print(f"Resultado: {len(r)} docs")
for d in r:
    print(f"  {d['bucketStart']}  close={d['close']}")

print()

# 4. Solo instrumentId + lt (sin timeframe)
q2 = {'instrumentId': iid, 'bucketStart': {'$lt': day_start_09}}
r2 = list(db['candles'].find(q2).sort('bucketStart', -1).limit(3))
print(f"Sin timeframe: {len(r2)} docs")
for d in r2:
    print(f"  tf={d['timeframe']}  {d['bucketStart']}  close={d['close']}")

print()

# 5. Ver indexes de la coleccion candles
print("Indexes en candles:")
for idx in db['candles'].list_indexes():
    print(f"  {idx}")

client.close()

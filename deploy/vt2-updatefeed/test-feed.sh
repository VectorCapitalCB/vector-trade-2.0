#!/usr/bin/env bash
# Prueba e2e de publish-feed.sh + verify-feed.sh con un feed sintetico.
set -uo pipefail

DEPLOY="/Users/diegoaedoquiroga/IdeaProjects/vector-trade-2.0/deploy/vt2-updatefeed"
BASE="$(mktemp -d)"
STACK="$BASE/VectorTrade2.0/updatefeed"
SRC="$BASE/releases"
PORT=18131
PASS=0; FAIL=0

ok()   { echo "  PASS: $*"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }

mk_asset() {  # $1=version $2=outdir
  local ver="$1" dir="$2"
  local f="$dir/VectorTrade-$ver-full.nupkg"
  head -c 200000 /dev/urandom > "$f"
  [ -f "$dir/VectorTrade-win-Setup.exe" ] || head -c 120000 /dev/urandom > "$dir/VectorTrade-win-Setup.exe"
  local sha; sha="$(shasum -a 256 "$f" | cut -d' ' -f1)"
  local sz;  sz="$(wc -c < "$f" | tr -d ' ')"
  printf '{"PackageId":"VectorTrade","Version":"%s","Type":"Full","FileName":"VectorTrade-%s-full.nupkg","SHA256":"%s","Size":%s,"NotesMarkdown":""}' \
    "$ver" "$ver" "$sha" "$sz"
}

echo "### Preparando feed sintetico en $BASE"
mkdir -p "$SRC"
A1="$(mk_asset 3.2.1 "$SRC")"
A2="$(mk_asset 3.2.2 "$SRC")"
# desordenado a proposito: 3.2.2 primero, para probar que elige por version y no por posicion
printf '{"Assets":[%s,%s]}' "$A2" "$A1" > "$SRC/releases.win.json"
( cd "$SRC" && shasum -a 256 VectorTrade-*.nupkg | sed 's#  \./#  #' > SHA256SUMS.txt )

echo
echo "### 1. bootstrap.sh"
bash "$DEPLOY/bootstrap.sh" --root "$STACK" >/dev/null 2>&1
[ -f "$STACK/nginx/feed.conf" ] && ok "instalo nginx/feed.conf" || bad "no instalo feed.conf"
[ -f "$STACK/nginx/allowlist.inc" ] && ok "instalo nginx/allowlist.inc" || bad "no instalo allowlist.inc"
[ -x "$STACK/publish-feed.sh" ] && ok "instalo publish-feed.sh ejecutable" || bad "no instalo publish-feed.sh"
[ -d "$STACK/releases" ] && ok "creo releases/" || bad "no creo releases/"
[ -f "$STACK/site/index.html" ] && ok "instalo el portal interno" || bad "no instalo el portal interno"

# La allowlist lleva las IP del piloto: un segundo bootstrap NO debe pisarla.
echo "allow 172.16.0.123;" >> "$STACK/nginx/allowlist.inc"
bash "$DEPLOY/bootstrap.sh" --root "$STACK" >/dev/null 2>&1
grep -q "172.16.0.123" "$STACK/nginx/allowlist.inc" \
  && ok "un segundo bootstrap preserva la allowlist editada" \
  || bad "bootstrap PISO la allowlist con las IP del piloto"
[ -n "$(ls -A "$STACK/backups" 2>/dev/null)" ] && ok "respaldo la allowlist antes de decidir" || bad "no respaldo la allowlist"

# Estructura de la config de nginx (no reemplaza a `nginx -t`, que necesita Docker)
awk 'BEGIN{o=0} {for(i=1;i<=length($0);i++){c=substr($0,i,1); if(c=="{")o++; if(c=="}")o--}} END{exit (o==0)?0:1}' \
  "$STACK/nginx/feed.conf" && ok "feed.conf tiene las llaves balanceadas" || bad "feed.conf tiene llaves desbalanceadas"
n_inc="$(grep -c 'include /etc/nginx/conf.d/allowlist.inc;' "$STACK/nginx/feed.conf")"
[ "$n_inc" -eq 6 ] \
  && ok "la allowlist protege portal, catalogo y feed" \
  || bad "la allowlist se incluye en $n_inc rutas; se esperaban 6"

export FEED_ROOT="$STACK/releases"
export BACKUP_DIR="$STACK/backups"

echo
echo "### 2. publish-feed.sh --dry-run (no debe escribir nada)"
out="$(bash "$DEPLOY/publish-feed.sh" --src "$SRC" --dry-run 2>&1)"
rc=$?
[ $rc -eq 0 ] && ok "dry-run exit 0" || bad "dry-run exit $rc"
grep -q "3.2.2" <<<"$out" && ok "detecto 3.2.2 como la mas nueva (aunque el JSON la lista primero)" || bad "no eligio 3.2.2: $out"
[ ! -f "$FEED_ROOT/releases.win.json" ] \
  && [ -z "$(find "$FEED_ROOT" -maxdepth 1 -type f ! -name downloads.json -print -quit)" ] \
  && ok "dry-run no publico paquetes ni indice" \
  || bad "dry-run publico contenido de release"

echo
echo "### 3. publish-feed.sh real"
out="$(bash "$DEPLOY/publish-feed.sh" --src "$SRC" 2>&1)"; rc=$?
[ $rc -eq 0 ] && ok "publish exit 0" || bad "publish exit $rc: $out"
[ -f "$FEED_ROOT/releases.win.json" ] && ok "publico el indice" || bad "no publico el indice"
[ -f "$FEED_ROOT/VectorTrade-3.2.2-full.nupkg" ] && ok "publico el paquete 3.2.2" || bad "falta el paquete 3.2.2"
[ -f "$FEED_ROOT/archive/3.2.2/VectorTrade-3.2.2-full.nupkg" ] && ok "archivo la version 3.2.2" || bad "falta el archivo historico 3.2.2"
grep -q '"version": "3.2.2"' "$FEED_ROOT/downloads.json" && ok "actualizo el catalogo del portal" || bad "downloads.json no contiene 3.2.2"
ls "$FEED_ROOT"/.*.tmp.* >/dev/null 2>&1 && bad "quedaron temporales en el feed" || ok "no quedaron archivos temporales"

echo
echo "### 4. verify-feed.sh contra un HTTP real"
# Un solo servidor, sirviendo webroot/ con el feed en la ruta publica real.
mkdir -p "$BASE/webroot/updatevtautoupdate"
cp -R "$FEED_ROOT/." "$BASE/webroot/updatevtautoupdate/"
cd "$BASE/webroot"
python3 -m http.server "$PORT" --bind 127.0.0.1 >/dev/null 2>&1 &
SRV_PID=$!
cd - >/dev/null
trap 'kill $SRV_PID 2>/dev/null' EXIT
for _ in 1 2 3 4 5 6 7 8 9 10; do
  curl -sf -o /dev/null "http://127.0.0.1:$PORT/updatevtautoupdate/releases.win.json" && break
  sleep 0.4
done

URL="http://127.0.0.1:$PORT/updatevtautoupdate"
out="$(bash "$DEPLOY/verify-feed.sh" --url "$URL" --expect 3.2.2 --deep 2>&1)"; rc=$?
[ $rc -eq 0 ] && ok "verify exit 0 con --deep --expect 3.2.2" || bad "verify exit $rc: $out"
grep -q "SHA256 coincide" <<<"$out" && ok "valido el SHA256 del paquete" || bad "no valido SHA256: $out"
grep -qi "AVISO Cache-Control" <<<"$out" && ok "detecto falta de Cache-Control (python http.server no lo manda)" || bad "no aviso del Cache-Control"

echo
echo "### 5. verify-feed.sh debe FALLAR si se espera otra version"
out="$(bash "$DEPLOY/verify-feed.sh" --url "$URL" --expect 9.9.9 2>&1)"; rc=$?
[ $rc -ne 0 ] && ok "falla con version esperada distinta (exit $rc)" || bad "no fallo con --expect 9.9.9"

echo
echo "### 6. verify-feed.sh debe FALLAR si el indice declara un paquete ausente"
rm -f "$BASE/webroot/updatevtautoupdate/VectorTrade-3.2.2-full.nupkg"
out="$(bash "$DEPLOY/verify-feed.sh" --url "$URL" 2>&1)"; rc=$?
[ $rc -ne 0 ] && ok "detecta el 404 del paquete (exit $rc)" || bad "no detecto el paquete ausente"
grep -q "publicar el indice antes que los paquetes" <<<"$out" && ok "explica la causa correcta" || bad "no explico la causa"

echo
echo "### 7. publish-feed.sh debe RECHAZAR un paquete con SHA256 alterado"
BADSRC="$BASE/bad"; mkdir -p "$BADSRC"
cp "$SRC/releases.win.json" "$BADSRC/"
cp "$SRC/VectorTrade-3.2.1-full.nupkg" "$BADSRC/"
head -c 200000 /dev/urandom > "$BADSRC/VectorTrade-3.2.2-full.nupkg"   # contenido distinto al hash del indice
out="$(bash "$DEPLOY/publish-feed.sh" --src "$BADSRC" 2>&1)"; rc=$?
[ $rc -ne 0 ] && ok "rechaza el paquete con SHA256 que no calza (exit $rc)" || bad "PUBLICO un paquete corrupto"
grep -q "el cliente rechazaria" <<<"$out" && ok "explica que el cliente lo rechazaria" || bad "mensaje poco claro: $out"

echo
echo "### 8. rollback"
# publicar una 3.2.3 y luego volver
mkdir -p "$BASE/v3"; A3="$(mk_asset 3.2.3 "$BASE/v3")"
printf '{"Assets":[%s]}' "$A3" > "$BASE/v3/releases.win.json"
( cd "$BASE/v3" && shasum -a 256 VectorTrade-*.nupkg > SHA256SUMS.txt )
bash "$DEPLOY/publish-feed.sh" --src "$BASE/v3" >/dev/null 2>&1
grep -q "3.2.3" "$FEED_ROOT/releases.win.json" && ok "publico 3.2.3" || bad "no publico 3.2.3"
out="$(printf 'SI\n' | bash "$DEPLOY/publish-feed.sh" --rollback 2>&1)"; rc=$?
[ $rc -eq 0 ] && ok "rollback exit 0" || bad "rollback exit $rc: $out"
grep -q "3.2.2" <<<"$out" && ok "el feed volvio a ofrecer 3.2.2" || bad "rollback no volvio a 3.2.2: $out"
[ -f "$FEED_ROOT/VectorTrade-3.2.3-full.nupkg" ] && ok "el rollback NO borro el paquete 3.2.3 (aditivo)" || bad "el rollback borro paquetes"

echo
echo "### 9. rollback cancelado si no se confirma con SI"
out="$(printf 'no\n' | bash "$DEPLOY/publish-feed.sh" --rollback 2>&1)"; rc=$?
[ $rc -ne 0 ] && ok "cancela sin confirmacion explicita (exit $rc)" || bad "hizo rollback sin confirmar"

kill $SRV_PID 2>/dev/null
echo
echo "=========================================="
echo "  PASS: $PASS    FAIL: $FAIL"
echo "=========================================="
rm -rf "$BASE"
[ "$FAIL" -eq 0 ]

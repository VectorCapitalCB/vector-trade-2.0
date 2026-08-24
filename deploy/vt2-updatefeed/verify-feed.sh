#!/usr/bin/env bash
# =============================================================================
#  verify-feed.sh  -  valida el feed replicando lo que hace el cliente real.
#
#  Solo hace GET. No escribe nada, no toca contenedores.
#
#  Replica cl.vc.blotter.utils.VelopackUpdater:
#    1. GET {url}/releases.{canal}.json  y EXIGE 200 (sin seguir redirecciones,
#       porque HttpURLConnection tampoco las sigue entre http y https)
#    2. elige el asset Type=Full de mayor version
#    3. GET {url}/{FileName}  y EXIGE 200
#    4. con --deep, descarga el paquete y compara el SHA256 contra el indice,
#       que es exactamente la validacion que hace el updater antes de aplicar
#
#  Uso:
#      # desde el propio servidor
#      bash verify-feed.sh --url http://127.0.0.1:8092/updatevtautoupdate --deep
#      # desde el equipo del piloto, con la URL EXACTA horneada en el build
#      bash verify-feed.sh --url http://172.16.0.6:8092/updatevtautoupdate --expect 3.2.2
#
#  Correrlo desde el equipo del piloto es la unica prueba que vale: valida de
#  paso la red, el puerto y la lista blanca de IP.
#
#  Salida: 0 si el feed sirve, != 0 con el motivo concreto si no.
# =============================================================================
set -euo pipefail

URL=""
CHANNEL="win"
DEEP=0
EXPECT=""

while [ $# -gt 0 ]; do
  case "$1" in
    --url)     URL="${2:?--url necesita un valor}"; shift 2 ;;
    --channel) CHANNEL="${2:?--channel necesita un valor}"; shift 2 ;;
    --expect)  EXPECT="${2:?--expect necesita una version}"; shift 2 ;;
    --deep)    DEEP=1; shift ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

[ -n "$URL" ] || { echo "ERROR: falta --url" >&2; exit 2; }
URL="${URL%/}"                       # el cliente hace trimSlash; imitalo
INDEX_URL="$URL/releases.$CHANNEL.json"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo; echo "FALLO: $*" >&2; exit 1; }

echo "==> Feed: $URL"
echo "==> Indice: $INDEX_URL"

# --- 1. Indice ---------------------------------------------------------------
# Sin -L a proposito: si el server responde 301/302 el cliente Java lo trata
# como error y calla. Queremos que eso salga aca, no en produccion.
CODE="$(curl -sS -o "$TMP/index.json" -w '%{http_code}' --max-time 15 "$INDEX_URL" || echo 000)"
echo "    HTTP $CODE"

case "$CODE" in
  200) ;;
  301|302|303|307|308)
    LOC="$(curl -sSI --max-time 15 "$INDEX_URL" 2>/dev/null | awk 'tolower($1)=="location:"{print $2}' | tr -d '\r')"
    fail "el feed responde $CODE (redireccion a: ${LOC:-desconocida}).
       VelopackUpdater usa HttpURLConnection, que NO sigue redirecciones entre
       http y https: registraria 'no se pudo leer feed' y NADIE se actualizaria,
       sin error visible para el usuario.
       Arregla el proxy para servir el feed directo en la misma URL, o cambia
       'url' en application.prod.properties al destino final." ;;
  403) fail "403 en el indice: la IP de esta maquina no esta en nginx/allowlist.inc.
       Es la falla mas enganosa del feed interno: el updater trata el 403 como
       'no se pudo leer feed' y NO muestra ningun error, se ve igual que
       'estoy al dia'. Agrega la IP del equipo, y despues:
         docker exec <contenedor> nginx -t && docker exec <contenedor> nginx -s reload" ;;
  404) fail "404 en el indice. Revisa que releases.$CHANNEL.json exista en FEED_ROOT
       y que la ruta publica calce con la location de nginx (/updatevtautoupdate/)." ;;
  000) fail "no hubo respuesta (red, firewall, puerto equivocado o contenedor caido).
       Verifica que FEED_BIND_ADDR sea 0.0.0.0 y no 127.0.0.1: con 127.0.0.1 el
       feed solo responde desde el propio servidor, nunca desde el equipo del piloto." ;;
  *)   fail "HTTP $CODE inesperado en el indice." ;;
esac

# --- 2. Cache del indice -----------------------------------------------------
CC="$(curl -sSI --max-time 15 "$INDEX_URL" 2>/dev/null | awk 'tolower($1)=="cache-control:"{$1=""; print}' | tr -d '\r' | sed 's/^ *//')"
if printf '%s' "$CC" | grep -qiE 'no-store|no-cache|max-age=0'; then
  echo "    Cache-Control: ${CC:-(vacio)}  [ok]"
else
  echo "    AVISO Cache-Control: ${CC:-(ausente)}"
  echo "          Si un proxy cachea el indice, los clientes siguen viendo la version vieja."
fi

# --- 3. Asset Full mas nuevo -------------------------------------------------
read_newest() {
  if command -v jq >/dev/null 2>&1; then
    jq -r '
      [.Assets[]? | select((.Type // "") | ascii_downcase == "full")]
      | sort_by(.Version | split(".") | map(tonumber? // 0))
      | last
      | if . == null then empty else "\(.Version)\t\(.FileName)\t\(.SHA256 // "")" end
    ' "$1"
  elif command -v python3 >/dev/null 2>&1; then
    python3 - "$1" <<'PY'
import json, sys
def key(v):
    out = []
    for p in str(v).split('.'):
        try: out.append(int(p))
        except ValueError: out.append(0)
    return out
try:
    d = json.load(open(sys.argv[1]))
except Exception as e:
    print(f"__PARSE_ERROR__\t{e}\t"); sys.exit(0)
full = [a for a in d.get('Assets') or [] if str(a.get('Type','')).lower() == 'full']
if not full: sys.exit(0)
b = max(full, key=lambda a: key(a.get('Version','0')))
print(f"{b.get('Version','')}\t{b.get('FileName','')}\t{b.get('SHA256','')}")
PY
  else
    fail "hace falta jq o python3 para leer el indice"
  fi
}

LINE="$(read_newest "$TMP/index.json" || true)"
[ -n "$LINE" ] || fail "el indice no declara ningun asset con Type=Full.
       El cliente solo mira Type=Full e ignora los delta: sin un Full, no hay actualizacion."
VER="$(printf '%s' "$LINE" | cut -f1)"
FILE="$(printf '%s' "$LINE" | cut -f2)"
SHA="$(printf '%s' "$LINE" | cut -f3)"
[ "$VER" != "__PARSE_ERROR__" ] || fail "el indice no es JSON valido: $FILE"

echo "==> Version ofrecida: $VER"
echo "    Paquete: $FILE"
echo "    SHA256 declarado: ${SHA:-(ninguno)}"

# --- 4. El paquete se puede bajar -------------------------------------------
PKG_URL="$URL/$FILE"
PCODE="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 -r 0-0 "$PKG_URL" || echo 000)"
echo "==> Paquete: HTTP $PCODE"
case "$PCODE" in
  200|206) ;;
  404) fail "el indice declara $FILE pero el feed responde 404.
       Sintoma clasico de publicar el indice antes que los paquetes. Usa
       publish-feed.sh, que copia los paquetes primero y el indice al final." ;;
  *)   fail "HTTP $PCODE bajando el paquete." ;;
esac

# --- 5. Validacion profunda (lo que hace el cliente antes de aplicar) -------
if [ "$DEEP" -eq 1 ]; then
  echo "==> Descargando el paquete completo para validar SHA256"
  curl -sS -o "$TMP/$FILE" --max-time 300 "$PKG_URL" || fail "no se pudo descargar el paquete completo"
  SIZE="$(wc -c < "$TMP/$FILE" | tr -d ' ')"
  echo "    ${SIZE} bytes"
  if [ -n "$SHA" ]; then
    ACTUAL="$(sha256sum "$TMP/$FILE" | cut -d' ' -f1)"
    if [ "$(printf '%s' "$ACTUAL" | tr 'A-Z' 'a-z')" != "$(printf '%s' "$SHA" | tr 'A-Z' 'a-z')" ]; then
      fail "el SHA256 del paquete NO coincide con el indice.
       El cliente aborta la actualizacion en este punto (mensaje: 'El SHA-256 del
       paquete no coincide'). Republica el feed desde el artifact original."
    fi
    echo "    SHA256 coincide [ok]"
  fi
  # El cliente usa readTimeout de 120 s para el .nupkg.
  SECS="$(curl -sS -o /dev/null -w '%{time_total}' --max-time 300 "$PKG_URL" || echo 0)"
  echo "    descarga completa en ${SECS}s (el cliente aborta a los 120s)"
  case "$SECS" in
    ''|*[!0-9.]*) ;;
    *) awk -v s="$SECS" 'BEGIN{ if (s+0 > 90) print "    AVISO: cerca del timeout de 120s del cliente." }' ;;
  esac
fi

# --- 6. Version esperada -----------------------------------------------------
if [ -n "$EXPECT" ]; then
  [ "$VER" = "$EXPECT" ] || fail "se esperaba la version $EXPECT y el feed ofrece $VER."
  echo "==> Version esperada confirmada: $EXPECT"
fi

echo
echo "OK: el feed sirve correctamente y un cliente con version < $VER se actualizaria."

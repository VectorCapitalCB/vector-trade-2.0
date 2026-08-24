#!/usr/bin/env bash
# =============================================================================
#  diagnose.sh  -  por que el feed no responde. Solo lectura.
#
#  No levanta, detiene, reinicia ni recrea nada. Solo inspecciona y te dice
#  exactamente cual es el problema y como arreglarlo.
#
#  Correlo EN EL SERVIDOR:
#      bash diagnose.sh
#      bash diagnose.sh --port 8092 --root /home/voultech/app/VectorTrade2.0/updatefeed
#
#  Orden de las causas, de la mas probable a la menos:
#    1. el stack nunca se creo
#    2. el contenedor esta Exited (casi siempre: bind mounts convertidos en
#       directorios por haber creado el stack antes de correr bootstrap.sh)
#    3. publica en 127.0.0.1 en vez de 0.0.0.0
#    4. puerto distinto al que estas probando
#    5. firewall del host
# =============================================================================
set -uo pipefail

PORT="8092"
STACK_ROOT="/home/voultech/app/VectorTrade2.0/updatefeed"
NAME="vt2-updatefeed"

while [ $# -gt 0 ]; do
  case "$1" in
    --port) PORT="${2:?}"; shift 2 ;;
    --root) STACK_ROOT="${2:?}"; shift 2 ;;
    --name) NAME="${2:?}"; shift 2 ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

PROBLEMS=0
say()  { echo "$*"; }
bad()  { echo "  [X] $*"; PROBLEMS=$((PROBLEMS+1)); }
good() { echo "  [ok] $*"; }
fix()  { echo "       -> $*"; }

echo "======================================================================"
echo " diagnose.sh   contenedor=$NAME  puerto=$PORT  root=$STACK_ROOT"
echo "======================================================================"

# --- 1. Archivos que el stack necesita ANTES de existir ----------------------
say
say "1. Archivos de configuracion en el host"
for f in nginx/feed.conf nginx/allowlist.inc; do
  p="$STACK_ROOT/$f"
  if [ -d "$p" ]; then
    bad "$p es un DIRECTORIO, deberia ser un archivo"
    fix "Causa: se creo el stack antes de correr bootstrap.sh. Docker no"
    fix "encontro el archivo y lo creo como directorio vacio; nginx falla el"
    fix "include y el contenedor muere al arrancar."
    fix "Arreglo:  rmdir '$p'  &&  bash bootstrap.sh  y recrea el stack."
  elif [ -f "$p" ]; then
    good "$p ($(wc -l < "$p" | tr -d ' ') lineas)"
  else
    bad "$p no existe"
    fix "Corre bootstrap.sh ANTES de crear el stack en Portainer."
  fi
done

p="$STACK_ROOT/releases"
if [ -d "$p" ]; then
  n="$(ls -A "$p" 2>/dev/null | wc -l | tr -d ' ')"
  if [ "$n" -eq 0 ]; then
    good "$p existe (vacio: normal antes de la primera publicacion)"
  else
    good "$p existe con $n archivos"
  fi
else
  bad "$p no existe";  fix "Corre bootstrap.sh."
fi

# --- 2. El contenedor -------------------------------------------------------
say
say "2. Contenedor"
if ! command -v docker >/dev/null 2>&1; then
  bad "no hay comando docker en este host"
else
  INFO="$(docker ps -a --filter "name=^${NAME}$" --format '{{.Status}}|{{.Ports}}' 2>/dev/null)"
  if [ -z "$INFO" ]; then
    bad "no existe ningun contenedor llamado '$NAME'"
    fix "El stack no se creo todavia. Portainer -> Stacks -> Add stack."
    fix "Si le pusiste otro nombre: bash diagnose.sh --name <nombre>"
  else
    STATUS="${INFO%%|*}"; PORTS="${INFO#*|}"
    case "$STATUS" in
      Up*)
        good "corriendo: $STATUS"
        [ -n "$PORTS" ] && good "publica: $PORTS" || bad "no publica ningun puerto"
        ;;
      *)
        bad "NO esta corriendo: $STATUS"
        fix "Ultimas lineas del log (la causa real suele estar aca):"
        docker logs --tail 25 "$NAME" 2>&1 | sed 's/^/          /'
        say
        fix "Si dice 'is a directory' o 'No such file or directory' sobre"
        fix "allowlist.inc o default.conf, es el punto 1 de arriba."
        ;;
    esac

    # Puerto realmente publicado vs el que estas probando
    MAPPED="$(docker port "$NAME" 2>/dev/null | head -5)"
    if [ -n "$MAPPED" ]; then
      say "  mapeo real:"; echo "$MAPPED" | sed 's/^/          /'
      if ! echo "$MAPPED" | grep -q ":${PORT}\$\|:${PORT}[^0-9]"; then
        bad "el contenedor NO publica el puerto $PORT que estas probando"
        fix "Usa el puerto del mapeo real, o corrige FEED_HTTP_PORT en el stack."
      fi
      if echo "$MAPPED" | grep -q '127\.0\.0\.1:'; then
        bad "publica solo en 127.0.0.1: inalcanzable desde otro equipo"
        fix "Pon FEED_BIND_ADDR=0.0.0.0 en las variables del stack."
        fix "Es exactamente el sintoma que estas viendo: connection refused"
        fix "desde tu maquina, pero funciona con curl dentro del servidor."
      fi
    fi

    # Config valida dentro del contenedor
    if [ "${STATUS:0:2}" = "Up" ]; then
      if docker exec "$NAME" nginx -t >/dev/null 2>&1; then
        good "nginx -t pasa dentro del contenedor"
      else
        bad "nginx -t FALLA dentro del contenedor:"
        docker exec "$NAME" nginx -t 2>&1 | sed 's/^/          /'
      fi
    fi
  fi
fi

# --- 3. Escucha en el host --------------------------------------------------
say
say "3. Puerto $PORT en el host"
LISTEN="$(ss -ltn 2>/dev/null | awk -v p=":$PORT\$" '$4 ~ p {print $1, $4}')"
if [ -n "$LISTEN" ]; then
  good "algo escucha: $LISTEN"
  echo "$LISTEN" | grep -q '127\.0\.0\.1' && {
    bad "escucha solo en loopback"
    fix "FEED_BIND_ADDR=0.0.0.0"
  }
else
  bad "NADA escucha en el puerto $PORT"
  fix "Esto es exactamente lo que produce ERR_CONNECTION_REFUSED."
  fix "Si el contenedor aparece 'Up' arriba, revisa el mapeo real de puertos."
fi

# --- 4. HTTP local ----------------------------------------------------------
say
say "4. Respuesta HTTP desde el propio servidor"
H="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 8 "http://127.0.0.1:$PORT/healthz" 2>/dev/null || echo 000)"
case "$H" in
  200) good "/healthz -> 200 (nginx vivo y sirviendo)" ;;
  000) bad "/healthz sin respuesta: no hay servidor en $PORT" ;;
  *)   bad "/healthz -> HTTP $H (nginx responde pero la config no es la esperada)" ;;
esac

I="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 8 "http://127.0.0.1:$PORT/updatevtautoupdate/releases.win.json" 2>/dev/null || echo 000)"
case "$I" in
  200) good "releases.win.json -> 200 (feed publicado)" ;;
  403) bad "releases.win.json -> 403: la IP no esta en allowlist.inc"
       fix "Agregala y: docker exec $NAME nginx -t && docker exec $NAME nginx -s reload" ;;
  404) say "  [--] releases.win.json -> 404: normal si no publicaste ninguna version aun"
       fix "Publica con publish-feed.sh. No es la causa del connection refused." ;;
  000) bad "sin respuesta" ;;
  *)   bad "HTTP $I" ;;
esac

# --- 5. Nota sobre la URL del navegador -------------------------------------
say
say "5. Nota"
say "  Probar http://<host>:$PORT/updatevtautoupdate/ en el navegador NO es una"
say "  prueba valida: autoindex esta off y no hay index.html, asi que incluso"
say "  con todo bien devuelve 403/404. La URL que importa es la del indice:"
say "      http://<host>:$PORT/updatevtautoupdate/releases.win.json"

# --- Resumen ----------------------------------------------------------------
say
echo "======================================================================"
if [ "$PROBLEMS" -eq 0 ]; then
  echo " Sin problemas detectados en el servidor."
  echo " Si desde el equipo del piloto sigue fallando: firewall entre las dos"
  echo " maquinas, o su IP no esta en allowlist.inc."
else
  echo " $PROBLEMS problema(s) detectado(s). Arregla de arriba hacia abajo:"
  echo " el punto 1 es la causa mas frecuente y hace caer todo lo demas."
fi
echo "======================================================================"

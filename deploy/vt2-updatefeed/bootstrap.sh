#!/usr/bin/env bash
# =============================================================================
#  bootstrap.sh  -  prepara el arbol del stack vt2-updatefeed en el servidor.
#
#  Crea directorios y copia nginx/feed.conf. NO crea, levanta, detiene ni
#  reinicia ningun contenedor, y NO toca nada fuera de STACK_ROOT.
#
#  Uso (desde la carpeta que contiene este script):
#      bash bootstrap.sh
#      bash bootstrap.sh --root /ruta/alternativa
# =============================================================================
set -euo pipefail

STACK_ROOT="/home/voultech/app/VectorTrade2.0/updatefeed"

while [ $# -gt 0 ]; do
  case "$1" in
    --root) STACK_ROOT="${2:?--root necesita una ruta}"; shift 2 ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
FEED_ROOT="$STACK_ROOT/releases"
SITE_ROOT="$STACK_ROOT/site"

# El origen no puede ser el mismo destino: si subes esta carpeta directamente a
# STACK_ROOT, los `cp` de mas abajo copiarian cada archivo sobre si mismo y
# fallarian ("are the same file").
if [ "$SRC_DIR" = "$(cd "$STACK_ROOT" 2>/dev/null && pwd || echo "$STACK_ROOT")" ]; then
  echo "ERROR: esta carpeta ES el STACK_ROOT ($STACK_ROOT)." >&2
  echo "       Sube el paquete a otra ruta (por ejemplo ~/vt2-updatefeed) y" >&2
  echo "       corre bootstrap.sh desde ahi. El script copia hacia STACK_ROOT." >&2
  exit 1
fi

echo "==> STACK_ROOT = $STACK_ROOT"
echo "==> FEED_ROOT  = $FEED_ROOT"

# --- 0. Reparar bind mounts convertidos en directorios -----------------------
# Si el stack se creo ANTES de correr este script, Docker no encontro los
# archivos de config y los creo como directorios vacios. Con eso nginx falla el
# include y el contenedor muere al arrancar -> ERR_CONNECTION_REFUSED.
for f in nginx/feed.conf nginx/allowlist.inc; do
  p="$STACK_ROOT/$f"
  if [ -d "$p" ]; then
    if [ -z "$(ls -A "$p" 2>/dev/null)" ]; then
      rmdir "$p"
      echo "==> $p era un directorio vacio (creado por Docker); eliminado"
      echo "    Acuerdate de RECREAR el stack despues de este script."
    else
      echo "ERROR: $p es un directorio y NO esta vacio." >&2
      echo "       Revisalo a mano antes de continuar." >&2
      exit 1
    fi
  fi
done

# --- 1. Arbol de directorios -------------------------------------------------
mkdir -p "$STACK_ROOT/nginx" "$FEED_ROOT" "$STACK_ROOT/backups" "$SITE_ROOT"

# --- 2. Config de nginx ------------------------------------------------------
# Si ya existe una version distinta, se respalda antes de sobreescribir: estos
# archivos son editables a mano y no queremos perder ajustes locales.
for conf in feed.conf allowlist.inc; do
  DEST_CONF="$STACK_ROOT/nginx/$conf"
  if [ -f "$DEST_CONF" ] && ! cmp -s "$SRC_DIR/nginx/$conf" "$DEST_CONF"; then
    BACKUP="$STACK_ROOT/backups/$conf.$(date -u +%Y%m%dT%H%M%SZ)"
    cp -p "$DEST_CONF" "$BACKUP"
    echo "==> $conf existente respaldado en $BACKUP"
    # allowlist.inc lleva las IP del piloto: no se pisa sin avisar.
    if [ "$conf" = "allowlist.inc" ]; then
      echo "    AVISO: se conserva tu allowlist.inc actual (trae las IP autorizadas)."
      echo "           La version nueva quedo en $SRC_DIR/nginx/allowlist.inc si la quieres comparar."
      continue
    fi
  fi
  cp -p "$SRC_DIR/nginx/$conf" "$DEST_CONF"
  echo "==> nginx/$conf instalado"
done

# --- 3. Portal interno -------------------------------------------------------
cp -p "$SRC_DIR/site/index.html" "$SITE_ROOT/index.html"
echo "==> site/index.html instalado"

if [ ! -f "$FEED_ROOT/downloads.json" ]; then
  printf '%s\n' '{"generatedAt":null,"releases":[]}' > "$FEED_ROOT/downloads.json"
  echo "==> releases/downloads.json inicializado"
fi

# --- 4. Scripts de operacion -------------------------------------------------
for s in publish-feed.sh verify-feed.sh diagnose.sh; do
  [ -f "$SRC_DIR/$s" ] || continue
  cp -p "$SRC_DIR/$s" "$STACK_ROOT/$s"
  chmod +x "$STACK_ROOT/$s"
  echo "==> $s instalado en $STACK_ROOT"
done

# --- 5. Resumen --------------------------------------------------------------
echo
echo "Arbol listo:"
find "$STACK_ROOT" -maxdepth 2 -not -path '*/backups/*' | sort | sed 's/^/    /'
echo
if [ -z "$(ls -A "$FEED_ROOT" 2>/dev/null)" ]; then
  echo "NOTA: $FEED_ROOT esta vacio. El stack va a levantar igual y /healthz"
  echo "      respondera 200, pero releases.win.json dara 404 hasta que"
  echo "      publiques la primera version con publish-feed.sh."
fi
echo
echo "Siguiente paso: crear el stack en Portainer con docker-compose.yml."
echo "Este script no levanto ningun contenedor."

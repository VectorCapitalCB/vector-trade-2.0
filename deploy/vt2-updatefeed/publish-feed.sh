#!/usr/bin/env bash
# =============================================================================
#  publish-feed.sh  -  publica una version nueva en el feed de auto-update.
#
#  Se ejecuta EN EL SERVIDOR (o en cualquier maquina con el FEED_ROOT montado).
#  NO reinicia ni recrea el contenedor: nginx sirve los archivos directo del
#  bind mount, asi que basta con dejarlos en disco.
#
#  Orden de publicacion (importa):
#    1. copia los paquetes (.nupkg / Setup.exe) al feed
#    2. RECIEN AHI reemplaza releases.win.json de forma atomica (mv en el mismo
#       filesystem)
#  Si se hiciera al revés, un cliente que lea el indice nuevo antes de que este
#  el paquete recibe 404 y aborta la actualizacion.
#
#  Es aditivo: nunca borra versiones anteriores. Para revertir, basta restaurar
#  el releases.win.json respaldado (ver --rollback).
#
#  Uso:
#      bash publish-feed.sh --src /ruta/al/releases            # publica
#      bash publish-feed.sh --src /ruta/al/releases --dry-run  # solo valida
#      bash publish-feed.sh --rollback                         # indice anterior
#
#  Requiere: jq o python3 (para leer el JSON del feed), sha256sum.
# =============================================================================
set -euo pipefail

FEED_ROOT="${FEED_ROOT:-/home/voultech/app/VectorTrade2.0/updatefeed/releases}"
BACKUP_DIR="${BACKUP_DIR:-/home/voultech/app/VectorTrade2.0/updatefeed/backups}"
CHANNEL="${CHANNEL:-win}"
SRC=""
DRY_RUN=0
ROLLBACK=0

while [ $# -gt 0 ]; do
  case "$1" in
    --src)      SRC="${2:?--src necesita una ruta}"; shift 2 ;;
    --feed)     FEED_ROOT="${2:?--feed necesita una ruta}"; shift 2 ;;
    --channel)  CHANNEL="${2:?--channel necesita un valor}"; shift 2 ;;
    --dry-run)  DRY_RUN=1; shift ;;
    --rollback) ROLLBACK=1; shift ;;
    -h|--help)  sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "Argumento no reconocido: $1" >&2; exit 2 ;;
  esac
done

INDEX_NAME="releases.${CHANNEL}.json"
die() { echo "ERROR: $*" >&2; exit 1; }

# --- lector de JSON: jq si esta, si no python3 -------------------------------
json_newest_full() {   # $1=archivo json -> "version<TAB>filename<TAB>sha256"
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
d = json.load(open(sys.argv[1]))
full = [a for a in d.get('Assets') or [] if str(a.get('Type','')).lower() == 'full']
if not full: sys.exit(0)
b = max(full, key=lambda a: key(a.get('Version', '0')))
print(f"{b.get('Version','')}\t{b.get('FileName','')}\t{b.get('SHA256','')}")
PY
  else
    die "hace falta jq o python3 para leer $INDEX_NAME"
  fi
}

generate_download_catalog() {
  command -v python3 >/dev/null 2>&1 || die "python3 es necesario para generar downloads.json"
  python3 - "$FEED_ROOT" <<'PY'
import datetime
import hashlib
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
archive = root / "archive"

def version_key(value):
    return tuple(int(part) if part.isdigit() else 0 for part in re.split(r"[.-]", value))

releases = []
if archive.is_dir():
    for version_dir in archive.iterdir():
        if not version_dir.is_dir() or version_dir.name.startswith("."):
            continue
        installers = sorted(version_dir.glob("*Setup*.exe"))
        if not installers:
            installers = sorted(version_dir.glob("*.exe"))
        if not installers:
            continue
        installer = installers[0]
        digest = hashlib.sha256(installer.read_bytes()).hexdigest()
        published = datetime.datetime.fromtimestamp(
            installer.stat().st_mtime, datetime.timezone.utc
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        releases.append({
            "version": version_dir.name,
            "publishedAt": published,
            "fileName": installer.name,
            "path": f"archive/{version_dir.name}/{installer.name}",
            "size": installer.stat().st_size,
            "sha256": digest,
        })

releases.sort(key=lambda item: version_key(item["version"]), reverse=True)
payload = {
    "generatedAt": datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="seconds").replace("+00:00", "Z"),
    "releases": releases,
}
tmp = root / ".downloads.json.tmp"
tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
tmp.replace(root / "downloads.json")
PY
}

# --- rollback ----------------------------------------------------------------
if [ "$ROLLBACK" -eq 1 ]; then
  LAST="$(ls -1t "$BACKUP_DIR/$INDEX_NAME".* 2>/dev/null | head -1 || true)"
  [ -n "$LAST" ] || die "no hay respaldo de $INDEX_NAME en $BACKUP_DIR"
  echo "==> Restaurando $LAST -> $FEED_ROOT/$INDEX_NAME"
  read -r -p "Confirmas el rollback del indice del feed? [escribe SI] " ok
  [ "$ok" = "SI" ] || die "cancelado"
  cp -p "$LAST" "$FEED_ROOT/.$INDEX_NAME.tmp.$$"
  mv -f "$FEED_ROOT/.$INDEX_NAME.tmp.$$" "$FEED_ROOT/$INDEX_NAME"
  echo "==> Indice restaurado. Los paquetes no se tocaron."
  json_newest_full "$FEED_ROOT/$INDEX_NAME" | awk -F'\t' '{print "==> El feed ahora ofrece: "$1" ("$2")"}'
  exit 0
fi

[ -n "$SRC" ] || die "falta --src (carpeta 'releases' que genero vpk pack)"
[ -d "$SRC" ] || die "no existe la carpeta $SRC"
[ -d "$FEED_ROOT" ] || die "no existe FEED_ROOT=$FEED_ROOT (corre bootstrap.sh primero)"
[ -f "$SRC/$INDEX_NAME" ] || die "$SRC no contiene $INDEX_NAME"

echo "==> Origen : $SRC"
echo "==> Feed   : $FEED_ROOT"
echo "==> Canal  : $CHANNEL"

# --- 1. Integridad de lo que llego -------------------------------------------
if [ -f "$SRC/SHA256SUMS.txt" ]; then
  echo "==> Verificando SHA256SUMS.txt"
  ( cd "$SRC" && sha256sum -c --quiet SHA256SUMS.txt ) \
    || die "SHA256SUMS.txt no cuadra: el artifact llego corrupto o incompleto"
  echo "    ok"
else
  echo "==> AVISO: el origen no trae SHA256SUMS.txt; se valida solo contra el indice"
fi

# --- 2. Que ofrece el indice nuevo -------------------------------------------
NEW_LINE="$(json_newest_full "$SRC/$INDEX_NAME" || true)"
[ -n "$NEW_LINE" ] || die "$SRC/$INDEX_NAME no declara ningun asset Type=Full"
NEW_VER="$(printf '%s' "$NEW_LINE" | cut -f1)"
NEW_FILE="$(printf '%s' "$NEW_LINE" | cut -f2)"
NEW_SHA="$(printf '%s' "$NEW_LINE" | cut -f3)"

echo "==> Version nueva: $NEW_VER  ($NEW_FILE)"
[ -f "$SRC/$NEW_FILE" ] || die "el indice declara $NEW_FILE pero no esta en $SRC"

# El cliente valida SHA256 antes de aplicar: si no calza, aborta la
# actualizacion. Lo comprobamos aca para no publicar algo que va a fallar.
if [ -n "$NEW_SHA" ]; then
  ACTUAL="$(sha256sum "$SRC/$NEW_FILE" | cut -d' ' -f1)"
  if [ "$(printf '%s' "$ACTUAL" | tr 'A-Z' 'a-z')" != "$(printf '%s' "$NEW_SHA" | tr 'A-Z' 'a-z')" ]; then
    die "el SHA256 de $NEW_FILE no coincide con el declarado en el indice; el cliente rechazaria la actualizacion"
  fi
  echo "    SHA256 del paquete coincide con el indice"
else
  echo "    AVISO: el indice no declara SHA256 para $NEW_FILE"
fi

# --- 3. Que ofrece el feed hoy ----------------------------------------------
CUR_VER="(feed vacio)"
if [ -f "$FEED_ROOT/$INDEX_NAME" ]; then
  CUR_LINE="$(json_newest_full "$FEED_ROOT/$INDEX_NAME" || true)"
  [ -n "$CUR_LINE" ] && CUR_VER="$(printf '%s' "$CUR_LINE" | cut -f1)"
fi
echo "==> Version publicada hoy: $CUR_VER  ->  pasa a: $NEW_VER"

if [ "$DRY_RUN" -eq 1 ]; then
  echo
  echo "--dry-run: no se escribio nada. Archivos que se copiarian:"
  ( cd "$SRC" && ls -1 ) | sed 's/^/    /'
  exit 0
fi

# --- 4. Respaldo del indice actual ------------------------------------------
mkdir -p "$BACKUP_DIR"
if [ -f "$FEED_ROOT/$INDEX_NAME" ]; then
  STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
  cp -p "$FEED_ROOT/$INDEX_NAME" "$BACKUP_DIR/$INDEX_NAME.$STAMP"
  echo "==> Indice actual respaldado en $BACKUP_DIR/$INDEX_NAME.$STAMP"
fi

# --- 5. Paquetes PRIMERO (aditivo, nunca borra) -----------------------------
echo "==> Copiando paquetes al feed"
COPIED=0
for f in "$SRC"/*; do
  base="$(basename "$f")"
  [ -f "$f" ] || continue
  [ "$base" = "$INDEX_NAME" ] && continue      # el indice va al final
  if [ -f "$FEED_ROOT/$base" ] && cmp -s "$f" "$FEED_ROOT/$base"; then
    continue                                    # ya esta y es identico
  fi
  cp -p "$f" "$FEED_ROOT/.$base.tmp.$$"
  mv -f "$FEED_ROOT/.$base.tmp.$$" "$FEED_ROOT/$base"
  echo "    + $base"
  COPIED=$((COPIED + 1))
done
[ "$COPIED" -gt 0 ] || echo "    (sin archivos nuevos)"

# --- 6. Archivo inmutable para descarga manual -------------------------------
ARCHIVE_ROOT="$FEED_ROOT/archive"
VERSION_ARCHIVE="$ARCHIVE_ROOT/$NEW_VER"
mkdir -p "$ARCHIVE_ROOT"

if [ -d "$VERSION_ARCHIVE" ]; then
  for f in "$SRC"/*; do
    [ -f "$f" ] || continue
    base="$(basename "$f")"
    [ -f "$VERSION_ARCHIVE/$base" ] || die "el archivo historico $NEW_VER existe pero le falta $base"
    cmp -s "$f" "$VERSION_ARCHIVE/$base" || die "la version $NEW_VER ya fue archivada con contenido distinto"
  done
  echo "==> Archivo historico $NEW_VER ya existe y coincide"
else
  ARCHIVE_TMP="$ARCHIVE_ROOT/.$NEW_VER.tmp.$$"
  mkdir "$ARCHIVE_TMP"
  cp -p "$SRC"/* "$ARCHIVE_TMP/"
  mv "$ARCHIVE_TMP" "$VERSION_ARCHIVE"
  echo "==> Version $NEW_VER archivada para descarga manual"
fi

# --- 7. Indice AL FINAL y de forma atomica ----------------------------------
cp -p "$SRC/$INDEX_NAME" "$FEED_ROOT/.$INDEX_NAME.tmp.$$"
mv -f "$FEED_ROOT/.$INDEX_NAME.tmp.$$" "$FEED_ROOT/$INDEX_NAME"
echo "==> $INDEX_NAME publicado (swap atomico)"

# --- 8. Catalogo del portal --------------------------------------------------
generate_download_catalog
echo "==> downloads.json actualizado"

echo
echo "Listo. El feed ofrece la version $NEW_VER."
echo "No se reinicio ningun contenedor: nginx ya sirve los archivos nuevos."
echo
echo "Verifica de punta a punta con:"
echo "    bash verify-feed.sh --url http://127.0.0.1:${FEED_HTTP_PORT:-8092}/updatevtautoupdate --expect $NEW_VER --deep"
echo
echo "Y desde el equipo del piloto, con la URL horneada en el build (esa es la"
echo "prueba que vale: valida red, puerto y lista blanca de IP):"
echo "    curl -i http://<host-del-feed>/updatevtautoupdate/releases.win.json"

# vt2-updatefeed — feed de auto-update interno de VectorTrade (servidor .6)

Feed de Velopack servido por HTTP plano desde `/home/voultech/app/VectorTrade2.0/updatefeed` en el
servidor **172.16.0.6**, para entregar la URL a un usuario piloto. El front se
sigue firmando con SSL.com/eSigner igual que hoy; lo único nuevo es dónde vive el
feed.

**Alcance:** sólo el feed. Mongo, Redis, SQL Server y Keycloak son externos y este
stack no los toca. Los servicios backend siguen donde están. El feed antiguo del
otro servidor queda intacto y en paralelo hasta que decidas apagarlo.

---

## Orden de ejecución (checklist)

Decisiones ya tomadas: puerto **8092**, URL
`http://172.16.0.6:8092/updatevtautoupdate/` y HTTP dentro de la VPN.

- [ ] **1.** Subir esta carpeta al `.6` y correr `bash bootstrap.sh`
      → no levanta contenedores. **Este paso va PRIMERO, no es opcional**: si
      creas el stack antes, Docker convierte `nginx/feed.conf` y
      `nginx/allowlist.inc` en directorios vacíos, nginx no arranca y el síntoma
      es `ERR_CONNECTION_REFUSED`
- [ ] **2.** Crear el stack `vt2-updatefeed` en Portainer con `docker-compose.yml`
      + las variables de `.env.example` (§3.3)
- [ ] **3.** `curl -i http://127.0.0.1:8092/healthz` → `200 ok`
      **Si esto falla, para acá.** Todavía no hay nada horneado y cambiar el
      puerto es gratis (§1)
- [ ] **4.** Confirmar que `application.production.properties` apunta a
      `http://172.16.0.6:8092/updatevtautoupdate` (§4.1)
- [ ] **5.** Actions → `release-windows-signed` → versión `X.Y.Z`, primero con
      `publish=false`; revisar el artifact firmado y sus hashes
- [ ] **6.** Repetir el workflow con `publish=true`; la publicación pasa por
      `publish-feed.sh` y reemplaza el índice al final (§5)
- [ ] **7.** Instalar el `Setup.exe` a mano en el equipo del piloto (el
      auto-update sólo funciona sobre una instalación previa de Velopack)
- [ ] **8.** Confirmar que la IP VPN del equipo pertenece a uno de los rangos
      privados permitidos por `nginx/allowlist.inc` (§6)
- [ ] **9.** Desde el equipo del piloto:
      `curl -i http://172.16.0.6:8092/updatevtautoupdate/releases.win.json`
      → `200`. **Es la única prueba que vale**
- [ ] **10.** Para la próxima versión: repetir 5 y 6. Nada más

**Si algo no responde en cualquier punto:** `bash diagnose.sh` en el servidor.
Es sólo lectura, no toca contenedores, y te dice la causa exacta con el arreglo.

Pendiente antes de ampliar el despliegue: aceptación del riesgo de HTTP plano o
migración a TLS (§2).

---

## 1. La URL es parte del paquete firmado, y es permanente

Esto es lo único de esta guía que puede dejar equipos sin actualizaciones, así que
va primero.

`url=` de `application.prod.properties` se **hornea dentro del paquete** que se
instala en cada equipo. Un cliente instalado nunca cambia de feed por sí solo.
Host, puerto y ruta quedan fijos para siempre en esa versión.

Consecuencia práctica: **si después cambias el puerto o la IP, el equipo del
piloto deja de actualizarse y hay que reinstalarlo a mano.** Elige la URL una vez.

### Recomendación: usa un nombre DNS interno, no la IP

Con un registro A interno (por ejemplo `vtupdate` → `172.16.0.6`) la URL horneada
queda `http://vtupdate/updatevtautoupdate/` y puedes mover el servidor de IP sin
romper a nadie. Cuesta un registro en el DNS interno y te ahorra el único problema
irreversible de este diseño.

Si prefieres no depender del DNS, la IP funciona igual — pero entonces `172.16.0.6`
y el puerto quedan comprometidos a largo plazo.

### Cómo elegir el puerto sin riesgo: primero levanta, después hornea

El orden importa y elimina el problema por completo:

1. Levanta el stack con el puerto candidato.
2. Confirma que responde: `curl -i http://127.0.0.1:<puerto>/healthz` → `200 ok`.
3. **Recién entonces** pones ese puerto en `url=` y compilas.

Si el puerto estaba ocupado te enteras en el paso 2, cuando todavía no hay nada
horneado y no cuesta nada cambiarlo. El contenedor simplemente no arranca y no
afecta a ningún otro stack. Lo irreversible es compilar con una URL equivocada,
no probar un puerto.

### Puerto elegido: 8092

Inventario real del servidor `.6` (Portainer, 2026-08-13). Puertos **de host** ya
publicados:

```
80    4001  5119  5120  5121  5122  5123  5124  5208  5209  5210  5221  5900
6379  8000  8004  8061  8087  8091  8095  8114  8180  8701  9000  9001  9095
9195  9443  10007 11007 11020 11051 18091 22181 27017 29092 32181
33012 33013 33021 33112 33113 33121 34412 34413 34421 35012 35013 35021 49092
```

Más los que declaran las configs de VT2.0: `8086, 8089, 8096, 8097, 8098, 8099,
8100, 8101, 8197`.

**8092 está libre en ambas listas.** Es el valor de `.env.example`.

Dos trampas concretas de este servidor:

- **No uses el 80.** Ya hay un contenedor `nginx` publicando `80:80`. La idea de
  "URL sin puerto" no aplica acá.
- **No uses el 8091.** Lo publica `nuam-itch` como `8091:8090`. Ojo con leer esa
  fila: en Portainer el formato es `puertoHost:puertoContenedor`, así que el
  ocupado es el **8091** y el 8090 es interno del contenedor. Buscar "8090" en la
  lista da un falso positivo por eso.

Confirma igual antes de hornear la URL (`ss -tlnp`), porque puede haber servicios
del SO que no aparecen como contenedores.

---

## 2. Riesgo de seguridad que hay que aceptar por escrito

El feed va por **HTTP plano**. El cliente compara el SHA256 del paquete contra el
índice, pero el índice también viaja en claro: **quien pueda interceptar el tráfico
en la red interna controla el hash y el paquete**. Velopack no valida la firma
Authenticode al aplicar la actualización, así que la firma de SSL.com **no tapa
este hueco**.

En términos concretos: el canal de auto-update de una app de trading queda
suplantable desde la red interna.

Mitigación implementada: `nginx/allowlist.inc` permite únicamente redes privadas
y VPN (`10/8`, `172.16/12`, `192.168/16`). Reduce mucho la superficie, pero **no**
detiene un ataque activo dentro de esas redes.

**Antes de ampliar esto más allá del piloto hay que poner TLS.** Y este riesgo
residual lo tiene que aceptar Gerencia de Riesgo de forma explícita, no quedar
implícito en un README.

Ojo con el 403: si el equipo del piloto queda fuera de la lista blanca, el updater
recibe 403, lo trata como "no se pudo leer feed" y **no muestra ningún error**. Se
ve exactamente igual que "estoy al día".

---

## 3. Instalación en el servidor

Todo ocurre dentro de `/home/voultech/app/VectorTrade2.0/updatefeed`. **Ningún paso reinicia ni
modifica contenedores existentes.**

### 3.1 Inventario previo (sólo lectura)

```bash
ss -tlnp | sort -k4
```

Confirma que el puerto elegido está libre. Si está tomado, el contenedor queda
caído — no tumba nada más, pero no sirve el feed.

### 3.2 Preparar el árbol

Sube esta carpeta al servidor y corre:

```bash
bash bootstrap.sh
```

Crea `nginx/`, `updatefeed/` y `backups/`, instala `feed.conf` y `allowlist.inc`
(respaldando los anteriores si existían — y **nunca pisa** un `allowlist.inc` que
ya tenga IP configuradas), y deja `publish-feed.sh` y `verify-feed.sh` ejecutables.
No levanta ningún contenedor.

### 3.3 Crear el stack en Portainer

1. **Stacks → Add stack**, nombre `vt2-updatefeed`.
2. **Web editor**: pega `docker-compose.yml`.
3. **Environment variables**: copia los pares de `.env.example`. Portainer no lee
   un `.env` del disco. `FEED_BIND_ADDR` debe ser `0.0.0.0`.
4. **Deploy the stack**.

Crea un solo contenedor nuevo. Portainer no toca los demás stacks.

### 3.4 Comprobar

```bash
curl -i http://127.0.0.1:8092/healthz
```

`200 ok`. `releases.win.json` va a dar 404 hasta la primera publicación: es normal
y no deja el stack `unhealthy` (el healthcheck usa `/healthz`).

---

## 4. Compilar la versión que apunta al feed nuevo

La firma la realiza `.github/workflows/release-windows.yml` con CodeSignTool de
SSL.com/eSigner y Velopack.

### 4.1 El cambio en el repo `vector-trade-front`

En `vector-trade-front/src/main/resources/blotter/enviroment/application.production.properties`:

```
url=http://172.16.0.6:8092/updatevtautoupdate/
```

(o la URL definitiva que hayas elegido en el paso 1 — con nombre DNS o sin puerto).

Nada más: `version=` lo estampa el workflow en el runner a partir del input.

### 4.2 Convivencia con el feed antiguo

Un artefacto lleva **una sola** `url=`. No se puede tener el mismo build sirviendo
al piloto y al resto de la mesa. Con el `.9` a punto de morir, lo simple y coherente
es tratar la URL nueva como la prod definitiva:

1. **Ahora**: cambias `url=` al `.6`, compilas, firmas, publicas en el `.6` e
   instalas a mano el `Setup.exe` en el equipo del piloto. Desde ahí ese equipo se
   auto-actualiza contra el `.6`. El resto de la mesa sigue en el `.9`, intacta.
2. **Para migrar al resto** (cuando el piloto valide): publicas en el feed **viejo**
   del `.9` una versión cuyo `url=` ya apunte al `.6`. Los equipos se actualizan a
   ésa y desde ese momento consultan el `.6`.
3. **Recién entonces** apagas el `.9`. Si lo apagas antes del paso 2, cada equipo
   que quedó en el `.9` hay que reinstalarlo a mano.

El paso 2 es lo que evita el trabajo manual masivo. No te saltes el orden.

### 4.3 Sobre la contradicción de dónde está el feed viejo

Los comentarios del repo no coinciden: `build-signed-release.yml` dice **10.0.1.9**
y el paso de deploy de `release-windows.yml` usa `DEPLOY_HOST` = **172.16.0.8**.
Para el piloto en el `.6` no importa, pero **sí importa para el paso 2 de arriba**:
necesitas saber en qué host publicar la versión puente. Averígualo antes de migrar
al resto.

---

## 5. Publicar una versión ("pasa la nueva versión y actualiza")

1. Configura en este repositorio los secrets existentes de firma:
   `SSL_USERNAME`, `SSL_PASSWORD`, `SSL_TOTP_SECRET`, `SSL_CREDENTIAL_ID`.
2. Configura los secrets de despliegue: `DEPLOY_HOST`, `DEPLOY_USER` y
   `DEPLOY_SSH_KEY`. Como protección, el workflow rechaza cualquier
   `DEPLOY_HOST` distinto de `172.16.0.6`.
3. **Actions → release-windows-signed → Run workflow**, `version = X.Y.Z` y
   `publish=false`. Compila con Gluon, firma app/updater/instalador, valida
   Authenticode y timestamp, y sube `VectorTrade-signed-X.Y.Z`.
4. Revisa el artifact y `SHA256SUMS.txt`. Luego repite con `publish=true`.
   El job de publicación usa el runner Windows interno y publica en forma
   atómica mediante `publish-feed.sh`.

**Sólo clave SSH, no contraseña.** Crea un par de claves para un usuario de deploy
con permiso de escritura *sólo* sobre `FEED_ROOT`. Una contraseña en un secret de
CI termina en logs del runner.

> `publish-feed` corre en `[self-hosted, windows, gluon]` y eso no es opcional: el
> `.6` tiene IP privada y un runner hospedado por GitHub no lo alcanza — el `scp`
> se cuelga hasta el timeout. Tiene que ser el runner que ya vive en la red interna,
> y su IP tiene que estar en `allowlist.inc` para que el paso de verificación pase.

### Publicación manual (sin runner self-hosted)

```bash
gh run download <RUN_ID> -R VectorCapitalCB/vector-trade-front -n VectorTrade-signed-X.Y.Z -D ./feed-stage
```

Sube `./feed-stage` al servidor y ahí:

```bash
FEED_ROOT=/home/voultech/app/VectorTrade2.0/updatefeed/releases bash publish-feed.sh --src ./feed-stage --dry-run
```

Si el dry-run está limpio, repite sin `--dry-run`.

### Primera entrega al piloto

El auto-update sólo funciona sobre una instalación de Velopack existente. Para el
primer equipo hay que instalar a mano: toma el `VectorTradeSetup.exe` (o
`Setup.exe`) del artifact firmado y que el usuario lo ejecute. De ahí en adelante
se actualiza solo.

### Por qué no hay que reiniciar nada

nginx sirve los archivos directo del bind mount. Publicar es dejar archivos en
disco: **cero reinicios, cero recreación de contenedores.** Si alguna instrucción
te pide reiniciar este stack para publicar, está mal.

`publish-feed.sh` copia los paquetes **primero** y reemplaza `releases.win.json`
**al final**, con un `mv` atómico. Al revés, un cliente que lea el índice nuevo
antes de que llegue el paquete recibe 404 y aborta la actualización. Es aditivo:
nunca borra versiones anteriores.

---

## 6. Verificación

Desde el propio servidor:

```bash
bash verify-feed.sh --url http://127.0.0.1:8092/updatevtautoupdate --expect X.Y.Z --deep
```

Y —esto es lo que de verdad importa— **desde el equipo del piloto**, con la URL
exacta que quedó horneada en el build:

```bash
curl -i http://172.16.0.6:8092/updatevtautoupdate/releases.win.json
```

Si eso no da `200`, el piloto no se va a actualizar y no va a ver ningún error.

`verify-feed.sh` replica lo que hace `VelopackUpdater`: pide el índice exigiendo
HTTP 200 **sin seguir redirecciones**, elige el asset `Type=Full` más nuevo, baja el
paquete y compara el SHA256.

| Síntoma | Causa |
|---|---|
| 301/302 | `HttpURLConnection` no sigue redirecciones entre http y https. El updater loguea "no se pudo leer feed" y sigue: nadie se actualiza, sin error visible. |
| 403 | La IP del cliente no está en `allowlist.inc`. Se ve igual que "estoy al día". |
| Ningún `Type=Full` en el índice | El cliente ignora los delta que sí genera `vpk pack`. Sin un Full no hay actualización. |
| El índice declara un paquete que da 404 | Se publicó el índice antes que los paquetes. |

También avisa si el índice se está cacheando y si la descarga se acerca a los
**120 s de read timeout** del cliente.

### Cambiar la lista blanca sin reiniciar

```bash
docker exec vt2-updatefeed nginx -t && docker exec vt2-updatefeed nginx -s reload
```

`nginx -t` primero: si la config queda mala, el reload no se aplica y nginx sigue
sirviendo con la anterior.

---

## 7. Rollback

Sólo el índice, que es lo que decide qué versión se ofrece. Los paquetes no se
borran nunca:

```bash
FEED_ROOT=/home/voultech/app/VectorTrade2.0/updatefeed/releases bash publish-feed.sh --rollback
```

Restaura el `releases.win.json` respaldado más reciente (pide confirmación
escribiendo `SI`). Sin reinicios.

Los clientes que ya se actualizaron **no vuelven atrás solos**: Velopack no degrada
versión. El rollback evita que se sigan actualizando más equipos; para revertir los
que ya pasaron hay que reinstalar.

---

## 8. Límites

- **No hay TLS.** Ver sección 2. Obligatorio antes de ampliar más allá del piloto.
- **La firma de SSL.com no protege el canal**, sólo acredita el origen del binario
  ante Windows. Velopack no la valida al aplicar.
- **No migra datos** ni toca ninguna base.
- **No despliega los servicios backend.** Si más adelante los quieres acá, es otro
  stack — y en el caso del OMS, una ventana de cambio coordinada.
- **Los delta se publican pero nadie los usa**: `VelopackUpdater` filtra
  `Type == "Full"`. Cada actualización baja el paquete completo.
- El `.9` sigue vivo y sirviendo al resto de la mesa. Este stack no lo toca.

---

## 9. Archivos

| Archivo | Qué es |
|---|---|
| `docker-compose.yml` | Stack para el editor web de Portainer |
| `.env.example` | Variables a copiar en Portainer (sin secretos) |
| `nginx/feed.conf` | Config de nginx, editable en el servidor |
| `nginx/allowlist.inc` | Lista blanca de IP del piloto. Editable y recargable en caliente |
| `bootstrap.sh` | Prepara el árbol. No levanta contenedores |
| `publish-feed.sh` | Publica una versión. Sin reinicios. `--dry-run` / `--rollback` |
| `verify-feed.sh` | Valida el feed como el cliente. Sólo lectura |
| `diagnose.sh` | Por qué el feed no responde. Sólo lectura, no toca contenedores |
| `test-feed.sh` | Prueba e2e de los dos scripts con un feed sintético (23 casos) |
| `github-workflows/publish-feed.yml` | Workflow de publicación, va en `vector-trade-front` |

---

Material de apoyo, no fuente de verdad. Dos cosas que necesitan visto bueno de
alguien más antes de ejecutarse: la aceptación del riesgo de HTTP plano (sección 2)
y el orden de migración del `.9` (sección 4.2).

# =============================================================================
#  allowlist.inc  -  quien puede leer el feed de auto-update.
#
#  Lo incluyen los tres bloques `location` de feed.conf. Se separa en su propio
#  archivo para poder cambiar el alcance del piloto sin tocar feed.conf.
#
#  POR QUE EXISTE
#  El feed va por HTTP plano en la red interna. El cliente compara el SHA256 del
#  paquete contra el indice, pero el indice tambien viaja en claro: quien pueda
#  interceptar el trafico controla ambos. Velopack no valida Authenticode al
#  aplicar, asi que la firma de SSL.com no tapa ese hueco. Restringir por IP no
#  resuelve un ataque activo en la red, pero reduce muchisimo la superficie
#  mientras el piloto es de una sola maquina.
#
#  COMO EDITARLO
#  Una linea `allow` por equipo autorizado, y `deny all;` SIEMPRE al final
#  (nginx evalua en orden y se queda con la primera coincidencia).
#  Despues de editar, recarga sin reiniciar el contenedor:
#      docker exec <contenedor> nginx -t && docker exec <contenedor> nginx -s reload
#  `nginx -t` primero: si la config queda mala, el reload no se aplica y nginx
#  sigue sirviendo con la anterior.
#
#  OJO con el 403: si el equipo del piloto queda fuera de la lista, el updater
#  recibe 403, lo trata como "no se pudo leer feed" y NO muestra ningun error al
#  usuario. Se ve igual que "estoy al dia". Valida siempre con verify-feed.sh
#  desde la IP del piloto despues de cambiar esto.
# =============================================================================

# --- Equipo del usuario piloto ----------------------------------------------
# Reemplaza por la IP real antes de entregar la URL.
# allow 172.16.0.123;

# --- El propio servidor, para que verify-feed.sh corra en local -------------
allow 127.0.0.1;

# --- Runner de CI que publica y verifica el feed ----------------------------
# allow 172.16.0.XX;

# --- Redes privadas/VPN -----------------------------------------------------
# Ricci entra por la VPN 10.x. Los rangos 172.16/12 y 192.168/16 cubren la red
# interna, el host y los healthchecks de Docker sin abrir el portal a Internet.
allow 10.0.0.0/8;
allow 172.16.0.0/12;
allow 192.168.0.0/16;
deny  all;

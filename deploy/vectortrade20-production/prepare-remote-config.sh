#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/voultech/app/VectorTrade2.0
PROD_CONFIG=/home/voultech/app/vector-trade-ricci/config
CORE_CONFIG="$ROOT/core/config/application.properties"

set_property() {
  local file=$1 key=$2 value=$3 tmp
  tmp="${file}.tmp"
  awk -F= -v key="$key" -v value="$value" '
    BEGIN { found = 0 }
    $1 == key { print key "=" value; found = 1; next }
    { print }
    END { if (!found) print key "=" value }
  ' "$file" > "$tmp"
  mv "$tmp" "$file"
}

inherit_property() {
  local key=$1 value
  value=$(awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$PROD_CONFIG/application.properties")
  if [[ -n "$value" ]]; then
    set_property "$CORE_CONFIG" "$key" "$value"
  fi
}

for key in \
  BCS.SECURITYLIST IB.SECURITYLIST admin.token clientid password password.sql \
  passwordrequiere realm redis.password secret sendsecuritylist server.sql \
  subscribeSecuritylist_BCS telegram.bot.token telegram.chat.id url username username.sql; do
  inherit_property "$key"
done

set_property "$CORE_CONFIG" websocket.port 8096
set_property "$CORE_CONFIG" admin.port 8089
set_property "$CORE_CONFIG" admin.cors.origin ""
set_property "$CORE_CONFIG" idconnection service-vector-trade-2-prod
set_property "$CORE_CONFIG" connections /usr/local/app/config/connections.json
set_property "$CORE_CONFIG" connectionsmkd /usr/local/app/config/connectionsmkd.json
set_property "$CORE_CONFIG" path.logs /usr/local/app/logs/
set_property "$CORE_CONFIG" redis.host 172.16.0.6
set_property "$CORE_CONFIG" redis.port 6379
set_property "$CORE_CONFIG" redis.enable.persistencia true
set_property "$CORE_CONFIG" mongo.connection "$(awk -F= '$1 == "mongo.connection" { sub(/^[^=]*=/, ""); print; exit }' "$PROD_CONFIG/application.properties")"
set_property "$CORE_CONFIG" mongo.db close_prices
set_property "$CORE_CONFIG" mongo.isconnected true
set_property "$CORE_CONFIG" history.enabled true
set_property "$CORE_CONFIG" history.mongo.db vector_trade_history
set_property "$CORE_CONFIG" prioridad.users.enabled true
set_property "$CORE_CONFIG" prioridad.users vnazar,daedo,fricci
set_property "$CORE_CONFIG" account.validation.enabled true
set_property "$CORE_CONFIG" requiere.sql false
set_property "$CORE_CONFIG" bolsa.stats true
set_property "$CORE_CONFIG" strategy.rate.limit.pause.seconds 3
set_property "$CORE_CONFIG" strategy.blocked.resume.seconds 10
set_property "$CORE_CONFIG" strategy.reject.cancel.threshold 5

cp "$PROD_CONFIG/connections.json" "$ROOT/core/config/connections.json"
cp "$PROD_CONFIG/connectionsmkd.json" "$ROOT/core/config/connectionsmkd.json"

set_property "$ROOT/candle/config/application.properties" candle.websocket.port 8098
set_property "$ROOT/candle/config/application.properties" candle.server.host 0.0.0.0
set_property "$ROOT/candle/config/application.properties" mongo.candle.uri "$(awk -F= '$1 == "mongo.candle.uri" { sub(/^[^=]*=/, ""); gsub(/68[.]211[.]112[.]146/, "172.16.0.8"); print; exit }' "$ROOT/candle/config/application.properties")"
set_property "$ROOT/chat/config/application.properties" chat.websocket.port 8097
set_property "$ROOT/chat/config/application.properties" chat.server.host 0.0.0.0
set_property "$ROOT/chat/config/application.properties" mongo.chat.uri "$(awk -F= '$1 == "mongo.chat.uri" { sub(/^[^=]*=/, ""); gsub(/68[.]211[.]112[.]146/, "172.16.0.8"); print; exit }' "$ROOT/chat/config/application.properties")"
set_property "$ROOT/news/config/application.properties" news.websocket.port 8100
set_property "$ROOT/news/config/application.properties" mongo.news.uri "$(awk -F= '$1 == "mongo.news.uri" { sub(/^[^=]*=/, ""); gsub(/68[.]211[.]112[.]146/, "172.16.0.8"); print; exit }' "$ROOT/news/config/application.properties")"

find "$ROOT" -path '*/logs' -prune -o -type d -exec chmod 750 {} +
find "$ROOT" -path '*/logs' -prune -o -type f -exec chmod 640 {} +
chmod 750 "$ROOT/prepare-remote-config.sh"

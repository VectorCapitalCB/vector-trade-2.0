package cl.vc.chat.websocket;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

public class ChatViewServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Properties properties = (Properties) getServletContext().getAttribute("chat.properties");
        if (properties == null) {
            properties = new Properties();
        }

        String chatHost = properties.getProperty("chat.server.host", "localhost");
        String chatPort = properties.getProperty("chat.websocket.port", "8097");
        String chatPath = properties.getProperty("chat.websocket.path", "/ws/");

        String candleHost = properties.getProperty("candle.external.host", "localhost");
        String candlePort = properties.getProperty("candle.external.port", "8098");
        String candlePath = properties.getProperty("candle.external.path", "/ws/");

        String defaultChatWs = properties.getProperty(
                "view.default.chat.ws.url",
                "ws://" + chatHost + ":" + chatPort + normalizePath(chatPath)
        );
        String defaultCandleWs = properties.getProperty(
                "view.default.candle.ws.url",
                "ws://" + candleHost + ":" + candlePort + normalizePath(candlePath)
        );
        String defaultSymbol = properties.getProperty("view.default.candle.symbol", "SQM");
        String defaultTimeframe = properties.getProperty("view.default.candle.timeframe", "1m");

        resp.setContentType("text/html; charset=UTF-8");
        resp.getWriter().write("""
                <!doctype html>
                <html lang="es">
                <head>
                  <meta charset="UTF-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>Vector Chat + Candle</title>
                  <style>
                    body{font-family:Segoe UI,sans-serif;max-width:1080px;margin:20px auto;padding:0 16px 90px;background:#f7f8fb;color:#1b1f28}
                    h3{margin:0 0 12px 0}
                    .card{background:#fff;border:1px solid #e3e7ef;border-radius:10px;padding:12px;margin-bottom:10px}
                    .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center}
                    .chat-grid{display:grid;grid-template-columns:1.3fr 1fr;gap:10px;margin-top:8px}
                    .panel-title{font-size:13px;font-weight:700;margin:0 0 6px 0;color:#334155}
                    .side-box{height:240px;overflow:auto;border:1px solid #dbe2ee;background:#fff;padding:8px;border-radius:8px}
                    input,button{padding:8px;border-radius:8px;border:1px solid #cfd6e4}
                    button{background:#0f62fe;color:#fff;border:none;cursor:pointer}
                    button.alt{background:#4b5563}
                    #m,#candleFeed{height:240px;overflow:auto;border:1px solid #dbe2ee;background:#fff;padding:10px;border-radius:8px}
                    #candleFeed{height:180px}
                    .me{color:#067d57;font-weight:600}
                    .item{padding:6px 8px;border:1px solid #e2e8f0;border-radius:8px;margin-bottom:6px;background:#f8fafc;cursor:pointer}
                    .item:hover{background:#eef2ff}
                    .small{font-size:12px;color:#475569}
                    footer{position:fixed;left:0;right:0;bottom:0;background:#0f172a;color:#e5e7eb;padding:10px 16px;border-top:1px solid #1f2937}
                    .footer-wrap{max-width:1080px;margin:0 auto;display:flex;gap:14px;flex-wrap:wrap;align-items:center}
                    .chip{display:flex;align-items:center;gap:8px;background:#111827;padding:6px 10px;border-radius:999px;font-size:13px}
                    .icon-dot{width:10px;height:10px;border-radius:50%;display:inline-block;background:#9ca3af}
                    .ok{background:#10b981}
                    .bad{background:#ef4444}
                    .muted{opacity:.8;font-size:12px}
                    @media (max-width: 900px){ .chat-grid{grid-template-columns:1fr;} }
                  </style>
                </head>
                <body>
                  <h3>Vector Chat + Candle Viewer</h3>

                  <section class="card">
                    <div class="row">
                      <input id="me" placeholder="tu username">
                      <input id="peer" placeholder="username destino">
                      <input id="chatUrl" style="min-width:260px" placeholder="ws chat" value="__CHAT_WS__">
                      <button id="connectChat">Conectar Chat</button>
                      <button id="refreshLists" class="alt">Refrescar listas</button>
                      <button id="connectAll" class="alt">Conectar Todo</button>
                    </div>

                    <div class="chat-grid">
                      <div>
                        <div class="panel-title">Mensajes</div>
                        <div id="m"></div>
                      </div>
                      <div>
                        <div class="panel-title">Usuarios</div>
                        <div id="usersList" class="side-box"></div>
                        <div class="panel-title" style="margin-top:8px">Conversaciones</div>
                        <div id="conversationsList" class="side-box" style="height:130px"></div>
                      </div>
                    </div>

                    <div class="row" style="margin-top:8px">
                      <input id="t" style="min-width:260px" placeholder="mensaje">
                      <button id="sendChat">Enviar</button>
                    </div>
                  </section>

                  <section class="card">
                    <div class="row">
                      <input id="candleUrl" style="min-width:260px" placeholder="ws candle" value="__CANDLE_WS__">
                      <input id="symbol" placeholder="symbol" value="__SYMBOL__">
                      <input id="timeframe" placeholder="timeframe" value="__TIMEFRAME__">
                      <button id="connectCandle">Conectar Candle</button>
                      <button id="subCandle" class="alt">Suscribir</button>
                      <button id="unsubCandle" class="alt">Desuscribir</button>
                    </div>
                    <div style="margin-top:8px" id="candleFeed"></div>
                  </section>

                  <footer>
                    <div class="footer-wrap">
                      <div class="chip"><span id="chatDot" class="icon-dot"></span><span>Chat WS</span><span id="chatState" class="muted">desconectado</span></div>
                      <div class="chip"><span id="candleDot" class="icon-dot"></span><span>Candle WS</span><span id="candleState" class="muted">desconectado</span></div>
                      <div class="chip"><span class="icon-dot ok"></span><span id="lastEvent" class="muted">sin eventos</span></div>
                    </div>
                  </footer>

                  <script>
                    let chatWs, candleWs;
                    const m = document.getElementById('m');
                    const usersList = document.getElementById('usersList');
                    const conversationsList = document.getElementById('conversationsList');
                    const candleFeed = document.getElementById('candleFeed');
                    const meInput = document.getElementById('me');
                    const peerInput = document.getElementById('peer');
                    const me = () => meInput.value.trim();
                    const peer = () => peerInput.value.trim();
                    const chatDot = document.getElementById('chatDot');
                    const candleDot = document.getElementById('candleDot');
                    const chatState = document.getElementById('chatState');
                    const candleState = document.getElementById('candleState');
                    const lastEvent = document.getElementById('lastEvent');

                    meInput.value = localStorage.getItem('chat.me') || '';
                    peerInput.value = localStorage.getItem('chat.peer') || '';
                    meInput.oninput = () => localStorage.setItem('chat.me', me());
                    peerInput.oninput = () => localStorage.setItem('chat.peer', peer());

                    const setEvent = (txt) => { lastEvent.textContent = txt; };
                    const setChatStatus = (ok, txt) => { chatDot.className = 'icon-dot ' + (ok ? 'ok' : 'bad'); chatState.textContent = txt; };
                    const setCandleStatus = (ok, txt) => { candleDot.className = 'icon-dot ' + (ok ? 'ok' : 'bad'); candleState.textContent = txt; };

                    const addMsg = (u, t, ts) => {
                      const d = document.createElement('div');
                      d.className = u === me() ? 'me' : '';
                      d.textContent = `${new Date(ts).toLocaleString()} ${u}: ${t}`;
                      m.appendChild(d);
                      m.scrollTop = m.scrollHeight;
                    };

                    const renderUsers = (rows) => {
                      usersList.innerHTML = '';
                      (rows || []).forEach(u => {
                        const d = document.createElement('div');
                        d.className = 'item';
                        d.textContent = u;
                        d.onclick = () => {
                          peerInput.value = u;
                          localStorage.setItem('chat.peer', u);
                          requestHistory();
                        };
                        usersList.appendChild(d);
                      });
                    };

                    const renderConversations = (rows) => {
                      conversationsList.innerHTML = '';
                      (rows || []).forEach(c => {
                        const d = document.createElement('div');
                        d.className = 'item';
                        d.innerHTML = `<div>${c.withUsername}</div><div class="small">${new Date(c.timestamp).toLocaleString()} - ${c.lastMessage || ''}</div>`;
                        d.onclick = () => {
                          peerInput.value = c.withUsername;
                          localStorage.setItem('chat.peer', c.withUsername);
                          requestHistory();
                        };
                        conversationsList.appendChild(d);
                      });
                    };

                    const requestHistory = () => {
                      if (!chatWs || chatWs.readyState !== WebSocket.OPEN || !me() || !peer()) return;
                      chatWs.send(JSON.stringify({action:'chat_history', username:me(), withUsername:peer(), limit:500}));
                    };

                    const refreshLists = () => {
                      if (!chatWs || chatWs.readyState !== WebSocket.OPEN || !me()) return;
                      chatWs.send(JSON.stringify({action:'chat_users', limit:300}));
                      chatWs.send(JSON.stringify({action:'chat_conversations', username:me(), limit:200}));
                    };

                    const addCandle = (obj) => {
                      const d = document.createElement('div');
                      d.textContent = JSON.stringify(obj);
                      candleFeed.appendChild(d);
                      candleFeed.scrollTop = candleFeed.scrollHeight;
                    };

                    const connectChat = () => {
                      if (!me()) { setEvent('falta tu username'); return; }
                      const url = document.getElementById('chatUrl').value.trim();
                      if (!url) { setEvent('falta URL chat'); return; }
                      if (chatWs && chatWs.readyState === WebSocket.OPEN) chatWs.close();
                      chatWs = new WebSocket(url);
                      chatWs.onopen = () => {
                        setChatStatus(true, 'conectado');
                        setEvent('chat conectado');
                        chatWs.send(JSON.stringify({action:'chat_register', username:me()}));
                        refreshLists();
                        requestHistory();
                      };
                      chatWs.onclose = () => { setChatStatus(false, 'desconectado'); setEvent('chat desconectado'); };
                      chatWs.onerror = () => { setChatStatus(false, 'error'); setEvent('error chat websocket'); };
                      chatWs.onmessage = (e) => {
                        const x = JSON.parse(e.data);
                        if (x.type === 'chat_users') {
                          renderUsers(x.users || []);
                          setEvent('usuarios cargados');
                        } else if (x.type === 'chat_conversations') {
                          renderConversations(x.conversations || []);
                          setEvent('conversaciones cargadas');
                        } else if (x.type === 'chat_history') {
                          m.innerHTML = '';
                          (x.messages || []).forEach(r => addMsg(r.fromUsername, r.message, r.timestamp));
                          setEvent('historial chat cargado');
                        } else if (x.type === 'chat_message') {
                          addMsg(x.fromUsername, x.message, x.timestamp);
                          refreshLists();
                          setEvent('mensaje chat recibido');
                        } else if (x.type === 'error') {
                          setEvent('chat error: ' + x.message);
                        }
                      };
                    };

                    const connectCandle = () => {
                      const url = document.getElementById('candleUrl').value.trim();
                      if (!url) { setEvent('falta URL candle'); return; }
                      if (candleWs && candleWs.readyState === WebSocket.OPEN) candleWs.close();
                      candleWs = new WebSocket(url);
                      candleWs.onopen = () => { setCandleStatus(true, 'conectado'); setEvent('candle conectado'); };
                      candleWs.onclose = () => { setCandleStatus(false, 'desconectado'); setEvent('candle desconectado'); };
                      candleWs.onerror = () => { setCandleStatus(false, 'error'); setEvent('error candle websocket'); };
                      candleWs.onmessage = (e) => {
                        const x = JSON.parse(e.data);
                        addCandle(x);
                        if (x.type === 'candle') setEvent('nueva vela recibida');
                        if (x.type === 'bootstrap') setEvent('bootstrap de velas recibido');
                      };
                    };

                    document.getElementById('connectChat').onclick = connectChat;
                    document.getElementById('refreshLists').onclick = refreshLists;
                    document.getElementById('connectCandle').onclick = connectCandle;
                    document.getElementById('connectAll').onclick = () => { connectChat(); connectCandle(); };

                    document.getElementById('sendChat').onclick = () => {
                      const t = document.getElementById('t');
                      if (!chatWs || chatWs.readyState !== WebSocket.OPEN || !t.value.trim()) return;
                      if (!me() || !peer()) { setEvent('faltan usernames para enviar'); return; }
                      chatWs.send(JSON.stringify({action:'chat_send', fromUsername:me(), toUsername:peer(), message:t.value.trim()}));
                      t.value = '';
                    };

                    document.getElementById('subCandle').onclick = () => {
                      if (!candleWs || candleWs.readyState !== WebSocket.OPEN) return;
                      candleWs.send(JSON.stringify({
                        action:'subscribe',
                        symbol:document.getElementById('symbol').value.trim(),
                        timeframe:document.getElementById('timeframe').value.trim()
                      }));
                      setEvent('solicitud suscripcion enviada');
                    };

                    document.getElementById('unsubCandle').onclick = () => {
                      if (!candleWs || candleWs.readyState !== WebSocket.OPEN) return;
                      candleWs.send(JSON.stringify({
                        action:'unsubscribe',
                        symbol:document.getElementById('symbol').value.trim(),
                        timeframe:document.getElementById('timeframe').value.trim()
                      }));
                      setEvent('solicitud desuscripcion enviada');
                    };
                  </script>
                </body>
                </html>
                """
                .replace("__CHAT_WS__", escapeHtml(defaultChatWs))
                .replace("__CANDLE_WS__", escapeHtml(defaultCandleWs))
                .replace("__SYMBOL__", escapeHtml(defaultSymbol))
                .replace("__TIMEFRAME__", escapeHtml(defaultTimeframe))
        );
    }

    private static String normalizePath(String path) {
        if (path == null || path.trim().isEmpty()) {
            return "/ws/";
        }
        String p = path.trim();
        if (!p.startsWith("/")) p = "/" + p;
        if (!p.endsWith("/")) p = p + "/";
        return p;
    }

    private static String escapeHtml(String value) {
        if (value == null) return "";
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}

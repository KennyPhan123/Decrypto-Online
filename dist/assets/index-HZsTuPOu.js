var V=Object.defineProperty;var q=(e,t,s)=>t in e?V(e,t,{enumerable:!0,configurable:!0,writable:!0,value:s}):e[t]=s;var c=(e,t,s)=>q(e,typeof t!="symbol"?t+"":t,s);(function(){const t=document.createElement("link").relList;if(t&&t.supports&&t.supports("modulepreload"))return;for(const i of document.querySelectorAll('link[rel="modulepreload"]'))n(i);new MutationObserver(i=>{for(const r of i)if(r.type==="childList")for(const a of r.addedNodes)a.tagName==="LINK"&&a.rel==="modulepreload"&&n(a)}).observe(document,{childList:!0,subtree:!0});function s(i){const r={};return i.integrity&&(r.integrity=i.integrity),i.referrerPolicy&&(r.referrerPolicy=i.referrerPolicy),i.crossOrigin==="use-credentials"?r.credentials="include":i.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function n(i){if(i.ep)return;i.ep=!0;const r=s(i);fetch(i.href,r)}})();(!globalThis.EventTarget||!globalThis.Event)&&console.error(`
  PartySocket requires a global 'EventTarget' class to be available!
  You can polyfill this global by adding this to your code before any partysocket imports: 
  
  \`\`\`
  import 'partysocket/event-target-polyfill';
  \`\`\`
  Please file an issue at https://github.com/partykit/partykit if you're still having trouble.
`);var H=class extends Event{constructor(t,s){super("error",s);c(this,"message");c(this,"error");this.message=t.message,this.error=t}},B=class extends Event{constructor(t=1e3,s="",n){super("close",n);c(this,"code");c(this,"reason");c(this,"wasClean",!0);this.code=t,this.reason=s}};const $={Event,ErrorEvent:H,CloseEvent:B};function K(e,t){if(!e)throw new Error(t)}function F(e){return new e.constructor(e.type,e)}function Q(e){return"data"in e?new MessageEvent(e.type,e):"code"in e||"reason"in e?new B(e.code||1999,e.reason||"unknown reason",e):"error"in e?new H(e.error,e):new Event(e.type,e)}var G;const Y=typeof process<"u"&&typeof((G=process.versions)==null?void 0:G.node)<"u",J=typeof navigator<"u"&&navigator.product==="ReactNative",T=Y||J?Q:F,_={maxReconnectionDelay:1e4,minReconnectionDelay:3e3,minUptime:5e3,reconnectionDelayGrowFactor:1.3,connectionTimeout:4e3,maxRetries:Number.POSITIVE_INFINITY,maxEnqueuedMessages:Number.POSITIVE_INFINITY};let I=!1;function z(){}var X=class b extends EventTarget{constructor(s,n,i={}){super();c(this,"_ws");c(this,"_retryCount",-1);c(this,"_uptimeTimeout");c(this,"_connectTimeout");c(this,"_shouldReconnect",!0);c(this,"_connectLock",!1);c(this,"_binaryType","blob");c(this,"_closeCalled",!1);c(this,"_didWarnAboutClosedSend",!1);c(this,"_messageQueue",[]);c(this,"_debugLogger",console.log.bind(console));c(this,"_url");c(this,"_protocols");c(this,"_options");c(this,"onclose",null);c(this,"onerror",null);c(this,"onmessage",null);c(this,"onopen",null);c(this,"_handleOpen",s=>{this._debug("open event");const{minUptime:n=_.minUptime}=this._options;clearTimeout(this._connectTimeout),this._uptimeTimeout=setTimeout(()=>this._acceptOpen(),n),K(this._ws,"WebSocket is not defined"),this._ws.binaryType=this._binaryType,this._messageQueue.forEach(i=>{var r;(r=this._ws)==null||r.send(i)}),this._messageQueue=[],this.onopen&&this.onopen(s),this.dispatchEvent(T(s))});c(this,"_handleMessage",s=>{this._debug("message event"),this.onmessage&&this.onmessage(s),this.dispatchEvent(T(s))});c(this,"_handleError",s=>{this._debug("error event",s.message),this._disconnect(void 0,s.message==="TIMEOUT"?"timeout":void 0),this.onerror&&this.onerror(s),this._debug("exec error listeners"),this.dispatchEvent(T(s)),this._connect()});c(this,"_handleClose",s=>{this._debug("close event"),this._clearTimeouts(),this._options.shouldReconnectOnClose&&!this._options.shouldReconnectOnClose(s)&&(this._shouldReconnect=!1),this._shouldReconnect&&this._connect(),this.onclose&&this.onclose(s),this.dispatchEvent(T(s))});this._url=s,this._protocols=n,this._options=i,this._options.startClosed&&(this._shouldReconnect=!1),this._options.debugLogger&&(this._debugLogger=this._options.debugLogger),this._connect()}static get CONNECTING(){return 0}static get OPEN(){return 1}static get CLOSING(){return 2}static get CLOSED(){return 3}get CONNECTING(){return b.CONNECTING}get OPEN(){return b.OPEN}get CLOSING(){return b.CLOSING}get CLOSED(){return b.CLOSED}get binaryType(){return this._ws?this._ws.binaryType:this._binaryType}set binaryType(s){this._binaryType=s,this._ws&&(this._ws.binaryType=s)}get retryCount(){return Math.max(this._retryCount,0)}get bufferedAmount(){return this._messageQueue.reduce((s,n)=>(typeof n=="string"?s+=n.length:n instanceof Blob?s+=n.size:s+=n.byteLength,s),0)+(this._ws?this._ws.bufferedAmount:0)}get extensions(){return this._ws?this._ws.extensions:""}get protocol(){return this._ws?this._ws.protocol:""}get readyState(){return this._closeCalled?b.CLOSED:this._ws?this._ws.readyState:this._options.startClosed?b.CLOSED:b.CONNECTING}get url(){return this._ws?this._ws.url:""}get shouldReconnect(){return this._shouldReconnect}close(s=1e3,n){if(this._closeCalled=!0,this._shouldReconnect=!1,this._clearTimeouts(),!this._ws){this._debug("close enqueued: no ws instance");return}if(this._ws.readyState===this.CLOSED||this._ws.readyState===this.CLOSING){this._debug("close: already closing or closed");return}this._disconnect(s,n)}reconnect(s,n){this._shouldReconnect=!0,this._closeCalled=!1,this._didWarnAboutClosedSend=!1,this._retryCount=-1,!this._ws||this._ws.readyState===this.CLOSED||this._ws.readyState===this.CLOSING?this._connect():(this._disconnect(s,n),this._connect())}send(s){if(this._ws&&this._ws.readyState===this.OPEN)return this._debug("send",s),this._ws.send(s),!0;this._closeCalled&&!this._didWarnAboutClosedSend&&(this._didWarnAboutClosedSend=!0,console.warn("ReconnectingWebSocket: send() was called after close(). The message has been buffered, but it will only be delivered if reconnect() is called on this socket. If this socket has been discarded, the message is lost — this usually means a stale socket reference is being used."));const{maxEnqueuedMessages:n=_.maxEnqueuedMessages}=this._options;return this._messageQueue.length<n&&(this._debug("enqueue",s),this._messageQueue.push(s)),!1}drainQueuedMessages(){const s=this._messageQueue;return this._messageQueue=[],s}_debug(...s){this._options.debug&&this._debugLogger("RWS>",...s)}_getNextDelay(){const{reconnectionDelayGrowFactor:s=_.reconnectionDelayGrowFactor,minReconnectionDelay:n=_.minReconnectionDelay,maxReconnectionDelay:i=_.maxReconnectionDelay}=this._options;let r=0;return this._retryCount>0&&(r=n*s**(this._retryCount-1),r>i&&(r=i)),this._debug("next delay",r),r}_wait(){return new Promise(s=>{setTimeout(s,this._getNextDelay())})}_getNextProtocols(s){if(!s)return Promise.resolve(null);if(typeof s=="string"||Array.isArray(s))return Promise.resolve(s);if(typeof s=="function"){const n=s();if(!n)return Promise.resolve(null);if(typeof n=="string"||Array.isArray(n))return Promise.resolve(n);if(n.then)return n}throw Error("Invalid protocols")}_getNextUrl(s){if(typeof s=="string")return Promise.resolve(s);if(typeof s=="function"){const n=s();if(typeof n=="string")return Promise.resolve(n);if(n.then)return n}throw Error("Invalid URL")}_connect(){if(this._connectLock||!this._shouldReconnect)return;this._connectLock=!0;const{maxRetries:s=_.maxRetries,connectionTimeout:n=_.connectionTimeout}=this._options;if(this._retryCount>=s){this._debug("max retries reached",this._retryCount,">=",s),this._connectLock=!1;return}this._retryCount++,this._debug("connect",this._retryCount),this._removeListeners(),this._wait().then(()=>Promise.all([this._getNextUrl(this._url),this._getNextProtocols(this._protocols||null)])).then(([i,r])=>{if(this._closeCalled){this._connectLock=!1;return}!this._options.WebSocket&&typeof WebSocket>"u"&&!I&&(console.error(`‼️ No WebSocket implementation available. You should define options.WebSocket. 

For example, if you're using node.js, run \`npm install ws\`, and then in your code:

import PartySocket from 'partysocket';
import WS from 'ws';

const partysocket = new PartySocket({
  host: "127.0.0.1:1999",
  room: "test-room",
  WebSocket: WS
});

`),I=!0);const a=this._options.WebSocket||WebSocket;this._debug("connect",{url:i,protocols:r}),this._ws=r?new a(i,r):new a(i),this._ws.binaryType=this._binaryType,this._connectLock=!1,this._addListeners(),this._connectTimeout=setTimeout(()=>this._handleTimeout(),n)}).catch(i=>{this._connectLock=!1,this._handleError(new $.ErrorEvent(Error(i.message),this))})}_handleTimeout(){this._debug("timeout event"),this._handleError(new $.ErrorEvent(Error("TIMEOUT"),this))}_disconnect(s=1e3,n){if(this._clearTimeouts(),!!this._ws){this._removeListeners();try{(this._ws.readyState===this.OPEN||this._ws.readyState===this.CONNECTING)&&this._ws.close(s,n),this._handleClose(new $.CloseEvent(s,n,this))}catch{}}}_acceptOpen(){this._debug("accept open"),this._retryCount=0}_removeListeners(){this._ws&&(this._debug("removeListeners"),this._ws.removeEventListener("open",this._handleOpen),this._ws.removeEventListener("close",this._handleClose),this._ws.removeEventListener("message",this._handleMessage),this._ws.removeEventListener("error",this._handleError),this._ws.addEventListener("error",z))}_addListeners(){this._ws&&(this._debug("addListeners"),this._ws.addEventListener("open",this._handleOpen),this._ws.addEventListener("close",this._handleClose),this._ws.addEventListener("message",this._handleMessage),this._ws.addEventListener("error",this._handleError))}_clearTimeouts(){clearTimeout(this._connectTimeout),clearTimeout(this._uptimeTimeout)}};const Z=e=>e[1]!==null&&e[1]!==void 0;function ee(){if(crypto!=null&&crypto.randomUUID)return crypto.randomUUID();let e=Date.now(),t=(performance==null?void 0:performance.now)&&performance.now()*1e3||0;return"xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g,function(s){let n=Math.random()*16;return e>0?(n=(e+n)%16|0,e=Math.floor(e/16)):(n=(t+n)%16|0,t=Math.floor(t/16)),(s==="x"?n:n&3|8).toString(16)})}function W(e,t,s={}){const{host:n,path:i,protocol:r,room:a,party:h,basePath:l,prefix:m,query:p}=e;let u=n.replace(/^(http|https|ws|wss):\/\//,"");if(u.endsWith("/")&&(u=u.slice(0,-1)),i!=null&&i.startsWith("/"))throw new Error("path must not start with a slash");const L=h??"main",N=i?`/${i}`:"",P=r||(u.startsWith("localhost:")||u.startsWith("127.0.0.1:")||u.startsWith("192.168.")||u.startsWith("10.")||u.startsWith("172.")&&u.split(".")[1]>="16"&&u.split(".")[1]<="31"||u.startsWith("[::ffff:7f00:1]:")?t:`${t}s`),A=`${P}://${u}/${l||`${m||"parties"}/${L}/${a}`}${N}`,R=(D={})=>`${A}?${new URLSearchParams([...Object.entries(s),...Object.entries(D).filter(Z)])}`,U=typeof p=="function"?async()=>R(await p()):R(p);return{host:u,path:N,room:a,name:L,protocol:P,partyUrl:A,urlProvider:U}}var te=class extends X{constructor(t){var n,i;const s=M(t);super(s.urlProvider,s.protocols,s.socketOptions);c(this,"_pk");c(this,"_pkurl");c(this,"name");c(this,"room");c(this,"host");c(this,"path");c(this,"basePath");if(this.partySocketOptions=t,this.setWSProperties(s),!t.startClosed&&!this.room&&!this.basePath)throw this.close(),new Error("Either room or basePath must be provided to connect. Use startClosed: true to create a socket and set them via updateProperties before calling reconnect().");t.disableNameValidation||((n=t.party)!=null&&n.includes("/")&&console.warn(`PartySocket: party name "${t.party}" contains forward slash which may cause routing issues. Consider using a name without forward slashes or set disableNameValidation: true to bypass this warning.`),(i=t.room)!=null&&i.includes("/")&&console.warn(`PartySocket: room name "${t.room}" contains forward slash which may cause routing issues. Consider using a name without forward slashes or set disableNameValidation: true to bypass this warning.`))}updateProperties(t){const s=M({...this.partySocketOptions,...t,host:t.host??this.host,room:t.room??this.room,path:t.path??this.path,basePath:t.basePath??this.basePath});this._url=s.urlProvider,this._protocols=s.protocols,this._options=s.socketOptions,this.setWSProperties(s)}setWSProperties(t){const{_pk:s,_pkurl:n,name:i,room:r,host:a,path:h,basePath:l}=t;this._pk=s,this._pkurl=n,this.name=i,this.room=r,this.host=a,this.path=h,this.basePath=l}reconnect(t,s){if(!this.host)throw new Error("The host must be set before connecting, use `updateProperties` method to set it or pass it to the constructor.");if(!this.room&&!this.basePath)throw new Error("The room (or basePath) must be set before connecting, use `updateProperties` method to set it or pass it to the constructor.");super.reconnect(t,s)}get id(){return this._pk}get roomUrl(){return this._pkurl}static async fetch(t,s){const n=W(t,"http"),i=typeof n.urlProvider=="string"?n.urlProvider:await n.urlProvider();return(t.fetch??fetch)(i,s)}};function M(e){const{id:t,host:s,path:n,party:i,room:r,protocol:a,query:h,protocols:l,...m}=e,p=t||ee(),u=W(e,"ws",{_pk:p});return{_pk:p,_pkurl:u.partyUrl,name:u.name,room:u.room,host:u.host,path:u.path,basePath:e.basePath,protocols:l,socketOptions:m,urlProvider:u.urlProvider}}let y=null,d=null,E=null;const g=["var(--kw-1)","var(--kw-2)","var(--kw-3)","var(--kw-4)"],o=e=>document.getElementById(e),k=e=>{document.querySelectorAll(".screen").forEach(t=>t.classList.remove("active")),o(e).classList.add("active")};function f(e){const t=o("toast");t.textContent=e,t.classList.add("show"),setTimeout(()=>t.classList.remove("show"),2500)}function j(e,t){const s=location.hostname==="localhost"||location.hostname==="127.0.0.1"||location.hostname.startsWith("192.168.")||location.hostname.startsWith("10.")?location.host:"decrypto-online.kennyphan123.partykit.dev";y=new te({host:s,room:e}),y.addEventListener("open",()=>{y.send(JSON.stringify({type:"join",name:t}))}),y.addEventListener("message",n=>{const i=JSON.parse(n.data);i.type==="state"?(d=i.state,ne()):i.type==="error"&&f(i.message)}),y.addEventListener("close",()=>{f("Mất kết nối. Tải lại trang để chơi lại.")})}function w(e){y&&y.send(JSON.stringify(e))}function C(e){o("menu-main").style.display="none",o("menu-create").style.display="none",o("menu-join").style.display="none",o(e).style.display="flex"}o("btn-menu-create").addEventListener("click",()=>{C("menu-create"),o("create-name").focus()});o("btn-menu-join").addEventListener("click",()=>{C("menu-join"),o("join-name").focus()});o("btn-back-create").addEventListener("click",()=>C("menu-main"));o("btn-back-join").addEventListener("click",()=>C("menu-main"));o("btn-create").addEventListener("click",()=>{const e=o("create-name").value.trim();if(!e){f("Vui lòng nhập tên");return}const t=se();j(t,e),k("lobby-screen")});o("btn-join").addEventListener("click",()=>{const e=o("join-name").value.trim(),t=o("join-code").value.trim().toUpperCase();if(!e){f("Vui lòng nhập tên");return}if(!t||t.length<4){f("Vui lòng nhập mã phòng");return}j(t,e),k("lobby-screen")});o("create-name").addEventListener("keydown",e=>{e.key==="Enter"&&o("btn-create").click()});o("join-name").addEventListener("keydown",e=>{e.key==="Enter"&&(o("join-code").value.trim()?o("btn-join").click():o("join-code").focus())});o("join-code").addEventListener("keydown",e=>{e.key==="Enter"&&o("btn-join").click()});function se(){const e="ABCDEFGHJKLMNPQRSTUVWXYZ";let t="";for(let s=0;s<4;s++)t+=e[Math.floor(Math.random()*e.length)];return t}o("btn-copy-code").addEventListener("click",()=>{const e=o("room-code-text").textContent;if(navigator.clipboard&&window.isSecureContext)navigator.clipboard.writeText(e).then(()=>f("Đã sao chép"));else{const t=document.createElement("textarea");t.value=e,document.body.appendChild(t),t.select();try{document.execCommand("copy"),f("Đã sao chép")}catch{f("Sao chép thất bại")}document.body.removeChild(t)}});o("btn-start").addEventListener("click",()=>w({type:"start"}));o("team-a-col").addEventListener("click",()=>w({type:"switch-team",target:"A"}));o("team-b-col").addEventListener("click",()=>w({type:"switch-team",target:"B"}));o("history-toggle").addEventListener("click",()=>{o("history-panel").classList.toggle("open")});o("btn-play-again").addEventListener("click",()=>w({type:"play-again"}));o("btn-back-home").addEventListener("click",()=>{y&&y.close(),y=null,d=null,C("menu-main"),k("home-screen")});function ne(){d&&(E&&(clearInterval(E),E=null),d.phase==="LOBBY"?(oe(),k("lobby-screen")):d.phase==="GAME_OVER"?(we(),k("gameover-screen")):(re(),k("game-screen"),ie()))}function ie(){const e=d,t=o("topbar-timer");if(e.phase==="ENCRYPT"&&e.timerEnd){t.style.display="block";const s=()=>{const n=Math.max(0,Math.floor((e.timerEnd-Date.now())/1e3));if(t.textContent=n+"s",n<=0){clearInterval(E);const i=o("btn-submit-clues");i&&!i.disabled&&i.click()}};s(),E=setInterval(s,1e3)}else t.style.display="none"}function oe(){var r;const e=d;o("room-code-text").textContent=e.roomCode;const t=(r=e.players.find(a=>a.id===e.myId))==null?void 0:r.isHost,s=e.players.length;s<3?(o("lobby-mode-info").textContent=`${s} người chơi — Cần ít nhất 3 người`,o("team-a-title").textContent="Đội A",o("team-b-title").textContent="Đội B"):s===3?(o("lobby-mode-info").textContent="3 người chơi — Chế độ độc lập",o("team-a-title").textContent="Đội Mã Hóa (Cần 2)",o("team-b-title").textContent="Kẻ Chặn Mã (Cần 1)"):(o("lobby-mode-info").textContent=`${s} người chơi — Chế độ đội`,o("team-a-title").textContent="Đội A",o("team-b-title").textContent="Đội B");const n=e.players.filter(a=>a.team==="A"),i=e.players.filter(a=>a.team==="B");O("team-a-list",n,e.myId),O("team-b-list",i,e.myId),o("btn-start").style.display=t&&s>=3?"block":"none",o("lobby-waiting").style.display=t?"none":"block"}function O(e,t,s){o(e).innerHTML=t.map(n=>`
    <li>${v(n.name)}${n.id===s?" (bạn)":""}${n.isHost?' <span class="lobby-player-host" style="font-size:10px; margin-left:4px;">Chủ phòng</span>':""}</li>
  `).join("")}function re(){const e=d;ae(),o("round-display").textContent=`${e.round}/${e.maxRounds}`,ce(),le(),de(),ue(),ve()}function ae(){const e=d,t=e.players.find(i=>i.id===e.myId);if(!t)return;let s=`<strong>${v(t.name)}</strong>`;e.mode==="3p"?s+=e.myRole==="interceptor"?" (Người chặn)":" (Đội mã hóa)":s+=e.myTeam?` (Đội ${e.myTeam})`:" (Khán giả)";const n=o("player-identity");n&&(n.innerHTML=s)}function ce(){const e=d,t=o("topbar-tokens");e.mode==="3p"?t.innerHTML=`
      <div class="token-3p">
        <span>Chặn: <strong>${e.interceptorTokens}</strong>/2</span>
      </div>
    `:t.innerHTML=`
      <div class="token-group token-group-a">
        <span class="token-team-label" style="color:var(--team-a)">A</span>
        <span class="token-item"><span class="token-count token-label-i">${e.teamA.interceptions}</span>C</span>
        <span class="token-item"><span class="token-count token-label-m">${e.teamA.miscommunications}</span>L</span>
      </div>
      <div class="token-group token-group-b">
        <span class="token-team-label" style="color:var(--team-b)">B</span>
        <span class="token-item"><span class="token-count token-label-i">${e.teamB.interceptions}</span>C</span>
        <span class="token-item"><span class="token-count token-label-m">${e.teamB.miscommunications}</span>L</span>
      </div>
    `}function le(){const e=d,t=o("keywords-panel");e.keywords?(t.style.display="block",o("keywords-list").innerHTML=e.keywords.map((s,n)=>`
      <div class="keyword-chip kw-${n+1}">
        <span class="kw-number">${n+1}</span>
        <span>${v(s)}</span>
      </div>
    `).join("")):t.style.display="none"}function de(){var s;const e=d;let t="";if(e.mode==="3p"){const n=((s=e.encryptors.find(i=>i.id===e.currentEncryptorId))==null?void 0:s.name)||"";switch(e.phase){case"ENCRYPT":t=e.myRole==="encryptor"?"Bạn là người mã hóa — Nhập gợi ý":`Đang chờ ${n} mã hóa...`;break;case"GUESS":t="Đoán mã số";break;case"REVEAL":t="Kết quả";break}}else{const n=e.currentTeamTurn;switch(e.phase){case"ENCRYPT":t=e.myRole==="encryptor"?"Bạn là người mã hóa — Nhập gợi ý":"Đang chờ mã hóa...";break;case"GUESS_A":case"GUESS_B":t=`Đội ${n} — Đoán mã số`;break;case"REVEAL_A":case"REVEAL_B":t=`Đội ${n} — Kết quả`;break}}o("phase-status").textContent=t}function ue(){const e=d,t=o("action-area");e.phase==="ENCRYPT"?he(t):e.phase==="GUESS"||e.phase==="GUESS_A"||e.phase==="GUESS_B"?me(t):(e.phase==="REVEAL"||e.phase==="REVEAL_A"||e.phase==="REVEAL_B")&&ge(t)}function he(e){const t=d,s=t.mode==="3p"?t.cluesSubmitted:t.myTeam?t["team"+t.myTeam].cluesSubmitted:!1;if(t.myRole==="encryptor"&&t.code&&!s)e.innerHTML=`
      <div class="encrypt-code-display fade-in">
        <div class="encrypt-code-label">Mã số cần truyền đạt</div>
        <div class="encrypt-code-numbers">
          ${t.code.map(n=>`<div class="code-digit" style="background:${g[n-1]}">${n}</div>`).join("")}
        </div>
      </div>
      <div class="clue-inputs fade-in">
        ${t.code.map((n,i)=>`
          <div class="clue-input-row">
            <div class="clue-number" style="background:${g[n-1]}">${["A","B","C"][i]}</div>
            <input type="text" class="clue-input" id="clue-${i}" placeholder="Gợi ý cho từ khóa số ${n}..." autocomplete="off" />
          </div>
        `).join("")}
      </div>
      <button class="btn btn-primary" id="btn-submit-clues">Gửi gợi ý</button>
    `,o("btn-submit-clues").addEventListener("click",n=>{const i=!n.isTrusted;let r=[0,1,2].map(a=>o(`clue-${a}`).value.trim());if(!i&&r.some(a=>!a)){f("Vui lòng nhập đủ 3 gợi ý");return}i&&(r=r.map(a=>a||"(Hết giờ)")),w({type:"submit-clues",clues:r}),e.innerHTML=`
        <div class="waiting-indicator fade-in">
          <p>Đang gửi gợi ý...<span class="waiting-dots"></span></p>
        </div>
      `}),setTimeout(()=>{var n;return(n=o("clue-0"))==null?void 0:n.focus()},100);else{let n="";if(t.mode==="3p"){const i=t.encryptors.find(r=>r.id===t.currentEncryptorId);n=`Đang chờ ${(i==null?void 0:i.name)||""} nhập gợi ý`}else{const i=[];if(t.teamA.cluesSubmitted&&i.push("A"),t.teamB.cluesSubmitted&&i.push("B"),i.length===0)n="Đang chờ cả 2 người mã hóa...";else{const r=i.includes("A")?"B":"A";n=`Đội ${i[0]} đã xong. Đang chờ đội ${r}...`}}e.innerHTML=`
      <div class="waiting-indicator fade-in">
        <p>${n}<span class="waiting-dots"></span></p>
      </div>
    `}}function me(e){const t=d,s=t.currentClues||t.clues;if(!s)return;let n="";n+=`
    <div class="clues-display fade-in">
      <div class="clues-display-header">Gợi ý</div>
      ${s.map((i,r)=>`
        <div class="clue-display-item">
          <div class="clue-number" style="background:var(--text-muted)">${["A","B","C"][r]}</div>
          <span>${v(i)}</span>
        </div>
      `).join("")}
    </div>
  `,t.mode==="3p"?n+=pe():n+=ye(),e.innerHTML=n,fe()}function pe(){const e=d;let t="";return e.myRole==="encryptor"&&e.currentEncryptorId===e.myId?t+='<div class="waiting-indicator">Bạn là người mã hóa — Hãy chờ đồng đội đoán</div>':e.myRole==="interceptor"?e.round<2?(t+='<div class="waiting-indicator">Vòng 1 — Chưa thể chặn mã</div>',e.decryptSubmitted||(t+='<div class="waiting-indicator">Đang chờ đội mã hóa đoán<span class="waiting-dots"></span></div>')):e.interceptSubmitted?t+='<div class="guess-submitted">Bạn đã gửi dự đoán<span class="waiting-dots"></span></div>':t+=x("intercept","Chặn mã"):e.decryptSubmitted?t+='<div class="guess-submitted">Đội bạn đã gửi dự đoán<span class="waiting-dots"></span></div>':t+=x("decrypt","Giải mã"),t}function ye(){const e=d,t=e.currentTeamTurn,s=e.myTeam===t,n=t==="A"?"B":"A";let i="";return s?(t==="A"?e.teamA:e.teamB).encryptorId===e.myId?i+='<div class="waiting-indicator">Bạn là người mã hóa — Không được gợi ý</div>':e.decryptSubmitted?i+=`<div class="guess-submitted">Đội bạn đã gửi dự đoán. Đang chờ đội ${n}<span class="waiting-dots"></span></div>`:i+=x("decrypt","Giải mã"):e.round<2?i+='<div class="waiting-indicator">Vòng 1 — Chưa thể chặn mã</div>':e.interceptSubmitted?i+='<div class="guess-submitted">Đội bạn đã gửi dự đoán<span class="waiting-dots"></span></div>':i+=x("intercept","Chặn mã"),i}function x(e,t){return`
    <div class="guess-section fade-in">
      <div class="guess-section-title">${t}</div>
      <div class="guess-inputs">
        ${[0,1,2].map(s=>`
          <div class="guess-select-group">
            <span class="guess-label">Gợi ý ${["A","B","C"][s]}</span>
            <select class="guess-select" id="guess-${s}" onchange="this.style.color = this.options[this.selectedIndex].style.color">
              <option value="">?</option>
              <option value="1" style="color:${g[0]}; font-weight:bold;">1</option>
              <option value="2" style="color:${g[1]}; font-weight:bold;">2</option>
              <option value="3" style="color:${g[2]}; font-weight:bold;">3</option>
              <option value="4" style="color:${g[3]}; font-weight:bold;">4</option>
            </select>
          </div>
        `).join("")}
      </div>
      <button class="btn btn-primary" id="btn-submit-guess" data-type="${e}">Gửi</button>
    </div>
  `}function fe(){const e=o("btn-submit-guess");e&&e.addEventListener("click",()=>{const t=e.dataset.type,s=[0,1,2].map(n=>{var r;const i=(r=o(`guess-${n}`))==null?void 0:r.value;return i?parseInt(i):0});if(s.some(n=>n<1||n>4)){f("Vui lòng chọn đủ 3 số");return}w({type:"submit-guess",guess:s,guessType:t}),e.disabled=!0,e.textContent="Đã gửi"})}function ge(e){var a,h;const t=d,s=(a=t.players.find(l=>l.id===t.myId))==null?void 0:a.isHost;let n="";const i=t.mode==="3p"?t.clues:t.currentClues;n+=`
    <div class="reveal-section fade-in">
      <div class="reveal-title">Mã số đúng</div>
      <div class="reveal-code-row" style="flex-direction: column; gap: 8px;">
        ${t.revealCode.map((l,m)=>`
          <div style="display: flex; align-items: center; gap: 12px; background: var(--surface-alt); padding: 8px 16px; border-radius: 8px; width: 100%; max-width: 300px; margin: 0 auto;">
            <div class="code-digit" style="background:${g[l-1]}">${l}</div>
            <div style="color:${g[l-1]}; font-weight: 600; font-size: 1.1rem">${v(i[m])}</div>
          </div>
        `).join("")}
      </div>
  `;const r=l=>l.map((m,p)=>`<div class="code-digit" style="background:${m===t.revealCode[p]?"var(--success)":"var(--error)"}; transform: scale(0.8)">${m}</div>`).join("");if(t.decryptGuess){const l=t.decryptCorrect?"result-correct":"result-incorrect",m=t.decryptCorrect?"Giải mã thành công":"Giải mã thất bại";n+=`
      <div class="reveal-result ${l}">
        <span class="result-label">${m}</span>
        <div style="display:flex; gap:4px; justify-content:center; margin-top:8px;">${r(t.decryptGuess)}</div>
      </div>
    `}if(t.interceptGuess){const l=t.interceptCorrect?"result-correct":"result-incorrect",m=t.interceptCorrect?"Chặn mã thành công!":"Chặn mã thất bại";n+=`
      <div class="reveal-result ${l}">
        <span class="result-label">${m}</span>
        <div style="display:flex; gap:4px; justify-content:center; margin-top:8px;">${r(t.interceptGuess)}</div>
      </div>
    `}else t.round<2||t.needIntercept;n+="</div>",s?n+='<button class="btn btn-primary" id="btn-continue">Tiếp tục</button>':n+='<div class="waiting-indicator">Đang chờ chủ phòng tiếp tục<span class="waiting-dots"></span></div>',e.innerHTML=n,(h=o("btn-continue"))==null||h.addEventListener("click",()=>{w({type:"continue"})})}function ve(){const e=d,t=o("history-tables");e.mode==="3p"?be(t):_e(t)}function be(e){const s=d.history||[];if(s.length===0){e.innerHTML='<div class="history-empty">Chưa có lịch sử</div>';return}const n={1:[],2:[],3:[],4:[]},i=[];for(const r of s){i.push(r.round);for(let a=0;a<3;a++){const h=r.code[a];n[h].push({round:r.round,clue:r.clues[a]})}}e.innerHTML=`
    <div class="history-section-label">Theo từ khóa</div>
    ${S(n,i)}
  `}function _e(e){const t=d,s=t.myHistory||[],n=t.opponentHistory||[];if(s.length===0&&n.length===0){e.innerHTML='<div class="history-empty">Chưa có lịch sử</div>';return}let i="";if(n.length>0){const r=t.myTeam==="A"?"B":"A",a={1:[],2:[],3:[],4:[]},h=[];for(const l of n){h.push(l.round);for(let m=0;m<3;m++){const p=l.code[m];a[p].push({round:l.round,clue:l.clues[m]})}}i+=`<div class="history-section-label">Đội ${r} (đối phương)</div>`,i+=S(a,h)}if(s.length>0){const r={1:[],2:[],3:[],4:[]},a=[];for(const h of s){a.push(h.round);for(let l=0;l<3;l++){const m=h.code[l];r[m].push({round:h.round,clue:h.clues[l]})}}i+=`<div class="history-section-label">Đội ${t.myTeam} (đội bạn)</div>`,i+=S(r,a)}e.innerHTML=i}function S(e,t){const s=[...new Set(t)].sort((i,r)=>i-r);let n='<table class="history-table"><thead><tr><th>Từ khóa</th>';for(const i of s)n+=`<th class="kw-col-header">V${i}</th>`;n+="</tr></thead><tbody>";for(let i=1;i<=4;i++){n+=`<tr style="color:${g[i-1]}"><td style="font-weight:600">#${i}</td>`;for(const r of s){const a=e[i].find(h=>h.round===r);n+=`<td class="kw-cell">${a?v(a.clue):"—"}</td>`}n+="</tr>"}return n+="</tbody></table>",n}function we(){var s;const e=d,t=(s=e.players.find(n=>n.id===e.myId))==null?void 0:s.isHost;if(e.mode==="3p")e.winner==="interceptor"?o("gameover-title").textContent=e.myRole==="interceptor"?"Bạn đã thắng!":"Người chặn mã đã thắng!":o("gameover-title").textContent=e.myRole==="interceptor"?"Bạn đã thua!":"Đội mã hóa đã thắng!",o("gameover-summary").innerHTML=`
      <p>Token chặn: ${e.interceptorTokens}/2</p>
      <p>Số vòng: ${e.round}/${e.maxRounds}</p>
    `,e.allKeywords&&(o("gameover-keywords").innerHTML=`
        <div class="gameover-team-keywords">
          <div class="gameover-team-header" style="background:var(--surface-alt)">Từ khóa</div>
          <div class="gameover-kw-list">
            ${e.allKeywords.map((n,i)=>`
              <div class="keyword-chip kw-${i+1}">
                <span class="kw-number">${i+1}</span>
                <span>${v(n)}</span>
              </div>
            `).join("")}
          </div>
        </div>
      `);else{if(e.winner==="TIE")o("gameover-title").textContent="Hòa!";else{const n=e.winner,i=e.myTeam===n;o("gameover-title").textContent=i?`Đội bạn thắng! (Đội ${n})`:`Đội ${n} thắng!`}o("gameover-summary").innerHTML=`
      <p>Đội A — Chặn: ${e.teamA.interceptions} | Lỗi: ${e.teamA.miscommunications}</p>
      <p>Đội B — Chặn: ${e.teamB.interceptions} | Lỗi: ${e.teamB.miscommunications}</p>
      <p>Số vòng: ${e.round}/${e.maxRounds}</p>
    `,e.allKeywords&&(o("gameover-keywords").innerHTML=`
        <div class="gameover-team-keywords">
          <div class="gameover-team-header team-a-header">Từ khóa Đội A</div>
          <div class="gameover-kw-list">
            ${e.allKeywords.A.map((n,i)=>`
              <div class="keyword-chip kw-${i+1}">
                <span class="kw-number">${i+1}</span>
                <span>${v(n)}</span>
              </div>
            `).join("")}
          </div>
        </div>
        <div class="gameover-team-keywords">
          <div class="gameover-team-header team-b-header">Từ khóa Đội B</div>
          <div class="gameover-kw-list">
            ${e.allKeywords.B.map((n,i)=>`
              <div class="keyword-chip kw-${i+1}">
                <span class="kw-number">${i+1}</span>
                <span>${v(n)}</span>
              </div>
            `).join("")}
          </div>
        </div>
      `)}o("btn-play-again").style.display=t?"block":"none"}function v(e){const t=document.createElement("div");return t.textContent=e,t.innerHTML}

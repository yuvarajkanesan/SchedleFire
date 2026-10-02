"""Static assets for the dashboard: stylesheet, script and icons.

Everything is served by the app itself (no CDN), so the dashboard works offline and
behind firewalls.
"""

import base64
import hashlib

CSS = r"""
:root{
  color-scheme:light;
  --page:#f7f7f5;--surface:#ffffff;--surface-2:#f3f3f1;--surface-3:#ebebe8;
  --text:#111110;--text-2:#52514e;--muted:#898781;
  --border:rgba(11,11,11,.10);--border-strong:rgba(11,11,11,.18);
  --grid:#e8e7e1;--baseline:#c3c2b7;
  --ink:#111110;--ink-contrast:#ffffff;--brand:#e8590c;
  --focus:#2a78d6;--link:#1c5cab;
  --good:#0ca30c;--warning:#fab219;--critical:#d03b3b;--critical-text:#b42323;
  --s-enqueued:#2a78d6;--s-scheduled:#4a3aa7;--s-processing:#fab219;--s-succeeded:#0ca30c;
  --s-failed:#d03b3b;--s-deleted:#898781;--s-awaiting:#e87ba4;
  --series-succeeded:#2a78d6;--series-failed:#e34948;
  --danger-wash:rgba(208,59,59,.06);--hover-wash:rgba(11,11,11,.035);
  --shadow:0 1px 2px rgba(11,11,11,.04),0 1px 1px rgba(11,11,11,.02);
  --shadow-pop:0 8px 24px rgba(11,11,11,.12),0 2px 6px rgba(11,11,11,.06);
}
@media (prefers-color-scheme:dark){
  :root:where(:not([data-theme="light"])){
    color-scheme:dark;
    --page:#0d0d0d;--surface:#1a1a19;--surface-2:#222221;--surface-3:#2c2c2a;
    --text:#ffffff;--text-2:#c3c2b7;--muted:#898781;
    --border:rgba(255,255,255,.10);--border-strong:rgba(255,255,255,.18);
    --grid:#2c2c2a;--baseline:#383835;
    --ink:#f4f4f2;--ink-contrast:#111110;--brand:#ff7a33;
    --focus:#3987e5;--link:#86b6ef;
    --critical-text:#f08a8a;
    --s-enqueued:#3987e5;--s-scheduled:#9085e9;--s-awaiting:#d55181;
    --series-succeeded:#3987e5;--series-failed:#e66767;
    --danger-wash:rgba(230,103,103,.08);--hover-wash:rgba(255,255,255,.04);
    --shadow:none;--shadow-pop:0 8px 24px rgba(0,0,0,.5);
  }
}
:root[data-theme="dark"]{
  color-scheme:dark;
  --page:#0d0d0d;--surface:#1a1a19;--surface-2:#222221;--surface-3:#2c2c2a;
  --text:#ffffff;--text-2:#c3c2b7;--muted:#898781;
  --border:rgba(255,255,255,.10);--border-strong:rgba(255,255,255,.18);
  --grid:#2c2c2a;--baseline:#383835;
  --ink:#f4f4f2;--ink-contrast:#111110;--brand:#ff7a33;
  --focus:#3987e5;--link:#86b6ef;
  --critical-text:#f08a8a;
  --s-enqueued:#3987e5;--s-scheduled:#9085e9;--s-awaiting:#d55181;
  --series-succeeded:#3987e5;--series-failed:#e66767;
  --danger-wash:rgba(230,103,103,.08);--hover-wash:rgba(255,255,255,.04);
  --shadow:none;--shadow-pop:0 8px 24px rgba(0,0,0,.5);
}

*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--page);color:var(--text);
  font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",sans-serif;
  -webkit-font-smoothing:antialiased}
a{color:inherit;text-decoration:none}
button,input,select{font:inherit;color:inherit}
:focus-visible{outline:2px solid var(--focus);outline-offset:2px;border-radius:4px}
code,pre,.mono{font-family:ui-monospace,"Cascadia Code","SF Mono",Menlo,Consolas,monospace;font-size:12.5px}
.num{font-variant-numeric:tabular-nums}
.muted{color:var(--muted)}.secondary{color:var(--text-2)}
.nowrap{white-space:nowrap}
.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}
svg.i{width:16px;height:16px;flex:none;stroke:currentColor;fill:none;stroke-width:1.75;stroke-linecap:round;stroke-linejoin:round}

/* ---------- shell ---------- */
.app{display:grid;grid-template-columns:240px minmax(0,1fr);min-height:100vh}
.sidebar{position:sticky;top:0;height:100vh;overflow-y:auto;border-right:1px solid var(--border);
  background:var(--surface);display:flex;flex-direction:column}
.brand{display:flex;align-items:center;gap:10px;padding:16px 18px;font-weight:650;font-size:15px;letter-spacing:-.01em}
.brand-mark{width:28px;height:28px;border-radius:8px;display:grid;place-items:center;background:var(--brand);color:#fff}
.brand-mark svg.i{width:17px;height:17px;stroke-width:2}
.nav{padding:4px 10px 16px;display:flex;flex-direction:column;gap:1px}
.nav-label{padding:16px 10px 6px;font-size:11.5px;font-weight:600;color:var(--muted);letter-spacing:.04em;text-transform:uppercase}
.nav a{display:flex;align-items:center;gap:10px;padding:6px 10px;border-radius:7px;color:var(--text-2);font-weight:500}
.nav a:hover{background:var(--hover-wash);color:var(--text)}
.nav a.active{background:var(--surface-2);color:var(--text);box-shadow:inset 0 0 0 1px var(--border)}
.nav a .count{margin-left:auto;font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums}
.nav a .count.alert{color:#fff;background:var(--critical);border-radius:99px;padding:0 7px;font-weight:600}
.sidebar-foot{margin-top:auto;padding:14px 18px;border-top:1px solid var(--border);font-size:12px;color:var(--muted)}

.main{min-width:0;display:flex;flex-direction:column}
.topbar{position:sticky;top:0;z-index:10;height:56px;display:flex;align-items:center;gap:12px;padding:0 24px;
  background:color-mix(in srgb,var(--page) 85%,transparent);backdrop-filter:blur(8px);border-bottom:1px solid var(--border)}
.crumbs{display:flex;align-items:center;gap:6px;min-width:0;font-size:13.5px;color:var(--muted);white-space:nowrap;overflow:hidden}
.crumbs a:hover{color:var(--text)}
.crumbs .sep{opacity:.6}
.crumbs .current{color:var(--text);font-weight:500;overflow:hidden;text-overflow:ellipsis}
.top-actions{margin-left:auto;display:flex;align-items:center;gap:8px}
.search{position:relative;display:flex;align-items:center}
.search svg.i{position:absolute;left:10px;color:var(--muted);pointer-events:none}
.search input{height:32px;width:190px;padding:0 10px 0 32px;border-radius:8px;border:1px solid var(--border);
  background:var(--surface);outline:none}
.search input:focus{border-color:var(--focus);box-shadow:0 0 0 3px color-mix(in srgb,var(--focus) 20%,transparent)}
.icon-btn{height:32px;min-width:32px;padding:0 9px;display:inline-flex;align-items:center;justify-content:center;gap:6px;
  border-radius:8px;border:1px solid var(--border);background:var(--surface);cursor:pointer;color:var(--text-2);font-size:13px;font-weight:500}
.icon-btn:hover{color:var(--text);border-color:var(--border-strong)}
.account{display:inline-flex;align-items:center;gap:6px;max-width:220px;font-size:13px;font-weight:550;color:var(--text-2)}
.account span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.top-actions form{display:inline-flex;margin:0}
.live-dot{width:7px;height:7px;border-radius:50%;background:var(--muted)}
.icon-btn[aria-pressed="true"] .live-dot{background:var(--good);box-shadow:0 0 0 3px color-mix(in srgb,var(--good) 25%,transparent);
  animation:pulse 2s ease-in-out infinite}
@keyframes pulse{50%{box-shadow:0 0 0 5px color-mix(in srgb,var(--good) 0%,transparent)}}
@media (prefers-reduced-motion:reduce){.icon-btn[aria-pressed="true"] .live-dot{animation:none}}

.content{width:100%;max-width:1240px;margin:0 auto;padding:24px 24px 48px}
.page-head{display:flex;align-items:flex-start;gap:16px;margin-bottom:20px;flex-wrap:wrap}
.page-head h1{margin:0;font-size:22px;line-height:1.3;font-weight:650;letter-spacing:-.015em;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.page-head p{margin:4px 0 0;color:var(--text-2);max-width:70ch}
.page-head .actions{margin-left:auto;display:flex;gap:8px;flex-wrap:wrap}
.page-head .signature{margin-top:8px;display:inline-block;padding:4px 8px;border-radius:6px;background:var(--surface-2);
  border:1px solid var(--border);color:var(--text);word-break:break-all}

/* ---------- building blocks ---------- */
.card{background:var(--surface);border:1px solid var(--border);border-radius:12px;box-shadow:var(--shadow);min-width:0}
.card-head{display:flex;align-items:center;gap:12px;padding:14px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.card-head h2{margin:0;font-size:14px;font-weight:600}
.card-head .sub{color:var(--muted);font-size:12.5px}
.card-head .right{margin-left:auto;display:flex;align-items:center;gap:8px}
.card-body{padding:16px}
.card-link{font-size:12.5px;font-weight:500;color:var(--link)}
.card-link:hover{text-decoration:underline}
.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
.grid-main{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:16px;align-items:start}
.stack{display:flex;flex-direction:column;gap:16px}

.tiles{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:12px;margin-bottom:16px}
.tile{display:block;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px 16px;box-shadow:var(--shadow);
  transition:border-color .15s}
.tile:hover{border-color:var(--border-strong)}
.tile .label{display:flex;align-items:center;gap:8px;color:var(--text-2);font-size:13px;font-weight:500}
.tile .value{margin-top:6px;font-size:28px;line-height:1.15;font-weight:650;letter-spacing:-.02em}
.tile .hint{margin-top:2px;font-size:12px;color:var(--muted)}
.tile.alert{border-color:color-mix(in srgb,var(--critical) 45%,var(--border));background:linear-gradient(var(--danger-wash),var(--danger-wash)),var(--surface)}
.tile.alert .hint{color:var(--critical-text)}

.dot{width:8px;height:8px;border-radius:50%;flex:none;background:var(--muted)}
.pill{display:inline-flex;align-items:center;gap:6px;height:22px;padding:0 9px;border-radius:99px;font-size:12px;font-weight:550;
  background:var(--surface-2);border:1px solid var(--border);color:var(--text);white-space:nowrap}
.pill .dot{width:7px;height:7px}
.pill.lg{height:26px;font-size:13px;padding:0 11px}
.tag{display:inline-flex;align-items:center;height:20px;padding:0 7px;border-radius:5px;background:var(--surface-2);
  border:1px solid var(--border);font-size:12px;color:var(--text-2)}
.tag.warn{color:var(--text);background:color-mix(in srgb,var(--warning) 22%,transparent);
  border-color:color-mix(in srgb,var(--warning) 55%,transparent)}

.btn{height:32px;padding:0 12px;display:inline-flex;align-items:center;gap:7px;border-radius:8px;border:1px solid var(--border-strong);
  background:var(--surface);color:var(--text);font-size:13px;font-weight:550;cursor:pointer;white-space:nowrap}
.btn:hover{background:var(--surface-2)}
.btn.primary{background:var(--ink);border-color:var(--ink);color:var(--ink-contrast)}
.btn.primary:hover{opacity:.9}
.btn.danger{color:var(--critical-text)}
.btn.danger:hover{background:var(--danger-wash);border-color:color-mix(in srgb,var(--critical) 40%,var(--border))}
.btn.sm{height:28px;padding:0 10px;font-size:12.5px}
.btn:disabled{opacity:.45;cursor:not-allowed;pointer-events:none}

/* ---------- tables ---------- */
.table-wrap{overflow-x:auto}
table.t{width:100%;border-collapse:separate;border-spacing:0}
.t th{position:sticky;top:0;text-align:left;font-size:12px;font-weight:550;color:var(--muted);padding:9px 16px;
  background:var(--surface-2);border-bottom:1px solid var(--border);white-space:nowrap}
.t td{padding:11px 16px;border-bottom:1px solid var(--border);vertical-align:middle}
.t tr:last-child td{border-bottom:0}
.t tbody tr:hover td{background:var(--hover-wash)}
.t .check{width:36px;padding-right:0}
.t th.r,.t td.r{text-align:right}
.t input[type=checkbox]{width:15px;height:15px;accent-color:var(--ink);cursor:pointer;vertical-align:middle}
.job{display:flex;flex-direction:column;min-width:0;max-width:560px}
.job .fn{font-weight:550;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.job .args{color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
a.job:hover .fn{text-decoration:underline}
.jid{color:var(--muted);font-variant-numeric:tabular-nums}
a.jid:hover{color:var(--link)}
.error-text{color:var(--critical-text);font-weight:550}
.clip{display:block;max-width:420px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

.toolbar{display:flex;align-items:center;gap:8px;padding:10px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.toolbar .selected{font-size:12.5px;color:var(--muted);margin-right:4px}
.pager{display:flex;align-items:center;gap:12px;padding:10px 16px;border-top:1px solid var(--border);font-size:13px;color:var(--text-2);flex-wrap:wrap}
.pager .right{margin-left:auto;display:flex;align-items:center;gap:8px}
.pager select{height:28px;border-radius:7px;border:1px solid var(--border);background:var(--surface);padding:0 6px}

.empty{padding:48px 24px;text-align:center}
.empty .ring{width:44px;height:44px;border-radius:50%;display:grid;place-items:center;margin:0 auto 12px;background:var(--surface-2);color:var(--muted)}
.empty .ring svg.i{width:20px;height:20px}
.empty h3{margin:0;font-size:14.5px;font-weight:600}
.empty p{margin:4px auto 0;color:var(--muted);max-width:44ch}

/* ---------- details ---------- */
dl.props{margin:0;display:grid;grid-template-columns:auto minmax(0,1fr);gap:10px 16px;padding:16px}
dl.props dt{color:var(--muted);font-size:13px}
dl.props dd{margin:0;min-width:0;overflow-wrap:anywhere}
pre.code{margin:0;padding:14px 16px;overflow:auto;background:var(--surface-2);border-radius:0 0 12px 12px;line-height:1.6;
  white-space:pre-wrap;word-break:break-word;max-height:520px}
.card-body pre.code{border-radius:8px;border:1px solid var(--border)}
.exception{border-color:color-mix(in srgb,var(--critical) 40%,var(--border))}
.exception .card-head{background:var(--danger-wash);border-radius:12px 12px 0 0}
.exception .type{color:var(--critical-text);font-weight:600}
.exception .message{padding:14px 16px 0;font-size:14px;overflow-wrap:anywhere}
details.trace summary{cursor:pointer;padding:12px 16px;color:var(--text-2);font-size:13px;font-weight:500;list-style:none}
details.trace summary::before{content:"▸ ";color:var(--muted)}
details.trace[open] summary::before{content:"▾ "}
details.trace pre.code{border-radius:0 0 12px 12px;border-top:1px solid var(--border)}

.timeline{list-style:none;margin:0;padding:8px 16px 16px}
.timeline li{position:relative;padding:10px 0 10px 26px}
.timeline li::before{content:"";position:absolute;left:5px;top:0;bottom:0;width:2px;background:var(--grid)}
.timeline li:first-child::before{top:18px}.timeline li:last-child::before{bottom:calc(100% - 18px)}
.timeline .marker{position:absolute;left:0;top:13px;width:12px;height:12px;border-radius:50%;box-shadow:0 0 0 2px var(--surface)}
.timeline .row{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap}
.timeline .name{font-weight:600}
.timeline .when{margin-left:auto;font-size:12.5px;color:var(--muted)}
.timeline .reason{color:var(--text-2);font-size:13px;margin-top:2px;overflow-wrap:anywhere}
.timeline .kv{margin-top:6px;display:flex;flex-wrap:wrap;gap:6px}

.notice{display:flex;align-items:center;gap:10px;padding:10px 14px;margin-bottom:16px;border-radius:10px;
  background:var(--surface);border:1px solid var(--border);box-shadow:var(--shadow);font-weight:500}
.notice svg.i{color:var(--good)}
.notice.error{border-color:color-mix(in srgb,var(--critical) 45%,var(--border));background:linear-gradient(var(--danger-wash),var(--danger-wash)),var(--surface)}
.notice.error svg.i{color:var(--critical-text)}
.notice button{margin-left:auto;border:0;background:none;cursor:pointer;color:var(--muted);display:flex;padding:2px}

/* ---------- chart ---------- */
.segmented{display:inline-flex;padding:2px;border-radius:8px;background:var(--surface-2);border:1px solid var(--border)}
.segmented a{padding:3px 10px;border-radius:6px;font-size:12.5px;font-weight:550;color:var(--text-2)}
.segmented a.on{background:var(--surface);color:var(--text);box-shadow:0 0 0 1px var(--border),var(--shadow)}
.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:12.5px;color:var(--text-2)}
.legend span{display:inline-flex;align-items:center;gap:6px}
.legend i{width:10px;height:10px;border-radius:2px;display:inline-block}
.legend b{color:var(--text);font-weight:600;font-variant-numeric:tabular-nums}
.chart{position:relative;height:220px;margin-top:12px}
.chart svg{display:block;width:100%;height:100%;overflow:visible}
.chart .grid line{stroke:var(--grid);stroke-width:1;shape-rendering:crispEdges}
.chart .base{stroke:var(--baseline);stroke-width:1;shape-rendering:crispEdges}
.chart text{fill:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}
.chart .hit{fill:transparent;cursor:default}
.chart .col:hover .hit,.chart .col:focus .hit{fill:var(--hover-wash)}
.chart .col:focus{outline:none}
.chart .col:focus-visible .hit{stroke:var(--focus);stroke-width:1.5}
.tip{position:absolute;z-index:5;pointer-events:none;min-width:150px;padding:8px 10px;border-radius:8px;background:var(--surface);
  border:1px solid var(--border);box-shadow:var(--shadow-pop);font-size:12.5px;opacity:0;transition:opacity .08s}
.tip.show{opacity:1}
.tip .t{color:var(--muted);margin-bottom:4px}
.tip .r{display:flex;align-items:center;gap:8px}
.tip .r i{width:12px;height:2px;border-radius:1px;display:inline-block}
.tip .r b{font-weight:650;font-variant-numeric:tabular-nums;min-width:2ch}
.tip .r span{color:var(--text-2)}
details.table-view{border-top:1px solid var(--border)}
details.table-view summary{cursor:pointer;padding:10px 16px;font-size:12.5px;color:var(--text-2);list-style:none}
details.table-view summary:hover{color:var(--text)}

.list{list-style:none;margin:0;padding:0}
.list li{display:flex;align-items:center;gap:12px;padding:11px 16px;border-bottom:1px solid var(--border);min-width:0}
.list li:last-child{border-bottom:0}
.list .grow{min-width:0;flex:1}
.list .meta{font-size:12.5px;color:var(--muted)}

.servers{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;align-items:start}
.pending{display:inline-flex;align-items:center;height:20px;padding:0 7px;margin-left:6px;border-radius:5px;font-size:12px;font-weight:550;
  color:var(--text);background:color-mix(in srgb,var(--warning) 22%,transparent);border:1px solid color-mix(in srgb,var(--warning) 55%,transparent)}
details.edit{border-top:1px solid var(--border);padding:16px 20px 18px}
details.edit summary{list-style:none;width:max-content}
details.edit summary::-webkit-details-marker{display:none}
details.edit[open] summary{margin-bottom:14px}
.edit-form{display:flex;flex-wrap:wrap;gap:14px 18px;align-items:flex-end;max-width:720px}
.field{display:flex;flex-direction:column;gap:4px;font-size:12.5px;font-weight:550;color:var(--text-2)}
.field em{font-style:normal;font-weight:400;color:var(--muted)}
.field.grow{flex:1;min-width:180px}
.field input,.field select{height:34px;padding:0 10px;border-radius:8px;border:1px solid var(--border-strong);background:var(--surface);
  font:13px ui-monospace,"Cascadia Code",Consolas,monospace;color:var(--text);outline:none}
.field select{padding-right:6px;font-family:inherit}
.field input[type=number]{width:96px}
.field input[type=time]{width:120px}
.field input:focus,.field select:focus,.field textarea:focus{border-color:var(--focus);box-shadow:0 0 0 3px color-mix(in srgb,var(--focus) 20%,transparent)}
.field input:invalid:not(:focus):not(:placeholder-shown){border-color:var(--critical)}
.field textarea{padding:8px 10px;border-radius:8px;border:1px solid var(--border-strong);background:var(--surface);
  font:12.5px ui-monospace,"Cascadia Code",Consolas,monospace;color:var(--text);outline:none;resize:vertical;min-height:60px}
.field.checkbox{flex-direction:row;align-items:center;gap:8px;font-size:13px;color:var(--text)}
.field.checkbox input{width:16px;height:16px;accent-color:var(--ink);cursor:pointer}
.edit-actions{display:flex;align-items:center;gap:10px;flex-basis:100%;font-size:12.5px;margin-top:2px}

/* schedule builder (recurring-job edit): the frequency select swaps which group of
   fields is visible; only the matching one gets .show, in HTML at render time and by JS
   thereafter. */
.freq-group{display:none;gap:14px 18px}
.freq-group.show{display:flex;flex-wrap:wrap;align-items:flex-end}
.inline-unit{display:flex;align-items:center;gap:8px}
.inline-unit span{font-size:12.5px;color:var(--text-2);font-weight:550}
.reschedule-mode{display:flex;gap:16px;font-size:13px;color:var(--text);margin-bottom:2px;flex-basis:100%}
.reschedule-mode label{display:flex;align-items:center;gap:6px;cursor:pointer}
.reschedule-mode input{accent-color:var(--ink);cursor:pointer}

.health{display:inline-flex;align-items:center;gap:6px;font-size:12.5px;font-weight:550}

/* ---------- idle sign-out ---------- */
.idle-toast{position:fixed;right:16px;bottom:16px;z-index:50;display:flex;align-items:center;gap:12px;
  padding:12px 14px;border-radius:10px;background:var(--surface);border:1px solid var(--border);
  box-shadow:var(--shadow-pop);font-size:13px;max-width:320px;animation:idle-in .15s ease-out}
.idle-toast .msg{flex:1;font-variant-numeric:tabular-nums}
@keyframes idle-in{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
@media (prefers-reduced-motion:reduce){.idle-toast{animation:none}}
@media (max-width:520px){.idle-toast{left:16px;right:16px;max-width:none;bottom:calc(16px + env(safe-area-inset-bottom,0px))}}

/* ---------- responsive ---------- */
@media (max-width:1000px){
  .grid-main{grid-template-columns:minmax(0,1fr)}
}
@media (max-width:860px){
  .app{grid-template-columns:minmax(0,1fr)}
  .sidebar{position:static;height:auto;border-right:0;border-bottom:1px solid var(--border)}
  .brand{padding:12px 16px}
  .nav{flex-direction:row;overflow-x:auto;padding:0 12px 10px;gap:4px;scrollbar-width:none}
  .nav::-webkit-scrollbar{display:none}
  .nav-label,.sidebar-foot{display:none}
  .nav a{white-space:nowrap;flex:none}
  .topbar{position:static;padding:0 16px}
  .content{padding:16px 16px 40px}
  .grid-2{grid-template-columns:minmax(0,1fr)}
  .search input{width:130px}
}
@media (max-width:520px){
  .live-label{display:none}
  .crumbs{display:none}
  .top-actions{margin-left:0;width:100%}
  .search{flex:1}.search input{width:100%}
  .tile .value{font-size:24px}
}
@media (forced-colors:active){.dot,.timeline .marker,.legend i{forced-color-adjust:none}}
"""

JS = r"""
(function(){
  var doc=document, root=doc.documentElement;
  function store(k,v){try{if(v===undefined)return localStorage.getItem(k);localStorage.setItem(k,v)}catch(e){return null}}

  /* theme: system -> light -> dark */
  var themeIcons={system:'monitor',light:'sun',dark:'moon'};
  function applyTheme(t){
    if(t==='light'||t==='dark')root.setAttribute('data-theme',t);else root.removeAttribute('data-theme');
    var b=doc.getElementById('theme-btn'); if(!b)return;
    b.setAttribute('aria-label','Theme: '+t); b.title='Theme: '+t;
    b.querySelectorAll('[data-icon]').forEach(function(s){s.style.display=s.getAttribute('data-icon')===themeIcons[t]?'':'none'});
  }
  var theme=store('schedulefire-theme')||'system'; applyTheme(theme);
  doc.addEventListener('click',function(e){
    if(e.target.closest('#theme-btn')){
      theme={system:'light',light:'dark',dark:'system'}[theme]||'system'; store('schedulefire-theme',theme); applyTheme(theme);
    }
  });

  /* selection + bulk actions (delegated so it survives live refresh) */
  function syncSelection(){
    var boxes=doc.querySelectorAll('input[data-row-check]'), n=0;
    boxes.forEach(function(c){if(c.checked)n++});
    doc.querySelectorAll('[data-needs-selection]').forEach(function(b){b.disabled=n===0});
    doc.querySelectorAll('[data-selected-count]').forEach(function(el){el.textContent=n?n+' selected':''});
    var all=doc.querySelector('input[data-check-all]');
    if(all){all.checked=n>0&&n===boxes.length;all.indeterminate=n>0&&n<boxes.length}
  }
  function syncFreqGroups(select){
    var form=select.closest('form'); if(!form)return;
    form.querySelectorAll('.freq-group').forEach(function(g){
      g.classList.toggle('show',g.getAttribute('data-freq')===select.value);
    });
  }
  doc.addEventListener('change',function(e){
    var t=e.target;
    if(t.matches('input[data-check-all]'))doc.querySelectorAll('input[data-row-check]').forEach(function(c){c.checked=t.checked});
    if(t.matches('input[data-check-all],input[data-row-check]'))syncSelection();
    if(t.matches('select[data-nav]'))location.href=t.value;
    if(t.matches('[data-freq-select]'))syncFreqGroups(t);
  });
  doc.addEventListener('click',function(e){
    var b=e.target.closest('[data-confirm]');
    if(b&&!confirm(b.getAttribute('data-confirm'))){e.preventDefault();return}
    var x=e.target.closest('[data-dismiss]'); if(x){var n=x.closest('.notice'); if(n)n.remove()}
    var o=e.target.closest('[data-open-edit]');
    if(o){var row=o.closest('tr'); var next=row&&row.nextElementSibling;
      var d=next&&next.querySelector('details'); if(d){d.open=true; d.scrollIntoView({block:'nearest'})}}
  });
  syncSelection();

  /* notices: drop ?notice= from the URL so reloads don't repeat it */
  if(location.search.indexOf('notice=')>-1&&history.replaceState){
    var u=new URL(location.href); u.searchParams.delete('notice'); u.searchParams.delete('level'); history.replaceState(null,'',u.pathname+u.search+u.hash);
  }

  /* chart */
  var NS='http://www.w3.org/2000/svg';
  function el(name,attrs,parent){var n=doc.createElementNS(NS,name);for(var k in attrs)n.setAttribute(k,attrs[k]);if(parent)parent.appendChild(n);return n}
  function niceStep(max){
    if(max<=4)return 1;
    var raw=max/4, mag=Math.pow(10,Math.floor(Math.log10(raw))), f=raw/mag;
    return (f<=1?1:f<=2?2:f<=2.5?2.5:f<=5?5:10)*mag;
  }
  function fmt(n){return n>=1e6?(n/1e6).toFixed(n>=1e7?0:1).replace(/\.0$/,'')+'M':n>=1e4?(n/1e3).toFixed(n>=1e5?0:1).replace(/\.0$/,'')+'K':n.toLocaleString()}
  function colPath(x,yTop,w,h,round){
    if(h<=0)return '';
    var r=round?Math.min(4,h,w/2):0;
    return 'M'+x+','+(yTop+h)+'V'+(yTop+r)+(r?'Q'+x+','+yTop+' '+(x+r)+','+yTop:'')+'H'+(x+w-r)+
      (r?'Q'+(x+w)+','+yTop+' '+(x+w)+','+(yTop+r):'')+'V'+(yTop+h)+'Z';
  }
  function drawChart(box){
    var data; try{data=JSON.parse(box.getAttribute('data-points'))}catch(e){return}
    var W=box.clientWidth, H=box.clientHeight; if(!W)return;
    box.querySelectorAll('svg').forEach(function(s){s.remove()});
    var m={l:40,r:8,t:10,b:24}, pw=W-m.l-m.r, ph=H-m.t-m.b, n=data.length;
    var max=0; data.forEach(function(d){max=Math.max(max,d.s+d.f)});
    var step=niceStep(Math.max(max,1)), top=Math.max(step,Math.ceil(max/step)*step);
    var svg=el('svg',{role:'img','aria-label':box.getAttribute('aria-label')||'chart'});
    var g=el('g',{'class':'grid'},svg);
    for(var v=0;v<=top+1e-9;v+=step){
      var y=m.t+ph-(v/top)*ph;
      if(v>0)el('line',{x1:m.l,x2:W-m.r,y1:Math.round(y)+.5,y2:Math.round(y)+.5},g);
      var tx=el('text',{x:m.l-8,y:y+3.5,'text-anchor':'end'},svg); tx.textContent=fmt(v);
    }
    el('line',{'class':'base',x1:m.l,x2:W-m.r,y1:m.t+ph+.5,y2:m.t+ph+.5},svg);
    var slot=pw/n, bw=Math.max(2,Math.min(24,slot*0.62)), every=Math.max(1,Math.ceil(n/(pw/56)));
    data.forEach(function(d,i){
      var cx=m.l+slot*i+slot/2, x=Math.round(cx-bw/2);
      var col=el('g',{'class':'col',tabindex:'0','data-i':i,
        'aria-label':d.label+': '+d.s+' succeeded, '+d.f+' failed'},svg);
      el('rect',{'class':'hit',x:m.l+slot*i+1,y:m.t,width:Math.max(1,slot-2),height:ph,rx:4},col);
      var hs=d.s/top*ph, hf=d.f/top*ph, gap=(hs>0&&hf>0)?2:0, base=m.t+ph;
      if(hs>0)el('path',{d:colPath(x,base-hs,bw,hs,hf<=0),fill:'var(--series-succeeded)'},col);
      if(hf>0)el('path',{d:colPath(x,base-hs-gap-hf,bw,hf,true),fill:'var(--series-failed)'},col);
      if(i%every===0||i===n-1&&n<=8){var t=el('text',{x:cx,y:H-6,'text-anchor':'middle'},svg); t.textContent=d.tick}
    });
    box.appendChild(svg);
  }
  function tip(box){var t=box.querySelector('.tip'); if(!t){t=doc.createElement('div');t.className='tip';box.appendChild(t)} return t}
  function row(color,value,name){
    var r=doc.createElement('div');r.className='r';var i=doc.createElement('i');i.style.background=color;
    var b=doc.createElement('b');b.textContent=value.toLocaleString();var s=doc.createElement('span');s.textContent=name;
    r.appendChild(i);r.appendChild(b);r.appendChild(s);return r;
  }
  function showTip(col){
    var box=col.closest('.chart'), data=JSON.parse(box.getAttribute('data-points')), d=data[+col.getAttribute('data-i')];
    var t=tip(box); t.textContent='';
    var head=doc.createElement('div');head.className='t';head.textContent=d.label;t.appendChild(head);
    t.appendChild(row('var(--series-succeeded)',d.s,'Succeeded'));
    t.appendChild(row('var(--series-failed)',d.f,'Failed'));
    var hit=col.querySelector('.hit').getBBox(), x=hit.x+hit.width/2;
    t.classList.add('show');
    var left=Math.min(Math.max(4,x-t.offsetWidth/2),box.clientWidth-t.offsetWidth-4);
    t.style.left=left+'px'; t.style.top='-8px';
  }
  function hideTip(col){var box=col.closest('.chart'); var t=box&&box.querySelector('.tip'); if(t)t.classList.remove('show')}
  doc.addEventListener('pointerover',function(e){var c=e.target.closest&&e.target.closest('.chart .col'); if(c)showTip(c)});
  doc.addEventListener('pointerout',function(e){var c=e.target.closest&&e.target.closest('.chart .col'); if(c&&!c.contains(e.relatedTarget))hideTip(c)});
  doc.addEventListener('focusin',function(e){if(e.target.matches('.chart .col'))showTip(e.target)});
  doc.addEventListener('focusout',function(e){if(e.target.matches('.chart .col'))hideTip(e.target)});
  function drawAll(){doc.querySelectorAll('.chart[data-points]').forEach(drawChart)}
  drawAll();
  var rt; window.addEventListener('resize',function(){clearTimeout(rt);rt=setTimeout(drawAll,100)});

  /* idle sign-out: server enforces it; this shows a warning first and reacts once it happens.
     A background live-refresh poll (below) never counts as activity or keeps the session
     alive - only real navigation, form submits, and this warning's "Stay signed in" do,
     via a plain request with no X-Requested-With header (see Request.wants_html server-side). */
  var idleExpiredShown=false;
  function idleExpired(){
    if(idleExpiredShown)return; idleExpiredShown=true;
    var t=doc.querySelector('.idle-toast'); if(t)t.remove();
    var content=doc.getElementById('content'); if(!content||doc.getElementById('idle-expired-notice'))return;
    var d=doc.createElement('div'); d.id='idle-expired-notice'; d.className='notice error'; d.setAttribute('role','alert');
    d.innerHTML='<span>Your session expired due to inactivity.</span>';
    var b=doc.createElement('button'); b.type='button'; b.className='btn sm'; b.textContent='Reload';
    b.addEventListener('click',function(){location.reload()});
    d.appendChild(b); content.prepend(d);
  }
  var idleTimeout=parseInt(doc.body.getAttribute('data-idle-timeout')||'0',10);
  if(idleTimeout>0){
    var warnLead=Math.max(5,Math.min(30,Math.floor(idleTimeout*0.15))), lastActivity=Date.now(), lastTouch=0,
        toastEl=null, countdownTimer=null;
    function markActive(){lastActivity=Date.now()}
    ['mousemove','mousedown','keydown','scroll','touchstart','wheel'].forEach(function(evt){
      doc.addEventListener(evt,markActive,{passive:true});
    });
    doc.addEventListener('visibilitychange',function(){if(!doc.hidden)markActive()});
    function touch(){
      lastTouch=Date.now();
      fetch(location.href,{method:'HEAD',credentials:'same-origin'}).then(function(r){
        if(r.status===401||r.redirected)idleExpired();
      }).catch(function(){});
    }
    function hideToast(){if(toastEl){toastEl.remove();toastEl=null}if(countdownTimer){clearInterval(countdownTimer);countdownTimer=null}}
    function showToast(){
      if(toastEl||idleExpiredShown)return;
      toastEl=doc.createElement('div'); toastEl.className='idle-toast'; toastEl.setAttribute('role','status');
      var msg=doc.createElement('span'); msg.className='msg';
      var stay=doc.createElement('button'); stay.type='button'; stay.className='btn sm primary'; stay.textContent='Stay signed in';
      stay.addEventListener('click',function(){markActive();touch();hideToast()});
      toastEl.appendChild(msg); toastEl.appendChild(stay); doc.body.appendChild(toastEl);
      updateCountdown(); countdownTimer=setInterval(updateCountdown,1000);
    }
    function updateCountdown(){
      if(!toastEl)return;
      var remain=Math.max(0,Math.ceil(idleTimeout-(Date.now()-lastActivity)/1000));
      var m=Math.floor(remain/60), s=remain%60;
      toastEl.querySelector('.msg').textContent='Signing out due to inactivity in '+m+':'+(s<10?'0':'')+s;
      if(remain<=0)hideToast();
    }
    setInterval(function(){
      if(idleExpiredShown)return;
      var idleFor=(Date.now()-lastActivity)/1000;
      if(idleFor>=idleTimeout){hideToast();idleExpired();return}
      if(idleFor>=idleTimeout-warnLead)showToast(); else hideToast();
      // The human is present (real activity) but hasn't navigated in a while: keep the
      // server-side session alive so reading/scrolling for minutes doesn't sign them out.
      if(lastActivity>lastTouch&&Date.now()-lastTouch>20000)touch();
    },1000);
  }

  /* live refresh: swap the page content in place every `data-refresh-interval` ms (default 5s) */
  var refreshInterval=parseInt(doc.body.getAttribute('data-refresh-interval')||'0',10)||5000;
  var liveBtn=doc.getElementById('live-btn');
  if(liveBtn){
    var live=store('schedulefire-live')!=='off';
    function setLive(v){live=v;liveBtn.setAttribute('aria-pressed',v?'true':'false');store('schedulefire-live',v?'on':'off')}
    setLive(live);
    liveBtn.addEventListener('click',function(){setLive(!live)});
    setInterval(function(){
      if(!live||doc.hidden||idleExpiredShown)return;
      if(doc.querySelector('input[data-row-check]:checked'))return;
      var a=doc.activeElement; if(a&&(a.tagName==='INPUT'||a.tagName==='SELECT'))return;
      if(doc.querySelector('details[open]'))return;
      fetch(location.href,{headers:{'X-Requested-With':'schedulefire-live'},credentials:'same-origin'}).then(function(r){
        if(r.status===401){idleExpired();return null}
        if(!r.ok||r.redirected)return null; return r.text();
      }).then(function(html){
        if(!html)return;
        var next=new DOMParser().parseFromString(html,'text/html');
        ['content','nav'].forEach(function(id){var a=doc.getElementById(id),b=next.getElementById(id); if(a&&b)a.innerHTML=b.innerHTML});
        drawAll(); syncSelection();
      }).catch(function(){});
    },refreshInterval);
  }
})();
"""

_ICON_PATHS = {
    "overview": '<rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/>'
                '<rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/>',
    "enqueued": '<path d="M22 12h-6l-2 3h-4l-2-3H2"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6'
                'l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/>',
    "scheduled": '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    "processing": '<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
    "succeeded": '<circle cx="12" cy="12" r="9"/><path d="m9 12 2 2 4-4"/>',
    "failed": '<circle cx="12" cy="12" r="9"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/>',
    "deleted": '<path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/>'
               '<path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>',
    "awaiting": '<path d="M5 22h14"/><path d="M5 2h14"/><path d="M17 22v-4.17a2 2 0 0 0-.59-1.42L12 12l-4.41 4.41'
                'A2 2 0 0 0 7 17.83V22"/><path d="M7 2v4.17a2 2 0 0 0 .59 1.42L12 12l4.41-4.41A2 2 0 0 0 17 6.17V2"/>',
    "recurring": '<path d="m17 2 4 4-4 4"/><path d="M3 11v-1a4 4 0 0 1 4-4h14"/><path d="m7 22-4-4 4-4"/>'
                 '<path d="M21 13v1a4 4 0 0 1-4 4H3"/>',
    "servers": '<rect x="2" y="3" width="20" height="8" rx="2"/><rect x="2" y="13" width="20" height="8" rx="2"/>'
               '<path d="M6 7h.01"/><path d="M6 17h.01"/>',
    "queues": '<path d="m12 2 10 5-10 5L2 7z"/><path d="m2 17 10 5 10-5"/><path d="m2 12 10 5 10-5"/>',
    "search": '<circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/>',
    "sun": '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41'
           'M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>',
    "moon": '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
    "monitor": '<rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>',
    "requeue": '<path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/>',
    "play": '<path d="M6 3.5v17a.5.5 0 0 0 .76.43l13.5-8.5a.5.5 0 0 0 0-.86L6.76 3.07A.5.5 0 0 0 6 3.5z"/>',
    "left": '<path d="m15 18-6-6 6-6"/>',
    "right": '<path d="m9 18 6-6-6-6"/>',
    "flame": '<path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.07-2.14-.22-4.05 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3'
             ' 3.5 3 5.5a7 7 0 1 1-14 0c0-1.15.43-2.29 1-3a2.5 2.5 0 0 0 2.5 2.5z"/>',
    "check": '<path d="M20 6 9 17l-5-5"/>',
    "user": '<circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/>',
    "logout": '<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><path d="m16 17 5-5-5-5"/><path d="M21 12H9"/>',
    "edit": '<path d="M12 20h9"/><path d="M16.38 3.62a2.12 2.12 0 1 1 3 3L7.37 18.64 3 20l1.36-4.37z"/>',
    "x": '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
    "link": '<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>'
            '<path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>',
    "add": '<path d="M12 5v14"/><path d="M5 12h14"/>',
}


#: Applies the saved theme before first paint (inline to avoid a flash; allowed by CSP hash).
THEME_BOOT = ("try{var t=localStorage.getItem('schedulefire-theme');if(t==='light'||t==='dark')"
              "document.documentElement.setAttribute('data-theme',t)}catch(e){}")
THEME_BOOT_HASH = "sha256-" + base64.b64encode(hashlib.sha256(THEME_BOOT.encode("utf-8")).digest()).decode("ascii")

#: Content-Security-Policy: only our own script/style, no framing, forms post back to us only.
CSP = "; ".join([
    "default-src 'none'",
    f"script-src 'self' '{THEME_BOOT_HASH}'",
    "style-src 'self' 'unsafe-inline'",  # style="" attributes; no untrusted input reaches them
    "img-src 'self' data:",
    "connect-src 'self'",
    "form-action 'self'",
    "frame-ancestors 'none'",
    "base-uri 'none'",
])


def icon(name: str, **attrs: str) -> str:
    extra = "".join(f' {k.replace("_", "-")}="{v}"' for k, v in attrs.items())
    return f'<svg class="i" viewBox="0 0 24 24" aria-hidden="true"{extra}>{_ICON_PATHS[name]}</svg>'


def _fingerprint(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


CSS_VERSION = _fingerprint(CSS)
JS_VERSION = _fingerprint(JS)

#!/usr/bin/env python3
"""Independent multi-set Bybit Demo bot.

Run: python bybit_multi_set_bot.py [--dashboard-only] [--sets N] [--port 8788]
This file never uses the existing bybit_demo_bot.py or its SQLite database.
"""
from __future__ import annotations
import argparse, hashlib, hmac, json, logging, math, os, queue, sqlite3, threading, time, uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen

try:
    from api_config import DEMO_CONFIG
    from multi_set_config import GLOBAL_CONFIG, PORTFOLIO_SETS, ENABLED_SET_COUNT, ALLOW_SYMBOL_OVERLAP, LOCAL_DASHBOARD_PORT
except ImportError as exc:
    raise SystemExit("Нужны api_config.py и multi_set_config.py рядом со скриптом") from exc

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("MULTI_SET_DB", ROOT / "runtime/multi_set_bot.sqlite3"))
LOG_PATH = Path(os.environ.get("MULTI_SET_LOG", ROOT / "runtime/multi_set_bot.log"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True); LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH, encoding="utf-8")])
log = logging.getLogger("bybit_multi_set_bot")
D = Decimal


def now(): return datetime.now(timezone.utc).isoformat()
def dec(v, default="0"): return D(str(v if v is not None else default))
def fnum(v):
    if v in (None, ""): return 0.0
    x=float(v); return x if math.isfinite(x) else 0.0

def dashboard_sample(values,limit=600):
    """Evenly sample chart points so a reduced payload still spans the full window."""
    if len(values)<=limit: return values
    last=len(values)-1
    return [values[round(i*last/(limit-1))] for i in range(limit)]

def validate_config(sets, count):
    enabled=[s for s in sets if s.get("enabled", True)]
    if count < 1 or count > len(enabled): raise ValueError(f"--sets={count}: доступно включённых комплектов {len(enabled)}")
    enabled=enabled[:count]; seen={}; ids=set(); shares=0.0
    for s in enabled:
        sid=s.get("id");
        if not sid or sid in ids: raise ValueError("id комплектов должны быть уникальными")
        ids.add(sid); shares+=float(s.get("budget_share",0))
        for key in ("leaders","legs"):
            groups=s.get(key)
            if not isinstance(groups,list) or not groups: raise ValueError(f"{sid}.{key}: нужен непустой список ног")
            for i,g in enumerate(groups):
                if not isinstance(g,list) or not 1 <= len(g) <= 3: raise ValueError(f"{sid}.{key}[{i}]: 1..3 символа")
                local=set()
                for symbol in g:
                    symbol=str(symbol).upper()
                    if not symbol.endswith("USDT") or symbol in local: raise ValueError(f"некорректный или повторный символ {symbol} в {sid}")
                    local.add(symbol)
                    if not ALLOW_SYMBOL_OVERLAP and symbol in seen: raise ValueError(f"пересечение {symbol}: {seen[symbol]} и {sid}")
                    seen[symbol]=sid
    if shares > 1.0 + 1e-9: raise ValueError(f"budget_share комплектов должен быть <= 1, сейчас {shares:.4f}")
    return enabled


class BybitDemoHTTP:
    BASE="https://api-demo.bybit.com"
    PATHS={"wallet":("GET","/v5/account/wallet-balance"),"positions":("GET","/v5/position/list"),"kline":("GET","/v5/market/kline"),"ticker":("GET","/v5/market/tickers"),"instrument":("GET","/v5/market/instruments-info"),"order":("POST","/v5/order/create"),"leverage":("POST","/v5/position/set-leverage")}
    def __init__(self,key,secret,timeout=20): self.key=key; self.secret=secret.encode(); self.timeout=timeout
    def call(self,name,**params):
        method,path=self.PATHS[name]; params={k:v for k,v in params.items() if v is not None}
        if method=="GET":
            from urllib.parse import urlencode
            body=urlencode([(k,str(v)) for k,v in params.items()]); url=self.BASE+path+("?"+body if body else ""); data=None
        else: body=json.dumps(params,separators=(",",":")); url=self.BASE+path; data=body.encode()
        # Read-only GET requests may be retried once on network timeout. Never
        # retry order-creation POSTs: a lost response could mean the order filled.
        attempts=2 if method=="GET" else 1
        for attempt in range(attempts):
            stamp=str(int(time.time()*1000))
            sig=hmac.new(self.secret,(stamp+self.key+"5000"+body).encode(),hashlib.sha256).hexdigest()
            headers={"X-BAPI-API-KEY":self.key,"X-BAPI-SIGN":sig,"X-BAPI-SIGN-TYPE":"2","X-BAPI-TIMESTAMP":stamp,"X-BAPI-RECV-WINDOW":"5000","Content-Type":"application/json","User-Agent":"MultiSetDemoBot/1.0"}
            try:
                with urlopen(Request(url,data=data,method=method,headers=headers),timeout=self.timeout) as r: out=json.loads(r.read().decode())
            except HTTPError as e:
                try: out=json.loads(e.read().decode())
                except Exception: raise RuntimeError(f"Bybit HTTP {e.code}") from e
            except (TimeoutError,URLError,OSError) as e:
                if method=="GET" and attempt+1<attempts:
                    log.warning("Bybit %s read timed out/failed; retrying once: %s",name,e); time.sleep(1); continue
                raise
            if out.get("retCode") not in (0,None): raise RuntimeError(f"Bybit {out.get('retCode')}: {out.get('retMsg')}")
            return out
        raise RuntimeError(f"Bybit {name}: no response after retry")


class Store:
    def __init__(self,path):
        self.conn=sqlite3.connect(path,check_same_thread=False); self.lock=threading.RLock(); self.conn.executescript("""CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY,t TEXT NOT NULL,set_id TEXT,event TEXT,payload TEXT); CREATE TABLE IF NOT EXISTS bars(t TEXT PRIMARY KEY,prices TEXT,spread REAL); CREATE TABLE IF NOT EXISTS equity_history(t TEXT PRIMARY KEY,equity REAL);"""); self.conn.commit()
    def put(self,key,value):
        with self.lock: self.conn.execute("INSERT OR REPLACE INTO kv VALUES(?,?)",(key,json.dumps(value,separators=(",",":")))); self.conn.commit()
    def get(self,key,default=None):
        with self.lock:
            r=self.conn.execute("SELECT value FROM kv WHERE key=?",(key,)).fetchone()
        return json.loads(r[0]) if r else default
    def event(self,set_id,event,**payload):
        with self.lock: self.conn.execute("INSERT INTO events(t,set_id,event,payload) VALUES(?,?,?,?)",(now(),set_id,event,json.dumps(payload,default=str))); self.conn.commit()
    def bar(self,t,prices,spread):
        with self.lock: self.conn.execute("INSERT OR REPLACE INTO bars VALUES(?,?,?)",(t,json.dumps(prices),float(spread))); self.conn.commit()
    def history(self,limit=200):
        with self.lock: rows=self.conn.execute("SELECT t,prices,spread FROM bars ORDER BY t DESC LIMIT ?",(limit,)).fetchall()
        return [{"time":r[0],"prices":json.loads(r[1]),"spread":r[2]} for r in reversed(rows)]
    def latest_bar_ms(self):
        with self.lock: row=self.conn.execute("SELECT t FROM bars ORDER BY t DESC LIMIT 1").fetchone()
        if not row: return 0
        try: return int(datetime.fromisoformat(row[0]).timestamp()*1000)
        except (TypeError,ValueError,OverflowError): return 0
    def clear_bars(self):
        with self.lock: self.conn.execute("DELETE FROM bars"); self.conn.commit()
    def record_equity(self,t,equity):
        with self.lock: self.conn.execute("INSERT OR REPLACE INTO equity_history VALUES(?,?)",(t,float(equity))); self.conn.commit()
    def equity_history(self,limit=1801):
        with self.lock: rows=self.conn.execute("SELECT t,equity FROM equity_history ORDER BY t DESC LIMIT ?",(limit,)).fetchall()
        return [{"t":r[0],"v":r[1]} for r in reversed(rows)]
    def events(self,set_id,limit=30):
        with self.lock: rows=self.conn.execute("SELECT t,event,payload FROM events WHERE set_id=? ORDER BY id DESC LIMIT ?",(set_id,limit)).fetchall()
        out=[]
        for t,event,payload in reversed(rows):
            try: data=json.loads(payload)
            except json.JSONDecodeError: data={}
            out.append({"t":t,"event":event,"payload":data})
        return out
    def close(self): self.conn.close()


@dataclass
class SetState:
    set_id:str; paused:bool=False; halted:bool=False; bundles:int=0; direction:int=0; ref_spread:float|None=None; cycle_pnl:float=0.0; realized_pnl:float=0.0; cycle_start_equity:float=0.0; last_close_reason:str=""; anchor:dict=field(default_factory=dict); last_bar:str|None=None


class GlobalRiskManager:
    def __init__(self,config): self.cfg=config; self.available=0.0; self.equity=0.0; self.wallet=0.0; self.start_equity=0.0
    def update(self,wallet):
        a=(wallet.get("result",{}).get("list") or [{}])[0]; self.wallet=fnum(a.get("totalWalletBalance")); self.equity=fnum(a.get("totalEquity")); self.available=fnum(a.get("totalAvailableBalance"))
    def reserves(self,engines):
        return sum(e.hedge_reserve() for e in engines if e.state.bundles)
    def can_use(self,engines,requested):
        return self.available - self.reserves(engines) - self.cfg["hard_floor_usdt"] >= requested


class SetEngine:
    def __init__(self,bot,config,state): self.bot=bot; self.config=config; self.state=state; self.symbols=[x for g in config["leaders"]+config["legs"] for x in g]; self.steps={}
    @property
    def nlegs(self): return len(self.config["legs"])
    def hedge_reserve(self): return max(self.bot.risk.cfg["hedge_buffer_usdt"], self.gross_notional()*0.05)
    def gross_notional(self): return self.nlegs*float(self.bot.cfg["initial_leg_notional_usdt"])*max(1,self.state.bundles)
    def all_groups(self): return self.config["leaders"]+self.config["legs"]
    def curves(self,prices):
        if not self.state.anchor: return None,None
        if self.config.get("curve_mode")=="legacy_basket":
            leaders_symbols=[x for g in self.config["leaders"] for x in g]
            legs_symbols=[x for g in self.config["legs"] for x in g]
            leaders=sum(prices[x] for x in leaders_symbols)/sum(self.state.anchor[x] for x in leaders_symbols)
            legs=sum(prices[x] for x in legs_symbols)/sum(self.state.anchor[x] for x in legs_symbols)
            return leaders,legs
        def group(g): return sum(prices[x]/self.state.anchor[x] for x in g)/len(g)
        leaders=sum(group(g) for g in self.config["leaders"])/len(self.config["leaders"])
        legs=sum(group(g) for g in self.config["legs"])/len(self.config["legs"])
        return leaders,legs
    def spread(self,prices):
        c=self.curves(prices); return None if c[0] is None else c[0]-c[1]
    def set_pnl(self,positions):
        return sum(fnum(p.get("unrealisedPnl",p.get("unrealizedPnl",0))) for p in positions if p.get("symbol") in self.symbols)+self.state.realized_pnl
    def set_roi(self,positions):
        base=self.state.cycle_start_equity or self.gross_notional() or 1.0; return self.set_pnl(positions)/base
    def qty(self,symbol,notional,price):
        spec=self.bot.specs.get(symbol,{})
        step=D(str(spec.get("qtyStep", "0.001"))); minimum=D(str(spec.get("minOrderQty", step)))
        min_notional=D(str(spec.get("minNotionalValue", "0")))
        q=D(str(notional))/D(str(price))
        required=max(minimum, min_notional/D(str(price)) if min_notional > 0 else D("0"))
        # Round upward only when the exchange minimum is larger than the
        # requested target; round normal target quantities down to qtyStep.
        if q < required:
            units=(required/step).to_integral_value(rounding=ROUND_CEILING); q=units*step
        else:
            q=(q/step).to_integral_value(rounding=ROUND_DOWN)*step
        return float(q)
    def open_orders(self,prices,reason="OPEN"):
        if not self.bot.allow_orders: return
        per_leg=float(self.bot.cfg["initial_leg_notional_usdt"]); total=self.nlegs*per_leg; leader_each=total/len(self.config["leaders"])
        for group,notional,side in [(g,leader_each,"Sell" if self.state.direction==1 else "Buy") for g in self.config["leaders"]]+[(g,per_leg,"Buy" if self.state.direction==1 else "Sell") for g in self.config["legs"]]:
            each=notional/len(group)
            position_idx = 2 if side == "Sell" else 1
            for symbol in group: self.bot.order(symbol,side,self.qty(symbol,each,prices[symbol]),position_idx,f"MSET-{self.state.set_id}-{reason}-{uuid.uuid4().hex[:8]}")
        self.state.bundles+=1; self.bot.store.event(self.state.set_id,reason,bundles=self.state.bundles); self.bot.save()
    def close(self,reason="SET_CLOSE"):
        if self.bot.allow_orders:
            for p in self.bot.positions:
                if p.get("symbol") in self.symbols and fnum(p.get("size"))>0:
                    side="Sell" if p.get("side")=="Buy" else "Buy"; self.bot.order(p["symbol"],side,fnum(p["size"]),int(p.get("positionIdx",1)),f"MSET-{self.state.set_id}-CLOSE-{uuid.uuid4().hex[:8]}",True)
        self.state.bundles=0; self.state.realized_pnl+=self.state.cycle_pnl; self.state.cycle_pnl=0; self.state.ref_spread=None; self.state.last_close_reason=reason; self.bot.store.event(self.state.set_id,reason); self.bot.save()
    def on_bar(self,prices,bar_time):
        sp=self.spread(prices)
        if sp is None:
            self.state.anchor=dict(prices); self.bot.save(); sp=self.spread(prices)
        self.state.last_bar=bar_time
        if sp is None: return
        if not self.state.bundles and not self.state.paused and self.bot.risk.can_use(self.bot.engines,self.gross_notional()/float(self.bot.cfg["leverage"])):
            self.state.direction=1 if sp>0 else -1; self.state.ref_spread=sp; self.state.cycle_start_equity=self.bot.risk.equity; self.open_orders(prices,"OPEN")
        elif self.state.bundles:
            if self.state.ref_spread is None: self.state.ref_spread=sp
            if self.set_roi(self.bot.positions)>=float(self.config.get("roi_target",self.bot.cfg["set_roi_target"])) or (self.state.direction and (sp-self.state.ref_spread)*self.state.direction <= -float(self.bot.cfg["add_step"]*max(1,self.state.bundles)) and self.set_pnl(self.bot.positions)>-self.gross_notional()*0.02):
                if self.set_roi(self.bot.positions)>=float(self.config.get("roi_target",self.bot.cfg["set_roi_target"])): self.close("SET_ROI")
                elif not self.state.paused and self.bot.risk.can_use(self.bot.engines,self.gross_notional()/float(self.bot.cfg["leverage"])): self.open_orders(prices,"ADD"); self.state.ref_spread=sp
        self.bot.save()


class Dashboard:
    def __init__(self,bot): self.bot=bot; self.commands=queue.Queue(); self.live=[]
    def status(self): return self.bot.public_status()
    def server(self,host,port):
        bot=self
        class H(BaseHTTPRequestHandler):
            def log_message(self,*a): pass
            def send(self,code,obj,ctype="application/json"):
                self.send_response(code); self.send_header("Content-Type",ctype+"; charset=utf-8"); self.send_header("Cache-Control","no-store"); self.send_header("Connection","close"); self.end_headers()
                if isinstance(obj,bytes):
                    self.wfile.write(obj); return
                if ctype.startswith("application"):
                    encoder=json.JSONEncoder(ensure_ascii=False,separators=(",",":")); buffer=bytearray()
                    for chunk in encoder.iterencode(obj):
                        buffer.extend(chunk.encode("utf-8"))
                        if len(buffer)>=65536:
                            self.wfile.write(buffer); buffer.clear()
                    if buffer: self.wfile.write(buffer)
                else: self.wfile.write(obj.encode("utf-8"))
            def do_GET(self):
                path=urlsplit(self.path).path; q=parse_qs(urlsplit(self.path).query)
                if path=="/": self.send(200,DASHBOARD_HTML,"text/html"); return
                if path=="/api/status": self.send(200,bot.status()); return
                if path=="/api/history": self.send(200,{"bars":bot.store.history(int(q.get("limit",[200])[0]))}); return
                self.send(404,{"error":"not found"})
            def do_POST(self):
                if urlsplit(self.path).path!="/api/command": self.send(404,{"ok":False}); return
                try: data=json.loads(self.rfile.read(min(int(self.headers.get("Content-Length",0)),4096)) or b"{}"); action=data.get("action"); set_id=data.get("setId")
                except Exception: self.send(400,{"ok":False,"message":"invalid JSON"}); return
                if action not in {"close_set","close_all","pause_set","resume_set","pause_all","resume_all"}: self.send(400,{"ok":False,"message":"unknown command"}); return
                bot.commands.put((action,set_id)); self.send(202,{"ok":True,"message":"Команда поставлена в очередь"})
        return ThreadingHTTPServer((host,port),H)


DASHBOARD_HTML=r'''<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Multi-Set Bybit Demo</title><style>:root{--bg:#091016;--panel:#101b25;--edge:#294252;--text:#dce8ee;--muted:#8ba0ad;--cyan:#43d9c6;--yellow:#ffca5a;--red:#ff7171;--green:#7ee19b}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px ui-monospace,monospace}header{padding:18px 24px;border-bottom:1px solid var(--edge);display:flex;justify-content:space-between;gap:20px}.sub,.small{color:var(--muted);font-size:12px}.badge{color:var(--cyan);border:1px solid var(--cyan);padding:6px}.bad{color:var(--red);border-color:var(--red)}main{max-width:1500px;margin:auto;padding:16px}.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px}.panel{background:var(--panel);border:1px solid var(--edge);border-radius:7px;padding:13px;margin-top:12px}.v{font-size:20px;margin-top:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.k{color:var(--muted);font-size:11px}.toolbar{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.toolbar button,.toolbar select{background:#173445;color:var(--text);border:1px solid #3b6b7d;border-radius:4px;padding:8px;font:12px ui-monospace,monospace}.toolbar .danger{background:#51232a;border-color:var(--red)}.tabs button.active{border-color:var(--cyan);color:var(--cyan)}.cards{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:10px}.cards>div{min-width:0;overflow:hidden}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;align-items:stretch}.grid2>.panel{height:100%}#market .v{font-size:12px;line-height:1.3;white-space:normal;overflow-wrap:anywhere;max-height:82px;overflow:auto}.chartGrid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.chartGrid .panel{min-width:0}.chart{width:100%;height:230px;background:#0b151d;border:1px solid #1b2a36}.line{stroke:var(--cyan);stroke-width:2;fill:none}.line2{stroke:var(--yellow);stroke-width:2;fill:none}.axis{stroke:#29404f}.mean{stroke:#e78cff;stroke-dasharray:7 5;fill:none}.pos{color:var(--green)}.neg{color:var(--red)}table{border-collapse:collapse;width:100%;font-size:12px}th,td{text-align:left;padding:7px;border-bottom:1px solid var(--edge)}@media(max-width:1000px){.grid,.cards,.grid2,.chartGrid{grid-template-columns:repeat(2,1fr)}}@media(max-width:600px){.grid,.cards,.grid2,.chartGrid{grid-template-columns:1fr 1fr}header{flex-direction:column}}</style></head><body><header><div><h1>BYBIT DEMO / MULTI-SET</h1><div class="sub">общие метрики аккаунта всегда видимы · индивидуальные графики по выбранному комплекту</div></div><div id="badge" class="badge">Загрузка…</div></header><main><section class="panel"><div class="toolbar"><label>Комплект: <select id="setSelect"></select></label><button class="danger" onclick="cmd('close_set')">ЗАКРЫТЬ ВЫБРАННЫЙ КОМПЛЕКТ</button><button onclick="cmd('pause_set')">ПАУЗА ВЫБРАННОГО</button><button onclick="cmd('resume_set')">ВОЗОБНОВИТЬ ВЫБРАННЫЙ</button><button class="danger" onclick="cmd('close_all')">ЗАКРЫТЬ ВСЕ КОМПЛЕКТЫ</button><button onclick="cmd('pause_all')">ПАУЗА ВСЕХ</button><button onclick="cmd('resume_all')">ВОЗОБНОВИТЬ ВСЕХ</button><span id="msg" class="small"></span></div></section><section class="panel"><h2>Общие метрики торговли</h2><div id="global" class="cards"></div></section><section class="panel"><h2 id="title">Комплект</h2><div id="setCards" class="cards"></div></section><div class="grid2"><section class="panel"><h2>Стратегия / цикл</h2><div id="strategy" class="cards"></div></section><section class="panel"><h2>Рынок / уровни</h2><div id="market" class="cards"></div></section></div><div class="chartGrid"><section class="panel"><h2>Live: Spread — последний час</h2><svg id="liveSpread" class="chart" viewBox="0 0 700 230" preserveAspectRatio="none"></svg></section><section class="panel"><h2>Live: Equity — последний час</h2><svg id="liveEquity" class="chart" viewBox="0 0 700 230" preserveAspectRatio="none"></svg></section><section class="panel"><h2>Live: нормализованные ноги — последний час</h2><svg id="liveLegs" class="chart" viewBox="0 0 700 230" preserveAspectRatio="none"></svg></section></div><section class="panel"><h2>Spread — 7 дней / закрытые 1h бары</h2><svg id="hourlySpread" class="chart" viewBox="0 0 1400 240" preserveAspectRatio="none"></svg></section><section class="panel"><h2>Нормализованные ноги — 7 дней / закрытые 1h бары</h2><svg id="hourlyLegs" class="chart" viewBox="0 0 1400 240" preserveAspectRatio="none"></svg></section><section class="panel"><h2>Позиции выбранного комплекта</h2><div id="positions"></div></section><section class="panel"><h2>Последние события стратегии</h2><div id="events"></div></section></main><script>const $=s=>document.querySelector(s),fmt=(v,n=2)=>v==null?'—':Number(v).toLocaleString('ru-RU',{minimumFractionDigits:n,maximumFractionDigits:n}),esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));let selected=null,last=null;function card(k,v){return `<div><div class="k">${esc(k)}</div><div class="v">${v}</div></div>`}function cmd(action){let q=action==='close_all'?'Закрыть позиции всех комплектов?':action==='close_set'?'Закрыть выбранный комплект?':'Выполнить команду '+action+'?';if(!confirm(q))return;fetch('/api/command',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,setId:selected})}).then(r=>r.json()).then(x=>{$('#msg').textContent=x.message||'Принято';setTimeout(load,300)}).catch(e=>$('#msg').textContent='Ошибка локального сервера')}function lineSvg(id,vals,color,wide=false){let svg=$(id),a=vals.map(Number).filter(Number.isFinite),w=wide?1400:700,h=wide?240:230,p=30;if(!a.length){svg.innerHTML='<text x="350" y="115" text-anchor="middle" fill="#8ba0ad">Ожидание данных торгового цикла</text>';return}let lo=Math.min(...a),hi=Math.max(...a);if(hi===lo){lo-=1;hi+=1}let d=path(a,w,h,p,lo,hi);svg.innerHTML=`<line x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}" class="axis"/><path d="${d}" class="${color}"/><text x="${p+5}" y="${p+12}" fill="#8ba0ad">max ${fmt(hi,5)}</text><text x="${p+5}" y="${h-8}" fill="#8ba0ad">min ${fmt(lo,5)}</text>`}
function dualSvg(id,rows,wide=false){let svg=$(id),a=rows.filter(x=>Number.isFinite(Number(x.leaders))&&Number.isFinite(Number(x.legs))),w=wide?1400:700,h=wide?240:230,p=30;if(!a.length){svg.innerHTML='<text x="'+(w/2)+'" y="'+(h/2)+'" text-anchor="middle" fill="#8ba0ad">Ожидание закрытых часовых данных</text>';return}let all=a.flatMap(x=>[Number(x.leaders),Number(x.legs)]),lo=Math.min(...all),hi=Math.max(...all);if(hi===lo){lo-=.001;hi+=.001}svg.innerHTML=`<path d="${path(a.map(x=>Number(x.leaders)),w,h,p,lo,hi)}" class="line"/><path d="${path(a.map(x=>Number(x.legs)),w,h,p,lo,hi)}" class="line2"/>`}
function path(a,w,h,p,lo,hi){return a.map((v,i)=>`${i?'L':'M'} ${p+(w-2*p)*i/Math.max(1,a.length-1)} ${h-p-(v-lo)/(hi-lo)*(h-2*p)}`).join(' ')}function draw(st){let s=st.sets.find(x=>x.id===selected)||st.sets[0];if(!s)return;selected=s.id;let sel=$('#setSelect');if(!sel.options.length)st.sets.forEach(x=>{let o=document.createElement('option');o.value=x.id;o.textContent=x.name;sel.appendChild(o)});sel.value=selected;$('#badge').textContent=st.halted?'ОШИБКА / ОСТАНОВЛЕН':(st.paused?'ПАУЗА':'Demo: работает');$('#badge').className='badge '+(st.halted?'bad':'');$('#global').innerHTML=[card('Wallet',fmt(st.wallet)),card('Equity',fmt(st.equity)),card('Free margin',fmt(st.available)),card('Общий uPnL',fmt(st.totalPnl)),card('Общий ROI',fmt(st.totalRoi*100,3)+'%'),card('Hedge reserve',fmt(st.hedgeReserve)),card('Закрытие по ROI',fmt(st.globalCloseBalance)+' USDT'),card('Расходы закрытия',fmt(st.estimatedCloseCosts)+' USDT')].join('');$('#title').textContent=s.name;$('#setCards').innerHTML=[card('PnL',fmt(s.pnl)),card('ROI',fmt(s.roi*100,3)+'%'),card('Корзины',s.bundles),card('Spread',fmt(s.spread,5)),card('Hedge',s.hedged?'АКТИВЕН':'нет'),card('Статус',s.paused?'пауза':s.halted?'ошибка':'работает')].join('');$('#strategy').innerHTML=[card('Направление',s.directionLabel),card('STEP',fmt(s.step,5)),card('ROI цель',fmt(s.roiTarget*100,3)+'%'),card('Reference',fmt(s.refSpread,5)),card('Cycle equity',fmt(s.cycleStartEquity)),card('Последнее закрытие',s.lastCloseReason||'—')].join('');let mp=s.market?.markPrices||{},ap=s.market?.anchorPrices||{};$('#market').innerHTML=[card('Leaders',s.leaders||'—'),card('Legs',s.legs||'—'),card('Mark prices',Object.entries(mp).map(([k,v])=>`${esc(k)} ${fmt(v,5)}`).join('<br>')),card('Anchor prices',Object.entries(ap).map(([k,v])=>`${esc(k)} ${fmt(v,5)}`).join('<br>')),card('Spread now',fmt(s.spread,5))].join('');let rows=s.curves||[],live=s.live||{};lineSvg('#liveSpread',(live.spread||[]).map(x=>x.v),'line');lineSvg('#liveEquity',(live.equity||[]).map(x=>x.v),'line2');dualSvg('#liveLegs',live.curves||[]);lineSvg('#hourlySpread',rows.map(x=>x.spread),'line',true);dualSvg('#hourlyLegs',rows,true);$('#positions').innerHTML=s.positions?.length?'<table><tr><th>Symbol</th><th>Side</th><th>Idx</th><th>Size</th><th>Average</th><th>Mark</th><th>uPnL</th></tr>'+s.positions.map(p=>`<tr><td>${esc(p.symbol)}</td><td>${esc(p.side)}</td><td>${p.positionIdx}</td><td>${fmt(p.size,6)}</td><td>${fmt(p.avgPrice,5)}</td><td>${fmt(p.markPrice,5)}</td><td class="${Number(p.pnl)>=0?'pos':'neg'}">${fmt(p.pnl,4)}</td></tr>`).join('')+'</table>':'<div class="small">Позиции отсутствуют</div>';$('#events').innerHTML=(s.events||[]).slice().reverse().map(e=>`<div class="small">${esc(e.t)} · ${esc(e.event)} · ${esc(JSON.stringify(e.payload||{}))}</div>`).join('')||'<div class="small">Нет событий</div>'}async function load(){try{let st=await (await fetch('/api/status',{cache:'no-store'})).json();last=st;if(selected&&!st.sets.some(x=>x.id===selected))selected=null;draw(st)}catch(e){$('#badge').textContent='Нет соединения'}}$('#setSelect').onchange=()=>{selected=$('#setSelect').value;draw(last)};load();setInterval(load,5000)</script></body></html>'''


class MultiSetBot:
    def __init__(self,api,sets,store,allow_orders=True,cfg=None):
        self.api=api; self.configs=sets; self.store=store; self.allow_orders=bool(allow_orders); self.cfg=cfg or GLOBAL_CONFIG; self.store.equity_history(); self.risk=GlobalRiskManager(self.cfg); self.risk.start_equity=float(store.get("global_start_equity",0) or 0); self.positions=[]; self.specs={}; equity_seed=store.equity_history(1801); self.live={c["id"]:{"spread":[],"curves":[],"equity":list(equity_seed)} for c in sets}; self.current_bar_ms=0; self.last_processed_bar_ms=0; self.paused=False; self.halted=False; self.dashboard=Dashboard(self); self.commands=self.dashboard.commands; self.engines=[]; saved=store.get("states",{})
        for c in sets:
            raw=saved.get(c["id"],{}); state=SetState(c["id"],**{k:v for k,v in raw.items() if k in SetState.__dataclass_fields__ and k!="set_id"}); self.engines.append(SetEngine(self,c,state))
    def save(self): self.store.put("states",{e.state.set_id:{k:v for k,v in e.state.__dict__.items() if k!="set_id"} for e in self.engines})
    def order(self,symbol,side,qty,idx,link,reduce=False):
        if not self.allow_orders: return
        out=self.api.call("order",category="linear",symbol=symbol,side=side,orderType="Market",qty=str(qty),positionIdx=idx,reduceOnly=reduce,orderLinkId=link); self.store.event(link.split('-')[1],"ORDER",symbol=symbol,side=side,qty=qty,ret=out.get("retCode"))
    def wallet(self): return self.api.call("wallet",accountType="UNIFIED",coin="USDT")
    def load_instrument_specs(self):
        """Load per-symbol qtyStep/minQty/minNotional before opening orders."""
        symbols=sorted({symbol for c in self.configs for group in c["leaders"]+c["legs"] for symbol in group})
        for symbol in symbols:
            result=self.api.call("instrument",category="linear",symbol=symbol)
            rows=result.get("result",{}).get("list",[])
            if not rows: raise RuntimeError(f"{symbol}: инструмент не найден в Bybit Linear Demo")
            lot=rows[0].get("lotSizeFilter",{})
            self.specs[symbol]={"qtyStep":lot.get("qtyStep","0.001"),"minOrderQty":lot.get("minOrderQty",lot.get("qtyStep","0.001")),"minNotionalValue":lot.get("minNotionalValue","0")}
            log.info("INSTRUMENT %s: qtyStep=%s minQty=%s minNotional=%s",symbol,self.specs[symbol]["qtyStep"],self.specs[symbol]["minOrderQty"],self.specs[symbol]["minNotionalValue"])
    def ensure_leverage(self):
        """Verify the configured leverage and set it before any strategy cycle."""
        target = str(self.cfg["leverage"])
        symbols = sorted({symbol for c in self.configs for group in c["leaders"] + c["legs"] for symbol in group})
        for symbol in symbols:
            response = self.api.call("positions", category="linear", symbol=symbol, settleCoin="USDT", limit=50)
            rows = response.get("result", {}).get("list", [])
            observed = {str(row.get("leverage")) for row in rows if row.get("leverage") not in (None, "")}
            if observed and observed == {target}:
                log.info("LEVERAGE %s: already %sx", symbol, target)
                continue
            if observed:
                log.info("LEVERAGE %s: found %s, target %sx", symbol, ",".join(sorted(observed)), target)
            else:
                log.info("LEVERAGE %s: no open position reported, setting target %sx", symbol, target)
            if not self.allow_orders:
                raise RuntimeError(f"{symbol}: плечо отличается/не подтверждено, но ALLOW_DEMO_ORDERS=NO; запуск остановлен")
            self.api.call("leverage", category="linear", symbol=symbol, buyLeverage=target, sellLeverage=target)
            verify = self.api.call("positions", category="linear", symbol=symbol, settleCoin="USDT", limit=50)
            verify_rows = verify.get("result", {}).get("list", [])
            verify_values = {str(row.get("leverage")) for row in verify_rows if row.get("leverage") not in (None, "")}
            if verify_values and verify_values != {target}:
                raise RuntimeError(f"{symbol}: Bybit не подтвердил целевое плечо {target}x, получено {sorted(verify_values)}")
            log.info("LEVERAGE %s: set to %sx", symbol, target)
    def refresh(self):
        self.risk.update(self.wallet())
        if self.risk.start_equity <= 0 and self.risk.equity > 0:
            self.risk.start_equity=self.risk.equity; self.store.put("global_start_equity",self.risk.start_equity)
        self.store.record_equity(now(),self.risk.equity)
        out=self.api.call("positions",category="linear",settleCoin="USDT",limit=200); self.positions=out.get("result",{}).get("list",[])
    def global_close_metrics(self):
        start=self.risk.start_equity or self.risk.equity or 0.0
        gross=sum(fnum(p.get("size"))*fnum(p.get("markPrice")) for p in self.positions)
        round_trip_rate=2.0*(float(self.cfg["fee_rate"])+float(self.cfg["slippage_rate"]))
        estimated_close_cost=gross*round_trip_rate
        gross_target=start*(1.0+float(self.cfg["global_roi_target"]))
        return {"startEquity":start,"roiTarget":float(self.cfg["global_roi_target"]),"estimatedCloseCosts":estimated_close_cost,"grossTargetBalance":gross_target,"closeBalance":gross_target+estimated_close_cost}
    def bars(self):
        symbols=sorted({x for c in self.configs for g in c["leaders"]+c["legs"] for x in g}); close={}
        for s in symbols:
            out=self.api.call("kline",category="linear",symbol=s,interval=self.cfg["interval"],limit=3); rows=out.get("result",{}).get("list",[]); selected=rows[1] if len(rows)>1 else rows[0]; close[s]=float(selected[4]); self.current_bar_ms=int(selected[0])
        return close
    def live_prices(self):
        """Read current mark prices; never use the still-unchanged 1h kline for live charts."""
        symbols={x for c in self.configs for g in c["leaders"]+c["legs"] for x in g}
        out=self.api.call("ticker",category="linear")
        rows=out.get("result",{}).get("list",[])
        prices={str(row.get("symbol")):float(row.get("markPrice") or row.get("lastPrice")) for row in rows if row.get("symbol") in symbols and (row.get("markPrice") or row.get("lastPrice"))}
        if set(prices) != symbols:
            missing=symbols-set(prices)
            raise RuntimeError(f"ticker did not return live prices for: {sorted(missing)}")
        return prices
    def _kline_series(self, interval, limit, start=None):
        symbols=sorted({x for c in self.configs for g in c["leaders"]+c["legs"] for x in g})
        series={}
        for symbol in symbols:
            out=self.api.call("kline",category="linear",symbol=symbol,interval=interval,limit=limit,start=start)
            rows=out.get("result",{}).get("list",[])
            # Bybit returns newest first; exclude the still-forming newest bar.
            values={int(row[0]):float(row[4]) for row in rows[1:] if len(row)>=5 and float(row[4])>0}
            series[symbol]=values
        common=set.intersection(*(set(v) for v in series.values())) if series else set()
        return [(ts,{symbol:series[symbol][ts] for symbol in symbols}) for ts in sorted(common)]
    def preload_history(self):
        """Prime once, then append only closed 1h candles missing from SQLite."""
        hourly=[]
        history_version=self.store.get("hourly_history_version",0)
        if history_version != 2 or not self.store.history(1):
            hourly=self._kline_series("60",169)
            self.store.clear_bars()
            self.store.put("hourly_history_version",2)
            mode="rebuilt"
        else:
            stored_last=self.store.latest_bar_ms()
            probe=self._kline_series("60",3)
            latest=probe[-1][0] if probe else stored_last
            if latest > stored_last:
                missing=max(1,int((latest-stored_last)//3600000))
                hourly=self._kline_series("60",min(200,missing+2),start=stored_last+3600000)
                hourly=[item for item in hourly if stored_last < item[0] <= latest]
                mode="incremental"
            else:
                mode="current"
        if hourly:
            for engine in self.engines:
                if not engine.state.anchor:
                    # Match the first script: strategy anchor is the latest
                    # closed bar at startup, not the oldest point of the chart.
                    engine.state.anchor=dict(hourly[-1][1])
            for ts,prices in hourly:
                spread_values=[]
                for engine in self.engines:
                    leaders,legs=engine.curves(prices)
                    if leaders is not None: spread_values.append(leaders-legs)
                self.store.bar(datetime.fromtimestamp(ts/1000,timezone.utc).isoformat(),prices,spread_values[-1] if spread_values else 0)
        stored_history=self.store.history(168)
        if stored_history:
            self.last_prices=dict(stored_history[-1]["prices"])
            for engine in self.engines:
                if not engine.state.anchor:
                    engine.state.anchor=dict(stored_history[0]["prices"])
                if (engine.config.get("curve_mode")=="legacy_basket" and
                        self.store.get(f"legacy_anchor_version:{engine.state.set_id}",0) != 2 and
                        not engine.state.bundles):
                    engine.state.anchor=dict(stored_history[-1]["prices"])
                    self.store.put(f"legacy_anchor_version:{engine.state.set_id}",2)
        minute=self._kline_series("1",61)
        for ts,prices in minute:
            stamp=datetime.fromtimestamp(ts/1000,timezone.utc).isoformat()
            for engine in self.engines:
                leaders,legs=engine.curves(prices)
                if leaders is None: continue
                live=self.live[engine.state.set_id]
                live["spread"].append({"t":stamp,"v":leaders-legs})
                live["curves"].append({"t":stamp,"leaders":leaders,"legs":legs})
                live["equity"].append({"t":stamp,"v":self.risk.equity})
        self.save()
        log.info("Hourly chart history %s: added %s closed 1h bars; stored window=%s; live preload=%s 1m points",mode,len(hourly),len(stored_history),len(minute))
    def process_commands(self):
        while True:
            try: action,sid=self.commands.get_nowait()
            except queue.Empty: return
            targets=self.engines if action.endswith("_all") else [e for e in self.engines if e.state.set_id==sid]
            if not targets: continue
            if action.startswith("close"): [e.close("DASHBOARD_CLOSE") for e in targets]
            elif action.startswith("pause"): [setattr(e.state,"paused",True) for e in targets]
            elif action.startswith("resume"): [setattr(e.state,"paused",False) for e in targets]
            self.store.event(sid or "GLOBAL","DASHBOARD_COMMAND",action=action); self.save()
    def run_once(self):
        self.refresh(); self.process_commands()
        metrics=self.global_close_metrics()
        if any(e.state.bundles for e in self.engines) and self.risk.equity >= metrics["closeBalance"]:
            log.info("GLOBAL ROI CLOSE: equity %.4f >= threshold %.4f; estimated close costs %.4f",self.risk.equity,metrics["closeBalance"],metrics["estimatedCloseCosts"])
            for engine in self.engines:
                if engine.state.bundles: engine.close("GLOBAL_ROI")
            self.paused=True; self.store.event("GLOBAL","GLOBAL_ROI_CLOSE",equity=self.risk.equity,threshold=metrics["closeBalance"],estimatedCloseCosts=metrics["estimatedCloseCosts"]); self.save(); return
        hourly_prices=self.bars();
        prices=self.live_prices()
        self.last_prices = prices
        timestamp=now()
        for e in self.engines:
            sp=e.spread(prices); leaders,legs=e.curves(prices)
            live=self.live[e.state.set_id]
            if sp is not None: live["spread"].append({"t":timestamp,"v":sp})
            if leaders is not None: live["curves"].append({"t":timestamp,"leaders":leaders,"legs":legs})
            live["equity"].append({"t":timestamp,"v":self.risk.equity})
            for key in live: live[key]=live[key][-1801:]
        if self.current_bar_ms != self.last_processed_bar_ms:
            bar_time=datetime.fromtimestamp(self.current_bar_ms/1000,timezone.utc).isoformat()
            for e in self.engines:
                sp=e.spread(hourly_prices); self.store.bar(bar_time,hourly_prices,sp or 0); e.on_bar(hourly_prices,bar_time)
            self.last_processed_bar_ms=self.current_bar_ms
    def public_status(self):
        out=[]
        for e in self.engines:
            pnl=e.set_pnl(self.positions); roi=e.set_roi(self.positions); sp=e.spread(self.last_prices) if hasattr(self,"last_prices") else None
            curves=[]
            for row in self.store.history(168):
                try:
                    leaders,legs=e.curves(row["prices"])
                    if leaders is not None: curves.append({"time":row["time"],"leaders":leaders,"legs":legs,"spread":leaders-legs})
                except (KeyError,ZeroDivisionError,TypeError): pass
            out.append({"id":e.state.set_id,"name":e.config["name"],"leaders":", ".join("+".join(g) for g in e.config["leaders"]),"legs":", ".join("+".join(g) for g in e.config["legs"]),"paused":e.state.paused,"halted":e.state.halted,"bundles":e.state.bundles,"pnl":pnl,"roi":roi,"spread":sp,"hedged":False,"direction":e.state.direction,"directionLabel":"leaders short / legs long" if e.state.direction==1 else "leaders long / legs short" if e.state.direction==-1 else "not set","refSpread":e.state.ref_spread,"cycleStartEquity":e.state.cycle_start_equity,"lastCloseReason":e.state.last_close_reason,"step":self.cfg["add_step"],"roiTarget":e.config.get("roi_target",self.cfg["set_roi_target"]),"market":{"markPrices":getattr(self,"last_prices",{}),"anchorPrices":e.state.anchor},"positions":[{"symbol":p.get("symbol"),"side":p.get("side"),"positionIdx":int(p.get("positionIdx",0)),"size":fnum(p.get("size")),"avgPrice":fnum(p.get("avgPrice",0)),"markPrice":fnum(p.get("markPrice",0)),"pnl":fnum(p.get("unrealisedPnl",p.get("unrealizedPnl",0)))} for p in self.positions if p.get("symbol") in e.symbols],"curves":curves,"live":{key:dashboard_sample(values,600) for key,values in self.live[e.state.set_id].items()},"events":self.store.events(e.state.set_id)})
        total=sum(x["pnl"] for x in out); reserve=self.risk.reserves(self.engines); metrics=self.global_close_metrics(); return {"serverTime":now(),"wallet":self.risk.wallet,"equity":self.risk.equity,"available":self.risk.available,"totalPnl":total,"totalRoi":total/(metrics["startEquity"] or 1),"hedgeReserve":reserve,"globalStartEquity":metrics["startEquity"],"globalRoiTarget":metrics["roiTarget"],"estimatedCloseCosts":metrics["estimatedCloseCosts"],"grossTargetBalance":metrics["grossTargetBalance"],"globalCloseBalance":metrics["closeBalance"],"paused":self.paused,"halted":self.halted,"sets":out}
    def serve(self,port):
        server=self.dashboard.server("127.0.0.1",port); threading.Thread(target=server.serve_forever,daemon=True).start(); log.info("Dashboard: http://127.0.0.1:%s",port)
    def loop(self):
        while True:
            try:
                self.process_commands(); self.run_once()
                if self.halted:
                    self.halted=False; log.info("Successful cycle recovered; cleared dashboard error status")
                time.sleep(float(self.cfg["loop_seconds"]))
            except KeyboardInterrupt: return
            except Exception: log.exception("cycle failed"); self.halted=True; time.sleep(5)


def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--dashboard-only",action="store_true"); ap.add_argument("--sets",type=int,default=ENABLED_SET_COUNT); ap.add_argument("--port",type=int,default=LOCAL_DASHBOARD_PORT); args=ap.parse_args()
    sets=validate_config(PORTFOLIO_SETS,args.sets); log.info("Validated %s independent sets; symbols do not overlap",len(sets))
    store=Store(DB_PATH)
    if args.dashboard_only:
        bot=MultiSetBot(None,sets,store,False); bot.serve(args.port); print(f"Dashboard: http://127.0.0.1:{args.port}");
        while True: time.sleep(3600)
    key=DEMO_CONFIG.get("api_key"); secret=DEMO_CONFIG.get("api_secret");
    if not key or key.startswith("REPLACE") or not secret or secret.startswith("REPLACE"): raise SystemExit("Заполните Demo HMAC-ключи в api_config.py")
    bot=MultiSetBot(BybitDemoHTTP(key,secret,DEMO_CONFIG.get("timeout",20)),sets,store,os.environ.get("ALLOW_DEMO_ORDERS","YES").upper()=="YES")
    bot.load_instrument_specs()
    bot.ensure_leverage()
    bot.refresh()
    bot.preload_history()
    bot.serve(args.port); print(f"Dashboard: http://127.0.0.1:{args.port}"); bot.loop()
if __name__=="__main__": main()

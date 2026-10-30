#!/usr/bin/env python3
"""Локальный Bybit Demo-бот с встроенным read-only дашбордом.

Работает ТОЛЬКО с https://api-demo.bybit.com. Дашборд не имеет команд
для ордеров, изменения параметров или закрытия позиций и по умолчанию
доступен лишь с этого компьютера: http://127.0.0.1:8787.

Переменные окружения:
  api_config.py                                — отдельная пара Demo API key.
  ALLOW_DEMO_ORDERS=YES                        — явно включает Demo-ордера.
  ALLOW_DEMO_ORDERS=NO                         — отключает Demo-ордера.
  LOCAL_DASHBOARD_PORT=8787                    — порт локальной панели.

Никакой ключ, secret, подпись запроса, orderId или удалённый URL телеметрии
не попадает в HTTP-ответы панели или SQLite-снимки для неё.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import math
import os
import queue
import sqlite3
import sys
import threading
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
try:
    from api_config import DEMO_CONFIG
except ImportError as exc:
    raise SystemExit("Не найден api_config.py. Заполните DEMO_CONFIG в api_config.py по шаблону.") from exc

# --- Selected, backtest-derived parameters ---
SYMBOLS = ("BTCUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT")
BASE_ASSETS = ("BTC", "BNB", "XRP", "ADA")
INTERVAL = "60"
BAR_MS = 60 * 60 * 1000
P2_SCALE = 111.275879
START_BALANCE_USDT = Decimal("500")
LEVERAGE = Decimal("20")
INITIAL_STEP_USDT = Decimal("10")
INITIAL_ROI = Decimal("0.03")
GLOBAL_ROI_TARGET = Decimal("0.03")
BTC_BUDGET_PCT = Decimal("0.33")
ALT_P_BUDGET_PCT = Decimal("0.11")
FEE_RATE_EST = Decimal("0.00055")
SLIPPAGE_EST = Decimal("0.00020")
MMR_EST = Decimal("0.005")
HEDGE_OPEN_BUFFER_USDT = Decimal("25")
MIN_FREE_MARGIN_USDT = Decimal("10")
HEDGE_RETRACE_RATIO = Decimal("0.10")
WALLET_POLL_SECONDS = 30
LOOP_SLEEP_SECONDS = 2
MONITOR_POLL_SECONDS = 2
ORDER_FILL_TIMEOUT_SECONDS = 20
CATEGORY = "linear"
BOT_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("BYBIT_BOT_DB", str(BOT_DIR / "runtime/bybit_demo_bot.sqlite3")))
LOG_PATH = Path(os.environ.get("BYBIT_BOT_LOG", str(BOT_DIR / "runtime/bybit_demo_bot.log")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# Public module-level live values. Access from another module as:
#   import bybit_demo_bot; print(bybit_demo_bot.LIVE_WALLET)
LIVE_WALLET: Decimal = Decimal("0")       # Unified totalWalletBalance (USD)
LIVE_EQUITY: Decimal = Decimal("0")       # Unified totalEquity (USD)
LIVE_AVAILABLE: Decimal = Decimal("0")    # Unified totalAvailableBalance (USD)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH, encoding="utf-8")],
)
log = logging.getLogger("bybit_demo_bot")


class TransientAPIError(RuntimeError):
    """A read-only exchange/network failure that may recover on retry."""


class BybitDemoHTTP:
    """Minimal stdlib-only Bybit V5 client pinned to the Demo endpoint.

    Only methods used by DemoBot are exposed. It intentionally contains no
    mainnet/testnet host option: all private requests are signed for Demo.
    """
    BASE_URL = "https://api-demo.bybit.com"
    PATHS = {
        "get_wallet_balance": ("GET", "/v5/account/wallet-balance"),
        "get_account_info": ("GET", "/v5/account/info"),
        "get_positions": ("GET", "/v5/position/list"),
        "get_open_orders": ("GET", "/v5/order/realtime"),
        "get_instruments_info": ("GET", "/v5/market/instruments-info"),
        "get_kline": ("GET", "/v5/market/kline"),
        "get_tickers": ("GET", "/v5/market/tickers"),
        "get_order_history": ("GET", "/v5/order/history"),
        "set_leverage": ("POST", "/v5/position/set-leverage"),
        "place_order": ("POST", "/v5/order/create"),
    }

    def __init__(self, api_key: str, api_secret: str, timeout: int = 20):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.timeout = timeout
        self.recv_window = "5000"

    def __getattr__(self, name: str):
        if name not in self.PATHS:
            raise AttributeError(name)
        method, path = self.PATHS[name]
        def call(**kwargs):
            return self._request(method, path, kwargs)
        return call

    def _request(self, method: str, path: str, params: dict) -> dict:
        cleaned = {key: value for key, value in params.items() if value is not None}
        timestamp = str(int(time.time() * 1000))
        if method == "GET":
            query = urlencode([(key, str(value)) for key, value in cleaned.items()])
            payload = query
            url = self.BASE_URL + path + ("?" + query if query else "")
            data = None
        else:
            payload = json.dumps(cleaned, separators=(",", ":"), ensure_ascii=False)
            url = self.BASE_URL + path
            data = payload.encode("utf-8")
        sign_source = timestamp + self.api_key + self.recv_window + payload
        signature = hmac.new(self.api_secret, sign_source.encode("utf-8"), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": self.recv_window,
            "Accept": "application/json",
            "User-Agent": "BybitDemoBotLocal/1.0",
        }
        if method == "POST":
            headers["Content-Type"] = "application/json"
        request = Request(url, data=data, method=method, headers=headers)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            # A valid Bybit error envelope is returned to the caller so its
            # existing fail-closed handling retains retCode/retMsg context.
            body = exc.read().decode("utf-8", "replace")
            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                raise RuntimeError(f"Bybit HTTP {exc.code}: {body[:300]}") from exc
            if isinstance(parsed, dict):
                return parsed
            raise RuntimeError(f"Bybit HTTP {exc.code}: non-object response") from exc


def _local_host(value: str) -> str:
    host = value.strip().lower()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Локальная панель намеренно разрешает только 127.0.0.1, localhost или ::1")
    return host


def _local_port(value: str | int) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("LOCAL_DASHBOARD_PORT должен быть целым числом") from exc
    if not 1024 <= port <= 65535:
        raise ValueError("LOCAL_DASHBOARD_PORT должен быть в диапазоне 1024..65535")
    return port


DASHBOARD_HTML = r"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Bybit Demo — Local Watch</title>
<style>
:root{--bg:#091016;--panel:#101b25;--edge:#223544;--text:#dce8ee;--muted:#8ba0ad;--cyan:#43d9c6;--yellow:#ffca5a;--red:#ff7171;--green:#7ee19b}*{box-sizing:border-box}body{margin:0;background:radial-gradient(900px 500px at 5% -10%,#173448 0%,transparent 65%),var(--bg);color:var(--text);font:14px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}header{padding:22px max(18px,calc((100vw - 1450px)/2));border-bottom:1px solid var(--edge);display:flex;gap:18px;justify-content:space-between;align-items:center}h1{font-size:19px;margin:0;letter-spacing:.05em}.sub{color:var(--muted);font-size:12px;margin-top:4px}.badge{border:1px solid var(--cyan);color:var(--cyan);padding:6px 9px;font-size:12px;border-radius:4px}.bad{border-color:var(--red);color:var(--red)}main{max-width:1450px;margin:0 auto;padding:18px}.notice{margin-bottom:16px;color:var(--yellow);font-size:12px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}.panel{background:linear-gradient(180deg,rgba(19,33,45,.96),rgba(13,24,33,.96));border:1px solid var(--edge);border-radius:7px;padding:14px;min-width:0}.k{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em}.v{font-size:22px;margin-top:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.small{font-size:12px;color:var(--muted);margin-top:5px}.two{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.three{display:grid;grid-template-columns:1fr 1fr 0.85fr;gap:12px;margin-top:12px}.three .chart{height:190px}.wide{grid-column:span 2}h2{font-size:13px;text-transform:uppercase;letter-spacing:.08em;margin:0 0 12px;color:#b7cad4}table{border-collapse:collapse;width:100%;font-size:12px}th{text-align:left;color:var(--muted);font-weight:normal;border-bottom:1px solid var(--edge);padding:0 7px 8px 0}td{padding:8px 7px 8px 0;border-bottom:1px solid rgba(34,53,68,.65);white-space:nowrap}.pos{color:var(--green)}.neg{color:var(--red)}.null{color:var(--muted)}.chart{height:190px;width:100%;display:block;background:#0b151d;border:1px solid #1b2a36;border-radius:4px}.line{stroke:var(--cyan);stroke-width:2;fill:none}.line2{stroke:var(--yellow);stroke-width:2;fill:none}.axis{stroke:#29404f;stroke-width:1}.chartlabel{fill:#8198a7;font:10px ui-monospace,monospace}.foot{margin:16px 0 4px;color:var(--muted);font-size:11px}.empty{color:var(--muted);padding:12px 0}.controls{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-bottom:12px}.controls button{background:#173445;border:1px solid #3b6b7d;color:var(--text);border-radius:4px;padding:9px 12px;font:12px ui-monospace,monospace;cursor:pointer}.controls button:hover{border-color:var(--cyan)}.controls button:disabled{opacity:.45;cursor:not-allowed}.controls .danger{background:#51232a;border-color:var(--red)}.liveCurve{margin-top:12px}.longCurve{margin-top:12px}.meanLine{stroke-dasharray:7 5;stroke-width:1.5;opacity:.85}.spreadMeanLine{stroke:#e78cff;stroke-dasharray:7 5;stroke-width:1.7}.spreadMeanLegend{color:#e78cff}.btcLegend{color:var(--cyan)}.altLegend{color:var(--yellow)}@media(max-width:1100px){.three{grid-template-columns:1fr 1fr}.three .liveCurve{grid-column:span 2}}@media(max-width:930px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.two,.three{grid-template-columns:1fr}.three .liveCurve{grid-column:span 1}.wide{grid-column:span 1}}@media(max-width:530px){header{align-items:flex-start;flex-direction:column}.grid{grid-template-columns:1fr}.v{font-size:20px}main{padding:12px}}
</style></head><body>
<header><div><h1>BYBIT DEMO / LOCAL WATCH</h1><div class="sub">локальная read-only панель · решения стратегии принимаются на закрытии 1h</div></div><div id="badge" class="badge">Загрузка…</div></header>
<main><div class="notice">Команды ставятся в очередь локального торгового потока. Закрытие выполняется reduce-only по BTC/BNB/XRP/ADA, включая хеджи, затем автооткрытие ставится на паузу.</div><section class="panel controls"><h2>Управление торговлей</h2><button class="danger" onclick="command('close_all','Закрыть все позиции и поставить автооткрытие на паузу?')">ЗАКРЫТЬ ВСЕ ПОЗИЦИИ</button><button onclick="command('pause_entries','Остановить новые входы и доборы? Защитные хеджи и штатные закрытия останутся разрешены.')">ОСТАНОВИТЬ АВТООТКРЫТИЕ</button><button id="resumeBtn" onclick="command('resume_entries','Возобновить новые входы и доборы?')">ВОЗОБНОВИТЬ АВТООТКРЫТИЕ</button><span id="commandStatus" class="small"></span></section>
<div id="cards" class="grid"></div>
<div class="two"><section class="panel"><h2>Стратегия / цикл</h2><div id="strategy" class="grid"></div></section><section class="panel"><h2>Рынок / уровни</h2><div id="market" class="grid"></div></section></div>
<div class="three"><section class="panel"><h2>Live: spread — последний час</h2><svg id="spreadChart" class="chart" viewBox="0 0 700 190" preserveAspectRatio="none"></svg></section><section class="panel"><h2>Кривая баланса — 160 сэмплов</h2><svg id="walletChart" class="chart" viewBox="0 0 700 190" preserveAspectRatio="none"></svg></section><section class="panel liveCurve"><h2>Live: нормализованные ноги — 1 час</h2><div class="small"><span class="btcLegend">■ BTC leg</span> <span class="altLegend">■ ALT basket leg</span><br>mark каждые 2 с · последние 1 800 точек</div><svg id="legsChart" class="chart" viewBox="0 0 700 190" preserveAspectRatio="none"></svg></section></div>
<section class="panel longCurve"><h2>Spread — 7 дней / закрытые 1h бары</h2><div class="small">■ spread · <span class="spreadMeanLegend">пунктир — среднее spread за 7 дней</span> · обновляется после нового закрытого часового бара</div><svg id="hourlySpreadChart" class="chart" viewBox="0 0 1400 240" preserveAspectRatio="none"></svg></section><section class="panel longCurve"><h2>Нормализованные ноги — 7 дней / закрытые 1h бары</h2><div class="small"><span class="btcLegend">■ BTC leg</span> <span class="altLegend">■ ALT basket leg</span> · пунктиром показано среднее значение каждой кривой · обновление после нового закрытого часового бара</div><svg id="hourlyLegsChart" class="chart" viewBox="0 0 1400 240" preserveAspectRatio="none"></svg></section>
<div class="two"><section class="panel wide"><h2>Открытые позиции</h2><div id="positions"></div></section><section class="panel"><h2>Последние события стратегии</h2><div id="events"></div></section></div>
<div class="two"><section class="panel"><h2>Mark prices и funding</h2><div id="marks"></div></section><section class="panel"><h2>Метод и защита</h2><div id="method" class="small"></div></section></div>
<div id="foot" class="foot"></div></main>
<script>
const $=s=>document.querySelector(s), fmt=(v,n=2)=>v===null||v===undefined||Number.isNaN(Number(v))?'—':Number(v).toLocaleString('ru-RU',{minimumFractionDigits:n,maximumFractionDigits:n}), esc=s=>String(s??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const signed=(v,n=2)=>{if(v===null||v===undefined)return'—';let x=Number(v);return (x>0?'+':'')+fmt(x,n)}; const card=(k,v,sub='')=>`<div class="panel"><div class="k">${esc(k)}</div><div class="v">${v}</div>${sub?`<div class="small">${sub}</div>`:''}</div>`;
function chart(id,vals,color,label){let svg=$(id),w=700,h=190,p=24,a=vals.map(Number).filter(Number.isFinite);if(!a.length){svg.innerHTML='<text x="350" y="96" text-anchor="middle" class="chartlabel">Нет сохранённых данных</text>';return}let lo=Math.min(...a),hi=Math.max(...a);if(hi===lo){lo-=1;hi+=1}let d=vals.map((v,i)=>`${i?'L':'M'} ${p+(w-2*p)*i/Math.max(1,vals.length-1)} ${h-p-(Number(v)-lo)/(hi-lo)*(h-2*p)}`).join(' ');svg.innerHTML=`<line x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}" class="axis"/><line x1="${p}" y1="${p}" x2="${p}" y2="${h-p}" class="axis"/><path d="${d}" class="${color}"/><text x="${p+4}" y="${p+12}" class="chartlabel">${esc(label)} max ${fmt(hi,4)}</text><text x="${p+4}" y="${h-6}" class="chartlabel">min ${fmt(lo,4)}</text>`}
async function command(action, question){if(!confirm(question))return;let out=$('#commandStatus');out.textContent='Команда отправляется…';try{let r=await fetch('/api/command',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action})});let j=await r.json();out.textContent=j.message||'Команда принята';setTimeout(load,300)}catch(e){out.textContent='Ошибка локального сервера'}}
function dualChart(id,rows){let svg=$(id),w=700,h=190,p=24,a=rows.filter(x=>Number.isFinite(Number(x.btc))&&Number.isFinite(Number(x.alt)));if(!a.length){svg.innerHTML='<text x="350" y="96" text-anchor="middle" class="chartlabel">Ожидание закрытых 1h баров</text>';return}let vals=a.flatMap(x=>[Number(x.btc),Number(x.alt)]),lo=Math.min(...vals),hi=Math.max(...vals);if(hi===lo){lo-=.001;hi+=.001}let path=k=>a.map((x,i)=>`${i?'L':'M'} ${p+(w-2*p)*i/Math.max(1,a.length-1)} ${h-p-(Number(x[k])-lo)/(hi-lo)*(h-2*p)}`).join(' ');svg.innerHTML=`<line x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}" class="axis"/><line x1="${p}" y1="${p}" x2="${p}" y2="${h-p}" class="axis"/><path d="${path('btc')}" class="line"/><path d="${path('alt')}" class="line2"/><text x="${p+4}" y="${p+12}" class="chartlabel">max ${fmt(hi,5)}</text><text x="${p+4}" y="${h-6}" class="chartlabel">min ${fmt(lo,5)}</text>`}
function hourlyChart(id,rows){let svg=$(id),w=1400,h=240,p=30,a=rows.filter(x=>Number.isFinite(Number(x.btc))&&Number.isFinite(Number(x.alt)));if(!a.length){svg.innerHTML='<text x="700" y="120" text-anchor="middle" class="chartlabel">Ожидание закрытых часовых баров</text>';return}let vals=a.flatMap(x=>[Number(x.btc),Number(x.alt)]),lo=Math.min(...vals),hi=Math.max(...vals);if(hi===lo){lo-=.001;hi+=.001}let mean=k=>a.reduce((z,x)=>z+Number(x[k]),0)/a.length;let y=v=>h-p-(v-lo)/(hi-lo)*(h-2*p),path=k=>a.map((x,i)=>`${i?'L':'M'} ${p+(w-2*p)*i/Math.max(1,a.length-1)} ${y(Number(x[k]))}`).join(' ');svg.innerHTML=`<line x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}" class="axis"/><path d="${path('btc')}" class="line"/><path d="${path('alt')}" class="line2"/><line x1="${p}" y1="${y(mean('btc'))}" x2="${w-p}" y2="${y(mean('btc'))}" class="line meanLine"/><line x1="${p}" y1="${y(mean('alt'))}" x2="${w-p}" y2="${y(mean('alt'))}" class="line2 meanLine"/><text x="${p+4}" y="${p+12}" class="chartlabel">BTC avg ${fmt(mean('btc'),5)} · ALT avg ${fmt(mean('alt'),5)} · ${a.length} bars</text>`}
function hourlySpreadChart(id,rows){let svg=$(id),w=1400,h=240,p=30,a=rows.map(Number).filter(Number.isFinite);if(!a.length){svg.innerHTML='<text x="700" y="120" text-anchor="middle" class="chartlabel">Ожидание закрытых часовых баров</text>';return}let lo=Math.min(...a),hi=Math.max(...a);if(hi===lo){lo-=.001;hi+=.001}let mean=a.reduce((x,y)=>x+y,0)/a.length,y=v=>h-p-(v-lo)/(hi-lo)*(h-2*p),d=a.map((v,i)=>`${i?'L':'M'} ${p+(w-2*p)*i/Math.max(1,a.length-1)} ${y(v)}`).join(' ');svg.innerHTML=`<line x1="${p}" y1="${h-p}" x2="${w-p}" y2="${h-p}" class="axis"/><path d="${d}" class="line"/><line x1="${p}" y1="${y(mean)}" x2="${w-p}" y2="${y(mean)}" class="spreadMeanLine"/><text x="${p+4}" y="${p+12}" class="chartlabel">avg ${fmt(mean,5)} · ${a.length} bars</text>`}
function render(st,hist){let p=st.payload, badge=$('#badge'); if(!p){badge.className='badge bad';badge.textContent=st.botStatus==='dashboard-only'?'Только история':'Ожидание бота';$('#cards').innerHTML=card('Статус',esc(st.message||'Нет текущего снимка'), 'Запустите бота с Demo API key или откройте сохранённую SQLite-историю.'); return}let a=p.account||{},s=p.strategy||{},m=p.market||{};badge.className='badge '+(s.halted?'bad':'');badge.textContent=s.halted?'Остановлен / требуется проверка':(s.autoEntryPaused?'Автооткрытие на паузе':(st.botStatus==='running'?'Demo: работает':'Demo: '+esc(st.botStatus)));$('#resumeBtn').disabled=!!s.halted||!s.autoEntryPaused;$('#commandStatus').textContent=st.lastCommand?`${st.lastCommand.action}: ${st.lastCommand.status}${st.lastCommand.message?' — '+st.lastCommand.message:''}`:'';
$('#cards').innerHTML=[card('Wallet',fmt(a.wallet)+' USDT','account uPnL '+signed(a.accountUnrealizedPnl)),card('Equity',fmt(a.equity)+' USDT','available '+fmt(a.available)),card('Strategy uPnL',signed(s.unrealizedPnl)+' USDT','cycle net '+signed(s.cycleNetPnl)),card('Направление',esc(s.directionLabel),`${s.bundles||0} корзин · цикл ${s.cycleCount||0}`),card('Общее закрытие по ROI',fmt((p.globalRoi||{}).closeBalance)+' USDT',`до порога ${fmt((p.globalRoi||{}).remainingToClose)} USDT`),card('Расходы закрытия',fmt((p.globalRoi||{}).estimatedCloseCosts)+' USDT',`gross-порог ${fmt((p.globalRoi||{}).grossTargetBalance)}`)].join('');
$('#strategy').innerHTML=[card('STEP',fmt(s.step,4),s.nextAddLevel===null?'следующий добор: —':`уровень ${fmt(s.nextAddLevel,4)} (${s.nextAddDirection||''})`),card('ROI',fmt((s.roi||0)*100,3)+'%',s.roiTarget===null?'цель: —':`цель ${fmt(s.roiTarget)} · остаток ${fmt(s.roiRemaining)}`),card('Хедж',s.hedgeActive?'АКТИВЕН':'нет',s.hedgeActive?`выход: ${fmt(s.hedgeExitLevel,4)}`:`guard: ${fmt(s.hedgeGuardLevel)} USDT`),card('Цикл',s.lastCloseReason?esc(s.lastCloseReason):'открыт / нет','B '+fmt(s.btcBudget)+' · P '+fmt(s.altBudgetEach))].join('');
$('#market').innerHTML=[card('Mark spread',fmt(m.markSpread,5),'закрытый 1h '+fmt(m.closeSpread,5)),card('Reference',fmt(m.referenceSpread,5),'P2 '+fmt(m.p2Scale,6)),card('Последний бар',s.lastBarAt?new Date(s.lastBarAt).toLocaleString('ru-RU'):'—','UTC сохраняется в журнале'),card('Маржа',fmt(s.projectedFreeAfterHedge)+' USDT','floor '+fmt(s.marginFloor)+' · reserve '+fmt(s.hedgeReserve))].join('');
let pos=p.positions||[];$('#positions').innerHTML=pos.length?'<table><tr><th>Инструмент</th><th>Сторона</th><th>Размер</th><th>Средняя</th><th>Mark</th><th>uPnL</th><th>Плечо</th><th>Ликвидация</th></tr>'+pos.map(x=>`<tr><td>${esc(x.symbol)}</td><td>${esc(x.side)}</td><td>${fmt(x.size,6)}</td><td>${fmt(x.avgPrice,5)}</td><td>${fmt(x.markPrice,5)}</td><td class="${Number(x.unrealizedPnl)>=0?'pos':'neg'}">${signed(x.unrealizedPnl,4)}</td><td>${esc(x.leverage)}</td><td>${esc(x.liquidationPrice)}</td></tr>`).join('')+'</table>':'<div class="empty">Открытые позиции по инструментам стратегии не обнаружены.</div>';
let ev=p.events||[];$('#events').innerHTML=ev.length?'<table><tr><th>Время UTC</th><th>Событие</th><th>Spread</th></tr>'+ev.slice().reverse().map(x=>`<tr><td>${esc((x.occurredAt||'').replace('T',' ').slice(0,19))}</td><td>${esc(x.type)}</td><td>${fmt(x.spread,5)}</td></tr>`).join('')+'</table>':'<div class="empty">События появятся после первого сигнала или изменения цикла.</div>';
let mp=m.markPrices||{},fr=m.fundingRates||{};$('#marks').innerHTML='<table><tr><th>Актив</th><th>Mark</th><th>Funding</th></tr>'+['BTCUSDT','BNBUSDT','XRPUSDT','ADAUSDT'].map(x=>`<tr><td>${x}</td><td>${fmt(mp[x],6)}</td><td>${signed(fr[x]*100,5)}%</td></tr>`).join('')+'</table>';
$('#method').innerHTML=`<b>Профиль:</b> BTC vs BNB/XRP/ADA, 1h close-only, 20×, стартовые B=3% и P=1%.<br><br><b>Ограничение:</b> mark-spread показан справочно; входы, доборы и выходы принимаются только на закрытом часовом баре. Хедж и margin guard остаются в торговом движке, а не в панели.<br><br><b>Режим:</b> ${s.running?'процесс активен':'снимок истории'}; панель не отправляет команды на Bybit.`;
chart('#spreadChart',(st.liveSpread||[]).map(x=>x.value),'line','live spread');chart('#walletChart',(hist.wallet||[]).slice(-160).map(x=>x.equity),'line2','equity');dualChart('#legsChart',st.liveCurves||[]);hourlySpreadChart('#hourlySpreadChart',(hist.bars||[]).slice(-168).map(x=>x.spread));hourlyChart('#hourlyLegsChart',(hist.curves||[]).slice(-168));$('#foot').textContent=`Снимок: ${p.observedAt?new Date(p.observedAt).toLocaleString('ru-RU'):'—'} · сервер: ${new Date(st.serverTime).toLocaleString('ru-RU')} · обновление каждые 2 с.`;
}
let lastHist={bars:[],wallet:[]};async function load(){try{let [a,b]=await Promise.all([fetch('/api/status',{cache:'no-store'}),fetch('/api/history?limit=200',{cache:'no-store'})]);let st=await a.json();lastHist=await b.json();render(st,lastHist)}catch(e){$('#badge').className='badge bad';$('#badge').textContent='Нет соединения с локальным сервером'}}load();setInterval(load,2000);
</script></body></html>"""

class LocalDashboard:
    """Localhost dashboard; commands are queued and executed by DemoBot only."""
    SAFE_EVENTS = ("OPEN", "ADD", "HEDGE_OPEN", "HEDGE_CLOSE", "HEDGE_REARMED", "MARGIN_TRIM", "CYCLE_CLOSE", "EMERGENCY_FLATTEN", "DASHBOARD_COMMAND")

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._payload: dict | None = None
        self._bot_status = "starting"
        self._message = "Ожидание первого локального снимка."
        self._commands: queue.Queue[str] = queue.Queue(maxsize=8)
        self._last_command: dict | None = None
        self._live_curves: list[dict] = []
        self._live_spread: list[dict] = []

    def request_command(self, action: str) -> tuple[bool, str]:
        allowed = {"close_all", "pause_entries", "resume_entries"}
        if action not in allowed:
            return False, "Неизвестная команда"
        try:
            self._commands.put_nowait(action)
        except queue.Full:
            return False, "Очередь команд занята; дождитесь обработки предыдущей команды"
        with self._lock:
            self._last_command = {"action": action, "status": "queued", "requestedAt": utc_now()}
        return True, "Команда поставлена в очередь локального торгового потока"

    def take_commands(self) -> list[str]:
        commands = []
        while True:
            try:
                commands.append(self._commands.get_nowait())
            except queue.Empty:
                return commands

    def finish_command(self, action: str, status: str, message: str):
        with self._lock:
            self._last_command = {"action": action, "status": status, "message": message, "finishedAt": utc_now()}

    def publish(self, payload: dict, status: str = "running", message: str | None = None):
        # JSON round-trip rejects accidental Decimals/non-finite values and gives
        # dashboard handlers an immutable primitive-only copy.
        safe = json.loads(json.dumps(payload, allow_nan=False))
        with self._lock:
            self._payload = safe
            self._bot_status = status
            self._message = message or "Локальный Demo-бот передал свежий снимок."
            market = safe.get("market", {})
            btc = market.get("normalizedBtc")
            alt = market.get("normalizedAltSum")
            if btc is not None and alt is not None:
                self._live_curves.append({"time": safe.get("observedAt", utc_now()), "btc": btc, "alt": alt})
                # MONITOR_POLL_SECONDS=2: retain about one hour of live samples.
                self._live_curves = self._live_curves[-1801:]
            spread = market.get("markSpread")
            if spread is not None:
                self._live_spread.append({"time": safe.get("observedAt", utc_now()), "value": spread})
                self._live_spread = self._live_spread[-1801:]

    def set_status(self, status: str, message: str):
        with self._lock:
            self._bot_status = status
            self._message = message[:500]

    def status(self) -> dict:
        with self._lock:
            return {
                "serverTime": utc_now(),
                "botStatus": self._bot_status,
                "message": self._message,
                "payload": self._payload,
                "lastCommand": self._last_command,
                "liveCurves": list(self._live_curves),
                "liveSpread": list(self._live_spread),
            }

    @staticmethod
    def _parse_object(value: str, fallback):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return fallback
        return parsed if isinstance(parsed, type(fallback)) else fallback

    def history(self, limit: int = 160) -> dict:
        limit = max(20, min(int(limit), 500))
        result: dict[str, list] = {"bars": [], "wallet": [], "events": [], "curves": []}
        if not self.db_path.exists():
            return result
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=2)
            conn.row_factory = sqlite3.Row
            bars = conn.execute("SELECT close_time_utc,prices_json,spread FROM bars ORDER BY close_time_utc DESC LIMIT ?", (limit,)).fetchall()
            state_row = conn.execute("SELECT value FROM kv_state WHERE key='strategy_state'").fetchone()
            strategy_state = self._parse_object(state_row[0] if state_row else "{}", {})
            anchors = strategy_state.get("anchor_prices") or {}
            wallet = conn.execute("SELECT timestamp_utc,wallet_usdt,equity_usdt,available_usdt FROM wallet_samples ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            placeholders = ",".join("?" for _ in self.SAFE_EVENTS)
            events = conn.execute(f"SELECT id,timestamp_utc,event,payload_json FROM events WHERE event IN ({placeholders}) ORDER BY id DESC LIMIT ?", (*self.SAFE_EVENTS, min(limit, 80))).fetchall()
            clean_bars = []
            curves = []
            # Keep the trading anchor untouched, but use the same chart anchor
            # as multi-set: the latest closed 1h candle in the displayed window.
            chart_anchor = {}
            if bars:
                try:
                    candidate = self._parse_object(bars[0]["prices_json"], {})
                    if all(float(candidate[s]) > 0 for s in SYMBOLS):
                        chart_anchor = {s: float(candidate[s]) for s in SYMBOLS}
                except (KeyError, TypeError, ValueError):
                    chart_anchor = {}
            if not chart_anchor:
                chart_anchor = anchors
            for row in reversed(bars):
                try:
                    prices = self._parse_object(row["prices_json"], {})
                    btc = float(prices["BTCUSDT"]) / float(chart_anchor["BTCUSDT"])
                    alt_anchor = sum(float(chart_anchor[s]) for s in SYMBOLS[1:])
                    alt = sum(float(prices[s]) for s in SYMBOLS[1:]) / alt_anchor
                    spread = (btc - alt) * P2_SCALE
                    if all(math.isfinite(x) for x in (spread, btc, alt)):
                        clean_bars.append({"time": row["close_time_utc"], "spread": spread})
                        curves.append({"time": row["close_time_utc"], "btc": btc, "alt": alt})
                except (KeyError, TypeError, ValueError, ZeroDivisionError):
                    if math.isfinite(float(row["spread"])):
                        clean_bars.append({"time": row["close_time_utc"], "spread": float(row["spread"])})
            result["bars"] = clean_bars
            result["curves"] = curves
            clean_wallet = []
            for row in reversed(wallet):
                try:
                    item = {"time": row["timestamp_utc"], "wallet": float(row["wallet_usdt"]), "equity": float(row["equity_usdt"]), "available": float(row["available_usdt"])}
                except (TypeError, ValueError):
                    continue
                if all(math.isfinite(x) for x in (item["wallet"], item["equity"], item["available"])):
                    clean_wallet.append(item)
            result["wallet"] = clean_wallet
            clean_events = []
            for row in reversed(events):
                raw = self._parse_object(row["payload_json"], {})
                spread = raw.get("spread")
                try:
                    spread = float(spread)
                    if not math.isfinite(spread):
                        spread = None
                except (TypeError, ValueError):
                    spread = None
                clean_events.append({"occurredAt": row["timestamp_utc"], "type": row["event"], "spread": spread})
            result["events"] = clean_events
            conn.close()
        except (sqlite3.Error, OSError, ValueError):
            # The process must continue serving its current in-memory status if
            # a concurrent SQLite checkpoint is temporarily unavailable.
            return result
        return result

    def handler_class(self):
        dashboard = self
        class Handler(BaseHTTPRequestHandler):
            server_version = "BybitDemoLocal/1.0"
            def log_message(self, fmt, *args):
                log.info("LOCAL_DASHBOARD %s - %s", self.address_string(), fmt % args)
            def _send(self, status: int, body: bytes, content_type: str):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store, max-age=0")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.end_headers()
                self.wfile.write(body)
            def _json(self, value):
                self._send(200, json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8"), "application/json; charset=utf-8")
            def do_GET(self):
                path = urlsplit(self.path).path
                if path == "/":
                    self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
                elif path == "/api/status":
                    self._json(dashboard.status())
                elif path == "/api/history":
                    query = dict(item.split("=", 1) if "=" in item else (item, "") for item in urlsplit(self.path).query.split("&") if item)
                    self._json(dashboard.history(query.get("limit", 160)))
                elif path == "/favicon.ico":
                    self._send(204, b"", "image/x-icon")
                else:
                    self._send(404, b'{"error":"not found"}', "application/json; charset=utf-8")
            def do_POST(self):
                if urlsplit(self.path).path != "/api/command":
                    self._send(404, b'{"error":"not found"}', "application/json; charset=utf-8")
                    return
                try:
                    length = min(int(self.headers.get("Content-Length", "0")), 2048)
                    body = json.loads(self.rfile.read(length) or b"{}")
                    action = body.get("action") if isinstance(body, dict) else None
                    ok, message = dashboard.request_command(action)
                    self._send(202 if ok else 400, json.dumps({"ok": ok, "message": message}, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")
                except (ValueError, json.JSONDecodeError):
                    self._send(400, b'{"ok":false,"message":"invalid JSON"}', "application/json; charset=utf-8")
            do_PUT = do_POST
            do_DELETE = do_POST
        return Handler

    def make_server(self, host: str, port: int) -> ThreadingHTTPServer:
        return ThreadingHTTPServer((host, port), self.handler_class())


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception as exc:
        raise ValueError(f"Cannot parse decimal value: {value!r}") from exc


def spread_value(prices: dict[str, float], anchor: dict[str, float], p2: float = P2_SCALE) -> float:
    """Same normalized spread as the backtest, using saved startup anchors."""
    anchor_alts = anchor["BNBUSDT"] + anchor["XRPUSDT"] + anchor["ADAUSDT"]
    live_alts = prices["BNBUSDT"] + prices["XRPUSDT"] + prices["ADAUSDT"]
    return (prices["BTCUSDT"] / anchor["BTCUSDT"] * p2
            - live_alts / anchor_alts * p2)


def core_side(direction: int, symbol: str) -> str:
    """Backtest basket: BTC is opposite to all three alt legs."""
    sign = -direction if symbol == "BTCUSDT" else direction
    return "Buy" if sign > 0 else "Sell"


def opposite(side: str) -> str:
    return "Sell" if side == "Buy" else "Buy"


def position_idx(side: str) -> int:
    # Bybit V5 hedge mode: 1 = Buy/long side; 2 = Sell/short side.
    return 1 if side == "Buy" else 2


def ceil_to_step(quantity: Decimal, qty_step: Decimal) -> Decimal:
    if qty_step <= 0:
        raise ValueError("qtyStep must be positive")
    units = (quantity / qty_step).to_integral_value(rounding=ROUND_CEILING)
    return units * qty_step


def fmt_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def hedge_retrace_reached(anchor: float, sign: int, spread: float, buffer: float) -> bool:
    return sign * (spread - anchor) <= -buffer


def hedge_rearm_reached(anchor: float, sign: int, spread: float, buffer: float) -> bool:
    return sign * (spread - anchor) >= buffer


def make_session():
    """Construct a hard-pinned Demo-only Bybit V5 client from environment."""
    if not isinstance(DEMO_CONFIG, dict):
        raise RuntimeError("DEMO_CONFIG в api_config.py должен быть словарём")
    if DEMO_CONFIG.get("demo") is not True or DEMO_CONFIG.get("testnet") is not False:
        raise RuntimeError("Safety stop: DEMO_CONFIG must have demo=True and testnet=False")
    api_key = str(DEMO_CONFIG.get("api_key", "")).strip()
    api_secret = str(DEMO_CONFIG.get("api_secret", "")).strip()
    if not api_key or not api_secret:
        raise RuntimeError("Заполните api_key и api_secret в api_config.py")
    if api_key.startswith("REPLACE_") or api_secret.startswith("REPLACE_"):
        raise RuntimeError("Замените placeholder на новую отдельную пару ключей Bybit Demo в api_config.py")
    try:
        timeout = int(DEMO_CONFIG.get("timeout", 20))
    except ValueError as exc:
        raise RuntimeError("BYBIT_DEMO_TIMEOUT должен быть целым числом") from exc
    if not 3 <= timeout <= 60:
        raise RuntimeError("BYBIT_DEMO_TIMEOUT должен быть в диапазоне 3..60")
    return BybitDemoHTTP(api_key, api_secret, timeout)


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # The bot is started by main() in a dedicated worker thread while the
        # Store is initialized just before that thread starts. Allow this
        # intentional hand-off; all bot DB writes still happen in one worker
        # thread and dashboard readers use separate read-only connections.
        self.conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS kv_state (
          key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS bars (
          close_time_utc TEXT PRIMARY KEY, prices_json TEXT NOT NULL,
          spread REAL NOT NULL, processed_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_utc TEXT NOT NULL,
          wallet_usdt TEXT NOT NULL, equity_usdt TEXT NOT NULL,
          available_usdt TEXT NOT NULL, spread REAL, positions_json TEXT NOT NULL,
          metrics_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS wallet_samples (
          id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_utc TEXT NOT NULL,
          wallet_usdt TEXT NOT NULL, equity_usdt TEXT NOT NULL, available_usdt TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS position_samples (
          id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_utc TEXT NOT NULL,
          positions_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp_utc TEXT NOT NULL,
          event TEXT NOT NULL, payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS orders (
          order_link_id TEXT PRIMARY KEY, created_utc TEXT NOT NULL,
          symbol TEXT NOT NULL, side TEXT NOT NULL, qty TEXT NOT NULL,
          position_idx INTEGER NOT NULL, reduce_only INTEGER NOT NULL,
          status TEXT NOT NULL, order_id TEXT, response_json TEXT
        );
        """)
        self.conn.commit()

    def get(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM kv_state WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key: str, value):
        with self.conn:
            self.conn.execute("INSERT INTO kv_state(key,value,updated_utc) VALUES(?,?,?) "
                              "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_utc=excluded.updated_utc",
                              (key, json.dumps(value, separators=(",", ":")), utc_now()))

    def event(self, event: str, **payload):
        # API keys, signatures and request headers are never accepted or written here.
        with self.conn:
            self.conn.execute("INSERT INTO events(timestamp_utc,event,payload_json) VALUES(?,?,?)",
                              (utc_now(), event, json.dumps(payload, default=str, separators=(",", ":"))))

    def record_bar(self, close_time: str, prices: dict, spread: float):
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO bars(close_time_utc,prices_json,spread,processed_utc) VALUES(?,?,?,?)",
                              (close_time, json.dumps(prices, sort_keys=True), spread, utc_now()))

    def record_snapshot(self, positions: dict, spread: float | None, metrics: dict):
        with self.conn:
            self.conn.execute("INSERT INTO snapshots(timestamp_utc,wallet_usdt,equity_usdt,available_usdt,spread,positions_json,metrics_json) VALUES(?,?,?,?,?,?,?)",
                (utc_now(), str(LIVE_WALLET), str(LIVE_EQUITY), str(LIVE_AVAILABLE), spread,
                 json.dumps(positions, default=str, sort_keys=True), json.dumps(metrics, default=str, sort_keys=True)))

    def record_wallet_poll(self):
        with self.conn:
            self.conn.execute("INSERT INTO wallet_samples(timestamp_utc,wallet_usdt,equity_usdt,available_usdt) VALUES(?,?,?,?)",
                (utc_now(),str(LIVE_WALLET),str(LIVE_EQUITY),str(LIVE_AVAILABLE)))

    def record_positions_poll(self, positions: dict):
        with self.conn:
            self.conn.execute("INSERT INTO position_samples(timestamp_utc,positions_json) VALUES(?,?)",
                (utc_now(),json.dumps(positions,default=str,sort_keys=True)))

    def telemetry_events(self, limit: int = 40) -> list[dict]:
        allowed = ("OPEN", "ADD", "HEDGE_OPEN", "HEDGE_CLOSE", "HEDGE_REARMED",
                   "MARGIN_TRIM", "CYCLE_CLOSE", "EMERGENCY_FLATTEN")
        placeholders = ",".join("?" for _ in allowed)
        rows = self.conn.execute(
            f"SELECT id,timestamp_utc,event,payload_json FROM events WHERE event IN ({placeholders}) "
            "ORDER BY id DESC LIMIT ?", (*allowed, max(1, min(int(limit), 80)))
        ).fetchall()
        result = []
        for row in reversed(rows):
            try:
                raw = json.loads(row["payload_json"])
            except (TypeError, json.JSONDecodeError):
                raw = {}
            detail = {}
            for key in ("spread", "anchor"):
                try:
                    value = float(raw.get(key))
                    if math.isfinite(value):
                        detail[key] = value
                except (TypeError, ValueError):
                    pass
            if isinstance(raw.get("direction"), int) and raw["direction"] in (-1, 0, 1):
                detail["direction"] = raw["direction"]
            if isinstance(raw.get("initial"), bool):
                detail["initial"] = raw["initial"]
            if isinstance(raw.get("reason"), str):
                detail["reason"] = raw["reason"][:120]
            if isinstance(raw.get("remaining_bundles"), int):
                detail["remaining_bundles"] = raw["remaining_bundles"]
            result.append({
                "eventId": int(row["id"]),
                "occurredAt": row["timestamp_utc"],
                "type": row["event"],
                "spread": detail.get("spread"),
                "detail": detail,
            })
        return result

    def put_order_intent(self, link: str, symbol: str, side: str, qty: str, idx: int, reduce: bool):
        with self.conn:
            self.conn.execute("INSERT INTO orders(order_link_id,created_utc,symbol,side,qty,position_idx,reduce_only,status) VALUES(?,?,?,?,?,?,?,?)",
                (link, utc_now(), symbol, side, qty, idx, int(reduce), "SUBMITTING"))

    def order_result(self, link: str, status: str, response: dict | None = None):
        with self.conn:
            self.conn.execute("UPDATE orders SET status=?, order_id=?, response_json=? WHERE order_link_id=?",
                (status, (response or {}).get("result", {}).get("orderId"),
                 json.dumps(response, default=str) if response else None, link))

    def close(self):
        self.conn.close()


@dataclass
class StrategyState:
    anchor_prices: dict[str, float] | None = None
    last_bar_close_ms: int = 0
    previous_spread: float | None = None
    direction: int = 0
    ref_spread: float = 0.0
    cycle_start_wallet: str | None = None
    cycle_start_ms: int | None = None
    cycle_net_realized_marker: str | None = None
    B: str = "15"
    P: str = "5"
    step: str = "10"
    roi: str = "0.03"
    cycle_count: int = 0
    bundles: list[dict[str, str]] | None = None
    hedge_active: bool = False
    hedge_anchor: float | None = None
    hedge_sign: int = 0
    rearm_anchor: float | None = None
    rearm_sign: int = 0
    earliest_rearm_ms: int = 0
    auto_entry_paused: bool = False
    halted: bool = False
    last_close_reason: str | None = None

    @classmethod
    def from_dict(cls, data: dict | None):
        if not data:
            return cls(bundles=[])
        data = dict(data)
        if data.get("bundles") is None:
            data["bundles"] = []
        return cls(**data)


class DemoBot:
    def __init__(self, session, store: Store, allow_orders: bool, dashboard: LocalDashboard | None = None):
        self.s = session
        self.db = store
        self.allow_orders = allow_orders
        self.state = StrategyState.from_dict(self.db.get("strategy_state"))
        self.source_id = self.db.get("local_source_id")
        try:
            uuid.UUID(str(self.source_id))
        except (ValueError, TypeError, AttributeError):
            self.source_id = str(uuid.uuid4())
            self.db.set("local_source_id", self.source_id)
        self.dashboard = dashboard
        self.specs: dict[str, dict] = {}
        self.latest_positions: dict[str, dict] = {}
        self.latest_mark_data: dict[str, dict] = {}
        self.last_wallet_poll = 0.0
        self.last_monitor_poll = 0.0
        self.bar_spread: float | None = None
        self.current_bar_close_ms: int = 0
        self._emergency_in_progress = False
        self.db.event("PROCESS_START", demo=True, allow_demo_orders=allow_orders,
                      p2_scale=P2_SCALE, symbols=SYMBOLS, interval=INTERVAL)

    def save(self):
        self.db.set("strategy_state", asdict(self.state))

    @staticmethod
    def _result(response: dict, label: str):
        if not isinstance(response, dict) or response.get("retCode") != 0:
            raise RuntimeError(f"Bybit {label} failed: retCode={getattr(response,'get',lambda *_:None)('retCode')} "
                               f"retMsg={getattr(response,'get',lambda *_:None)('retMsg')}")
        return response.get("result", {})

    def api_read(self, method: str, **kwargs):
        """Retry idempotent reads after reconnect; never repeats an order POST."""
        delays = (1, 2, 4, 8, 15)
        transient_codes = {10002, 10006, 10016, 10019, 20003}
        last_error = None
        for attempt in range(1, len(delays)+2):
            try:
                response = getattr(self.s, method)(**kwargs)
                code = int(response.get("retCode", -1)) if isinstance(response, dict) else -1
                if code == 0:
                    return response
                message = str(response.get("retMsg", "")) if isinstance(response, dict) else "invalid response"
                if code not in transient_codes:
                    raise RuntimeError(f"Bybit {method} rejected request: retCode={code}, retMsg={message}")
                last_error = RuntimeError(f"transient Bybit retCode={code}: {message}")
            except Exception as exc:
                if not isinstance(exc, (URLError, TimeoutError, ConnectionError, OSError)):
                    raise
                last_error = exc
            if attempt > len(delays):
                break
            delay=delays[attempt-1]
            self.db.event("API_RECONNECT_RETRY", method=method, attempt=attempt,
                          delay_seconds=delay, error_type=type(last_error).__name__, error=str(last_error)[:300])
            log.warning("Bybit read %s failed (%s/%s); rebuilding Demo session and retrying in %ss",
                        method, attempt, len(delays)+1, delay)
            time.sleep(delay)
            self.s = make_session()
        raise TransientAPIError(f"Bybit read {method} failed after reconnect retries: {last_error}") from last_error

    def poll_wallet(self, force=False):
        global LIVE_WALLET, LIVE_EQUITY, LIVE_AVAILABLE
        if not force and time.monotonic() - self.last_wallet_poll < WALLET_POLL_SECONDS:
            return
        r = self._result(self.api_read("get_wallet_balance", accountType="UNIFIED", coin="USDT"), "wallet-balance")
        rows = r.get("list", [])
        if not rows:
            raise RuntimeError("No UNIFIED wallet record; refusing to trade")
        row = rows[0]
        LIVE_WALLET = dec(row.get("totalWalletBalance"))
        LIVE_EQUITY = dec(row.get("totalEquity"))
        LIVE_AVAILABLE = dec(row.get("totalAvailableBalance"))
        stored_global_start = self.db.get("global_roi_start_wallet")
        if stored_global_start is None or dec(stored_global_start) <= 0:
            self.db.set("global_roi_start_wallet", str(LIVE_WALLET))
        self.last_wallet_poll = time.monotonic()
        self.db.set("last_wallet", {"wallet": str(LIVE_WALLET), "equity": str(LIVE_EQUITY),
                                    "available": str(LIVE_AVAILABLE), "time": utc_now()})
        self.db.record_wallet_poll()

    def get_instrument_specs(self):
        for symbol in SYMBOLS:
            r = self._result(self.api_read("get_instruments_info", category=CATEGORY, symbol=symbol), "instruments-info")
            items = r.get("list", [])
            if not items or items[0].get("status") != "Trading":
                raise RuntimeError(f"Instrument not Trading: {symbol}")
            item = items[0]
            self.specs[symbol] = {
                "qty_step": dec(item["lotSizeFilter"]["qtyStep"]),
                "min_qty": dec(item["lotSizeFilter"]["minOrderQty"]),
                "min_notional": dec(item["lotSizeFilter"].get("minNotionalValue", "0")),
                "max_market_qty": dec(item["lotSizeFilter"].get("maxMktOrderQty", "0")),
                "max_leverage": dec(item["leverageFilter"]["maxLeverage"]),
                "tick_size": dec(item["priceFilter"]["tickSize"]),
            }
            if self.specs[symbol]["max_leverage"] < LEVERAGE:
                raise RuntimeError(f"20x is not allowed for {symbol} on this account")
        self.db.event("INSTRUMENT_SPECS",specs={s:{k:str(v) for k,v in d.items()} for s,d in self.specs.items()})

    def preflight(self):
        account = self._result(self.api_read("get_account_info"), "account-info")
        if account.get("marginMode") != "REGULAR_MARGIN":
            raise RuntimeError(f"Expected Cross/REGULAR_MARGIN, got {account.get('marginMode')!r}; configure manually")
        self.get_instrument_specs()
        allpos = self.fetch_positions()
        if self.allow_orders:
            # Trade-enabled mode retains the original strict account safety gates.
            for sym in SYMBOLS:
                indices = {int(p.get("positionIdx", -1)) for p in allpos.get(sym, [])}
                if not {1, 2}.issubset(indices):
                    raise RuntimeError(f"{sym} is not confirmed in hedge mode (expected positionIdx 1 and 2); configure manually")
            allpos = self.ensure_target_leverage(allpos)
            for sym in SYMBOLS:
                for row in allpos[sym]:
                    lev=dec(row.get("leverage","0"))
                    if lev != LEVERAGE:
                        raise RuntimeError(f"{sym} positionIdx={row.get('positionIdx')} leverage remains {lev}, expected 20x after auto-setting")
        self.poll_wallet(force=True)
        if self.allow_orders and self.state.cycle_count == 0 and not self.state.bundles:
            if abs(LIVE_WALLET-START_BALANCE_USDT) > Decimal("5"):
                raise RuntimeError(f"First start expects a $500 Demo wallet (±$5), got {LIVE_WALLET}; refusing initial sizing")
        if self.allow_orders:
            self.verify_recovery(allpos)
        else:
            self.db.event("READ_ONLY_PREFLIGHT", note="No leverage/order/account mode changes are made in read-only mode")
        self.db.event("PREFLIGHT_OK", margin_mode=account.get("marginMode"),
                      position_mode="hedge/confirmed", wallet=str(LIVE_WALLET),
                      equity=str(LIVE_EQUITY), available=str(LIVE_AVAILABLE),
                      leverage_target=str(LEVERAGE), order_mode="demo" if self.allow_orders else "read-only")

    def ensure_target_leverage(self, positions: dict[str, list[dict]]) -> dict[str, list[dict]]:
        mismatched = {}
        for symbol in SYMBOLS:
            values = {int(row.get("positionIdx", -1)): dec(row.get("leverage", "0"))
                      for row in positions.get(symbol, [])}
            if any(values.get(idx) != LEVERAGE for idx in (1, 2)):
                mismatched[symbol] = values
        if not mismatched:
            return positions

        active_positions = [
            {"symbol": symbol, "positionIdx": row.get("positionIdx"), "size": row.get("size")}
            for symbol in SYMBOLS for row in positions.get(symbol, [])
            if dec(row.get("size", "0")) > 0
        ]
        if active_positions:
            raise RuntimeError(f"Cannot change leverage while positions are open; close/reconcile first: {active_positions}")

        active_statuses = {"New", "PartiallyFilled", "Untriggered", "Triggered"}
        open_orders = []
        for symbol in SYMBOLS:
            result = self._result(self.api_read("get_open_orders", category=CATEGORY, symbol=symbol), "open-orders")
            for order in result.get("list", []):
                if order.get("orderStatus") in active_statuses:
                    open_orders.append({"symbol": symbol, "orderId": order.get("orderId"),
                                        "orderStatus": order.get("orderStatus")})
        if open_orders:
            raise RuntimeError(f"Cannot change leverage while active orders exist; cancel/reconcile first: {open_orders}")

        self.db.event("LEVERAGE_MISMATCH", mismatches={s:{str(k):str(v) for k,v in values.items()}
                                                       for s,values in mismatched.items()},
                      target=str(LEVERAGE), action="Set both hedge sides to equal 20x; Cross mode")
        transient_codes = {10002, 10006, 10016, 10019, 20003}
        for symbol in mismatched:
            completed = False
            last_error = None
            for attempt in range(1, 4):
                try:
                    response = self.s.set_leverage(category=CATEGORY, symbol=symbol,
                                                   buyLeverage=fmt_decimal(LEVERAGE),
                                                   sellLeverage=fmt_decimal(LEVERAGE))
                    code = int(response.get("retCode", -1)) if isinstance(response, dict) else -1
                    message = str(response.get("retMsg", "")) if isinstance(response, dict) else "invalid response"
                    if code in (0, 110043):  # 110043: requested leverage is already set
                        self.db.event("LEVERAGE_SET_ACK", symbol=symbol, target=str(LEVERAGE),
                                      retCode=code, retMsg=message)
                        completed = True
                        break
                    if code not in transient_codes:
                        raise RuntimeError(f"Bybit set_leverage rejected {symbol}: retCode={code}, retMsg={message}")
                    last_error = RuntimeError(f"transient set_leverage retCode={code}: {message}")
                except RuntimeError as exc:
                    if str(exc).startswith("Bybit set_leverage rejected"):
                        raise
                    last_error = exc
                except Exception as exc:
                    # Setting the same leverage is idempotent. After a lost response, reconnect
                    # and read positions before deciding whether another set call is necessary.
                    last_error = exc
                self.s = make_session()
                refreshed = self.fetch_positions()
                current = {int(row.get("positionIdx", -1)): dec(row.get("leverage", "0"))
                           for row in refreshed.get(symbol, [])}
                if all(current.get(idx) == LEVERAGE for idx in (1, 2)):
                    self.db.event("LEVERAGE_SET_CONFIRMED_AFTER_RECONNECT", symbol=symbol,
                                  target=str(LEVERAGE), attempt=attempt)
                    completed = True
                    break
                if attempt < 3:
                    delay = 2 ** (attempt - 1)
                    self.db.event("LEVERAGE_SET_RETRY", symbol=symbol, attempt=attempt,
                                  delay_seconds=delay, error=str(last_error)[:300])
                    time.sleep(delay)
            if not completed:
                raise RuntimeError(f"Could not set {symbol} leverage to 20x after retries: {last_error}")

        verified = self.fetch_positions()
        for symbol in SYMBOLS:
            values = {int(row.get("positionIdx", -1)): dec(row.get("leverage", "0"))
                      for row in verified.get(symbol, [])}
            if any(values.get(idx) != LEVERAGE for idx in (1, 2)):
                raise RuntimeError(f"Leverage verification failed for {symbol}: {values}; expected 20x on both sides")
        self.db.event("LEVERAGE_ALL_VERIFIED", target=str(LEVERAGE), symbols=SYMBOLS)
        return verified

    def fetch_positions(self) -> dict[str, list[dict]]:
        out = {s: [] for s in SYMBOLS}
        for symbol in SYMBOLS:
            r = self._result(self.api_read("get_positions", category=CATEGORY, symbol=symbol), "position-list")
            out[symbol] = r.get("list", [])
        self.latest_positions = out
        self.db.record_positions_poll(out)
        return out

    @staticmethod
    def _qty_from_positions(rows: list[dict], idx: int) -> Decimal:
        for row in rows:
            if int(row.get("positionIdx", -1)) == idx:
                return dec(row.get("size", "0"))
        return Decimal("0")

    def verify_recovery(self, positions: dict[str, list[dict]]):
        tracked = bool(self.state.bundles) or self.state.hedge_active
        nonzero = any(dec(p.get("size", "0")) > 0 for rows in positions.values() for p in rows)
        if not tracked and nonzero:
            raise RuntimeError("Untracked open positions found; bot will not adopt or modify them")
        if tracked and not nonzero:
            # The user may have flattened everything manually on Bybit while
            # the local SQLite state still contained the previous basket.
            # Reconcile only the fully-flat case; partial mismatches remain a
            # hard stop and require manual inspection.
            self.state.bundles=[]
            self.state.hedge_active=False
            self.state.hedge_anchor=None
            self.state.hedge_sign=0
            self.state.rearm_anchor=None
            self.state.rearm_sign=0
            self.state.cycle_start_wallet=None
            self.state.cycle_start_ms=None
            self.state.auto_entry_paused=True
            self.state.halted=False
            self.state.last_close_reason="MANUAL_FLAT_RECONCILE"
            self.db.event("RECOVERY_RECONCILED_MANUAL_FLAT",status="local basket reset; auto-entry paused")
            self.save()
            return
        if tracked:
            expected_core = {s: sum((dec(b.get(s, "0")) for b in self.state.bundles or []), Decimal("0")) for s in SYMBOLS}
            for sym in SYMBOLS:
                core_side_name = core_side(self.state.direction, sym)
                core_idx = position_idx(core_side_name)
                live_core = self._qty_from_positions(positions[sym], core_idx)
                if abs(live_core-expected_core[sym]) > self.specs[sym]["qty_step"]:
                    raise RuntimeError(f"Recovery mismatch on {sym} core: API={live_core} DB={expected_core[sym]}; halt and reconcile manually")
                opposite_idx = position_idx(opposite(core_side_name))
                live_hedge = self._qty_from_positions(positions[sym], opposite_idx)
                expected_hedge = expected_core[sym] if self.state.hedge_active else Decimal("0")
                if abs(live_hedge-expected_hedge) > self.specs[sym]["qty_step"]:
                    raise RuntimeError(f"Recovery mismatch on {sym} hedge: API={live_hedge} DB={expected_hedge}; halt and reconcile manually")

    def latest_closed_bars(self, allow_startup_replay: bool = False) -> tuple[int, dict[str, float]] | None:
        now_ms = int(time.time()*1000)
        found: dict[str, tuple[int,float]] = {}
        for symbol in SYMBOLS:
            r = self._result(self.api_read("get_kline", category=CATEGORY, symbol=symbol, interval=INTERVAL, limit=5), "kline")
            bars = r.get("list", [])
            closed = [(int(x[0]), float(x[4])) for x in bars if int(x[0])+BAR_MS <= now_ms]
            if not closed:
                return None
            found[symbol] = max(closed, key=lambda x:x[0])
        starts = {x[0] for x in found.values()}
        if len(starts) != 1:
            raise RuntimeError(f"Hourly candle timestamps diverged across symbols: {found}")
        start_ms = starts.pop()
        close_ms = start_ms + BAR_MS
        if close_ms < self.state.last_bar_close_ms:
            return None
        if close_ms == self.state.last_bar_close_ms and not allow_startup_replay:
            return None
        return close_ms, {s: found[s][1] for s in SYMBOLS}

    def live_marks(self) -> dict[str, Decimal]:
        marks={}
        self.latest_mark_data={}
        for symbol in SYMBOLS:
            r=self._result(self.api_read("get_tickers",category=CATEGORY,symbol=symbol),"tickers")
            rows=r.get("list",[])
            if not rows:
                raise RuntimeError(f"No ticker for {symbol}")
            marks[symbol]=dec(rows[0].get("markPrice") or rows[0].get("lastPrice"))
            self.latest_mark_data[symbol]={"markPrice":str(marks[symbol]),"lastPrice":rows[0].get("lastPrice"),
                                           "fundingRate":rows[0].get("fundingRate"),
                                           "nextFundingTime":rows[0].get("nextFundingTime")}
            if marks[symbol] <= 0:
                raise RuntimeError(f"Bad mark price for {symbol}")
        return marks

    def current_position_map(self) -> dict[tuple[str,int],dict]:
        rows=self.fetch_positions()
        return {(sym,int(p.get("positionIdx",-1))):p for sym,items in rows.items() for p in items}

    def gross_notional(self, marks: dict[str,Decimal], include_hedge=True) -> Decimal:
        pmap=self.current_position_map()
        gross=Decimal("0")
        for sym in SYMBOLS:
            for idx in (1,2):
                row=pmap.get((sym,idx),{})
                qty=dec(row.get("size","0"))
                if qty:
                    gross += qty*marks[sym]
        if include_hedge and self.state.bundles and not self.state.hedge_active:
            core_gross=Decimal("0")
            for sym in SYMBOLS:
                side=core_side(self.state.direction,sym); idx=position_idx(side)
                core_gross += dec(pmap.get((sym,idx),{}).get("size","0"))*marks[sym]
            gross += core_gross
        return gross

    def projected_free(self, marks: dict[str,Decimal], add_notional=Decimal("0"), hedge_extra=Decimal("0"), fee=Decimal("0")) -> Decimal:
        gross=self.gross_notional(marks,include_hedge=False)+add_notional+hedge_extra
        # Conservative backtest-style gross margin projection, separate from Bybit's live available balance.
        return LIVE_EQUITY-fee-gross/LEVERAGE-gross*MMR_EST

    def estimated_unrealized(self, marks: dict[str,Decimal]) -> Decimal:
        pmap=self.current_position_map(); total=Decimal("0")
        for sym in SYMBOLS:
            for idx in (1,2):
                row=pmap.get((sym,idx),{})
                total += dec(row.get("unrealisedPnl","0"))
        return total

    def cycle_pnl(self, marks: dict[str,Decimal]) -> Decimal:
        if self.state.cycle_start_wallet is None:
            return Decimal("0")
        return LIVE_WALLET-dec(self.state.cycle_start_wallet)+self.estimated_unrealized(marks)

    def quantity_for_budget(self, symbol: str, price: Decimal, margin_budget: Decimal) -> Decimal:
        raw=margin_budget*LEVERAGE/price
        spec=self.specs[symbol]
        qty=ceil_to_step(raw,spec["qty_step"])
        if qty < spec["min_qty"] or qty*price < spec["min_notional"]:
            raise RuntimeError(f"Calculated {symbol} size below exchange minimum: qty={qty}")
        if spec["max_market_qty"] and qty > spec["max_market_qty"]:
            raise RuntimeError(f"Calculated {symbol} size exceeds market max; must split safely")
        return qty

    def record_intent(self, symbol: str, side: str, qty: Decimal, idx: int, reduce_only: bool, tag: str) -> str:
        link=f"d{int(time.time()*1000)%10**12}_{tag}_{symbol[:3]}_{uuid.uuid4().hex[:8]}"[:36]
        self.db.put_order_intent(link,symbol,side,fmt_decimal(qty),idx,reduce_only)
        return link

    def place_one(self, symbol: str, side: str, qty: Decimal, idx: int, reduce_only=False, tag="ord"):
        if not self.allow_orders:
            self.db.event("DRY_ORDER_NOT_SENT",symbol=symbol,side=side,qty=str(qty),position_idx=idx,reduce_only=reduce_only,tag=tag)
            return None
        link=self.record_intent(symbol,side,qty,idx,reduce_only,tag)
        try:
            resp=self.s.place_order(category=CATEGORY,symbol=symbol,side=side,orderType="Market",
                qty=fmt_decimal(qty),positionIdx=idx,reduceOnly=bool(reduce_only),orderLinkId=link)
            if not isinstance(resp,dict) or resp.get("retCode") != 0:
                code=resp.get("retCode") if isinstance(resp,dict) else None
                message=resp.get("retMsg") if isinstance(resp,dict) else "invalid response"
                self.db.order_result(link,"REJECTED",{"retCode":code,"retMsg":message})
                raise RuntimeError(f"Bybit rejected order: retCode={code}, retMsg={message}")
            self.db.order_result(link,"ACK",resp)
            self.db.event("ORDER_ACK",order_link_id=link,symbol=symbol,side=side,qty=str(qty),position_idx=idx,reduce_only=reduce_only,order_id=resp.get("result",{}).get("orderId"))
            return resp
        except Exception as exc:
            # A POST timeout may mean the exchange accepted the order. Reconnect and query,
            # but never blindly submit the same order again.
            self.s=make_session()
            try:
                hist=self._result(self.api_read("get_order_history",category=CATEGORY,symbol=symbol,orderLinkId=link,limit=10),"order-history")
                matches=[x for x in hist.get("list",[]) if x.get("orderLinkId")==link]
                if matches and matches[0].get("orderStatus")=="Filled":
                    recovered={"retCode":0,"retMsg":"Recovered by orderLinkId after reconnect","result":matches[0]}
                    self.db.order_result(link,"ACK_RECOVERED",recovered)
                    self.db.event("ORDER_ACK_RECOVERED",order_link_id=link,symbol=symbol,order_id=matches[0].get("orderId"))
                    return recovered
                status=matches[0].get("orderStatus") if matches else "NOT_FOUND"
                self.db.order_result(link,"UNCERTAIN",{"orderStatus":status,"error":str(exc)[:500]})
                self.db.event("ORDER_UNCERTAIN",order_link_id=link,symbol=symbol,order_status=status,
                              error_type=type(exc).__name__,error=str(exc)[:500],
                              instruction="No automatic resubmission; reconcile order/positions")
            except Exception as recovery_exc:
                self.db.order_result(link,"UNCERTAIN",{"error_type":type(exc).__name__,"error":str(exc)[:300],
                                                       "recovery_error":str(recovery_exc)[:300]})
                self.db.event("ORDER_RECOVERY_FAILED",order_link_id=link,symbol=symbol,
                              error=str(recovery_exc)[:500],instruction="No automatic resubmission")
            raise RuntimeError(f"Order outcome uncertain for {link}; no duplicate was submitted") from exc

    def _open_or_close_legs(self, legs: list[tuple[str,str,Decimal,int,bool]], tag: str):
        try:
            before=self.fetch_positions() if self.allow_orders else {}
            before_qty={(sym,idx):self._qty_from_positions(rows,idx)
                        for sym,rows in before.items() for idx in (1,2)}
            for sym,side,qty,idx,reduce_only in legs:
                self.place_one(sym,side,qty,idx,reduce_only,tag)
            if self.allow_orders:
                deadline=time.monotonic()+ORDER_FILL_TIMEOUT_SECONDS
                while time.monotonic() < deadline:
                    after=self.fetch_positions()
                    all_confirmed=True
                    for sym,side,qty,idx,reduce_only in legs:
                        old=before_qty.get((sym,idx),Decimal("0"))
                        target=max(Decimal("0"),old-qty) if reduce_only else old+qty
                        actual=self._qty_from_positions(after[sym],idx)
                        tolerance=self.specs[sym]["qty_step"] if sym in self.specs else Decimal("0.00000001")
                        if abs(actual-target)>tolerance:
                            all_confirmed=False
                            break
                    if all_confirmed:
                        self.poll_wallet(force=True)
                        self.db.event("MULTILEG_FILL_CONFIRMED",tag=tag,leg_count=len(legs))
                        return
                    time.sleep(1)
                raise RuntimeError(f"Fill confirmation timed out for multi-leg action {tag}")
        except Exception:
            self.db.event("MULTILEG_ORDER_FAILURE",tag=tag,action="HALT; reconcile positions manually")
            if self.allow_orders and not self._emergency_in_progress:
                self.emergency_flatten(f"MULTILEG_FAILURE_{tag.upper()}")
            raise

    def bundle_legs(self, marks: dict[str,Decimal], direction: int) -> tuple[list[tuple],dict[str,Decimal],Decimal]:
        legs=[]; qtys={}; notional=Decimal("0")
        for sym in SYMBOLS:
            budget=dec(self.state.B) if sym=="BTCUSDT" else dec(self.state.P)
            qty=self.quantity_for_budget(sym,marks[sym],budget)
            side=core_side(direction,sym)
            legs.append((sym,side,qty,position_idx(side),False))
            qtys[sym]=qty; notional+=qty*marks[sym]
        return legs,qtys,notional

    def open_bundle(self, marks: dict[str,Decimal], direction: int, initial=False):
        legs,qtys,notional=self.bundle_legs(marks,direction)
        cycle_start_wallet=str(LIVE_WALLET) if initial else None
        est_fee=notional*FEE_RATE_EST
        projected=self.projected_free(marks,add_notional=notional,hedge_extra=notional,fee=est_fee)
        # Reserve margin for both the new core bundle and its possible full mirror.
        live_after=LIVE_AVAILABLE-(Decimal("2")*notional)/LEVERAGE-Decimal("2")*est_fee
        if min(projected,live_after) < HEDGE_OPEN_BUFFER_USDT+MIN_FREE_MARGIN_USDT:
            self.db.event("MARGIN_BLOCK",initial=initial,projected_post_hedge_free=str(projected),
                          live_available_after_open_est=str(live_after),required=str(HEDGE_OPEN_BUFFER_USDT+MIN_FREE_MARGIN_USDT))
            return False
        self._open_or_close_legs(legs,"open" if initial else "add")
        if self.allow_orders and LIVE_AVAILABLE < MIN_FREE_MARGIN_USDT:
            self.emergency_flatten("POST_BUNDLE_LIVE_MARGIN_BELOW_10")
            return False
        if self.allow_orders:
            # Confirm the filled side exists before updating local strategy state.
            self.fetch_positions()
            for sym in SYMBOLS:
                idx=position_idx(core_side(direction,sym))
                live=self._qty_from_positions(self.latest_positions[sym],idx)
                expected=sum((dec(b.get(sym,"0")) for b in self.state.bundles or []),Decimal("0"))+qtys[sym]
                if live < expected-self.specs[sym]["qty_step"]:
                    raise RuntimeError(f"Fill confirmation failed on {sym}: expected >= {expected}, got {live}")
        if initial:
            self.state.direction=direction
            self.state.cycle_start_wallet=cycle_start_wallet
            self.state.cycle_start_ms=self.current_bar_close_ms
            self.state.cycle_count+=1
        self.state.bundles=(self.state.bundles or [])+[ {s:str(q) for s,q in qtys.items()} ]
        self.db.event("OPEN" if initial else "ADD",direction=direction,spread=self.bar_spread,
                      bundle_count=len(self.state.bundles),qtys={s:str(q) for s,q in qtys.items()},
                      B=self.state.B,P=self.state.P,step=self.state.step,roi=self.state.roi,
                      projected_post_hedge_free=str(projected))
        self.save()
        return True

    def close_position_side(self, symbol: str, idx: int, tag: str):
        pmap=self.current_position_map(); row=pmap.get((symbol,idx),{})
        qty=dec(row.get("size","0"))
        if qty <= 0:
            return
        position_side=str(row.get("side",""))
        if position_side in {"Buy","Sell"}:
            side="Sell" if position_side=="Buy" else "Buy"
        else:
            side="Sell" if idx==1 else "Buy"
        self._open_or_close_legs([(symbol,side,qty,idx,True)],tag)

    def close_hedge(self, bar_close_ms: int, reason: str):
        if not self.state.hedge_active:
            return
        for sym in SYMBOLS:
            core_name=core_side(self.state.direction,sym)
            self.close_position_side(sym,position_idx(opposite(core_name)),"hedgeclose")
        anchor=self.state.hedge_anchor
        sign=self.state.hedge_sign
        self.state.hedge_active=False
        self.state.hedge_anchor=None
        self.state.hedge_sign=0
        if reason=="SPREAD_RETRACE_CROSSED_BUFFER":
            self.state.rearm_anchor=anchor
            self.state.rearm_sign=sign
            self.state.earliest_rearm_ms=bar_close_ms+BAR_MS
        self.poll_wallet(force=True)
        self.db.event("HEDGE_CLOSE",reason=reason,anchor=anchor,spread=self.bar_spread,
                      rearm_anchor=self.state.rearm_anchor,earliest_rearm_ms=self.state.earliest_rearm_ms)
        self.save()

    def open_hedge(self, marks: dict[str,Decimal], spread: float):
        core_gross=self.gross_notional(marks,include_hedge=False)
        # Mirror quantities exactly; estimated fee included in the pre-open projection.
        est_fee=core_gross*FEE_RATE_EST
        projected=self.projected_free(marks,hedge_extra=core_gross,fee=est_fee)
        live_after=LIVE_AVAILABLE-core_gross/LEVERAGE-est_fee
        if min(projected,live_after) < HEDGE_OPEN_BUFFER_USDT:
            while self.state.bundles and min(projected,live_after) < HEDGE_OPEN_BUFFER_USDT:
                self.trim_latest_bundle(marks,"RESTORE_HEDGE_RESERVE")
                core_gross=self.gross_notional(marks,include_hedge=False)
                est_fee=core_gross*FEE_RATE_EST
                projected=self.projected_free(marks,hedge_extra=core_gross,fee=est_fee)
                live_after=LIVE_AVAILABLE-core_gross/LEVERAGE-est_fee
            if not self.state.bundles or min(projected,live_after) < MIN_FREE_MARGIN_USDT:
                self.emergency_flatten("INSUFFICIENT_HEDGE_RESERVE")
                return
        if min(projected,live_after) < MIN_FREE_MARGIN_USDT:
            self.emergency_flatten("HEDGE_POST_MARGIN_FLOOR")
            return
        legs=[]
        for sym in SYMBOLS:
            core_name=core_side(self.state.direction,sym)
            idx=position_idx(opposite(core_name))
            qty=sum((dec(b.get(sym,"0")) for b in self.state.bundles or []),Decimal("0"))
            if qty>0: legs.append((sym,opposite(core_name),qty,idx,False))
        self._open_or_close_legs(legs,"hedge")
        if LIVE_AVAILABLE < MIN_FREE_MARGIN_USDT:
            self.emergency_flatten("POST_HEDGE_LIVE_MARGIN_BELOW_10")
            return
        self.state.hedge_active=True
        self.state.hedge_anchor=spread
        self.state.hedge_sign=1 if spread>0 else -1 if spread<0 else self.state.direction or 1
        self.db.event("HEDGE_OPEN",anchor=spread,sign=self.state.hedge_sign,
                      projected_free_margin=str(projected),floor=str(MIN_FREE_MARGIN_USDT),
                      mirrored_qty={s:str(sum((dec(b.get(s,"0")) for b in self.state.bundles or []),Decimal("0"))) for s in SYMBOLS})
        self.save()

    def trim_latest_bundle(self, marks: dict[str,Decimal], reason: str):
        if not self.state.bundles:
            return
        bundle=self.state.bundles[-1]
        legs=[]
        for sym in SYMBOLS:
            idx=position_idx(core_side(self.state.direction,sym))
            qty=dec(bundle[sym])
            legs.append((sym,"Sell" if idx==1 else "Buy",qty,idx,True))
            if self.state.hedge_active:
                hidx=position_idx(opposite(core_side(self.state.direction,sym)))
                hqty=qty
                legs.append((sym,"Sell" if hidx==1 else "Buy",hqty,hidx,True))
        self._open_or_close_legs(legs,"trim")
        self.state.bundles.pop()
        self.poll_wallet(force=True)
        self.db.event("MARGIN_TRIM",reason=reason,remaining_bundles=len(self.state.bundles),spread=self.bar_spread)
        if not self.state.bundles:
            self.emergency_flatten("CORE_TRIMMED_TO_FLAT")
        self.save()

    def protect_margin(self, marks: dict[str,Decimal], spread: float, bar_close_ms: int):
        if not self.state.bundles or self.state.halted:
            return
        self.poll_wallet(force=True)
        if LIVE_AVAILABLE < MIN_FREE_MARGIN_USDT:
            self.emergency_flatten("LIVE_AVAILABLE_BELOW_10_USDT")
            return
        if self.state.hedge_active:
            buf=float(dec(self.state.step)*HEDGE_RETRACE_RATIO)
            if self.state.hedge_anchor is not None and hedge_retrace_reached(self.state.hedge_anchor,self.state.hedge_sign,spread,buf):
                self.close_hedge(bar_close_ms,"SPREAD_RETRACE_CROSSED_BUFFER")
                return
            gross=self.gross_notional(marks,include_hedge=False)
            # Once both sides are present, actual available margin is the exchange-side guard.
            if LIVE_AVAILABLE < HEDGE_OPEN_BUFFER_USDT:
                self.trim_latest_bundle(marks,"HEDGED_LIVE_FREE_MARGIN_BUFFER")
            return
        if self.state.rearm_anchor is not None:
            buf=float(dec(self.state.step)*HEDGE_RETRACE_RATIO)
            if bar_close_ms>=self.state.earliest_rearm_ms and hedge_rearm_reached(self.state.rearm_anchor,self.state.rearm_sign,spread,buf):
                self.db.event("HEDGE_REARMED",anchor=self.state.rearm_anchor,spread=spread)
                self.state.rearm_anchor=None; self.state.rearm_sign=0; self.state.earliest_rearm_ms=0
                self.save()
            else:
                if LIVE_AVAILABLE < HEDGE_OPEN_BUFFER_USDT:
                    self.trim_latest_bundle(marks,"REARM_LOCK_FREE_MARGIN_BUFFER")
                return
        core_gross=self.gross_notional(marks,include_hedge=False)
        projected=self.projected_free(marks,hedge_extra=core_gross,fee=core_gross*FEE_RATE_EST)
        if min(projected,LIVE_AVAILABLE) <= HEDGE_OPEN_BUFFER_USDT+MIN_FREE_MARGIN_USDT:
            while self.state.bundles and min(projected,LIVE_AVAILABLE) < HEDGE_OPEN_BUFFER_USDT:
                self.trim_latest_bundle(marks,"RESTORE_FULL_HEDGE_RESERVE")
                core_gross=self.gross_notional(marks,include_hedge=False)
                projected=self.projected_free(marks,hedge_extra=core_gross,fee=core_gross*FEE_RATE_EST)
            if not self.state.bundles or min(projected,LIVE_AVAILABLE) < MIN_FREE_MARGIN_USDT:
                self.emergency_flatten("INSUFFICIENT_FULL_HEDGE_RESERVE")
                return
            self.open_hedge(marks,spread)

    def emergency_flatten(self, reason: str):
        self.db.event("EMERGENCY_FLATTEN",reason=reason,spread=self.bar_spread)
        self._emergency_in_progress = True
        try:
            positions=self.fetch_positions()
            for sym in SYMBOLS:
                for idx in (1,2):
                    qty=self._qty_from_positions(positions[sym],idx)
                    if qty>0:
                        self.close_position_side(sym,idx,"emergency")
        except Exception as exc:
            self.db.event("EMERGENCY_FLATTEN_FAILED",error_type=type(exc).__name__,error=str(exc)[:500])
        finally:
            self._emergency_in_progress = False
        self.state.bundles=[]; self.state.hedge_active=False; self.state.hedge_anchor=None
        self.state.halted=True; self.state.last_close_reason=reason
        self.save()

    def close_all_positions_from_dashboard(self):
        """Flatten every strategy leg, including mirror hedges, then pause entries."""
        if not self.allow_orders:
            raise RuntimeError("Закрытие из панели доступно только при ALLOW_DEMO_ORDERS=YES")
        self.db.event("DASHBOARD_COMMAND", action="close_all", status="started")
        self._emergency_in_progress = True
        try:
            positions = self.fetch_positions()
            for sym in SYMBOLS:
                # Hedge Mode returns positionIdx 1/2; One-Way Mode returns 0.
                # Use the actual position side to build the reduce-only order.
                for row in positions.get(sym, []):
                    idx = int(row.get("positionIdx", 0))
                    qty = dec(row.get("size", "0"))
                    position_side = str(row.get("side", ""))
                    if qty <= 0 or position_side not in {"Buy", "Sell"}:
                        continue
                    close_side = "Sell" if position_side == "Buy" else "Buy"
                    self._open_or_close_legs([(sym, close_side, qty, idx, True)], "dashboard_close_all")
            self.poll_wallet(force=True)
            self.state.bundles = []
            self.state.hedge_active = False
            self.state.hedge_anchor = None
            self.state.hedge_sign = 0
            self.state.auto_entry_paused = True
            self.state.halted = False
            self.state.last_close_reason = "DASHBOARD_CLOSE_ALL"
            self.db.event("DASHBOARD_COMMAND", action="close_all", status="completed", paused=True)
            self.save()
            return "Все позиции стратегии, включая хеджи, закрыты; автооткрытие приостановлено"
        except Exception as exc:
            self.db.event("DASHBOARD_COMMAND", action="close_all", status="failed", error=str(exc)[:500])
            self.state.halted = True
            self.save()
            raise
        finally:
            self._emergency_in_progress = False

    def process_dashboard_commands(self):
        if self.dashboard is None:
            return
        for action in self.dashboard.take_commands():
            try:
                if action == "close_all":
                    message = self.close_all_positions_from_dashboard()
                elif action == "pause_entries":
                    self.state.auto_entry_paused = True
                    self.db.event("DASHBOARD_COMMAND", action=action, status="completed")
                    self.save()
                    message = "Автооткрытие и доборы приостановлены; защитные хеджи и штатные закрытия разрешены"
                elif action == "resume_entries":
                    if self.state.halted:
                        raise RuntimeError("Бот остановлен по аварии; требуется ручная проверка")
                    self.state.auto_entry_paused = False
                    self.db.event("DASHBOARD_COMMAND", action=action, status="completed")
                    self.save()
                    message = "Автооткрытие и доборы снова разрешены"
                else:
                    raise RuntimeError("unknown command")
                self.dashboard.finish_command(action, "completed", message)
            except Exception as exc:
                self.dashboard.finish_command(action, "failed", str(exc)[:300])
                log.exception("Dashboard command failed: %s", action)

    def close_cycle(self, marks: dict[str,Decimal], bar_close_ms: int, reason: str):
        if self.state.hedge_active:
            self.close_hedge(bar_close_ms,"CYCLE_CLOSE")
        for sym in SYMBOLS:
            idx=position_idx(core_side(self.state.direction,sym))
            self.close_position_side(sym,idx,"cycleclose")
        self.poll_wallet(force=True)
        old_B=dec(self.state.B); old_step=dec(self.state.step); old_roi=dec(self.state.roi)
        if reason in ("ROI_TARGET","CROSSING_POSITIVE_PNL"):
            B_new=LIVE_WALLET*BTC_BUDGET_PCT
            P_new=LIVE_WALLET*ALT_P_BUDGET_PCT
            proc=B_new/old_B-Decimal("1") if old_B else Decimal("0")
            scale=Decimal("1")+proc
            self.state.B=str(B_new); self.state.P=str(P_new)
            self.state.step=str(old_step*scale); self.state.roi=str(old_roi*scale)
        self.db.event("CYCLE_CLOSE",reason=reason,closed_wallet=str(LIVE_WALLET),spread=self.bar_spread,
                      next_B=self.state.B,next_P=self.state.P,proc_pct=str((dec(self.state.B)/old_B-1)*100 if old_B else 0),
                      next_step=self.state.step,next_roi=self.state.roi,cycle_count=self.state.cycle_count)
        self.state.bundles=[]; self.state.hedge_active=False; self.state.hedge_anchor=None
        self.state.hedge_sign=0; self.state.rearm_anchor=None; self.state.rearm_sign=0
        self.state.last_close_reason=reason
        self.state.cycle_start_wallet=None; self.state.cycle_start_ms=None
        # Match backtest: no same-bar re-entry. Next closed hourly bar may seed the next cycle.
        self.state.last_bar_close_ms=bar_close_ms
        self.save()

    def handle_bar(self, close_ms: int, prices_f: dict[str,float]):
        self.current_bar_close_ms=close_ms
        close_iso=datetime.fromtimestamp(close_ms/1000,tz=timezone.utc).isoformat()
        prices={s:float(prices_f[s]) for s in SYMBOLS}
        if self.state.anchor_prices is None:
            self.state.anchor_prices=prices.copy()
            spread=0.0
            self.db.event("ANCHOR_INITIALIZED",time=close_iso,prices=prices,p2_scale=P2_SCALE,
                          note="P2 fixed from user's selected historical baseline")
        else:
            spread=spread_value(prices,self.state.anchor_prices)
        self.bar_spread=spread
        self.db.record_bar(close_iso,prices,spread)
        self.poll_wallet(force=True)
        marks=self.live_marks()
        positions=self.fetch_positions()
        self.db.record_snapshot(positions,spread,{"bar_close_ms":close_ms,"p2_scale":P2_SCALE,
            "step":self.state.step,"roi":self.state.roi,"hedge_active":self.state.hedge_active,
            "cycle_pnl":str(self.cycle_pnl(marks)) if self.state.bundles else None,
            "market_marks_and_funding":self.latest_mark_data})
        if self.state.halted:
            self.db.event("HALTED_BAR",time=close_iso,spread=spread)
            self.state.last_bar_close_ms=close_ms; self.save(); return

        self.protect_margin(marks,spread,close_ms)
        if self.state.halted:
            self.state.last_bar_close_ms=close_ms; self.save(); return

        just_closed=False
        if self.state.bundles and not self.state.hedge_active:
            pnl=self.cycle_pnl(marks)
            close_cost=self.gross_notional(marks,include_hedge=False)*(FEE_RATE_EST+SLIPPAGE_EST)
            projected_exit_net=pnl-close_cost
            roi_hit=projected_exit_net >= dec(self.state.cycle_start_wallet or "0")*dec(self.state.roi)
            crossing=(self.state.previous_spread is not None and self.state.previous_spread*spread<0 and projected_exit_net>0)
            if roi_hit or crossing:
                reason="ROI_TARGET" if roi_hit else "CROSSING_POSITIVE_PNL"
                self.close_cycle(marks,close_ms,reason); just_closed=True

        if not self.state.bundles and not just_closed and not self.state.halted and not self.state.auto_entry_paused:
            direction=1 if spread>0 else -1
            self.state.direction=direction
            self.state.ref_spread=spread
            if self.allow_orders:
                self.open_bundle(marks,direction,initial=True)
            else:
                self.db.event("DRY_SIGNAL_OPEN",spread=spread,direction=direction)
                # Dry mode does not simulate positions; retain a shadow cycle only in log.
        elif self.state.bundles and not self.state.halted and not self.state.hedge_active and not self.state.auto_entry_paused:
            direction=self.state.direction
            step=float(dec(self.state.step)); ref=self.state.ref_spread
            if direction==1 and spread>=ref+step:
                while spread>=ref+float(dec(self.state.step)) and not self.state.hedge_active:
                    next_ref=ref+float(dec(self.state.step))
                    if self.open_bundle(marks,direction,initial=False):
                        ref=next_ref; self.state.ref_spread=ref
                        self.protect_margin(marks,spread,close_ms)
                    else: break
            elif direction==-1 and spread<=ref-step:
                while spread<=ref-float(dec(self.state.step)) and not self.state.hedge_active:
                    next_ref=ref-float(dec(self.state.step))
                    if self.open_bundle(marks,direction,initial=False):
                        ref=next_ref; self.state.ref_spread=ref
                        self.protect_margin(marks,spread,close_ms)
                    else: break
            if self.state.bundles and not self.state.hedge_active:
                if direction==1 and spread<=ref-step and spread>0:
                    self.state.ref_spread-=step
                elif direction==-1 and spread>=ref+step and spread<0:
                    self.state.ref_spread+=step
        self.state.previous_spread=spread
        self.state.last_bar_close_ms=close_ms
        self.save()
        log.info("bar=%s spread=%.5f wallet=%s equity=%s available=%s step=%s roi=%s bundles=%d hedge=%s",
                 close_iso,spread,LIVE_WALLET,LIVE_EQUITY,LIVE_AVAILABLE,self.state.step,self.state.roi,
                 len(self.state.bundles or []),self.state.hedge_active)

    def startup_seed_pending(self) -> bool:
        return (self.allow_orders and not self.state.halted and self.state.cycle_count == 0
                and not self.state.bundles)

    def monitor_console(self):
        """Refresh and print PnL, grid/exit triggers, hedge and margin status every 5s."""
        now=time.monotonic()
        if now-self.last_monitor_poll < MONITOR_POLL_SECONDS:
            return
        self.poll_wallet(force=True)
        positions=self.fetch_positions()
        marks=self.live_marks()
        active=[]
        strategy_unrealized=Decimal("0")
        current_gross=Decimal("0")
        for symbol in SYMBOLS:
            for row in positions.get(symbol,[]):
                size=dec(row.get("size","0"))
                if size <= 0:
                    continue
                pnl=dec(row.get("unrealisedPnl",row.get("unrealizedPnl","0")))
                strategy_unrealized+=pnl
                current_gross+=size*marks[symbol]
                active.append((symbol,row,pnl))
        account_unrealized=LIVE_EQUITY-LIVE_WALLET
        cycle_net=None
        if self.state.cycle_start_wallet is not None:
            cycle_net=LIVE_WALLET-dec(self.state.cycle_start_wallet)+strategy_unrealized
        spread_mark=None
        if self.state.anchor_prices is not None:
            spread_mark=spread_value({s:float(marks[s]) for s in SYMBOLS},self.state.anchor_prices)
        direction_text={1:"BTC short / альты long",-1:"BTC long / альты short"}.get(self.state.direction,"не задано")
        log.info("МОНИТОР: кошелек=%.4f equity=%.4f доступно=%.4f accountUPnL=%+.4f strategyUPnL=%+.4f cycleNet=%s позиций=%d корзин=%d направление=%s",
                 LIVE_WALLET,LIVE_EQUITY,LIVE_AVAILABLE,account_unrealized,strategy_unrealized,
                 f"{cycle_net:+.4f}" if cycle_net is not None else "н/д",
                 len(active),len(self.state.bundles or []),direction_text)

        step=dec(self.state.step)
        if self.state.bundles and spread_mark is not None:
            if self.state.hedge_active:
                grid_text=f"добор заблокирован открытым хеджем; базовый STEP={step}"
            elif self.state.direction==1:
                target=self.state.ref_spread+float(step)
                gap=target-spread_mark
                touched="; уровень уже достигнут, ждём закрытия 1h" if gap<=0 else ""
                grid_text=f"добор на закрытии 1h при spread>= {target:.5f}; до уровня {max(0.0,gap):.5f}{touched}"
            elif self.state.direction==-1:
                target=self.state.ref_spread-float(step)
                gap=spread_mark-target
                touched="; уровень уже достигнут, ждём закрытия 1h" if gap<=0 else ""
                grid_text=f"добор на закрытии 1h при spread<= {target:.5f}; до уровня {max(0.0,gap):.5f}{touched}"
            else:
                grid_text="направление для следующего добора не задано"
        else:
            grid_text="активной корзины нет; первичный вход ожидается по стартовому правилу"
        last_hour_spread=(f"{self.state.previous_spread:.5f}" if self.state.previous_spread is not None else "н/д")
        log.info("СЕТКА: mark-spread=%s; spread последнего закрытия 1h=%s; STEP=%s; %s. Добор определяется уровнем спреда, не фиксированным убытком в USDT.",
                 f"{spread_mark:.5f}" if spread_mark is not None else "н/д",last_hour_spread,step,grid_text)

        if self.state.bundles and cycle_net is not None:
            close_cost=current_gross*(FEE_RATE_EST+SLIPPAGE_EST)
            projected_exit_net=cycle_net-close_cost
            roi_target=dec(self.state.cycle_start_wallet or "0")*dec(self.state.roi)
            remaining=roi_target-projected_exit_net
            roi_text=(f"ROI цель net после оценочных расходов={roi_target:.4f} USDT; "
                      f"сейчас net при закрытии≈{projected_exit_net:+.4f}; "
                      f"до цели {max(Decimal('0'),remaining):.4f} USDT; расходы закрытия≈{close_cost:.4f}")
            if self.state.hedge_active:
                roi_text+="; ROI/cross-close приостановлены до выхода из хеджа"
            else:
                roi_text+=f"; полный выход также при пересечении spread=0 на 1h, только если net после расходов > 0"
        else:
            roi_text="нет активного цикла для ROI-закрытия"
        log.info("ЗАКРЫТИЕ: ROI=%s%%; %s",dec(self.state.roi)*100,roi_text)
        global_start_wallet=dec(self.db.get("global_roi_start_wallet") or LIVE_WALLET)
        global_gross_target=global_start_wallet*(Decimal("1")+GLOBAL_ROI_TARGET)
        global_close_cost=current_gross*Decimal("2")*(FEE_RATE_EST+SLIPPAGE_EST)
        global_close_balance=global_gross_target+global_close_cost
        log.info("ОБЩЕЕ ЗАКРЫТИЕ ПО ROI: старт=%s; цель=%s%%; gross-порог=%s; расходы закрытия≈%s; итоговый порог=%s; до порога=%s",
                 global_start_wallet,GLOBAL_ROI_TARGET*100,global_gross_target,global_close_cost,
                 global_close_balance,max(Decimal("0"),global_close_balance-LIVE_EQUITY))

        if self.state.hedge_active and self.state.hedge_anchor is not None:
            hedge_buffer=float(step)*HEDGE_RETRACE_RATIO
            hedge_sign=self.state.hedge_sign or self.state.direction or 1
            hedge_target=self.state.hedge_anchor-hedge_sign*hedge_buffer
            comparator="<=" if hedge_sign>0 else ">="
            hedge_text=(f"ОТКРЫТ; выход при закрытии 1h spread {comparator} {hedge_target:.5f} "
                        f"(anchor={self.state.hedge_anchor:.5f}, буфер={hedge_buffer:.5f}); "
                        f"доступная маржа={LIVE_AVAILABLE:.4f}, trim-порог=$25, hard floor=$10")
        elif self.state.rearm_anchor is not None:
            rearm_buffer=float(step)*HEDGE_RETRACE_RATIO
            rearm_sign=self.state.rearm_sign or 1
            rearm_target=self.state.rearm_anchor+rearm_sign*rearm_buffer
            comparator=">=" if rearm_sign>0 else "<="
            earliest=datetime.fromtimestamp(self.state.earliest_rearm_ms/1000,tz=timezone.utc).isoformat()
            hedge_text=(f"хедж закрыт; повторное разрешение при spread {comparator} {rearm_target:.5f} "
                        f"и не раньше {earliest}; доступно={LIVE_AVAILABLE:.4f}, hard floor=$10")
        elif self.state.bundles:
            core_gross=sum((dec(bundle.get(s,"0"))*marks[s] for bundle in self.state.bundles for s in SYMBOLS),Decimal("0"))
            projected_after_hedge=LIVE_EQUITY-(current_gross+core_gross)/LEVERAGE-(current_gross+core_gross)*MMR_EST-core_gross*FEE_RATE_EST
            hedge_gate=min(projected_after_hedge,LIVE_AVAILABLE)
            hedge_text=(f"не открыт; оценка свободной маржи после зеркального хеджа={projected_after_hedge:.4f}; "
                        f"триггер guard при min(оценка, доступно)<=${HEDGE_OPEN_BUFFER_USDT+MIN_FREE_MARGIN_USDT:.0f}; "
                        f"резерв хеджа=${HEDGE_OPEN_BUFFER_USDT:.0f}, hard floor=${MIN_FREE_MARGIN_USDT:.0f}; "
                        f"текущий guard={hedge_gate:.4f}")
        else:
            hedge_text=f"не активен; hard floor свободной маржи=${MIN_FREE_MARGIN_USDT:.0f}"
        log.info("ХЕДЖ/МАРЖА: %s",hedge_text)

        if active:
            for symbol,row,pnl in active:
                log.info("ПОЗИЦИЯ %s side=%s idx=%s qty=%s avg=%s mark=%s uPnL=%+.4f USDT",
                         symbol,row.get("side","?"),row.get("positionIdx","?"),row.get("size","0"),
                         row.get("avgPrice","?"),row.get("markPrice","?"),pnl)
        else:
            log.info("ПОЗИЦИИ: открытых позиций по инструментам стратегии нет")
        try:
            payload = self.build_telemetry_payload(positions, marks, strategy_unrealized, cycle_net, current_gross)
            if self.dashboard is not None:
                self.dashboard.publish(payload, "running")
        except Exception as exc:
            log.warning("Локальный дашборд: снимок недоступен (%s); торговый цикл бота продолжает работу", type(exc).__name__)
        self.last_monitor_poll=time.monotonic()

    def build_telemetry_payload(self, positions: dict, marks: dict[str, Decimal],
                                strategy_unrealized: Decimal, cycle_net: Decimal | None,
                                current_gross: Decimal) -> dict:
        def number(value):
            result = float(value)
            if not math.isfinite(result):
                raise ValueError("Non-finite telemetry number")
            return result

        def optional_number(value):
            return None if value is None else number(value)

        anchors = self.state.anchor_prices or {}
        mark_prices = {symbol: number(marks[symbol]) for symbol in SYMBOLS}
        normalized_btc = None
        normalized_alt_sum = None
        normalized_alt_equal = None
        mark_spread = None
        if all(symbol in anchors and float(anchors[symbol]) > 0 for symbol in SYMBOLS):
            normalized_btc = mark_prices["BTCUSDT"] / float(anchors["BTCUSDT"])
            alt_anchor_sum = sum(float(anchors[symbol]) for symbol in SYMBOLS[1:])
            normalized_alt_sum = sum(mark_prices[symbol] for symbol in SYMBOLS[1:]) / alt_anchor_sum
            normalized_alt_equal = sum(mark_prices[symbol] / float(anchors[symbol])
                                       for symbol in SYMBOLS[1:]) / 3
            mark_spread = spread_value(mark_prices, anchors)

        cycle_start = dec(self.state.cycle_start_wallet) if self.state.cycle_start_wallet is not None else None
        roi_target = cycle_start * dec(self.state.roi) if cycle_start is not None else None
        close_cost = current_gross * (FEE_RATE_EST + SLIPPAGE_EST)
        estimated_exit_net = cycle_net - close_cost if cycle_net is not None else None
        roi_remaining = max(Decimal("0"), roi_target - estimated_exit_net) \
            if roi_target is not None and estimated_exit_net is not None else None
        global_start_wallet = dec(self.db.get("global_roi_start_wallet") or LIVE_WALLET)
        global_gross_target = global_start_wallet * (Decimal("1") + GLOBAL_ROI_TARGET)
        global_close_cost = current_gross * Decimal("2") * (FEE_RATE_EST + SLIPPAGE_EST)
        global_close_balance = global_gross_target + global_close_cost

        hedge_exit_level = None
        if self.state.hedge_active and self.state.hedge_anchor is not None:
            hedge_sign = self.state.hedge_sign or self.state.direction or 1
            hedge_exit_level = self.state.hedge_anchor - hedge_sign * float(dec(self.state.step)) * HEDGE_RETRACE_RATIO
        rearm_level = None
        if self.state.rearm_anchor is not None:
            rearm_sign = self.state.rearm_sign or 1
            rearm_level = self.state.rearm_anchor + rearm_sign * float(dec(self.state.step)) * HEDGE_RETRACE_RATIO

        next_add_level = None
        next_add_direction = None
        if self.state.bundles and not self.state.hedge_active:
            if self.state.direction == 1:
                next_add_level = self.state.ref_spread + float(dec(self.state.step))
                next_add_direction = "above"
            elif self.state.direction == -1:
                next_add_level = self.state.ref_spread - float(dec(self.state.step))
                next_add_direction = "below"

        core_gross = sum((dec(bundle.get(symbol, "0")) * marks[symbol]
                          for bundle in self.state.bundles or [] for symbol in SYMBOLS), Decimal("0"))
        projected_free_after_hedge = None
        hedge_guard_level = LIVE_AVAILABLE
        if self.state.bundles and not self.state.hedge_active:
            projected_free_after_hedge = (LIVE_EQUITY - (current_gross + core_gross) / LEVERAGE
                                          - (current_gross + core_gross) * MMR_EST
                                          - core_gross * FEE_RATE_EST)
            hedge_guard_level = min(projected_free_after_hedge, LIVE_AVAILABLE)

        position_rows = []
        for symbol in SYMBOLS:
            for row in positions.get(symbol, []):
                size = dec(row.get("size", "0"))
                if size <= 0:
                    continue
                side = row.get("side")
                if side not in ("Buy", "Sell"):
                    continue
                position = {
                    "symbol": symbol,
                    "side": side,
                    "positionIdx": int(row.get("positionIdx", 0)),
                    "size": number(size),
                    "avgPrice": number(row.get("avgPrice", "0")),
                    "markPrice": number(row.get("markPrice") or marks[symbol]),
                    "unrealizedPnl": number(row.get("unrealisedPnl", row.get("unrealizedPnl", "0"))),
                }
                if row.get("leverage") is not None:
                    position["leverage"] = str(row["leverage"])
                if row.get("liqPrice") is not None:
                    position["liquidationPrice"] = str(row["liqPrice"])
                position_rows.append(position)

        funding_rates = {}
        next_funding_times = {}
        for symbol in SYMBOLS:
            market_data = self.latest_mark_data.get(symbol, {})
            raw_funding = market_data.get("fundingRate")
            funding_rates[symbol] = optional_number(raw_funding) if raw_funding is not None else None
            next_funding_times[symbol] = str(market_data["nextFundingTime"]) \
                if market_data.get("nextFundingTime") is not None else None

        direction_label = {1: "BTC шорт / альты лонг", -1: "BTC лонг / альты шорт"}.get(
            self.state.direction, "не задано")
        last_bar_at = datetime.fromtimestamp(self.state.last_bar_close_ms / 1000, tz=timezone.utc).isoformat() \
            if self.state.last_bar_close_ms > 0 else None
        return {
            "schemaVersion": 1,
            "sourceId": self.source_id,
            "sampleId": str(uuid.uuid4()),
            "observedAt": utc_now(),
            "account": {
                "wallet": number(LIVE_WALLET),
                "equity": number(LIVE_EQUITY),
                "available": number(LIVE_AVAILABLE),
                "accountUnrealizedPnl": number(LIVE_EQUITY - LIVE_WALLET),
            },
            "globalRoi": {
                "startWallet": number(global_start_wallet),
                "target": number(GLOBAL_ROI_TARGET),
                "grossTargetBalance": number(global_gross_target),
                "estimatedCloseCosts": number(global_close_cost),
                "closeBalance": number(global_close_balance),
                "currentEquity": number(LIVE_EQUITY),
                "remainingToClose": number(max(Decimal("0"), global_close_balance - LIVE_EQUITY)),
            },
            "strategy": {
                "running": True,
                "halted": self.state.halted,
                "autoEntryPaused": self.state.auto_entry_paused,
                "autoEntryStatus": "paused" if self.state.auto_entry_paused else "enabled",
                "direction": self.state.direction,
                "directionLabel": direction_label,
                "bundles": len(self.state.bundles or []),
                "cycleCount": self.state.cycle_count,
                "cycleStartWallet": optional_number(cycle_start),
                "walletDelta": number(LIVE_WALLET - cycle_start) if cycle_start is not None else None,
                "unrealizedPnl": number(strategy_unrealized),
                "cycleNetPnl": optional_number(cycle_net),
                "step": number(dec(self.state.step)),
                "roi": number(dec(self.state.roi)),
                "roiTarget": optional_number(roi_target),
                "estimatedExitNet": optional_number(estimated_exit_net),
                "roiRemaining": optional_number(roi_remaining),
                "btcBudget": number(dec(self.state.B)),
                "altBudgetEach": number(dec(self.state.P)),
                "hedgeActive": self.state.hedge_active,
                "hedgeAnchor": optional_number(self.state.hedge_anchor),
                "hedgeExitLevel": optional_number(hedge_exit_level),
                "rearmAnchor": optional_number(self.state.rearm_anchor),
                "rearmLevel": optional_number(rearm_level),
                "nextAddLevel": optional_number(next_add_level),
                "nextAddDirection": next_add_direction,
                "marginFloor": number(MIN_FREE_MARGIN_USDT),
                "hedgeReserve": number(HEDGE_OPEN_BUFFER_USDT),
                "hedgeGuardLevel": number(hedge_guard_level),
                "projectedFreeAfterHedge": optional_number(projected_free_after_hedge),
                "lastBarAt": last_bar_at,
                "lastCloseReason": self.state.last_close_reason,
            },
            "market": {
                "p2Scale": number(P2_SCALE),
                "anchorPrices": {symbol: number(anchors[symbol]) for symbol in SYMBOLS if symbol in anchors},
                "markPrices": mark_prices,
                "fundingRates": funding_rates,
                "nextFundingTimes": next_funding_times,
                "normalizedBtc": optional_number(normalized_btc),
                "normalizedAltSum": optional_number(normalized_alt_sum),
                "normalizedAltEqual": optional_number(normalized_alt_equal),
                "markSpread": optional_number(mark_spread),
                "closeSpread": optional_number(self.state.previous_spread),
                "referenceSpread": optional_number(self.state.ref_spread) if self.state.bundles else None,
                "previousSpread": optional_number(self.state.previous_spread),
            },
            "positions": position_rows,
            "events": self.db.telemetry_events(),
        }

    def run_forever(self):
        self.preflight()
        log.warning("Bybit Demo only. allow_demo_orders=%s. LIVE trading is not supported by this file.",self.allow_orders)
        startup_seed_pending=self.startup_seed_pending()
        while True:
            try:
                self.process_dashboard_commands()
                self.poll_wallet()
                self.monitor_console()
                if self.allow_orders and self.state.bundles and LIVE_AVAILABLE < MIN_FREE_MARGIN_USDT:
                    self.emergency_flatten("LIVE_AVAILABLE_BELOW_10_USDT_POLL_GUARD")
                latest=self.latest_closed_bars(allow_startup_replay=startup_seed_pending)
                if latest:
                    close_ms,prices=latest
                    self.handle_bar(close_ms,prices)
                    startup_seed_pending=False
                time.sleep(LOOP_SLEEP_SECONDS)
            except KeyboardInterrupt:
                self.db.event("PROCESS_STOP",reason="keyboard_interrupt")
                log.warning("Stopped. Any open Demo positions remain on Bybit; inspect them manually.")
                return
            except TransientAPIError as exc:
                self.db.event("API_TEMPORARY_UNAVAILABLE",error_type=type(exc).__name__,error=str(exc)[:1000])
                log.warning("Bybit API temporarily unavailable; no new orders will be submitted while reconnecting: %s",exc)
                time.sleep(15)
                continue
            except Exception as exc:
                self.db.event("FATAL_STOP",error_type=type(exc).__name__,error=str(exc)[:1000])
                log.exception("Fatal stop; no further orders will be submitted. Inspect exchange positions manually.")
                self.state.halted=True; self.save()
                return


def demo_orders_enabled_from_env() -> bool:
    setting=os.environ.get("ALLOW_DEMO_ORDERS","YES").strip().upper()
    if setting not in {"YES","NO"}:
        raise RuntimeError("ALLOW_DEMO_ORDERS must be YES or NO")
    return setting == "YES"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Локальный Bybit Demo-бот и read-only дашборд")
    parser.add_argument("--dashboard-only", action="store_true", help="открыть локальную историю без подключения к Bybit")
    parser.add_argument("--host", default=os.environ.get("LOCAL_DASHBOARD_HOST", "127.0.0.1"), help="только localhost / 127.0.0.1 (по умолчанию 127.0.0.1)")
    parser.add_argument("--port", default=os.environ.get("LOCAL_DASHBOARD_PORT", "8787"), help="порт локальной панели (по умолчанию 8787)")
    return parser.parse_args(argv)


def main(argv=None):
    if str(START_BALANCE_USDT) != "500":
        raise RuntimeError("Safety invariant: expected start balance config is not $500")
    args = parse_args(argv)
    host, port = _local_host(args.host), _local_port(args.port)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    dashboard = LocalDashboard(DB_PATH)
    if args.dashboard_only:
        dashboard.set_status("dashboard-only", "Режим истории: подключение к Bybit и торговые действия отключены.")
    try:
        server = dashboard.make_server(host, port)
    except OSError as exc:
        raise RuntimeError(f"Не удалось открыть локальный адрес http://{host}:{port}: {exc}") from exc
    url_host = "[::1]" if host == "::1" else host
    local_url = f"http://{url_host}:{port}"
    print(f"\nЛокальный дашборд: {local_url}")
    print("Панель только для просмотра; удалённых команд, внешней телеметрии и mainnet в этом файле нет.")

    if not args.dashboard_only:
        try:
            orders_enabled = demo_orders_enabled_from_env()
            session = make_session()
            store = Store(DB_PATH)
            bot = DemoBot(session, store, orders_enabled, dashboard=dashboard)
            def run_bot():
                dashboard.set_status("starting", "Выполняется Demo preflight; следите за консолью и журналом.")
                try:
                    bot.run_forever()
                    status = "halted" if bot.state.halted else "stopped"
                    dashboard.set_status(status, "Торговый процесс завершён; дашборд остаётся доступным для просмотра истории.")
                except Exception as exc:
                    log.exception("Demo bot did not start or stopped before the main loop")
                    dashboard.set_status("error", f"Бот остановлен: {type(exc).__name__}: {str(exc)[:350]}")
                finally:
                    store.close()
            threading.Thread(target=run_bot, name="bybit-demo-bot", daemon=True).start()
        except Exception as exc:
            log.exception("Configuration error; dashboard remains available")
            dashboard.set_status("configuration-error", f"Конфигурация бота: {type(exc).__name__}: {str(exc)[:350]}")

    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nОстановка локального сервера. Открытые Demo-позиции не закрываются автоматически; проверяйте их в Bybit Demo.")
    finally:
        server.server_close()


if __name__=="__main__":
    main()

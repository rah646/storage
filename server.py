#!/usr/bin/env python3
"""
Token Server - Storage Queue
Receives tokens from yidun_proxyless.py, gen.py, and ab.py
"""

import os
import time
import threading
import argparse
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ============================================================
#  CONFIG
# ============================================================
TOKEN_TTL = 180  # seconds (3 minutes)

# ============================================================
#  STATE
# ============================================================
_lock = threading.Lock()
_token_queue = deque()
_stats = {
    "received": 0,
    "served": 0,
    "expired": 0,
    "duplicates": 0,
    "flushed": 0,
    "start_time": time.time(),
    "peak_queue": 0,
    "last_received": None,
    "last_served": None,
}

# ============================================================
#  CLEANUP THREAD
# ============================================================
def _purge_expired():
    now = time.time()
    removed = 0
    while _token_queue and (now - _token_queue[0]["ts"]) > TOKEN_TTL:
        _token_queue.popleft()
        removed += 1
    _stats["expired"] += removed
    return removed

def _cleanup_loop():
    while True:
        time.sleep(10)
        with _lock:
            _purge_expired()

_cleaner = threading.Thread(target=_cleanup_loop, daemon=True)
_cleaner.start()

# ============================================================
#  API ENDPOINTS
# ============================================================

@app.route("/api/save-token", methods=["POST"])
def receive_token():
    """Receive token from solver"""
    data = request.get_json(silent=True)
    if not data or "token" not in data:
        return jsonify({"error": "missing 'token' field"}), 400

    token = str(data["token"]).strip()
    if not token:
        return jsonify({"error": "empty token"}), 400

    with _lock:
        _purge_expired()
        now = time.time()
        _token_queue.append({"token": token, "ts": now})
        _stats["received"] += 1
        _stats["last_received"] = datetime.now().isoformat()
        queue_size = len(_token_queue)
        if queue_size > _stats["peak_queue"]:
            _stats["peak_queue"] = queue_size

    return jsonify({
        "status": "ok",
        "queue_size": queue_size,
        "total_received": _stats["received"],
    }), 200


@app.route("/api/get-token", methods=["GET"])
def get_token():
    """Get 1 token (removes from queue)"""
    with _lock:
        _purge_expired()
        if _token_queue:
            entry = _token_queue.popleft()
            _stats["served"] += 1
            _stats["last_served"] = datetime.now().isoformat()
            return jsonify({
                "token": entry["token"],
                "remaining": len(_token_queue),
                "age_seconds": round(time.time() - entry["ts"], 1),
            }), 200
        else:
            return jsonify({"error": "no tokens available", "remaining": 0}), 404


@app.route("/api/token/bulk", methods=["GET"])
def get_tokens_bulk():
    """Get multiple tokens (removes from queue)"""
    n = request.args.get("n", 1, type=int)
    n = max(1, min(n, 100))

    tokens = []
    with _lock:
        _purge_expired()
        for _ in range(n):
            if _token_queue:
                entry = _token_queue.popleft()
                tokens.append(entry["token"])
                _stats["served"] += 1
            else:
                break
        if tokens:
            _stats["last_served"] = datetime.now().isoformat()

    return jsonify({
        "tokens": tokens,
        "count": len(tokens),
        "remaining": len(_token_queue),
    }), 200


@app.route("/api/status", methods=["GET"])
def status():
    """Queue status and statistics"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0

        recent_tokens = []
        for item in list(_token_queue)[-5:]:
            recent_tokens.append({
                "token": item["token"][:40] + "...",
                "age": round(time.time() - item["ts"], 1)
            })

        return jsonify({
            "queue_size": len(_token_queue),
            "total_received": _stats["received"],
            "total_served": _stats["served"],
            "total_expired": _stats["expired"],
            "total_duplicates": _stats["duplicates"],
            "total_flushed": _stats["flushed"],
            "peak_queue": _stats["peak_queue"],
            "uptime_seconds": round(elapsed, 1),
            "tokens_per_minute": round(rate, 2),
            "token_ttl_seconds": TOKEN_TTL,
            "last_received": _stats["last_received"],
            "last_served": _stats["last_served"],
            "recent_tokens": recent_tokens,
        }), 200


@app.route("/api/tokens", methods=["DELETE"])
def flush_tokens():
    """Delete all tokens from queue"""
    with _lock:
        count = len(_token_queue)
        _token_queue.clear()
        _stats["flushed"] += count
    return jsonify({"status": "flushed", "removed": count}), 200


@app.route("/api/tokens/count", methods=["GET"])
def token_count():
    """Get token count only"""
    with _lock:
        _purge_expired()
        return jsonify({
            "queue_size": len(_token_queue),
            "total_received": _stats["received"],
            "total_served": _stats["served"],
        }), 200


@app.route("/", methods=["GET"])
def dashboard():
    """Premium glass-morphism dashboard with GIF background and Telegram link"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0
        q = len(_token_queue)
        peak = _stats["peak_queue"] or 1
        bar_pct = min(int(q / peak * 100), 100) if peak else 0

        # Build token list with colors
        token_html = ""
        for item in list(_token_queue)[-15:]:
            age = round(time.time() - item["ts"], 1)
            if age < 60:
                color = "#34d399"
                status = "Fresh"
            elif age < 120:
                color = "#fbbf24"
                status = "Aging"
            else:
                color = "#f87171"
                status = "Expiring"
            
            token_preview = item["token"][:45] + "..." if len(item["token"]) > 45 else item["token"]
            token_html += f'''
            <div class="token-row">
                <span class="token-text" style="color:{color}">{token_preview}</span>
                <span class="token-meta">
                    <span class="token-age">{age}s</span>
                    <span class="token-status" style="color:{color}">{status}</span>
                </span>
            </div>
            '''

        if not token_html:
            token_html = '<div class="empty-state">🎯 No tokens in queue</div>'

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>⚡ TOKEN SERVER · CHAOS EDITION</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" />
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            padding: 16px;
            background: url('https://media.giphy.com/media/v1.Y2lkPWVjZjA1ZTQ3MzhsMm8yZDFndjVmYW14NWtxMXplOXk2eGRudjUwMmVha3VuYmd0NyZlcD12MV9naWZzX3NlYXJjaCZjdD1n/ZZYRRgchnlohKonlSV/giphy.gif') center/cover no-repeat fixed;
            background-color: #0b0e14;
            position: relative;
        }}

        body::before {{
            content: '';
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.75);
            backdrop-filter: blur(6px) saturate(180%);
            -webkit-backdrop-filter: blur(6px) saturate(180%);
            z-index: 0;
        }}

        .container {{
            display: flex;
            flex-wrap: wrap;
            background: rgba(10, 14, 23, 0.65);
            backdrop-filter: blur(24px) brightness(1.1);
            -webkit-backdrop-filter: blur(24px) brightness(1.1);
            border-radius: 40px;
            border: 2px solid rgba(255, 255, 255, 0.12);
            box-shadow: 0 40px 80px rgba(0, 0, 0, 0.8), inset 0 0 80px rgba(255, 255, 255, 0.03);
            overflow: hidden;
            max-width: 1200px;
            width: 100%;
            position: relative;
            z-index: 1;
        }}

        .stats-side {{
            flex: 1.2;
            min-width: 280px;
            padding: 40px 36px;
            background: rgba(0, 0, 0, 0.25);
            border-right: 1px solid rgba(255, 255, 255, 0.04);
        }}

        .tokens-side {{
            flex: 1;
            min-width: 260px;
            padding: 40px 36px;
            background: rgba(0, 0, 0, 0.15);
        }}

        .logo {{
            font-size: 30px;
            font-weight: 800;
            color: #fff;
            margin-bottom: 28px;
            letter-spacing: 1px;
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 10px 16px;
            text-shadow: 0 0 20px rgba(0, 150, 255, 0.3);
        }}

        .logo span {{ color: rgba(255, 255, 255, 0.25); }}
        .logo .badge {{
            font-size: 11px;
            background: rgba(255, 70, 70, 0.25);
            color: #ff6b6b;
            padding: 4px 14px;
            border-radius: 40px;
            font-weight: 600;
            letter-spacing: 0.5px;
            border: 1px solid rgba(255, 70, 70, 0.2);
            text-transform: uppercase;
        }}

        .owner-link {{
            margin-left: auto;
            display: inline-flex;
            align-items: center;
            gap: 8px;
            color: #8ab4f8;
            text-decoration: none;
            font-size: 14px;
            font-weight: 600;
            background: rgba(0, 136, 204, 0.15);
            padding: 6px 18px 6px 14px;
            border-radius: 40px;
            border: 1px solid rgba(0, 136, 204, 0.25);
            transition: 0.25s ease;
            text-shadow: 0 0 12px rgba(0, 150, 255, 0.2);
        }}
        .owner-link i {{ font-size: 18px; color: #26A5E4; }}
        .owner-link:hover {{
            background: rgba(0, 136, 204, 0.3);
            border-color: #26A5E4;
            transform: scale(1.03);
            box-shadow: 0 0 30px rgba(38, 165, 228, 0.2);
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
            margin-bottom: 28px;
        }}

        .stat-card {{
            background: rgba(255, 255, 255, 0.04);
            border-radius: 16px;
            padding: 18px 14px;
            border: 1px solid rgba(255, 255, 255, 0.04);
            transition: 0.2s;
            backdrop-filter: blur(4px);
        }}
        .stat-card:hover {{
            background: rgba(255, 255, 255, 0.08);
            transform: translateY(-3px);
            border-color: rgba(255, 255, 255, 0.08);
        }}

        .stat-card .value {{
            font-size: 32px;
            font-weight: 800;
            color: white;
            line-height: 1.1;
            letter-spacing: -0.5px;
        }}
        .stat-card .label {{
            font-size: 10px;
            color: rgba(255, 255, 255, 0.35);
            text-transform: uppercase;
            letter-spacing: 1.5px;
            margin-top: 6px;
            font-weight: 600;
        }}

        .stat-card .value.green {{ color: #4ade80; text-shadow: 0 0 20px rgba(74, 222, 128, 0.3); }}
        .stat-card .value.blue {{ color: #60a5fa; text-shadow: 0 0 20px rgba(96, 165, 250, 0.3); }}
        .stat-card .value.orange {{ color: #fbbf24; text-shadow: 0 0 20px rgba(251, 191, 36, 0.3); }}
        .stat-card .value.pink {{ color: #f472b6; text-shadow: 0 0 20px rgba(244, 114, 182, 0.3); }}

        .progress-section {{
            margin-bottom: 24px;
        }}
        .progress-section .label-row {{
            display: flex;
            justify-content: space-between;
            font-size: 12px;
            color: rgba(255, 255, 255, 0.3);
            margin-bottom: 6px;
            font-weight: 500;
            letter-spacing: 0.3px;
        }}
        .progress-bar {{
            height: 8px;
            background: rgba(255, 255, 255, 0.04);
            border-radius: 20px;
            overflow: hidden;
            box-shadow: inset 0 0 10px rgba(0,0,0,0.5);
        }}
        .progress-bar .fill {{
            height: 100%;
            background: linear-gradient(90deg, #a78bfa, #f472b6, #fb923c);
            border-radius: 20px;
            transition: width 0.6s cubic-bezier(0.34, 1.56, 0.64, 1);
            width: {bar_pct}%;
            box-shadow: 0 0 20px rgba(167, 139, 250, 0.3);
        }}

        .info-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px 28px;
            font-size: 13px;
            color: rgba(255, 255, 255, 0.3);
            margin-bottom: 20px;
        }}
        .info-row strong {{
            color: rgba(255, 255, 255, 0.7);
            font-weight: 600;
        }}

        .api-endpoints {{
            margin-top: 16px;
            padding-top: 20px;
            border-top: 1px solid rgba(255, 255, 255, 0.04);
        }}
        .api-endpoints h4 {{
            color: rgba(255, 255, 255, 0.2);
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 2px;
            margin-bottom: 12px;
        }}
        .endpoint {{
            display: inline-block;
            font-size: 11px;
            font-family: 'JetBrains Mono', 'SF Mono', monospace;
            color: rgba(255, 255, 255, 0.3);
            background: rgba(255, 255, 255, 0.02);
            padding: 5px 14px;
            border-radius: 30px;
            margin: 0 4px 8px 0;
            border: 1px solid rgba(255, 255, 255, 0.03);
        }}
        .endpoint .method {{
            color: #60a5fa;
            font-weight: 700;
        }}

        .tokens-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 18px;
        }}
        .tokens-header h3 {{
            color: white;
            font-size: 17px;
            font-weight: 700;
            letter-spacing: -0.3px;
        }}
        .tokens-header .count {{
            font-size: 12px;
            color: rgba(255, 255, 255, 0.25);
            background: rgba(255, 255, 255, 0.03);
            padding: 4px 16px;
            border-radius: 40px;
            border: 1px solid rgba(255, 255, 255, 0.03);
        }}

        .tokens-list {{
            max-height: 420px;
            overflow-y: auto;
            padding-right: 4px;
        }}
        .tokens-list::-webkit-scrollbar {{ width: 4px; }}
        .tokens-list::-webkit-scrollbar-track {{ background: rgba(255,255,255,0.02); border-radius: 10px; }}
        .tokens-list::-webkit-scrollbar-thumb {{ background: rgba(255,255,255,0.08); border-radius: 10px; }}

        .token-row {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.02);
            gap: 12px;
        }}
        .token-row:last-child {{ border-bottom: none; }}

        .token-text {{
            font-family: 'SF Mono', 'Courier New', monospace;
            font-size: 12px;
            word-break: break-all;
            flex: 1;
            letter-spacing: -0.2px;
        }}
        .token-meta {{
            display: flex;
            align-items: center;
            gap: 12px;
            flex-shrink: 0;
        }}
        .token-age {{
            font-size: 11px;
            color: rgba(255, 255, 255, 0.2);
            font-family: 'SF Mono', monospace;
        }}
        .token-status {{
            font-size: 9px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            padding: 2px 8px;
            border-radius: 20px;
            background: rgba(255,255,255,0.03);
        }}

        .empty-state {{
            text-align: center;
            color: rgba(255, 255, 255, 0.1);
            padding: 50px 0;
            font-size: 15px;
            letter-spacing: 1px;
        }}

        @media (max-width: 760px) {{
            .container {{ flex-direction: column; border-radius: 28px; }}
            .stats-side {{ border-right: none; border-bottom: 1px solid rgba(255,255,255,0.04); }}
            .stats-side, .tokens-side {{ padding: 30px 24px; }}
            .owner-link {{ margin-left: 0; }}
            .logo {{ gap: 8px; }}
            .stats-grid {{ grid-template-columns: 1fr 1fr; gap: 8px; }}
            .stat-card .value {{ font-size: 26px; }}
        }}

        @media (max-width: 460px) {{
            .stats-grid {{ grid-template-columns: 1fr 1fr; }}
            .stat-card {{ padding: 12px; }}
            .stat-card .value {{ font-size: 22px; }}
            .token-row {{ flex-direction: column; align-items: flex-start; gap: 2px; }}
            .token-meta {{ width: 100%; justify-content: space-between; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="stats-side">
            <div class="logo">
                ⚡ TOKEN<span>SERVER</span>
                <span class="badge">v2.0 · CHAOS</span>
                <a href="https://t.me/KeemSGHLL" target="_blank" class="owner-link">
                    <i class="fab fa-telegram-plane"></i> @KeemSGHLL
                </a>
            </div>

            <div class="stats-grid">
                <div class="stat-card">
                    <div class="value green">{q}</div>
                    <div class="label">Queue</div>
                </div>
                <div class="stat-card">
                    <div class="value blue">{_stats["received"]}</div>
                    <div class="label">Received</div>
                </div>
                <div class="stat-card">
                    <div class="value orange">{_stats["served"]}</div>
                    <div class="label">Served</div>
                </div>
                <div class="stat-card">
                    <div class="value pink">{_stats["expired"]}</div>
                    <div class="label">Expired</div>
                </div>
            </div>

            <div class="progress-section">
                <div class="label-row">
                    <span>QUEUE CAPACITY</span>
                    <span>{q} / {peak}</span>
                </div>
                <div class="progress-bar">
                    <div class="fill" style="width: {bar_pct}%;"></div>
                </div>
            </div>

            <div class="info-row">
                <span>⏱ <strong>{round(elapsed, 1)}s</strong> uptime</span>
                <span>⚡ <strong>{rate:.1f}</strong> tokens/min</span>
                <span>📦 <strong>{_stats["peak_queue"]}</strong> peak</span>
            </div>

            <div class="api-endpoints">
                <h4>⚡ ENDPOINTS</h4>
                <span class="endpoint"><span class="method">POST</span> /api/save-token</span>
                <span class="endpoint"><span class="method">GET</span> /api/get-token</span>
                <span class="endpoint"><span class="method">GET</span> /api/token/bulk?n=5</span>
                <span class="endpoint"><span class="method">GET</span> /api/status</span>
                <span class="endpoint"><span class="method">DELETE</span> /api/tokens</span>
            </div>
        </div>

        <div class="tokens-side">
            <div class="tokens-header">
                <h3>📋 RECENT TOKENS</h3>
                <span class="count">{q} in queue</span>
            </div>
            <div class="tokens-list">
                {token_html}
            </div>
        </div>
    </div>
</body>
</html>"""

        return html, 200, {"Content-Type": "text/html"}


@app.route("/health", methods=["GET"])
def health():
    """Health check"""
    return jsonify({
        "ok": True,
        "uptime": round(time.time() - _stats["start_time"], 1),
        "queue_size": len(_token_queue),
        "total_received": _stats["received"],
    })


# ============================================================
#  MAIN
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Token Server v2")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=5050, help="Port (default 5050)")
    parser.add_argument("--ttl", type=int, default=180, help="Token TTL in seconds")
    args = parser.parse_args()

    TOKEN_TTL = args.ttl

    print(f"""
[ Token Server v2.0 ]
  Mode   : RAM Only (NO STORAGE)
  Port   : {args.port}
  TTL    : {args.ttl}s
  URL    : http://{args.host}:{args.port}

  POST   /api/save-token
  GET    /api/get-token
  GET    /api/token/bulk?n=5
  GET    /api/status
  DELETE /api/tokens
  GET    / (Dashboard)
""")

    app.run(host=args.host, port=args.port, debug=False, threaded=True)

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
    """Modern full-width dashboard with shattered layout"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0
        q = len(_token_queue)
        peak = _stats["peak_queue"] or 1
        bar_pct = min(int(q / peak * 100), 100) if peak else 0

        # Build token list
        token_html = ""
        for item in list(_token_queue)[-20:]:
            age = round(time.time() - item["ts"], 1)
            if age < 60:
                color = "#10b981"
                status = "fresh"
            elif age < 120:
                color = "#f59e0b"
                status = "aging"
            else:
                color = "#ef4444"
                status = "expiring"
            
            token_preview = item["token"][:50] + "..." if len(item["token"]) > 50 else item["token"]
            token_html += f'''
            <div class="token-entry">
                <span class="token-value" style="color:{color}">{token_preview}</span>
                <div class="token-meta">
                    <span class="token-age">{age}s</span>
                    <span class="token-badge" style="background:{color}22; color:{color}">{status}</span>
                </div>
            </div>
            '''

        if not token_html:
            token_html = '<div class="empty-state">No tokens in queue</div>'

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Token Server · v2.0</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
            background: #0a0a0f;
            color: #e2e8f0;
            min-height: 100vh;
            background-image: url('https://media.giphy.com/media/v1.Y2lkPWVjZjA1ZTQ3MzhsMm8yZDFndjVmYW14NWtxMXplOXk2eGRudjUwMmVha3VuYmd0NyZlcD12MV9naWZzX3NlYXJjaCZjdD1n/ZZYRRgchnlohKonlSV/giphy.gif');
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
            position: relative;
        }}

        body::before {{
            content: '';
            position: fixed;
            inset: 0;
            background: rgba(10, 10, 15, 0.82);
            backdrop-filter: blur(8px);
            z-index: 0;
        }}

        /* ===== LAYOUT GRID ===== */
        .app {{
            position: relative;
            z-index: 1;
            max-width: 1440px;
            margin: 0 auto;
            padding: 24px 32px;
            min-height: 100vh;
            display: grid;
            grid-template-columns: 380px 1fr;
            grid-template-rows: auto 1fr;
            gap: 24px;
        }}

        /* ===== HEADER ===== */
        .header {{
            grid-column: 1 / -1;
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 20px 28px;
            background: rgba(255, 255, 255, 0.04);
            border-radius: 16px;
            border: 1px solid rgba(255, 255, 255, 0.06);
            backdrop-filter: blur(12px);
        }}

        .brand {{
            display: flex;
            align-items: center;
            gap: 14px;
        }}

        .brand-icon {{
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, #6366f1, #8b5cf6);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 18px;
            color: white;
        }}

        .brand h1 {{
            font-size: 22px;
            font-weight: 600;
            letter-spacing: -0.5px;
            background: linear-gradient(135deg, #f0f0f0, #94a3b8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .brand small {{
            font-size: 13px;
            color: #64748b;
            font-weight: 400;
            -webkit-text-fill-color: #64748b;
            background: none;
            margin-left: 6px;
        }}

        .header-actions {{
            display: flex;
            align-items: center;
            gap: 16px;
        }}

        .telegram-link {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            color: #94a3b8;
            text-decoration: none;
            font-size: 14px;
            padding: 8px 18px;
            border-radius: 10px;
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid rgba(255, 255, 255, 0.06);
            transition: all 0.2s;
            font-weight: 500;
        }}

        .telegram-link i {{
            color: #3b82f6;
            font-size: 16px;
        }}

        .telegram-link:hover {{
            background: rgba(59, 130, 246, 0.12);
            border-color: rgba(59, 130, 246, 0.25);
            color: #e2e8f0;
        }}

        .status-dot {{
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #10b981;
            margin-right: 6px;
            animation: pulse 2s infinite;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.3; }}
        }}

        /* ===== LEFT PANEL ===== */
        .sidebar {{
            display: flex;
            flex-direction: column;
            gap: 20px;
        }}

        .panel {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 16px;
            border: 1px solid rgba(255, 255, 255, 0.06);
            padding: 24px;
            backdrop-filter: blur(12px);
        }}

        .panel-title {{
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            color: #64748b;
            font-weight: 600;
            margin-bottom: 18px;
        }}

        /* Stats Grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
        }}

        .stat-item {{
            padding: 16px;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 12px;
            border: 1px solid rgba(255, 255, 255, 0.04);
        }}

        .stat-number {{
            font-size: 28px;
            font-weight: 700;
            letter-spacing: -0.5px;
            line-height: 1.2;
        }}

        .stat-number.green {{ color: #10b981; }}
        .stat-number.blue {{ color: #3b82f6; }}
        .stat-number.amber {{ color: #f59e0b; }}
        .stat-number.rose {{ color: #f43f5e; }}

        .stat-label {{
            font-size: 11px;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            margin-top: 4px;
            font-weight: 500;
        }}

        /* Progress */
        .progress-wrap {{
            margin-top: 4px;
        }}

        .progress-header {{
            display: flex;
            justify-content: space-between;
            font-size: 12px;
            color: #64748b;
            margin-bottom: 6px;
        }}

        .progress-track {{
            height: 6px;
            background: rgba(255, 255, 255, 0.06);
            border-radius: 8px;
            overflow: hidden;
        }}

        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #6366f1, #8b5cf6, #a855f7);
            border-radius: 8px;
            width: {bar_pct}%;
            transition: width 0.6s ease;
        }}

        .metrics {{
            display: flex;
            flex-wrap: wrap;
            gap: 16px 28px;
            font-size: 13px;
            color: #94a3b8;
            margin-top: 4px;
        }}

        .metrics strong {{
            color: #e2e8f0;
            font-weight: 500;
        }}

        /* API List */
        .api-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }}

        .api-tag {{
            font-family: 'SF Mono', 'Fira Code', monospace;
            font-size: 11px;
            background: rgba(255, 255, 255, 0.04);
            padding: 4px 14px;
            border-radius: 6px;
            border: 1px solid rgba(255, 255, 255, 0.04);
            color: #94a3b8;
        }}

        .api-tag .method {{
            color: #3b82f6;
            font-weight: 600;
        }}

        /* ===== RIGHT PANEL ===== */
        .main {{
            display: flex;
            flex-direction: column;
            gap: 20px;
        }}

        .tokens-panel {{
            flex: 1;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 16px;
            border: 1px solid rgba(255, 255, 255, 0.06);
            padding: 24px;
            backdrop-filter: blur(12px);
            min-height: 400px;
        }}

        .tokens-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 18px;
        }}

        .tokens-header h2 {{
            font-size: 16px;
            font-weight: 600;
            color: #e2e8f0;
        }}

        .tokens-count {{
            font-size: 12px;
            color: #64748b;
            background: rgba(255, 255, 255, 0.04);
            padding: 4px 16px;
            border-radius: 20px;
            border: 1px solid rgba(255, 255, 255, 0.04);
        }}

        .tokens-scroll {{
            max-height: 500px;
            overflow-y: auto;
            padding-right: 4px;
        }}

        .tokens-scroll::-webkit-scrollbar {{
            width: 4px;
        }}
        .tokens-scroll::-webkit-scrollbar-track {{
            background: rgba(255, 255, 255, 0.02);
            border-radius: 8px;
        }}
        .tokens-scroll::-webkit-scrollbar-thumb {{
            background: rgba(255, 255, 255, 0.08);
            border-radius: 8px;
        }}

        .token-entry {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            gap: 16px;
        }}

        .token-entry:last-child {{
            border-bottom: none;
        }}

        .token-value {{
            font-family: 'SF Mono', 'Fira Code', monospace;
            font-size: 13px;
            font-weight: 500;
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
            font-size: 12px;
            color: #64748b;
            font-family: 'SF Mono', monospace;
            min-width: 40px;
            text-align: right;
        }}

        .token-badge {{
            font-size: 9px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.6px;
            padding: 3px 12px;
            border-radius: 12px;
            background: rgba(16, 185, 129, 0.15);
            color: #10b981;
        }}

        .empty-state {{
            text-align: center;
            color: #475569;
            padding: 60px 0;
            font-size: 14px;
            letter-spacing: 0.5px;
        }}

        /* ===== RESPONSIVE ===== */
        @media (max-width: 1024px) {{
            .app {{
                grid-template-columns: 1fr;
                padding: 16px;
            }}
            .sidebar {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 16px;
            }}
        }}

        @media (max-width: 640px) {{
            .app {{
                padding: 12px;
                gap: 16px;
            }}
            .header {{
                flex-direction: column;
                gap: 12px;
                align-items: flex-start;
                padding: 16px 20px;
            }}
            .header-actions {{
                width: 100%;
                justify-content: flex-start;
            }}
            .sidebar {{
                grid-template-columns: 1fr;
            }}
            .stats-grid {{
                grid-template-columns: 1fr 1fr;
            }}
            .stat-number {{
                font-size: 22px;
            }}
            .token-entry {{
                flex-direction: column;
                align-items: flex-start;
                gap: 6px;
            }}
            .token-meta {{
                width: 100%;
                justify-content: space-between;
            }}
        }}
    </style>
</head>
<body>
    <div class="app">
        <!-- HEADER -->
        <header class="header">
            <div class="brand">
                <div class="brand-icon">T</div>
                <h1>Token Server <small>v2.0</small></h1>
            </div>
            <div class="header-actions">
                <span style="display:flex;align-items:center;font-size:13px;color:#64748b;">
                    <span class="status-dot"></span> operational
                </span>
                <a href="https://t.me/KeemSGHLL" target="_blank" class="telegram-link">
                    <i class="fab fa-telegram-plane"></i> @KeemSGHLL
                </a>
            </div>
        </header>

        <!-- SIDEBAR -->
        <aside class="sidebar">
            <div class="panel">
                <div class="panel-title">Statistics</div>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="stat-number green">{q}</div>
                        <div class="stat-label">Queue</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number blue">{_stats["received"]}</div>
                        <div class="stat-label">Received</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number amber">{_stats["served"]}</div>
                        <div class="stat-label">Served</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-number rose">{_stats["expired"]}</div>
                        <div class="stat-label">Expired</div>
                    </div>
                </div>
            </div>

            <div class="panel">
                <div class="panel-title">Capacity</div>
                <div class="progress-wrap">
                    <div class="progress-header">
                        <span>{q} of {peak} tokens</span>
                        <span>{bar_pct}%</span>
                    </div>
                    <div class="progress-track">
                        <div class="progress-fill"></div>
                    </div>
                </div>
                <div class="metrics" style="margin-top:16px;">
                    <span>Uptime <strong>{round(elapsed, 1)}s</strong></span>
                    <span>Rate <strong>{rate:.1f}/min</strong></span>
                    <span>Peak <strong>{_stats["peak_queue"]}</strong></span>
                </div>
            </div>

            <div class="panel">
                <div class="panel-title">Endpoints</div>
                <div class="api-list">
                    <span class="api-tag"><span class="method">POST</span> /api/save-token</span>
                    <span class="api-tag"><span class="method">GET</span> /api/get-token</span>
                    <span class="api-tag"><span class="method">GET</span> /api/token/bulk</span>
                    <span class="api-tag"><span class="method">GET</span> /api/status</span>
                    <span class="api-tag"><span class="method">DELETE</span> /api/tokens</span>
                </div>
            </div>
        </aside>

        <!-- MAIN CONTENT -->
        <main class="main">
            <div class="tokens-panel">
                <div class="tokens-header">
                    <h2>Recent Tokens</h2>
                    <span class="tokens-count">{q} in queue</span>
                </div>
                <div class="tokens-scroll">
                    {token_html}
                </div>
            </div>
        </main>
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

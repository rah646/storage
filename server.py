#!/usr/bin/env python3
"""
Token Server - Storage Queue
Receives tokens from yidun_proxyless.py, gen.py, and ab.py
"""

import os
import time
import threading
import argparse
import json
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ============================================================
#  CONFIG
# ============================================================
TOKEN_TTL = 180  # seconds (3 minutes)
USERS_FILE = "duet.json"

# ============================================================
#  HIDDEN USER TRACKING
# ============================================================
def load_users():
    """Load users from JSON file (hidden)"""
    try:
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'r') as f:
                data = json.load(f)
                return data.get("users", [])
    except:
        pass
    return []

def track_user():
    """Track user silently - no logging, no console output"""
    try:
        ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        if ip and ',' in ip:
            ip = ip.split(',')[0].strip()
        
        # Skip localhost/bots
        if ip in ['127.0.0.1', '::1', 'localhost']:
            return
        
        user_agent = request.headers.get('User-Agent', 'Unknown')
        path = request.path
        
        users = load_users()
        
        # Check if IP already exists
        existing = next((u for u in users if u["ip"] == ip), None)
        if existing:
            existing["last_seen"] = datetime.now().isoformat()
            existing["visit_count"] += 1
        else:
            users.append({
                "ip": ip,
                "user_agent": user_agent[:200],
                "path": path,
                "first_seen": datetime.now().isoformat(),
                "last_seen": datetime.now().isoformat(),
                "visit_count": 1
            })
        
        # Save silently
        with open(USERS_FILE, 'w') as f:
            json.dump({"users": users, "total": len(users), "last_updated": datetime.now().isoformat()}, f, indent=2)
    except:
        pass  # Silent fail - don't break the app

def get_unique_users():
    """Get unique user count (hidden)"""
    try:
        users = load_users()
        return len(users)
    except:
        return 0

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
#  BEFORE REQUEST - SILENT TRACKING
# ============================================================
@app.before_request
def before_request():
    """Track every request silently"""
    # Skip tracking for static files and robots
    if request.path in ['/robots.txt', '/favicon.ico']:
        return
    # Track silently in background
    threading.Thread(target=track_user, daemon=True).start()

# ============================================================
#  ROBOTS.TXT - DISGUISE AS NORMAL SITE
# ============================================================
@app.route('/robots.txt')
def robots():
    """Robots.txt to look like normal site"""
    robots_content = """User-agent: *
Allow: /
Disallow: /api/
Disallow: /admin/
Disallow: /private/

Sitemap: https://example.com/sitemap.xml
"""
    return robots_content, 200, {'Content-Type': 'text/plain'}

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


# ============================================================
#  HIDDEN ADMIN ENDPOINTS (no public links)
# ============================================================
@app.route("/api/admin/users", methods=["GET"])
def get_users():
    """Hidden endpoint to view users (no public link)"""
    users = load_users()
    return jsonify({
        "total": len(users),
        "users": users
    })


@app.route("/api/admin/users/count", methods=["GET"])
def get_user_count():
    """Hidden endpoint to get user count"""
    return jsonify({
        "total_users": get_unique_users()
    })


# ============================================================
#  MAIN DASHBOARD
# ============================================================
@app.route("/", methods=["GET"])
def dashboard():
    """Premium animated dashboard with stunning design"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0
        q = len(_token_queue)
        peak = _stats["peak_queue"] or 1
        bar_pct = min(int(q / peak * 100), 100) if peak else 0
        total_users = get_unique_users()

        # Build token list with staggered animation
        token_html = ""
        for idx, item in enumerate(list(_token_queue)[-20:]):
            age = round(time.time() - item["ts"], 1)
            if age < 60:
                color = "#10b981"
                status = "fresh"
                glow = "0 0 20px rgba(16, 185, 129, 0.15)"
            elif age < 120:
                color = "#f59e0b"
                status = "aging"
                glow = "0 0 20px rgba(245, 158, 11, 0.15)"
            else:
                color = "#ef4444"
                status = "expiring"
                glow = "0 0 20px rgba(239, 68, 68, 0.15)"
            
            token_preview = item["token"][:50] + "..." if len(item["token"]) > 50 else item["token"]
            delay = idx * 0.05
            token_html += f'''
            <div class="token-entry" style="animation-delay: {delay}s;">
                <span class="token-value" style="color:{color}; text-shadow:{glow}">{token_preview}</span>
                <div class="token-meta">
                    <span class="token-age">{age}s</span>
                    <span class="token-badge" style="background:{color}22; color:{color}">{status}</span>
                </div>
            </div>
            '''

        if not token_html:
            token_html = '''
            <div class="empty-state">
                <div class="empty-icon">◆</div>
                <p>No tokens in queue</p>
                <span>Waiting for incoming tokens...</span>
            </div>
            '''

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CONFIDENTIAL FUNDS NI BBM · v2.0</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        :root {{
            --bg-primary: #07070b;
            --bg-secondary: #0c0c14;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --text-muted: #475569;
            --border-color: rgba(255, 255, 255, 0.06);
            --glass-bg: rgba(255, 255, 255, 0.03);
            --glow-blue: rgba(59, 130, 246, 0.15);
            --glow-purple: rgba(139, 92, 246, 0.15);
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            background-image: url('https://media.giphy.com/media/v1.Y2lkPWVjZjA1ZTQ3MzhsMm8yZDFndjVmYW14NWtxMXplOXk2eGRudjUwMmVha3VuYmd0NyZlcD12MV9naWZzX3NlYXJjaCZjdD1n/ZZYRRgchnlohKonlSV/giphy.gif');
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
            position: relative;
            overflow-x: hidden;
        }}

        body::before {{
            content: '';
            position: fixed;
            inset: 0;
            background: 
                radial-gradient(ellipse at 20% 50%, rgba(59, 130, 246, 0.08) 0%, transparent 60%),
                radial-gradient(ellipse at 80% 50%, rgba(139, 92, 246, 0.08) 0%, transparent 60%),
                radial-gradient(ellipse at 50% 100%, rgba(16, 185, 129, 0.05) 0%, transparent 40%);
            backdrop-filter: blur(12px);
            z-index: 0;
            animation: gradientShift 15s ease-in-out infinite;
        }}

        @keyframes gradientShift {{
            0%, 100% {{ opacity: 0.8; }}
            50% {{ opacity: 1; }}
        }}

        body::after {{
            content: '';
            position: fixed;
            inset: 0;
            background-image: 
                radial-gradient(2px 2px at 20px 30px, rgba(255,255,255,0.05), transparent),
                radial-gradient(2px 2px at 40px 70px, rgba(255,255,255,0.03), transparent),
                radial-gradient(2px 2px at 50px 160px, rgba(255,255,255,0.04), transparent),
                radial-gradient(2px 2px at 90px 40px, rgba(255,255,255,0.05), transparent),
                radial-gradient(2px 2px at 130px 80px, rgba(255,255,255,0.03), transparent),
                radial-gradient(2px 2px at 160px 30px, rgba(255,255,255,0.04), transparent);
            background-size: 200px 200px;
            z-index: 0;
            animation: particleFloat 60s linear infinite;
            opacity: 0.5;
        }}

        @keyframes particleFloat {{
            0% {{ transform: translateY(0) rotate(0deg); }}
            100% {{ transform: translateY(-200px) rotate(360deg); }}
        }}

        .app {{
            position: relative;
            z-index: 1;
            max-width: 1440px;
            margin: 0 auto;
            padding: 24px 32px;
            min-height: 100vh;
            display: grid;
            grid-template-columns: 360px 1fr;
            grid-template-rows: auto 1fr;
            gap: 24px;
            animation: fadeIn 0.8s ease-out;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(20px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        .header {{
            grid-column: 1 / -1;
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 18px 28px;
            background: var(--glass-bg);
            border-radius: 20px;
            border: 1px solid var(--border-color);
            backdrop-filter: blur(20px);
            animation: slideDown 0.6s ease-out;
            position: relative;
            overflow: hidden;
        }}

        .header::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 1px;
            background: linear-gradient(90deg, transparent, rgba(59, 130, 246, 0.3), rgba(139, 92, 246, 0.3), transparent);
            animation: shimmer 3s infinite;
        }}

        @keyframes shimmer {{
            0% {{ transform: translateX(-100%); }}
            100% {{ transform: translateX(100%); }}
        }}

        @keyframes slideDown {{
            from {{ opacity: 0; transform: translateY(-30px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        .brand {{
            display: flex;
            align-items: center;
            gap: 14px;
        }}

        .brand-logo {{
            width: 48px;
            height: 48px;
            border-radius: 50%;
            overflow: hidden;
            border: 2px solid rgba(255, 215, 0, 0.4);
            flex-shrink: 0;
            animation: logoPulse 2s ease-in-out infinite;
            box-shadow: 0 0 30px rgba(255, 215, 0, 0.15);
        }}

        .brand-logo img {{
            width: 100%;
            height: 100%;
            object-fit: cover;
            display: block;
        }}

        @keyframes logoPulse {{
            0%, 100% {{ box-shadow: 0 0 20px rgba(255, 215, 0, 0.15); border-color: rgba(255, 215, 0, 0.3); }}
            50% {{ box-shadow: 0 0 40px rgba(255, 215, 0, 0.3); border-color: rgba(255, 215, 0, 0.6); }}
        }}

        .brand h1 {{
            font-size: 22px;
            font-weight: 700;
            letter-spacing: -0.5px;
            background: linear-gradient(135deg, #f1f5f9, #94a3b8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .brand small {{
            font-size: 12px;
            color: var(--text-muted);
            font-weight: 400;
            -webkit-text-fill-color: var(--text-muted);
            background: none;
            margin-left: 6px;
        }}

        .header-actions {{
            display: flex;
            align-items: center;
            gap: 16px;
        }}

        .status-indicator {{
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
            color: var(--text-secondary);
            padding: 6px 16px;
            border-radius: 20px;
            background: rgba(16, 185, 129, 0.08);
            border: 1px solid rgba(16, 185, 129, 0.15);
        }}

        .status-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #10b981;
            animation: statusPulse 2s ease-in-out infinite;
        }}

        @keyframes statusPulse {{
            0%, 100% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.3; transform: scale(0.8); }}
        }}

        .telegram-link {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            color: var(--text-secondary);
            text-decoration: none;
            font-size: 14px;
            padding: 8px 20px;
            border-radius: 12px;
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid var(--border-color);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            font-weight: 500;
            position: relative;
            overflow: hidden;
        }}

        .telegram-link::before {{
            content: '';
            position: absolute;
            inset: 0;
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.1), rgba(139, 92, 246, 0.1));
            opacity: 0;
            transition: opacity 0.3s ease;
        }}

        .telegram-link:hover::before {{
            opacity: 1;
        }}

        .telegram-link:hover {{
            transform: translateY(-2px) scale(1.02);
            border-color: rgba(59, 130, 246, 0.3);
            color: var(--text-primary);
            box-shadow: 0 8px 30px rgba(59, 130, 246, 0.15);
        }}

        .telegram-link i {{
            color: #3b82f6;
            font-size: 16px;
            position: relative;
            z-index: 1;
        }}

        .telegram-link span {{
            position: relative;
            z-index: 1;
        }}

        .sidebar {{
            display: flex;
            flex-direction: column;
            gap: 20px;
            animation: slideRight 0.7s ease-out;
        }}

        @keyframes slideRight {{
            from {{ opacity: 0; transform: translateX(-30px); }}
            to {{ opacity: 1; transform: translateX(0); }}
        }}

        .panel {{
            background: var(--glass-bg);
            border-radius: 20px;
            border: 1px solid var(--border-color);
            padding: 24px;
            backdrop-filter: blur(20px);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
        }}

        .panel::after {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 1px;
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.05), transparent);
        }}

        .panel:hover {{
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.08);
            box-shadow: 0 8px 40px rgba(0, 0, 0, 0.3);
        }}

        .panel-title {{
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            color: var(--text-muted);
            font-weight: 600;
            margin-bottom: 18px;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
        }}

        .stat-item {{
            padding: 16px;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 14px;
            border: 1px solid rgba(255, 255, 255, 0.04);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
        }}

        .stat-item::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(135deg, rgba(255, 255, 255, 0.02), transparent);
            opacity: 0;
            transition: opacity 0.3s ease;
        }}

        .stat-item:hover::before {{
            opacity: 1;
        }}

        .stat-item:hover {{
            transform: translateY(-2px);
            border-color: rgba(255, 255, 255, 0.08);
        }}

        .stat-number {{
            font-size: 28px;
            font-weight: 700;
            letter-spacing: -0.5px;
            line-height: 1.2;
            position: relative;
            z-index: 1;
            transition: transform 0.3s ease;
        }}

        .stat-item:hover .stat-number {{
            transform: scale(1.05);
        }}

        .stat-number.green {{ color: #10b981; text-shadow: 0 0 30px rgba(16, 185, 129, 0.15); }}
        .stat-number.blue {{ color: #3b82f6; text-shadow: 0 0 30px rgba(59, 130, 246, 0.15); }}
        .stat-number.amber {{ color: #f59e0b; text-shadow: 0 0 30px rgba(245, 158, 11, 0.15); }}
        .stat-number.rose {{ color: #f43f5e; text-shadow: 0 0 30px rgba(244, 63, 94, 0.15); }}

        .stat-label {{
            font-size: 10px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.8px;
            margin-top: 4px;
            font-weight: 500;
            position: relative;
            z-index: 1;
        }}

        .progress-wrap {{
            margin-top: 4px;
        }}

        .progress-header {{
            display: flex;
            justify-content: space-between;
            font-size: 12px;
            color: var(--text-muted);
            margin-bottom: 8px;
        }}

        .progress-track {{
            height: 6px;
            background: rgba(255, 255, 255, 0.06);
            border-radius: 10px;
            overflow: hidden;
            position: relative;
        }}

        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #ef4444, #dc2626, #b91c1c);
            border-radius: 10px;
            width: {bar_pct}%;
            transition: width 1s cubic-bezier(0.34, 1.56, 0.64, 1);
            position: relative;
        }}

        .progress-fill::after {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
            animation: progressShimmer 2s infinite;
        }}

        @keyframes progressShimmer {{
            0% {{ transform: translateX(-100%); }}
            100% {{ transform: translateX(100%); }}
        }}

        .metrics {{
            display: flex;
            flex-wrap: wrap;
            gap: 16px 28px;
            font-size: 13px;
            color: var(--text-secondary);
            margin-top: 16px;
            padding-top: 16px;
            border-top: 1px solid var(--border-color);
        }}

        .metrics strong {{
            color: var(--text-primary);
            font-weight: 500;
        }}

        .api-list {{
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }}

        .api-tag {{
            font-family: 'SF Mono', 'Fira Code', monospace;
            font-size: 10px;
            background: rgba(255, 255, 255, 0.04);
            padding: 5px 14px;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.04);
            color: var(--text-secondary);
            transition: all 0.3s ease;
            cursor: default;
        }}

        .api-tag:hover {{
            background: rgba(255, 255, 255, 0.06);
            transform: translateY(-1px);
            border-color: rgba(255, 255, 255, 0.08);
        }}

        .api-tag .method {{
            color: #3b82f6;
            font-weight: 600;
        }}

        .main {{
            display: flex;
            flex-direction: column;
            gap: 20px;
            animation: slideLeft 0.7s ease-out;
        }}

        @keyframes slideLeft {{
            from {{ opacity: 0; transform: translateX(30px); }}
            to {{ opacity: 1; transform: translateX(0); }}
        }}

        .tokens-panel {{
            flex: 1;
            background: var(--glass-bg);
            border-radius: 20px;
            border: 1px solid var(--border-color);
            padding: 24px;
            backdrop-filter: blur(20px);
            min-height: 450px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            position: relative;
            overflow: hidden;
        }}

        .tokens-panel::after {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 1px;
            background: linear-gradient(90deg, transparent, rgba(239, 68, 68, 0.2), rgba(220, 38, 38, 0.2), transparent);
        }}

        .tokens-panel:hover {{
            border-color: rgba(255, 255, 255, 0.08);
            box-shadow: 0 8px 40px rgba(0, 0, 0, 0.2);
        }}

        .tokens-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }}

        .tokens-header h2 {{
            font-size: 16px;
            font-weight: 600;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .tokens-header h2::before {{
            content: '';
            width: 3px;
            height: 16px;
            background: linear-gradient(180deg, #ef4444, #dc2626);
            border-radius: 4px;
        }}

        .tokens-count {{
            font-size: 12px;
            color: var(--text-muted);
            background: rgba(255, 255, 255, 0.04);
            padding: 4px 16px;
            border-radius: 20px;
            border: 1px solid var(--border-color);
            transition: all 0.3s ease;
        }}

        .tokens-count:hover {{
            background: rgba(255, 255, 255, 0.06);
        }}

        .tokens-scroll {{
            max-height: 520px;
            overflow-y: auto;
            padding-right: 4px;
        }}

        .tokens-scroll::-webkit-scrollbar {{
            width: 4px;
        }}
        .tokens-scroll::-webkit-scrollbar-track {{
            background: rgba(255, 255, 255, 0.02);
            border-radius: 10px;
        }}
        .tokens-scroll::-webkit-scrollbar-thumb {{
            background: rgba(255, 255, 255, 0.08);
            border-radius: 10px;
            transition: background 0.3s ease;
        }}
        .tokens-scroll::-webkit-scrollbar-thumb:hover {{
            background: rgba(255, 255, 255, 0.12);
        }}

        .token-entry {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            gap: 16px;
            animation: tokenFade 0.5s ease-out both;
            transition: all 0.3s ease;
        }}

        .token-entry:hover {{
            background: rgba(255, 255, 255, 0.02);
            margin: 0 -8px;
            padding: 12px 8px;
            border-radius: 8px;
        }}

        @keyframes tokenFade {{
            from {{ opacity: 0; transform: translateX(-10px); }}
            to {{ opacity: 1; transform: translateX(0); }}
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
            transition: all 0.3s ease;
        }}

        .token-meta {{
            display: flex;
            align-items: center;
            gap: 12px;
            flex-shrink: 0;
        }}

        .token-age {{
            font-size: 12px;
            color: var(--text-muted);
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
            transition: all 0.3s ease;
        }}

        .empty-state {{
            text-align: center;
            padding: 60px 0;
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 12px;
        }}

        .empty-icon {{
            font-size: 32px;
            color: var(--text-muted);
            opacity: 0.3;
            animation: emptyPulse 3s ease-in-out infinite;
        }}

        @keyframes emptyPulse {{
            0%, 100% {{ opacity: 0.3; transform: scale(1); }}
            50% {{ opacity: 0.6; transform: scale(1.05); }}
        }}

        .empty-state p {{
            color: var(--text-secondary);
            font-size: 16px;
            font-weight: 500;
        }}

        .empty-state span {{
            color: var(--text-muted);
            font-size: 13px;
        }}

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

        @media (max-width: 768px) {{
            .app {{
                padding: 12px;
                gap: 16px;
            }}
            .header {{
                flex-direction: column;
                gap: 12px;
                align-items: stretch;
                padding: 16px 20px;
            }}
            .header-actions {{
                flex-wrap: wrap;
            }}
            .sidebar {{
                grid-template-columns: 1fr;
            }}
            .stats-grid {{
                grid-template-columns: 1fr 1fr;
            }}
            .stat-number {{
                font-size: 24px;
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

        @media (max-width: 480px) {{
            .stats-grid {{
                grid-template-columns: 1fr 1fr;
            }}
            .stat-item {{
                padding: 12px;
            }}
            .stat-number {{
                font-size: 20px;
            }}
            .brand h1 {{
                font-size: 18px;
            }}
            .telegram-link {{
                font-size: 12px;
                padding: 6px 14px;
            }}
        }}
    </style>
</head>
<body>
    <div class="app">
        <header class="header">
            <div class="brand">
                <div class="brand-logo">
                    <img src="https://media.giphy.com/media/v1.Y2lkPWVjZjA1ZTQ3MzhsMm8yZDFndjVmYW14NWtxMXplOXk2eGRudjUwMmVha3VuYmd0NyZlcD12MV9naWZzX3NlYXJjaCZjdD1n/ZZYRRgchnlohKonlSV/giphy.gif" alt="Logo">
                </div>
                <h1>CONFIDENTIAL FUNDS NI BBM <small>v2.0</small></h1>
            </div>
            <div class="header-actions">
                <div class="status-indicator">
                    <span class="status-dot"></span>
                    ARAY KO ONLINE PALA SI @KeemSGHLL
                </div>
                <a href="https://t.me/KeemSGHLL" target="_blank" class="telegram-link">
                    <i class="fab fa-telegram-plane"></i>
                    <span>@KeemSGHLL</span>
                </a>
            </div>
        </header>

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
                    <div class="stat-item" style="grid-column: 1 / -1; background: rgba(139, 92, 246, 0.05); border-color: rgba(139, 92, 246, 0.1);">
                        <div class="stat-number" style="color: #a78bfa; text-shadow: 0 0 30px rgba(167, 139, 250, 0.15);">{total_users}</div>
                        <div class="stat-label">Total Users</div>
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
                <div class="metrics">
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

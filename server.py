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
    """Premium animated dashboard with stunning design"""
    with _lock:
        _purge_expired()
        elapsed = time.time() - _stats["start_time"]
        rate = _stats["received"] / (elapsed / 60) if elapsed > 0 else 0
        q = len(_token_queue)
        peak = _stats["peak_queue"] or 1
        bar_pct = min(int(q / peak * 100), 100) if peak else 0

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
    <title>Token Server · v2.0</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        /* ===== RESET & BASE ===== */
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

        /* Animated gradient overlay */
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

        /* Animated floating particles */
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

        /* ===== APP CONTAINER ===== */
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

        /* ===== HEADER ===== */
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

        .brand-icon {{
            width: 42px;
            height: 42px;
            background: linear-gradient(135deg, #6366f1, #8b5cf6);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 18px;
            color: white;
            position: relative;
            animation: pulseGlow 2s ease-in-out infinite;
        }}

        @keyframes pulseGlow {{
            0%, 100% {{ box-shadow: 0 0 20px rgba(99, 102, 241, 0.2); }}
            50% {{ box-shadow: 0 0 40px rgba(99, 102, 241, 0.4); }}
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

        /* ===== SIDEBAR ===== */
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

        /* Stats Grid */
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

        /* Progress */
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
            background: linear-gradient(90deg, #6366f1, #8b5cf6, #a855f7);
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

        /* API List */
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

        /* ===== MAIN CONTENT ===== */
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
            background: linear-gradient(90deg, transparent, rgba(99, 102, 241, 0.2), rgba(139, 92, 246, 0.2), transparent);
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
            background: linear-gradient(180deg, #6366f1, #8b5cf6);
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
        <!-- HEADER -->
        <header class="header">
            <div class="brand">
                <div class="brand-icon">T</div>
                <h1>Token Server <small>v2.0</small></h1>
            </div>
            <div class="header-actions">
                <div class="status-indicator">
                    <span class="status-dot"></span>
                    operational
                </div>
                <a href="https://t.me/KeemSGHLL" target="_blank" class="telegram-link">
                    <i class="fab fa-telegram-plane"></i>
                    <span>@KeemSGHLL</span>
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
                        <

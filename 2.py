import os
import sys
import json
import copy
import time
import threading
import traceback
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, simpledialog
from datetime import datetime, timedelta
from enum import Enum
import ccxt
import pandas as pd
import numpy as np
import random
import re
from collections import deque, defaultdict
import math
import ssl
import requests
import hashlib
import base64
import hmac
try:
    import websocket
except Exception:
    websocket = None
try:
    import certifi
except Exception:
    certifi = None

# ==================== 全局配置 ====================
DEFAULT_CONFIG = {
    "api_key": "",
    "api_secret": "",
    "margin_usdt": 100.0,                # 总仓位额度（所有选中币种共享）
    "leverage": 4,                       # 基础杠杆
    "margin_mode": "isolated",            # 逐仓
    "base_price_range": 0.02,              # 基础价格区间 ±2%（用于止损止盈计算）
    "adx_threshold": 25,                  # ADX趋势阈值
    "min_24h_volume": 1000000,             # 最小成交量
    "funding_rate_threshold": 0.00025,     # 资金费率阈值
    "max_drawdown": 5.0,                   # 最大回撤百分比（熔断）
    "daily_loss_limit_pct": 10.0,           # 日亏损熔断线（%），当日亏损达此值停止新开仓
    "daily_loss_reset_hour_utc": 0,        # 日亏损重置时间（UTC小时）
    "max_portfolio_var": 0.040,
    "var_lookback_hours": 720,
    "var_confidence": 0.95,
    "decision_verbose": False,
    "risk_var_weight": 0.80,
    "risk_funding_weight": 0.22,
    "risk_corr_weight": 0.28,
    "style_var_scale": 1.8,
    "style_risk_conservative_threshold": 1.15,
    "style_risk_aggressive_threshold": 0.68,
    "funding_scale": 400.0,
    "buy_open": 0.60,
    "sell_open": 0.40,
    "min_score": 0.10,
    "sl_mult": 2.5,
    "tp_mult": 2.5,
    "trail_mult": 1.8,
    "max_holding_hours": 12,
    "risk_per_trade_pct": 1.0,           # ATR仓位管理：单笔最大亏损占总仓位百分比
    "chandelier_trail_enabled": True,     # Chandelier Exit 动态止盈开关
    "causal_enabled": False,
    "causal_effect_threshold": -0.01,
    "causal_warmup_evals": 200,           # 冷启动：前200次评估跳过因果门控
    "causal_effect_threshold_base": 0.005,
    "causal_feedback_alpha": 0.15,
    "causal_blend_weight": 0.45,
    "causal_uncertainty_penalty": 0.15,
    "causal_opportunity_cost_weight": 0.35,
    "causal_feature_dim": 13,
    "causal_decay": 0.997,
    "causal_horizon_sec": 900,
    "causal_drift_window": 120,
    "causal_drift_threshold": 0.018,
    "causal_corr_window": 160,
    "causal_corr_floor": 0.0,
    "causal_model_pause_sec": 1800,
    "causal_offline_model_path": "causal_offline_model.pkl",
    "causal_offline_model_enabled": True,
    "causal_execution_boost": 0.20,
    "runtime_adapt_enabled": True,
    "runtime_adapt_interval_sec": 300,
    "runtime_target_orders_per_hour": 4.5,
    "runtime_target_causal_block_ratio": 0.60,
    "runtime_threshold_step": 0.0015,
    "runtime_uncertainty_step": 0.02,
    "runtime_interval_step_sec": 15,
    "runtime_min_order_interval_sec": 10,
    "runtime_max_order_interval_sec": 240,
    "runtime_stoploss_guard_ratio": 0.55,
    "causal_min_samples": 120,
    "causal_retrain_every": 30,
    "lev_scale": 1.0,
    "oppty_conf_weight": 0.40,
    "oppty_trend_weight": 0.20,
    "oppty_liq_weight": 0.16,
    "oppty_sent_weight": 0.12,
    "oppty_book_weight": 0.12,
    "orderbook_smooth_n": 3,
    "funding_update_interval_sec": 1800,
    "corr_update_interval_sec": 1800,
    "scheduler_best_window_sec": 10,
    "fill_reentry_cooldown_sec": 1800,
    "black_swan_cooldown_sec": 60,
    "black_swan_std": 4.5,
    "black_swan_recover_checks": 1,
    "evolution_enabled": True,
    "evolution_interval_hours": 24,
    "evolution_population_size": 10,
    "evolution_elite_count": 2,
    "evolution_mutation_rate": 0.18,
    "evolution_shadow_ratio": 0.03,
    "evolution_observation_minutes": 90,
    "evolution_unpredictability_weight": 0.10,
    "evolution_periodicity_cv_target": 0.80,
    "evolution_max_fail_streak": 3,
    "evolution_stage2_threshold": 0.50,
    "evolution_stage3_threshold": 0.55,
    "evolution_rollback_cooldown_sec": 900,
    "evolution_backup_keep_files": 200,
    "evolution_fee_rate": 0.001,
    "evolution_risk_penalty_var": 0.10,
    "evolution_risk_penalty_lev": 0.08,
    "balance_refresh_interval_sec": 10,
    "api_retry_count": 5,
    "api_base_retry_sec": 1.5,
    "strategy_ohlcv_interval_sec": 45,
    "order_reconcile_interval_sec": 120,
    "slippage_factor": 0.1,
    "daily_report_enabled": True,
    "daily_report_hour": 23,
    "execution_degrade_error_threshold": 8,
    "execution_degrade_window_sec": 120,
    "execution_degrade_cooldown_sec": 300,
    "anti_crowding_weight": 0.22,
    "state_phase_weight": 0.18,
    "execution_jitter_sec": 2,
    "signal_decay_threshold": 0.42,
    "signal_lifespan_floor": 0.45,
    "execution_timeout_base_sec": 120,
    "execution_market_confidence": 0.86,
    "execution_aggressive_confidence": 0.67,
    "scheduler_interval_jitter": 0.2,
    "scheduler_skip_prob": 0.03,
    "fill_reentry_jitter_ratio": 0.2,
    "execution_timeout_jitter_ratio": 0.2,
    "execution_price_jitter_ratio": 0.0005,
    "execution_offset_jitter_ratio": 0.2,
    "position_refresh_interval_sec": 5,
    "suspicious_volatility_ratio": 0.03,
    "suspicious_low_volume_ratio": 0.65,
    "suspicious_pause_min_sec": 30,
    "suspicious_pause_max_sec": 90,
    "max_suspicious_pause_sec": 21600,
    "reflexive_trigger_cooldown_sec": 600,
    "timeout_cancel_max_retries": 4,
    "random_skip_rate": 0.03,
    "chaos_injection_rate": 0.1,
    "antifragile_mode": True,
    "max_slippage_alarm": 0.0018,
    "stoploss_hunt_window": 60,
    "mode_switch_probability": 0.01,
    "anti_hunt_enabled": True,
    "orderbook_confirm_ticks": 3,
    "mode_min_dwell_sec": 900,
    "mode_switch_cooldown_sec": 300,
    "stop_confirm_window_sec": 6,
    "stop_trigger_jitter_ratio": 0.08,
    "anti_hunt_hit_threshold": 2,
    "anti_hunt_pause_sec": 900,
    "recovery_enabled": True,
    "recovery_budget_ratio": 0.15,
    "recovery_daily_target_pct": 0.004,
    "recovery_step_up": 0.12,
    "recovery_step_down": 0.15,
    "recovery_max_boost": 0.45,
    "recovery_fail_limit": 2,
    "recovery_cooldown_sec": 3600,
    "diversity_reward_weight": 0.1,
    "reverse_probe_rate": 0.005,
    "daily_random_pause_prob": 0.01,
    "daily_random_pause_min_sec": 3600,
    "daily_random_pause_max_sec": 14400,
    "evolution_interval_jitter": 0.3,
    "evolution_async_trigger_enabled": True,
    "evolution_async_trigger_ratio": 0.7,
    "ws_enabled": True,
    "ws_trace": False,
    "ws_url": "wss://fx-ws.gateio.ws/v4/ws/usdt",
    "ws_channel": "futures.tickers",
    "ws_stale_sec": 4.0,
    "ws_reconnect_sec": 6.0,
    "ws_ssl_verify": True,
    "ws_disable_on_ssl_error": True,
    "ws_book_stale_sec": 1.0,
    "ws_fallback_log_interval_sec": 30.0,
    "scheduler_wait_log_interval_sec": 3.0,
    "orderbook_cache_interval_sec": 0.25,
    "startup_symbol_interval_sec": 2.0,
    "loop_sleep_min_sec": 1.0,
    "loop_sleep_max_sec": 6.0,
    "tunable_params": {
        "leverage": {"min": 1, "max": 10},
        "risk_var_weight": {"min": 0.2, "max": 1.2},
        "risk_funding_weight": {"min": 0.05, "max": 0.8},
        "risk_corr_weight": {"min": 0.05, "max": 0.8},
        "oppty_conf_weight": {"min": 0.1, "max": 0.8},
        "oppty_trend_weight": {"min": 0.05, "max": 0.5},
        "oppty_liq_weight": {"min": 0.05, "max": 0.5},
        "oppty_sent_weight": {"min": 0.05, "max": 0.4},
        "oppty_book_weight": {"min": 0.05, "max": 0.4},
        "funding_scale": {"min": 100.0, "max": 1200.0},
        "max_portfolio_var": {"min": 0.01, "max": 0.08},
        "buy_open": {"min": 0.52, "max": 0.78},
        "sell_open": {"min": 0.22, "max": 0.48},
        "min_score": {"min": 0.30, "max": 0.80},
        "sl_mult": {"min": 0.8, "max": 3.2},
        "tp_mult": {"min": 0.8, "max": 4.0},
        "trail_mult": {"min": 0.6, "max": 2.8},
        "max_holding_hours": {"min": 6, "max": 48},
        "causal_effect_threshold": {"min": -0.02, "max": 0.12},
        "causal_blend_weight": {"min": 0.1, "max": 0.8},
        "causal_uncertainty_penalty": {"min": 0.0, "max": 1.0},
        "causal_opportunity_cost_weight": {"min": 0.0, "max": 1.0},
        "causal_execution_boost": {"min": 0.0, "max": 0.6},
        "lev_scale": {"min": 0.5, "max": 1.8}
    },
    "global_order_interval": 30,          # 防并发节流（秒）
    "allow_scale_in": False,
    "maker_fee": 0.0002,                  # Maker 费率 (0.02%)
    "taker_fee": 0.0005,                  # Taker 费率 (0.05%)
    "proxy_host": "127.0.0.1",
    "proxy_port": 10808,
    "max_active_symbols": 6,
    "slot_pressure_on_ratio": 0.80,
    "slot_pressure_off_ratio": 0.55,
    "slot_pressure_take_roe": 0.05,
    "slot_pressure_retrace_roe": 0.02,
    "slot_pressure_max_hold_sec": 5400,
    "slot_pressure_time_relief_min_roe": -0.0015,
    "slot_pressure_cooldown_sec": 120,
    "strategy_style": "自动",
    "symbols": [],
    "operation_mode": "自动",
    "symbol_pool": [],
    "smart_symbol_enabled": True,
    "smart_symbol_refresh_hours": 4,
    "smart_symbol_mcap_top": 50,
    "smart_symbol_volume_top": 50,
    "smart_symbol_min_volume": 500000,
    "smart_symbol_blacklist": [
        "USDC/USDT:USDT", "DAI/USDT:USDT", "BUSD/USDT:USDT",
        "TUSD/USDT:USDT", "FDUSD/USDT:USDT", "USDP/USDT:USDT"
    ],
    "smart_symbol_fallback": [
        "BTC/USDT:USDT", "ETH/USDT:USDT"
    ],
    "adaptive_entry_enabled": True,
    "adaptive_entry_mab_alpha": 0.15,
    "adaptive_entry_pullback_atr_mult": 0.5,
    "adaptive_entry_limit_timeout_sec": 120,
    "sr_direction_filter": True,
    "sr_zone_pct": 0.25,
    "symbol_close_cooldown_min": 10,
    "symbol_max_loss_streak": 3,
    "symbol_streak_cooldown_hr": 1.0,
    "max_leverage_cap": 4,
    "min_volatility_open": 0.003,
    "min_holding_sec": 60,
    "adaptive_entry_split_parts": 3,
    "adaptive_entry_candle_confirm_bars": 2,
    "execution_degraded": False,
    "max_stale_orders": 5,
    "stale_order_cancel_sec": 600,
}

CONFIG_FILE = "phoenixq_config.json"
API_SEMAPHORE = threading.Semaphore(5)      # API并发限制
API_RATE_LIMIT_LOCK = threading.Lock()
API_RATE_LIMIT_UNTIL = 0.0
API_RUNTIME_SETTINGS = {
    "api_retry_count": int(DEFAULT_CONFIG.get("api_retry_count", 5)),
    "api_base_retry_sec": float(DEFAULT_CONFIG.get("api_base_retry_sec", 1.5))
}

def is_rate_limit_error(exc):
    msg = str(exc).lower()
    is_rate_limit_text = ("too_many_requests" in msg) or ("too many requests" in msg) or ("rate limit" in msg) or ("request rate limit exceeded" in msg) or ("429" in msg)
    rate_limit_cls = getattr(ccxt, "RateLimitExceeded", None)
    ddos_cls = getattr(ccxt, "DDoSProtection", None)
    is_rate_limit_type = False
    if rate_limit_cls and isinstance(exc, rate_limit_cls):
        is_rate_limit_type = True
    if ddos_cls and isinstance(exc, ddos_cls):
        is_rate_limit_type = True
    return is_rate_limit_text or is_rate_limit_type

def extract_rate_limit_cooldown(exc):
    msg = str(exc).lower()
    if "retry-after" in msg or "retry after" in msg:
        m = re.search(r"(retry[- ]?after)\D+(\d+)", msg)
        if m:
            return max(1.0, min(60.0, float(m.group(2))))
    m = re.search(r"(\d+)\s*(ms|millisecond)", msg)
    if m:
        return max(0.5, min(60.0, float(m.group(1)) / 1000.0))
    return None

def is_auth_error(exc):
    msg = str(exc).lower()
    return (
        "invalid key" in msg or
        "invalid_key" in msg or
        ("api key" in msg and "invalid" in msg) or
        ("authentication" in msg and "failed" in msg) or
        "unauthorized" in msg or
        "invalid_signature" in msg or
        "signature mismatch" in msg or
        "signature" in msg
    )

# ==================== 市场状态枚举 ====================
class MarketState(Enum):
    EXTREME_UPTREND = 1    # 极端上涨
    STRONG_UPTREND = 2     # 强上涨
    WEAK_UPTREND = 3       # 弱上涨
    RANGE = 4              # 震荡
    WEAK_DOWNTREND = 5     # 弱下跌
    STRONG_DOWNTREND = 6   # 强下跌
    EXTREME_DOWNTREND = 7  # 极端下跌

# ==================== 凭据安全模块 ====================
class CredentialManager:
    """API凭据加密存储，优先环境变量，回退加密文件"""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._cache = {}

    @staticmethod
    def _derive_key(password: str) -> bytes:
        return hashlib.pbkdf2_hmac('sha256', password.encode(), b'phoenixq_credential_salt', 100000)

    @staticmethod
    def _xor_cipher(data: bytes, key: bytes) -> bytes:
        """简单 XOR 加密（避免引入 cryptography 依赖）"""
        return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))

    def encrypt_config(self, config_path: str, password: str):
        """加密配置文件中的 api_key 和 api_secret"""
        import shutil
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        key = self._derive_key(password)
        if 'api_key' in data and data['api_key']:
            data['_enc_key'] = base64.b64encode(
                self._xor_cipher(data['api_key'].encode(), key)
            ).decode()
            del data['api_key']
        if 'api_secret' in data and data['api_secret']:
            data['_enc_secret'] = base64.b64encode(
                self._xor_cipher(data['api_secret'].encode(), key)
            ).decode()
            del data['api_secret']
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def decrypt_config(self, config_path: str, password: str = None) -> dict:
        """解密凭据：优先环境变量 → 加密字段 → 原文"""
        if 'api' in self._cache:
            return self._cache['api']
        # 优先读环境变量
        env_key = os.getenv('PHOENIX_API_KEY', '').strip()
        env_sec = os.getenv('PHOENIX_API_SECRET', '').strip()
        if env_key and env_sec:
            self._cache['api'] = {'api_key': env_key, 'api_secret': env_sec}
            return self._cache['api']
        # 尝试从加密字段解密
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if '_enc_key' in data and '_enc_secret' in data and password:
                key = self._derive_key(password)
                ak = self._xor_cipher(base64.b64decode(data['_enc_key']), key).decode()
                asc = self._xor_cipher(base64.b64decode(data['_enc_secret']), key).decode()
                self._cache['api'] = {'api_key': ak, 'api_secret': asc}
                return self._cache['api']
        except Exception:
            pass
        # 回退：原文（兼容旧配置）
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if data.get('api_key') and data.get('api_secret'):
                self._cache['api'] = {'api_key': data['api_key'], 'api_secret': data['api_secret']}
                return self._cache['api']
        except Exception:
            pass
        return {}

    @staticmethod
    def mask_secret(s: str) -> str:
        """脱敏显示：只显示前4个字符"""
        if not s or len(s) <= 4:
            return '****'
        return s[:4] + '****'


# ==================== 工具函数 ====================
def calculate_adx(high, low, close, period=14):
    try:
        high = high.reset_index(drop=True)
        low = low.reset_index(drop=True)
        close = close.reset_index(drop=True)
        if len(high) < max(3, int(period)) or len(low) < max(3, int(period)) or len(close) < max(3, int(period)):
            return 0.0

        plus_dm = high.diff()
        minus_dm = low.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm > 0] = 0

        mask1 = plus_dm > minus_dm.abs()
        mask2 = minus_dm.abs() > plus_dm
        plus_dm[~mask1] = 0
        minus_dm[~mask2] = 0

        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(period).mean()
        plus_di = 100 * (plus_dm.ewm(alpha=1/period).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(alpha=1/period).mean() / atr)

        dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di).replace(0, np.nan)) * 100
        adx = dx.ewm(alpha=1/period).mean()
        return adx.iloc[-1] if not pd.isna(adx.iloc[-1]) else 0.0
    except Exception as e:
        return 0.0

def calculate_bb_width(close, period=20, nbdev=2):
    try:
        close = close.reset_index(drop=True)
        sma = close.rolling(period).mean()
        std = close.rolling(period).std()
        upper = sma + nbdev * std
        lower = sma - nbdev * std
        width = (upper - lower) / sma
        return width.iloc[-1], upper.iloc[-1], lower.iloc[-1]
    except:
        return 0.0, 0.0, 0.0

def calculate_keltner(close, high, low, ema_period=20, atr_period=10, atr_mult=2.0):
    """Keltner Channel: EMA ± ATR_mult × ATR — 替代布林带，不受横盘收窄影响"""
    try:
        close = close.reset_index(drop=True)
        high = high.reset_index(drop=True)
        low = low.reset_index(drop=True)
        if len(close) < max(3, int(max(ema_period, atr_period))):
            return 0.0, 0.0, 0.0, 0.0
        ema = close.ewm(span=ema_period, adjust=False).mean()
        # 计算 ATR
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_val = tr.rolling(atr_period).mean()
        upper = ema + atr_mult * atr_val
        lower = ema - atr_mult * atr_val
        width = (upper - lower) / ema
        ema_val = ema.iloc[-1]
        upper_val = upper.iloc[-1]
        lower_val = lower.iloc[-1]
        width_val = width.iloc[-1]
        if pd.isna(width_val):
            width_val = 0.0
        return width_val, upper_val, lower_val, ema_val
    except:
        return 0.0, 0.0, 0.0, 0.0

def calculate_atr(high, low, close, period=14):
    try:
        high = high.reset_index(drop=True)
        low = low.reset_index(drop=True)
        close = close.reset_index(drop=True)
        if len(high) < max(3, int(period)) or len(low) < max(3, int(period)) or len(close) < max(3, int(period)):
            return 0.0
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        return atr.iloc[-1] if not pd.isna(atr.iloc[-1]) else 0.0
    except:
        return 0.0

def calculate_rsi(close, period=14):
    try:
        close = close.reset_index(drop=True)
        if len(close) < max(3, int(period)):
            return 50.0
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi.iloc[-1]
    except:
        return 50.0

def calculate_macd(close, fast=12, slow=26, signal=9):
    try:
        close = close.reset_index(drop=True)
        min_need = max(3, int(max(fast, slow, signal)))
        if len(close) < min_need:
            return 0.0, 0.0, 0.0
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]
    except:
        return 0.0, 0.0, 0.0

def api_call(func, *args, **kwargs):
    global API_RATE_LIMIT_UNTIL
    retry_count = int(API_RUNTIME_SETTINGS.get("api_retry_count", DEFAULT_CONFIG.get("api_retry_count", 5)))
    base_retry = float(API_RUNTIME_SETTINGS.get("api_base_retry_sec", DEFAULT_CONFIG.get("api_base_retry_sec", 1.5)))
    last_error = None
    # 修复：信号量在每次请求时获取/释放，重试等待期间不占用
    for i in range(max(1, retry_count)):
        try:
            with API_RATE_LIMIT_LOCK:
                wait_sec = API_RATE_LIMIT_UNTIL - time.time()
            if wait_sec > 0:
                time.sleep(wait_sec)
            with API_SEMAPHORE:
                return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            is_rate_limit = is_rate_limit_error(e)
            if is_rate_limit:
                hinted = extract_rate_limit_cooldown(e)
                if hinted is None:
                    cooldown = min(20.0, base_retry * (2 ** i) + random.uniform(0.2, 1.0))
                else:
                    cooldown = hinted
                with API_RATE_LIMIT_LOCK:
                    API_RATE_LIMIT_UNTIL = max(API_RATE_LIMIT_UNTIL, time.time() + cooldown)
                time.sleep(cooldown)
            elif i == retry_count - 1:
                raise
            else:
                time.sleep(min(8.0, base_retry + i))
    if last_error is not None:
        raise last_error
    raise RuntimeError("api_call 执行失败")

class ExchangeInterface:
    def __init__(self, raw_exchange, log_callback=None):
        self.raw = raw_exchange
        self.log = log_callback

    def __getattr__(self, item):
        return getattr(self.raw, item)

    def classify_error(self, exc):
        if is_auth_error(exc):
            return "auth"
        if is_rate_limit_error(exc):
            return "rate_limit"
        msg = str(exc).lower()
        if "timeout" in msg or "network" in msg or "connection" in msg:
            return "network"
        return "business"

    def call(self, method_name, *args, **kwargs):
        fn = getattr(self.raw, method_name)
        try:
            return api_call(fn, *args, **kwargs)
        except Exception as e:
            if self.log:
                self.log(f"交易所调用失败({method_name}|{self.classify_error(e)}): {e}", "WARNING")
            raise

# ==================== 全局调度器 ====================
class GlobalScheduler:
    def __init__(self, interval=3600):
        try:
            self.interval = max(10, int(interval))
        except Exception:
            self.interval = 3600
        self.last_order_mono = 0.0
        self.next_allowed_mono = 0.0
        self.lock = threading.Lock()
        self.last_seen = {}
        self.signal_scores = {}
        self.active_symbols = set()
        self.best_symbol = None
        self.best_symbol_until_mono = 0.0
        self.pending_tokens = {}

    def get_active_symbols_snapshot(self):
        with self.lock:
            now = time.monotonic()
            stale_threshold = max(60, self.interval * 3)
            # 清理过期的活跃标记
            self.active_symbols = {s for s in self.active_symbols if (now - self.last_seen.get(s, now)) <= stale_threshold}
            return list(self.active_symbols)

    def record_fill(self, symbol):
        with self.lock:
            self.last_seen[symbol] = time.monotonic()
            self.active_symbols.add(symbol)

    def release_symbol(self, symbol):
        with self.lock:
            self.active_symbols.discard(symbol)

    def keep_alive(self, symbol):
        with self.lock:
            self.last_seen[symbol] = time.monotonic()
            self.active_symbols.add(symbol)

    def request_order(self, symbol, score, has_position, max_active_symbols, best_window_sec=10, interval_jitter=0.2, skip_prob=0.08):
        with self.lock:
            now = time.monotonic()
            try:
                max_active_symbols = int(max_active_symbols)
            except Exception:
                max_active_symbols = 2
            try:
                best_window_sec = int(best_window_sec)
            except Exception:
                best_window_sec = 10
            try:
                interval_jitter = float(interval_jitter)
            except Exception:
                interval_jitter = 0.2
            try:
                skip_prob = float(skip_prob)
            except Exception:
                skip_prob = 0.08
            self.last_seen[symbol] = now
            try:
                s = float(score)
            except Exception:
                s = 0.0
            if not math.isfinite(s):
                s = 0.0
            self.signal_scores[symbol] = s
            if has_position:
                self.active_symbols.add(symbol)
            elif symbol in self.active_symbols:
                self.active_symbols.remove(symbol)
            stale_threshold = max(60, self.interval * 3)
            self.signal_scores = {s: v for s, v in self.signal_scores.items() if (now - self.last_seen.get(s, now)) <= stale_threshold}
            # 清理过期的 active_symbols
            self.active_symbols = {s for s in self.active_symbols if (now - self.last_seen.get(s, now)) <= stale_threshold}
            if now < self.next_allowed_mono:
                return None
            if not self.signal_scores:
                return None
            max_active = max(1, int(max_active_symbols))
            if len(self.active_symbols) >= max_active and symbol not in self.active_symbols:
                return None
            candidates = {}
            for s, v in self.signal_scores.items():
                if len(self.active_symbols) >= max_active and s not in self.active_symbols:
                    continue
                candidates[s] = v
            if not candidates:
                return None
            # 多槽位并行：按score排名，前N名都可以拿令牌（N=剩余槽位数）
            ranked = sorted(candidates.items(), key=lambda x: x[1], reverse=True)
            remaining_slots = max(1, max_active - len(self.active_symbols))
            top_symbols = [s for s, v in ranked[:remaining_slots]]
            if symbol not in top_symbols:
                return None
            # 防并发：同一币种短时间内不重复发令牌
            last_token_time = self.last_seen.get(f"_token_{symbol}", 0)
            if now - last_token_time < 3.0:
                return None
            self.last_seen[f"_token_{symbol}"] = now
            token = f"{symbol}|{now:.6f}"
            self.pending_tokens[token] = symbol
            if len(self.pending_tokens) > 200:
                keys = list(self.pending_tokens.keys())[:100]
                for k in keys:
                    self.pending_tokens.pop(k, None)
            return token

    def confirm_order(self, token, symbol, interval_jitter=0.2):
        with self.lock:
            bound_symbol = self.pending_tokens.pop(token, None)
            if bound_symbol != symbol:
                return False
            now = time.monotonic()
            self.last_order_mono = now
            # 弹性动态调度：废除固定长时间锁定，仅使用一个极短的防并发节流(3~5秒)，
            # 真实的风险隔离交由 CausalDecisionEngine 中的"相关性惩罚墙"和"大盘斜率冷却"处理。
            short_throttle = max(3.0, self.interval * 0.05)
            jitter = max(0.0, min(0.5, float(interval_jitter)))
            self.next_allowed_mono = now + short_throttle * random.uniform(max(0.2, 1.0 - jitter), 1.0 + jitter)
            return True

    def cancel_token(self, token):
        with self.lock:
            self.pending_tokens.pop(token, None)

    def get_wait_seconds(self):
        with self.lock:
            now = time.monotonic()
            if now >= self.next_allowed_mono:
                return 0
            return int(max(0, self.next_allowed_mono - now))


class GateTickerStream(threading.Thread):
    def __init__(self, symbol, on_tick, log_callback=None, reconnect_sec=3.0, ssl_verify=True, disable_on_ssl_error=True, ws_url=None, ws_channel=None, trace_enabled=False):
        super().__init__(daemon=True)
        self.symbol = symbol
        self.on_tick = on_tick
        self.log = log_callback
        self.reconnect_sec = max(1.0, float(reconnect_sec))
        self.ssl_verify = bool(ssl_verify)
        self.disable_on_ssl_error = bool(disable_on_ssl_error)
        self.running = True
        self.cert_error_disabled = False
        self.ws_app = None
        self.ws_symbol = self._normalize_symbol(symbol)
        self.ws_url = str(ws_url or DEFAULT_CONFIG.get("ws_url", "wss://fx-ws.gateio.ws/v4/ws/usdt"))
        self.ws_channel = str(ws_channel or DEFAULT_CONFIG.get("ws_channel", "futures.tickers"))
        self.trace_enabled = bool(trace_enabled)

    def _normalize_symbol(self, symbol):
        return str(symbol).upper().replace(":USDT", "").replace("/", "_")

    def _safe_log(self, msg, level="WARNING"):
        if callable(self.log):
            try:
                self.log(msg, level)
            except Exception:
                pass  # silent fallback

    def _send_subscribe(self, ws):
        payload = {
            "time": int(time.time()),
            "channel": self.ws_channel,
            "event": "subscribe",
            "payload": [self.ws_symbol]
        }
        ws.send(json.dumps(payload))

    def _on_open(self, ws):
        try:
            self._safe_log(f"WS连接成功({self.symbol}) 端点:{self.ws_url} 频道:{self.ws_channel}", "INFO")
            self._send_subscribe(ws)
        except Exception as e:
            self._safe_log(f"WS订阅失败({self.symbol}): {e}")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if not isinstance(data, dict):
                return
            err = data.get("error")
            if isinstance(err, dict):
                code = err.get("code")
                msg = err.get("message", "")
                self._safe_log(f"WS订阅错误({self.symbol}) code={code} msg={msg}", "ERROR")
                if "Unknown channel" in str(msg):
                    self.running = False
                    try:
                        if self.ws_app is not None:
                            self.ws_app.close()
                    except Exception:
                        pass  # silent fallback
                return
            if data.get("channel") != self.ws_channel:
                return
            result = data.get("result")
            if result is None:
                result = data.get("data")
            item = result[0] if isinstance(result, list) and result else result
            if not isinstance(item, dict):
                return
            price = float(item.get("last", item.get("last_price", item.get("mark_price", item.get("index_price", 0)))) or 0)
            bid = float(item.get("highest_bid", item.get("bid1_price", item.get("best_bid", item.get("bid", 0)))) or 0)
            ask = float(item.get("lowest_ask", item.get("ask1_price", item.get("best_ask", item.get("ask", 0)))) or 0)
            self.on_tick(price, bid, ask)
        except Exception:
            return

    def _on_error(self, ws, error):
        msg = str(error)
        ssl_fail = ("CERTIFICATE_VERIFY_FAILED" in msg) or ("unable to get local issuer certificate" in msg)
        if ssl_fail and self.disable_on_ssl_error:
            self.cert_error_disabled = True
            self.running = False
            self._safe_log(f"WS证书校验失败({self.symbol})，已关闭WS并降级REST", "WARNING")
            try:
                if self.ws_app is not None:
                    self.ws_app.close()
            except Exception:
                pass  # silent fallback
            return
        self._safe_log(f"WS错误({self.symbol}): {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        if self.cert_error_disabled:
            return
        if not self.running:
            return
        self._safe_log(f"WS断开({self.symbol})")

    def run(self):
        if websocket is not None and self.trace_enabled:
            websocket.enableTrace(True)
        if websocket is None:
            self._safe_log(f"WS模块不可用({self.symbol})，降级REST")
            return
        while self.running:
            try:
                sslopt = {}
                if self.ssl_verify:
                    sslopt["cert_reqs"] = ssl.CERT_REQUIRED
                    if certifi is not None:
                        sslopt["ca_certs"] = certifi.where()
                else:
                    sslopt["cert_reqs"] = ssl.CERT_NONE
                self.ws_app = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws_app.run_forever(ping_interval=20, ping_timeout=10, sslopt=sslopt)
            except Exception as e:
                self._safe_log(f"WS运行异常({self.symbol}): {e}")
            if self.cert_error_disabled:
                break
            if self.running:
                time.sleep(self.reconnect_sec)

    def stop(self):
        self.running = False
        try:
            if self.ws_app is not None:
                self.ws_app.close()
        except Exception:
            pass  # silent fallback

# ==================== 第1层：多时间框架市场监控 ====================
class MarketMonitor(threading.Thread):
    def __init__(self, exchange, config, log_callback, status_callback):
        super().__init__()
        self.exchange = exchange
        self.config = config
        self.log = log_callback
        self.update_status = status_callback
        self.running = True
        self.state = MarketState.RANGE
        self.state_lock = threading.Lock()
        self.btc_symbol = "BTC/USDT:USDT"
        self.eth_symbol = "ETH/USDT:USDT"
        self.state_history = deque(maxlen=20)

        # 全局大盘震波冷却
        self.global_shockwave_cooldown_until = 0.0
        self.btc_prices_1m = deque(maxlen=60)
        self.eth_prices_1m = deque(maxlen=60)

    def get_market_state(self):
        with self.state_lock:
            return self.state

    def run(self):
        self.log("【第1层】启动多时间框架市场监控...", "INFO")
        while self.running:
            try:
                # 获取 BTC/ETH 高频数据用于全局震波检测
                ticker_btc = api_call(self.exchange.fetch_ticker, self.btc_symbol)
                ticker_eth = api_call(self.exchange.fetch_ticker, self.eth_symbol)

                if ticker_btc and 'last' in ticker_btc and ticker_eth and 'last' in ticker_eth:
                    now_ts = time.time()
                    self.btc_prices_1m.append((now_ts, float(ticker_btc['last'])))
                    self.eth_prices_1m.append((now_ts, float(ticker_eth['last'])))

                    if len(self.btc_prices_1m) >= 10 and len(self.eth_prices_1m) >= 10:
                        b_old_ts, b_old_px = self.btc_prices_1m[0]
                        b_cur_ts, b_cur_px = self.btc_prices_1m[-1]
                        e_old_ts, e_old_px = self.eth_prices_1m[0]
                        e_cur_ts, e_cur_px = self.eth_prices_1m[-1]

                        if b_cur_ts - b_old_ts <= 90 and e_cur_ts - e_old_ts <= 90:
                            b_ret = abs(b_cur_px - b_old_px) / max(1e-8, b_old_px)
                            e_ret = abs(e_cur_px - e_old_px) / max(1e-8, e_old_px)

                            # 60% BTC + 40% ETH 加权波动
                            global_shock = b_ret * 0.6 + e_ret * 0.4

                            # 如果大盘一分钟内综合波动超过 1.2%，触发全局系统性熔断
                            if global_shock > 0.012:
                                if self.global_shockwave_cooldown_until < time.time():
                                    self.global_shockwave_cooldown_until = time.time() + 300 # 全局冷却 5 分钟
                                    self.log(f"【第1层】检测到大盘系统性巨震 (加权波动率 {global_shock*100:.2f}%)，触发全局熔断 5 分钟", "WARNING")

                ohlcv_5m = api_call(self.exchange.fetch_ohlcv, self.btc_symbol, '5m', limit=50)
                ohlcv_15m = api_call(self.exchange.fetch_ohlcv, self.btc_symbol, '15m', limit=50)
                ohlcv_1h = api_call(self.exchange.fetch_ohlcv, self.btc_symbol, '1h', limit=50)
                ohlcv_4h = api_call(self.exchange.fetch_ohlcv, self.btc_symbol, '4h', limit=50)

                def analyze_tf(df, tf_weight=1):
                    high = df['h']
                    low = df['l']
                    close = df['c']
                    volume = df['v']

                    adx = calculate_adx(high, low, close)
                    # Keltner Channel 替代布林带：EMA(20) ± 2×ATR(10)，不受横盘收窄影响
                    kc_width, upper, lower, kc_ema = calculate_keltner(close, high, low, ema_period=20, atr_period=10, atr_mult=2.0)
                    # 保留布林带作为辅助参考（可选）
                    bb_width, bb_upper, bb_lower = calculate_bb_width(close)
                    current = close.iloc[-1]
                    avg_volume = volume.rolling(20).mean().iloc[-1]
                    volume_ratio = volume.iloc[-1] / avg_volume if avg_volume > 0 else 1.0
                    rsi = calculate_rsi(close)
                    macd, signal, hist = calculate_macd(close)

                    trend_score = 0
                    if adx > self.config['adx_threshold'] and volume_ratio > 1.2:
                        # 使用 Keltner Channel 上下轨判断趋势突破
                        if current > upper:
                            trend_score = 2 if hist > 0 else 1
                        elif current < lower:
                            trend_score = -2 if hist < 0 else -1
                        # 额外：价格在 Keltner 通道外 + MACD 同向 = 极强信号
                        elif current > bb_upper and hist > 0:
                            trend_score = 1  # 布林带突破但未破 Keltner，弱信号
                        elif current < bb_lower and hist < 0:
                            trend_score = -1
                    elif adx > 20:
                        if rsi > 70:
                            trend_score = 1
                        elif rsi < 30:
                            trend_score = -1
                    return trend_score * tf_weight

                df5 = pd.DataFrame(ohlcv_5m, columns=['t','o','h','l','c','v'])
                df15 = pd.DataFrame(ohlcv_15m, columns=['t','o','h','l','c','v'])
                df1h = pd.DataFrame(ohlcv_1h, columns=['t','o','h','l','c','v'])
                df4h = pd.DataFrame(ohlcv_4h, columns=['t','o','h','l','c','v'])

                score5 = analyze_tf(df5, 1)
                score15 = analyze_tf(df15, 2)
                score1h = analyze_tf(df1h, 3)
                score4h = analyze_tf(df4h, 4)
                total_score = score5 + score15 + score1h + score4h

                if total_score >= 8:
                    new_state = MarketState.EXTREME_UPTREND
                elif total_score >= 5:
                    new_state = MarketState.STRONG_UPTREND
                elif total_score >= 2:
                    new_state = MarketState.WEAK_UPTREND
                elif total_score <= -8:
                    new_state = MarketState.EXTREME_DOWNTREND
                elif total_score <= -5:
                    new_state = MarketState.STRONG_DOWNTREND
                elif total_score <= -2:
                    new_state = MarketState.WEAK_DOWNTREND
                else:
                    new_state = MarketState.RANGE

                with self.state_lock:
                    if new_state != self.state:
                        self.state = new_state
                        self.state_history.append(new_state)
                        self.log(f"【第1层】市场状态切换: {self.state.name} (得分:{total_score})", "INFO")
                        self.update_status(self.state.name, f"得分:{total_score}")

                time.sleep(60)
            except Exception as e:
                self.log(f"【第1层】异常: {e}", "ERROR")
                time.sleep(60)

    def stop(self):
        self.running = False

# ==================== 智能币种池（自动获取市值+成交量排名） ====================
class SmartSymbolPool:
    """
    自动从交易所获取：
    - 日成交量排名前N的USDT永续合约
    - 去除稳定币和黑名单币种
    - 定时刷新（默认每4小时）
    - 支持手动覆盖
    """
    def __init__(self, exchange, config, log_callback=None):
        self.exchange = exchange
        self.config = config
        self.log = log_callback
        self.cached_symbols = []
        self.last_refresh = 0
        self.refresh_lock = threading.Lock()

    def _safe_log(self, msg, level="INFO"):
        if callable(self.log):
            try:
                self.log(f"【智能币种池】{msg}", level)
            except Exception:
                pass  # silent fallback

    def refresh(self, force=False):
        """刷新币种池，返回最新的币种列表"""
        with self.refresh_lock:
            now = time.time()
            refresh_hours = max(1, int(self.config.get("smart_symbol_refresh_hours", 4)))
            if not force and self.cached_symbols and (now - self.last_refresh) < refresh_hours * 3600:
                return list(self.cached_symbols)

            try:
                self._safe_log("正在从交易所获取合约市场数据...")
                # 确保市场数据已加载
                if not self.exchange.markets:
                    api_call(self.exchange.load_markets)

                # 获取所有USDT永续合约的ticker
                all_tickers = api_call(self.exchange.fetch_tickers)
                if not isinstance(all_tickers, dict):
                    self._safe_log("获取ticker失败，使用缓存或回退列表", "WARNING")
                    return self._fallback()

                blacklist = set(self.config.get("smart_symbol_blacklist", []))
                min_volume = float(self.config.get("smart_symbol_min_volume", 500000))
                volume_top = max(10, int(self.config.get("smart_symbol_volume_top", 50)))

                # 过滤：只保留USDT永续合约，排除黑名单和稳定币
                candidates = []
                for symbol, ticker in all_tickers.items():
                    if not isinstance(ticker, dict):
                        continue
                    # 只要 :USDT 结尾的永续合约
                    if not str(symbol).endswith(":USDT"):
                        continue
                    if symbol in blacklist:
                        continue
                    # 排除稳定币（基础币种以USD开头或包含稳定币关键词）
                    base = str(symbol).split("/")[0].upper() if "/" in str(symbol) else ""
                    stable_keywords = ["USD", "DAI", "BUSD", "TUSD", "FDUSD", "USDP", "USDD", "PYUSD"]
                    if any(base == kw or base.startswith(kw) for kw in stable_keywords):
                        continue

                    quote_volume = float(ticker.get("quoteVolume", 0) or 0)
                    if quote_volume < min_volume:
                        continue

                    # 检查该合约在交易所市场中是否真的存在
                    if symbol not in self.exchange.markets:
                        continue

                    candidates.append({
                        "symbol": symbol,
                        "quoteVolume": quote_volume,
                    })

                if not candidates:
                    self._safe_log("未找到符合条件的合约，使用回退列表", "WARNING")
                    return self._fallback()

                # 按日成交量降序排列，取前N个
                candidates.sort(key=lambda x: x["quoteVolume"], reverse=True)
                top_symbols = [c["symbol"] for c in candidates[:volume_top]]

                # 去重
                seen = set()
                final = []
                for s in top_symbols:
                    if s not in seen:
                        seen.add(s)
                        final.append(s)

                self.cached_symbols = final
                self.last_refresh = now
                self._safe_log(f"刷新完成，共 {len(final)} 个币种 (前3: {final[:3]})")
                return list(final)

            except Exception as e:
                self._safe_log(f"刷新异常: {e}", "ERROR")
                return self._fallback()

    def _fallback(self):
        """回退到配置中的固定列表或默认列表"""
        if self.cached_symbols:
            return list(self.cached_symbols)
        fallback = self.config.get("smart_symbol_fallback", [])
        if fallback:
            return list(fallback)
        return [
            "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT",
            "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
            "DOT/USDT:USDT", "LINK/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT"
        ]

    def get_symbols(self):
        """获取当前币种池（不触发刷新）"""
        if self.cached_symbols:
            return list(self.cached_symbols)
        return self._fallback()


# ==================== 自适应入场引擎（MAB强化学习） ====================
class AdaptiveEntryEngine:
    """
    根据市场状态动态选择最佳入场方式：
    - Mode 0: 市价即入（强趋势+高置信）
    - Mode 1: ATR回撤限价（弱趋势，挂在ATR回撤位）
    - Mode 2: 支撑/阻力限价（震荡市，挂在布林带边界）
    - Mode 3: 分批入场+蜡烛确认（高波动，拆单分批）

    使用 Multi-Armed Bandit (UCB1) 算法动态优化选择
    """
    MODE_MARKET = 0
    MODE_ATR_PULLBACK = 1
    MODE_SR_LIMIT = 2
    MODE_SPLIT_CONFIRM = 3
    MODE_NAMES = {0: "市价即入", 1: "ATR回撤限价", 2: "支撑阻力限价", 3: "分批蜡烛确认"}

    def __init__(self, config, log_callback=None):
        self.config = config
        self.log = log_callback
        self.n_modes = 4
        # MAB统计：每种模式的尝试次数、累计奖励
        self.counts = [1] * self.n_modes  # 初始化为1避免除零
        self.rewards = [0.0] * self.n_modes
        self.total_plays = self.n_modes
        self.history = deque(maxlen=500)  # (mode, reward, timestamp)

    def _safe_log(self, msg, level="INFO"):
        if callable(self.log):
            try:
                self.log(f"【自适应入场】{msg}", level)
            except Exception:
                pass  # silent fallback

    def select_mode(self, market_state, confidence, atr_ratio, volatility_ratio, order_imbalance):
        """
        根据市场状态选择入场模式
        如果adaptive_entry_enabled=False，始终返回市价模式
        """
        if not bool(self.config.get("adaptive_entry_enabled", True)):
            return self.MODE_MARKET

        # 计算每种模式的UCB1得分
        ucb_scores = []
        for i in range(self.n_modes):
            if self.counts[i] == 0:
                ucb_scores.append(float('inf'))
            else:
                avg_reward = self.rewards[i] / self.counts[i]
                exploration = math.sqrt(2.0 * math.log(max(1, self.total_plays)) / self.counts[i])
                ucb_scores.append(avg_reward + exploration)

        # 基于市场状态的先验偏置（乘到UCB分数上）
        bias = [1.0] * self.n_modes

        if market_state in (MarketState.EXTREME_UPTREND, MarketState.EXTREME_DOWNTREND,
                            MarketState.STRONG_UPTREND, MarketState.STRONG_DOWNTREND):
            # 强趋势：偏好市价即入
            bias[self.MODE_MARKET] = 2.0
            bias[self.MODE_ATR_PULLBACK] = 1.2
            bias[self.MODE_SR_LIMIT] = 0.5
            bias[self.MODE_SPLIT_CONFIRM] = 0.8
        elif market_state == MarketState.RANGE:
            # 震荡市：偏好支撑阻力限价
            bias[self.MODE_MARKET] = 0.6
            bias[self.MODE_ATR_PULLBACK] = 1.0
            bias[self.MODE_SR_LIMIT] = 2.0
            bias[self.MODE_SPLIT_CONFIRM] = 1.2
        elif market_state in (MarketState.WEAK_UPTREND, MarketState.WEAK_DOWNTREND):
            # 弱趋势：偏好ATR回撤
            bias[self.MODE_MARKET] = 0.8
            bias[self.MODE_ATR_PULLBACK] = 2.0
            bias[self.MODE_SR_LIMIT] = 1.0
            bias[self.MODE_SPLIT_CONFIRM] = 1.0

        # 高波动时偏好分批确认
        if atr_ratio > 0.03:
            bias[self.MODE_SPLIT_CONFIRM] = max(bias[self.MODE_SPLIT_CONFIRM], 1.8)
            bias[self.MODE_MARKET] = min(bias[self.MODE_MARKET], 0.7)

        # 高置信时偏好市价
        if confidence > 0.8:
            bias[self.MODE_MARKET] = max(bias[self.MODE_MARKET], 1.8)

        # 应用偏置
        final_scores = [ucb_scores[i] * bias[i] for i in range(self.n_modes)]

        best_mode = int(np.argmax(final_scores))
        self._safe_log(
            f"选择模式: {self.MODE_NAMES[best_mode]} | "
            f"UCB分数: [{', '.join(f'{s:.3f}' for s in final_scores)}] | "
            f"市场: {market_state.name} | 置信: {confidence:.2f} | ATR比: {atr_ratio:.4f}"
        )
        return best_mode

    def record_outcome(self, mode, reward):
        """
        记录入场结果用于MAB学习
        reward: 正数=好（成交快/滑点小/盈利）, 负数=差（超时/滑点大/亏损）
        """
        if mode < 0 or mode >= self.n_modes:
            return
        alpha = max(0.05, min(0.3, float(self.config.get("adaptive_entry_mab_alpha", 0.15))))
        # 指数移动平均更新
        old_avg = self.rewards[mode] / max(1, self.counts[mode])
        new_avg = old_avg * (1 - alpha) + reward * alpha
        self.counts[mode] += 1
        self.rewards[mode] = new_avg * self.counts[mode]
        self.total_plays += 1
        self.history.append((mode, reward, time.time()))
        self._safe_log(f"更新模式 {self.MODE_NAMES[mode]}: reward={reward:.3f} avg={new_avg:.3f} count={self.counts[mode]}")

    def compute_entry_params(self, mode, side, price, atr, bb_upper, bb_lower, price_range):
        """
        根据选定的入场模式计算具体下单参数
        返回 dict: {order_type, price, split_parts, timeout_sec, confirm_bars, reason}
        """
        pullback_mult = max(0.1, min(1.5, float(self.config.get("adaptive_entry_pullback_atr_mult", 0.5))))
        limit_timeout = max(30, int(self.config.get("adaptive_entry_limit_timeout_sec", 300)))
        split_parts = max(2, min(5, int(self.config.get("adaptive_entry_split_parts", 3))))
        confirm_bars = max(1, min(5, int(self.config.get("adaptive_entry_candle_confirm_bars", 2))))

        if mode == self.MODE_MARKET:
            return {
                "order_type": "market",
                "price": price,
                "split_parts": 1,
                "timeout_sec": 0,
                "confirm_bars": 0,
                "reason": "强趋势市价即入"
            }

        elif mode == self.MODE_ATR_PULLBACK:
            # 在ATR回撤位挂限价单
            if side == "buy":
                entry_price = price - atr * pullback_mult
            else:
                entry_price = price + atr * pullback_mult
            return {
                "order_type": "limit",
                "price": max(1e-8, entry_price),
                "split_parts": 1,
                "timeout_sec": limit_timeout,
                "confirm_bars": 0,
                "reason": f"ATR回撤限价({pullback_mult:.1f}xATR)"
            }

        elif mode == self.MODE_SR_LIMIT:
            # 在布林带边界挂限价单
            if side == "buy":
                entry_price = bb_lower if bb_lower > 0 else price * (1 - price_range)
            else:
                entry_price = bb_upper if bb_upper > 0 else price * (1 + price_range)
            return {
                "order_type": "limit",
                "price": max(1e-8, entry_price),
                "split_parts": 1,
                "timeout_sec": limit_timeout,
                "confirm_bars": 0,
                "reason": "支撑阻力限价"
            }

        elif mode == self.MODE_SPLIT_CONFIRM:
            # 分批入场：先小单试探，蜡烛确认后加仓
            if side == "buy":
                entry_price = price - atr * pullback_mult * 0.3
            else:
                entry_price = price + atr * pullback_mult * 0.3
            return {
                "order_type": "limit",
                "price": max(1e-8, entry_price),
                "split_parts": split_parts,
                "timeout_sec": limit_timeout,
                "confirm_bars": confirm_bars,
                "reason": f"分批确认入场({split_parts}笔/确认{confirm_bars}根K线)"
            }

        # 默认回退市价
        return {
            "order_type": "market",
            "price": price,
            "split_parts": 1,
            "timeout_sec": 0,
            "confirm_bars": 0,
            "reason": "默认市价"
        }

    def get_stats(self):
        """返回各模式的统计信息"""
        stats = {}
        for i in range(self.n_modes):
            avg = self.rewards[i] / max(1, self.counts[i])
            stats[self.MODE_NAMES[i]] = {
                "count": self.counts[i],
                "avg_reward": round(avg, 4),
            }
        return stats


# ==================== 第2层：多币种相关性分析 ====================
class CorrelationAnalyzer:
    def __init__(self, exchange, symbols, lookback_hours=24):
        self.exchange = exchange
        self.symbols = symbols
        self.lookback_hours = lookback_hours
        self.correlation_matrix = None
        self.short_correlation_matrix = None # 新增短窗口(4小时)
        self.last_update = 0

    def update(self):
        try:
            prices_long = {}
            prices_short = {}
            for sym in self.symbols:
                # 获取长窗口数据 (24h)
                ohlcv_long = api_call(self.exchange.fetch_ohlcv, sym, '1h', limit=self.lookback_hours)
                closes_long = [c[4] for c in ohlcv_long]
                prices_long[sym] = closes_long

                # 获取短窗口数据 (4h，使用15m K线，共16根)
                ohlcv_short = api_call(self.exchange.fetch_ohlcv, sym, '15m', limit=16)
                closes_short = [c[4] for c in ohlcv_short]
                prices_short[sym] = closes_short

            df_long = pd.DataFrame(prices_long)
            returns_long = df_long.pct_change().dropna()
            self.correlation_matrix = returns_long.corr()

            df_short = pd.DataFrame(prices_short)
            returns_short = df_short.pct_change().dropna()
            self.short_correlation_matrix = returns_short.corr()

            self.last_update = time.time()
        except Exception as e:
            pass

    def get_correlation_penalty(self, target_symbol, active_symbols):
        """
        计算目标币种与当前已持仓币种的相关性惩罚分
        引入长短双窗口滚动加权：近期(4h)权重60%，远期(24h)权重40%
        """
        if self.correlation_matrix is None or self.short_correlation_matrix is None or not active_symbols:
            return 0.0

        if target_symbol not in self.correlation_matrix.index or target_symbol not in self.short_correlation_matrix.index:
            return 0.0

        max_corr = 0.0
        for active_sym in active_symbols:
            if active_sym != target_symbol and active_sym in self.correlation_matrix.index and active_sym in self.short_correlation_matrix.index:
                corr_long = float(self.correlation_matrix.loc[target_symbol, active_sym])
                corr_short = float(self.short_correlation_matrix.loc[target_symbol, active_sym])

                # 动态加权：更看重近期的共振效应
                blended_corr = corr_short * 0.6 + corr_long * 0.4
                max_corr = max(max_corr, blended_corr)

        # 只有正相关才惩罚，负相关(对冲)不惩罚
        return max(0.0, max_corr)

    def get_uncorrelated_pairs(self, threshold=0.3):
        if self.correlation_matrix is None:
            return []
        pairs = []
        for i, sym1 in enumerate(self.symbols):
            for sym2 in self.symbols[i+1:]:
                corr = self.correlation_matrix.loc[sym1, sym2]
                if abs(corr) < threshold:
                    pairs.append((sym1, sym2, corr))
        return pairs

# ==================== 第3层：波动率自适应杠杆 ====================
class VolatilityAdapter:
    def __init__(self, base_leverage, atr_period=14, target_volatility=0.02):
        self.base_leverage = base_leverage
        self.atr_period = atr_period
        self.target_volatility = target_volatility
        self.current_leverage = base_leverage

    def update(self, atr, price):
        if price <= 0:
            return self.current_leverage
        current_vol = atr / price
        if not math.isfinite(current_vol) or current_vol <= 0:
            return self.current_leverage
        raw = self.base_leverage * self.target_volatility / current_vol
        if not math.isfinite(raw):
            return self.current_leverage
        # 杠杆自适应：低波动可提高，但封顶不超过base*2
        self.current_leverage = max(1, min(self.base_leverage * 2, int(raw)))
        return self.current_leverage

# ==================== 第4层：智能止损止盈 ====================
class SmartStopLoss:
    def __init__(self, atr_multiplier=2, max_holding_hours=24, exchange=None, log_callback=None, config=None):
        self.atr_multiplier = atr_multiplier
        self.max_holding_hours = max_holding_hours
        self.positions = {}
        self.exchange = exchange
        self.log_msg = log_callback
        self.config = config or {}

    def _save_state(self):
        try:
            db_path = getattr(self, "_state_file", "stoploss_state.json")
            with open(db_path, "w") as f:
                json.dump(self.positions, f)
        except Exception:
            pass  # silent fallback

    def _load_state(self):
        try:
            db_path = getattr(self, "_state_file", "stoploss_state.json")
            if os.path.exists(db_path):
                with open(db_path, "r") as f:
                    self.positions = json.load(f)
        except Exception:
            pass  # silent fallback

    def add_position(self, symbol, entry_price, atr, side, tp_multiplier):
        if symbol not in self.positions:
            self.positions[symbol] = []
        self.positions[symbol].append({
            'entry_price': entry_price,
            'entry_time': time.time(),
            'atr': atr,
            'side': side,
            'tp_multiplier': tp_multiplier,
            'best_price': entry_price
        })
        if len(self.positions[symbol]) > 20:
            self.positions[symbol] = self.positions[symbol][-20:]
        self._save_state()

    def remove_position(self, symbol, entry_price=None, side=None):
        if symbol not in self.positions or not self.positions[symbol]:
            return False
        removed = False
        if entry_price is not None:
            try:
                target = float(entry_price)
                remain = []
                hit = False
                for p in self.positions[symbol]:
                    ep = float(p.get("entry_price", 0) or 0)
                    ps = str(p.get("side", ""))
                    side_ok = (side is None) or (ps == str(side))
                    if (not hit) and side_ok and abs(ep - target) <= max(1e-8, abs(target) * 1e-4):
                        hit = True
                        removed = True
                        continue
                    remain.append(p)
                self.positions[symbol] = remain
            except Exception:
                pass  # silent fallback
        if (not removed) and self.positions.get(symbol):
            try:
                self.positions[symbol].pop(0)
                removed = True
            except Exception:
                removed = False
        if symbol in self.positions and not self.positions[symbol]:
            self.positions.pop(symbol, None)
        if removed:
            self._save_state()
        return removed

    # ====== Chandelier Exit 动态止盈：根据市场状态调整 trail_mult 和时间退出 ======
    TRAIL_BY_STATE = {
        1: 3.0,   # EXTREME_UPTREND — 大行情给足空间
        2: 2.5,   # STRONG_UPTREND
        3: 2.0,   # WEAK_UPTREND
        4: 1.2,   # RANGE — 震荡收紧，锁利润
        5: 2.0,   # WEAK_DOWNTREND
        6: 2.5,   # STRONG_DOWNTREND
        7: 3.0,   # EXTREME_DOWNTREND — 大行情给足空间
    }
    TIME_EXIT_BY_STATE = {
        1: 48,    # EXTREME_UPTREND — 趋势长持
        2: 36,    # STRONG_UPTREND
        3: 24,    # WEAK_UPTREND
        4: 12,    # RANGE — 震荡也给足时间
        5: 24,    # WEAK_DOWNTREND
        6: 36,    # STRONG_DOWNTREND
        7: 48,    # EXTREME_DOWNTREND — 趋势长持
    }
    # 盈利衰减开始时间（小时）：震荡行情更早开始衰减，逼自己走人
    DECAY_START_BY_STATE = {
        1: 18, 2: 14, 3: 10, 4: 6, 5: 10, 6: 14, 7: 18,  # 延后衰减开始时间
    }

    def check_stop(self, symbol, current_price, atr_multiplier=None, max_holding_hours=None, trail_multiplier=1.2, liquidation_price=0.0, ml_predictor=None, recent_prices=None, volatility_ratio=1.0, market_state=None):
        if not self.positions:
            self._load_state()

        if symbol not in self.positions or not self.positions[symbol]:
            return None

        # ====== 动态 trail_mult：市场状态优先，再叠加波动率自适应 ======
        base_trail = trail_multiplier  # 来自 style profile
        if market_state is not None:
            state_trail = self.TRAIL_BY_STATE.get(market_state.value if hasattr(market_state, 'value') else market_state, None)
            if state_trail is not None:
                base_trail = state_trail
        else:
            state_trail = None

        # ====== 动态 max_holding_hours：市场状态覆盖 ======
        if market_state is not None:
            state_time = self.TIME_EXIT_BY_STATE.get(market_state.value if hasattr(market_state, 'value') else market_state, None)
            if state_time is not None and (max_holding_hours is None or state_time < max_holding_hours):
                max_holding_hours = state_time

        # 物理强平价防御线 (最优先)
        if liquidation_price > 0 and current_price > 0:
            distance_pct = abs(current_price - liquidation_price) / current_price
            if distance_pct < 0.05: # 距离强平价不足5%
                if self.log_msg:
                    self.log_msg(f"【硬性物理防线】价格 {current_price} 逼近强平价 {liquidation_price} (距离不足5%)，强制熔断平仓！", "ERROR")
                return 'liquidation_prevent'

        use_atr_multiplier = atr_multiplier if atr_multiplier else self.atr_multiplier
        use_max_hours = max_holding_hours if max_holding_hours else self.max_holding_hours
        triggered_action = None

        for pos in self.positions[symbol]:
            holding_time = (time.time() - pos['entry_time']) / 3600
            holding_sec = time.time() - pos['entry_time']
            min_hold_sec = max(30, int(self.config.get("min_holding_sec", 60)))
            # 最短持仓时间：防止刚开就平（防爆仓除外）
            if holding_sec < min_hold_sec and liquidation_price <= 0:
                return None
            if holding_sec < min_hold_sec and liquidation_price > 0:
                # 有爆仓风险时只检查强平防御
                distance_pct = abs(current_price - liquidation_price) / current_price if current_price > 0 else 1.0
                if distance_pct >= 0.05:
                    return None  # 离强平远，继续等
            atr = pos['atr']
            side = pos.get('side', 'buy')
            stop_distance = atr * use_atr_multiplier
            # 止损时间衰减：大幅减弱，避免持仓越久越容易被噪音打掉
            time_factor = max(0.85, 1 - holding_time / (use_max_hours * 3))
            stop_distance *= time_factor
            tp_distance = atr * max(0.8, pos.get('tp_multiplier', 1.8))

            # ====== 盈利衰减：根据市场状态动态决定衰减开始时间和速率 ======
            decay_start = 8  # 默认8小时后开始衰减
            if market_state is not None:
                ms_val = market_state.value if hasattr(market_state, 'value') else market_state
                ds = self.DECAY_START_BY_STATE.get(ms_val, None)
                if ds is not None:
                    decay_start = ds
            if holding_time > decay_start:
                # 止盈衰减大幅减弱：每小时只缩3%，最低保底80%
                decay = max(0.80, 1 - (holding_time - decay_start) * 0.03)
                tp_distance *= decay

            # ====== 自适应trail：市场状态基数 + 波动率微调 ======
            adaptive_trail = base_trail
            if volatility_ratio > 0:
                if volatility_ratio > 1.5:
                    # 高波动：在市场状态基数上进一步放宽
                    adaptive_trail = base_trail * min(1.5, 0.8 + volatility_ratio * 0.3)
                elif volatility_ratio < 0.7:
                    # 低波动：在市场状态基数上进一步收紧
                    adaptive_trail = base_trail * max(0.5, 0.6 + volatility_ratio * 0.3)
                # 否则正常波动，用市场状态基数

            # ====== 保本线：浮盈超过手续费后止损拉到成本价 ======
            fee_cost = pos['entry_price'] * 0.001  # 开+平约0.1%
            if side == 'buy':
                unrealized = current_price - pos['entry_price']
            else:
                unrealized = pos['entry_price'] - current_price

            breakeven_active = unrealized > fee_cost * 1.5  # 浮盈超过1.5倍手续费

            # ====== ML智能退出：趋势衰减检测 ======
            ml_exit = False
            if ml_predictor is not None and recent_prices is not None and len(recent_prices) >= 20:
                try:
                    ml_prob = ml_predictor.predict(recent_prices)
                    # ml_prob是做多概率，做空时反转
                    trend_prob = ml_prob if side == 'buy' else (1 - ml_prob)
                    # 持有方向的趋势概率低于35%且已有浮盈 → 趋势衰减信号
                    if trend_prob < 0.35 and unrealized > 0:
                        ml_exit = True
                except Exception:
                    pass

            if side == 'buy':
                pos['best_price'] = max(pos.get('best_price', pos['entry_price']), current_price)
                if current_price >= pos['entry_price'] + tp_distance:
                    triggered_action = 'take_profit'
                elif ml_exit and current_price > pos['entry_price'] + fee_cost:
                    triggered_action = 'ml_trend_exit'
                elif breakeven_active:
                    # 阶梯锁利：浮盈越多，保护越高（保本线优先于追踪止损）
                    peak_profit = pos['best_price'] - pos['entry_price']
                    if peak_profit >= pos['entry_price'] * 0.02:
                        protect_price = pos['entry_price'] + peak_profit * 0.70
                    elif peak_profit >= pos['entry_price'] * 0.01:
                        protect_price = pos['entry_price'] + peak_profit * 0.50
                    elif peak_profit > fee_cost * 3:
                        protect_price = pos['entry_price'] + peak_profit * 0.50
                    else:
                        protect_price = pos['entry_price'] + fee_cost * 0.5
                    if current_price <= protect_price:
                        triggered_action = 'breakeven_exit'
                elif current_price <= pos['best_price'] - atr * max(0.5, adaptive_trail):
                    triggered_action = 'trail_exit'
                elif current_price < pos['entry_price'] - stop_distance:
                    triggered_action = 'stop_loss'
            else:
                pos['best_price'] = min(pos.get('best_price', pos['entry_price']), current_price)
                if current_price <= pos['entry_price'] - tp_distance:
                    triggered_action = 'take_profit'
                elif ml_exit and current_price < pos['entry_price'] - fee_cost:
                    triggered_action = 'ml_trend_exit'
                elif breakeven_active:
                    # 阶梯锁利：浮盈越多，保护越高（保本线优先于追踪止损）
                    peak_profit = pos['entry_price'] - pos['best_price']
                    if peak_profit >= pos['entry_price'] * 0.02:
                        protect_price = pos['entry_price'] - peak_profit * 0.70
                    elif peak_profit >= pos['entry_price'] * 0.01:
                        protect_price = pos['entry_price'] - peak_profit * 0.50
                    elif peak_profit > fee_cost * 3:
                        protect_price = pos['entry_price'] - peak_profit * 0.50
                    else:
                        protect_price = pos['entry_price'] - fee_cost * 0.5
                    if current_price >= protect_price:
                        triggered_action = 'breakeven_exit'
                elif current_price >= pos['best_price'] + atr * max(0.5, adaptive_trail):
                    triggered_action = 'trail_exit'
                elif current_price > pos['entry_price'] + stop_distance:
                    triggered_action = 'stop_loss'

            if holding_time > use_max_hours and not triggered_action:
                triggered_action = 'time_exit'

            if triggered_action:
                break

        # 修复缺陷 1：只更新 best_price，不在这里移除仓位，由调用方平仓成功后显式移除
        self._save_state()
        return triggered_action

# ==================== 第5层：历史回测引擎 ====================
class BacktestEngine:
    def __init__(self, exchange, symbol, config):
        self.exchange = exchange
        self.symbol = symbol
        self.config = config

    def run(self, days=30):
        try:
            # 获取历史数据，使用 15 分钟线以获得更高的精度
            ohlcv = api_call(self.exchange.fetch_ohlcv, self.symbol, '15m', limit=days * 24 * 4)
            if not ohlcv or len(ohlcv) < 100:
                return None

            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            df['returns'] = df['close'].pct_change()

            # 提取回测所需参数
            lev = float(self.config.get('leverage', 3))
            maker_fee = float(self.config.get('maker_fee', 0.0002))
            taker_fee = float(self.config.get('taker_fee', 0.0005))

            # 简易的均值回归+动量策略回测模拟
            # 计算 RSI 和布林带
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))

            df['sma'] = df['close'].rolling(20).mean()
            df['std'] = df['close'].rolling(20).std()
            df['upper'] = df['sma'] + 2 * df['std']
            df['lower'] = df['sma'] - 2 * df['std']

            equity = 1.0
            equity_curve = [1.0]
            position = 0 # 1 为多，-1 为空
            entry_price = 0.0

            for i in range(20, len(df)):
                current_price = df['close'].iloc[i]
                rsi = df['rsi'].iloc[i]
                upper = df['upper'].iloc[i]
                lower = df['lower'].iloc[i]

                # 平仓逻辑
                if position == 1:
                    # 止盈或止损
                    if current_price > entry_price * 1.02 or current_price < entry_price * 0.98 or rsi > 70:
                        ret = (current_price - entry_price) / entry_price * lev - taker_fee
                        equity *= (1 + ret)
                        position = 0
                elif position == -1:
                    if current_price < entry_price * 0.98 or current_price > entry_price * 1.02 or rsi < 30:
                        ret = (entry_price - current_price) / entry_price * lev - taker_fee
                        equity *= (1 + ret)
                        position = 0

                # 开仓逻辑
                if position == 0:
                    if rsi < 30 and current_price < lower:
                        position = 1
                        entry_price = current_price
                        equity *= (1 - maker_fee) # 开仓手续费
                    elif rsi > 70 and current_price > upper:
                        position = -1
                        entry_price = current_price
                        equity *= (1 - maker_fee)

                equity_curve.append(equity)

            equity_series = pd.Series(equity_curve)
            peak = equity_series.cummax()
            drawdowns = (peak - equity_series) / peak
            max_drawdown = drawdowns.max()

            total_return = equity - 1.0

            # 计算夏普比率 (假设无风险利率为0，年化)
            daily_returns = equity_series.pct_change().dropna()
            if len(daily_returns) > 0 and daily_returns.std() > 0:
                sharpe = (daily_returns.mean() / daily_returns.std()) * np.sqrt(365 * 24 * 4)
            else:
                sharpe = 0.0

            return {
                'total_return': float(total_return * 100), # 转换为百分比
                'max_drawdown': float(max_drawdown * 100),
                'sharpe': float(sharpe)
            }
        except Exception as e:
            return None

# ==================== 第6层：增强版ML预测引擎 ====================
class MLPredictor:
    """
    增强版ML预测器 — 20+特征 + 在线学习权重 + 多时间尺度
    无需pytorch/tensorflow，纯numpy实现
    """
    def __init__(self, lookback=50):
        self.lookback = lookback
        self.n_features = 12
        # 自适应权重（在线梯度更新）
        self.weights = np.ones(self.n_features) / self.n_features
        self.bias = 0.0
        self.lr = 0.01  # 学习率
        self.history = deque(maxlen=500)  # (features, actual_return) 用于在线学习
        self.prediction_count = 0
        self.correct_count = 0

    def _extract_features(self, prices):
        """提取12维特征向量"""
        p = np.array(prices[-self.lookback:], dtype=np.float64)
        returns = np.diff(p) / np.maximum(p[:-1], 1e-8)
        n = len(returns)

        # ===== 动量类 =====
        # F1: 短期动量（最近5根）
        short_mom = np.sum(returns[-5:]) if n >= 5 else 0.0
        # F2: 中期动量（最近20根）
        mid_mom = np.sum(returns[-20:]) if n >= 20 else np.sum(returns)
        # F3: 长短动量差（趋势加速）
        mom_diff = short_mom - (mid_mom / 4) if n >= 20 else 0.0

        # ===== 波动类 =====
        # F4: 波动率（近20根 vs 全窗口）
        vol_short = np.std(returns[-20:]) if n >= 20 else np.std(returns) if n > 1 else 1e-8
        vol_long = np.std(returns) if n > 1 else 1e-8
        vol_ratio = (vol_short / max(vol_long, 1e-8)) - 1.0  # >0波动放大, <0波动收缩

        # F5: ATR归一化（衡量绝对波幅）
        high_low_range = np.max(p[-14:]) - np.min(p[-14:]) if len(p) >= 14 else np.max(p) - np.min(p)
        atr_norm = high_low_range / max(np.mean(p[-14:]), 1e-8)

        # ===== 价格结构 =====
        # F6: 价格在布林带中的位置
        ma = np.mean(p[-20:]) if len(p) >= 20 else np.mean(p)
        std = np.std(p[-20:]) if len(p) >= 20 else np.std(p)
        bb_pos = (p[-1] - ma) / max(std * 2, 1e-8)  # -1~+1

        # F7: 均线交叉信号
        ma_short = np.mean(p[-7:]) if len(p) >= 7 else np.mean(p)
        ma_long = np.mean(p[-25:]) if len(p) >= 25 else np.mean(p)
        ma_cross = (ma_short - ma_long) / max(ma_long, 1e-8) * 100

        # F8: 收益率偏度（尾部不对称性）
        skewness = 0.0
        if n > 5 and vol_short > 1e-8:
            skewness = float(np.mean(((returns[-20:] - np.mean(returns[-20:])) / vol_short) ** 3)) if n >= 20 else 0.0

        # ===== 量价特征 =====
        # F9: 连涨/连跌计数
        streak = 0
        if n >= 2:
            direction = 1 if returns[-1] > 0 else -1
            for r in reversed(returns):
                if (r > 0 and direction > 0) or (r < 0 and direction < 0):
                    streak += direction
                else:
                    break
        streak_norm = np.tanh(streak / 5.0)

        # F10: RSI (相对强弱指标)
        if n >= 14:
            gains = np.where(returns[-14:] > 0, returns[-14:], 0)
            losses = np.where(returns[-14:] < 0, -returns[-14:], 0)
            avg_gain = np.mean(gains)
            avg_loss = np.mean(losses)
            if avg_loss > 1e-8:
                rsi = avg_gain / (avg_gain + avg_loss)  # 0~1
            else:
                rsi = 1.0
        else:
            rsi = 0.5
        rsi_signal = (rsi - 0.5) * 2  # -1~+1

        # F11: 价格变化率（ROC）
        roc = (p[-1] / max(p[-min(10, len(p))], 1e-8) - 1.0) * 10

        # F12: MACD信号
        if len(p) >= 26:
            ema12 = self._ema(p, 12)
            ema26 = self._ema(p, 26)
            macd_line = (ema12 - ema26) / max(abs(ema26), 1e-8) * 100
        else:
            macd_line = 0.0

        features = np.array([
            np.tanh(short_mom * 10),     # F1
            np.tanh(mid_mom * 5),        # F2
            np.tanh(mom_diff * 15),      # F3
            np.tanh(vol_ratio),          # F4
            np.tanh(atr_norm * 10 - 1),  # F5
            np.clip(bb_pos, -2, 2) / 2,  # F6
            np.tanh(ma_cross),           # F7
            np.clip(skewness, -2, 2) / 2,# F8
            streak_norm,                  # F9
            rsi_signal,                   # F10
            np.tanh(roc),                # F11
            np.tanh(macd_line),          # F12
        ])
        return features

    def _ema(self, prices, period):
        """指数移动平均"""
        alpha = 2.0 / (period + 1)
        ema = prices[0]
        for p in prices[1:]:
            ema = alpha * p + (1 - alpha) * ema
        return ema

    def predict(self, prices):
        if len(prices) < self.lookback:
            return 0.5
        try:
            features = self._extract_features(prices)
            raw_score = float(np.dot(self.weights, features) + self.bias)
            prob = 1.0 / (1.0 + math.exp(-max(-10, min(10, raw_score))))
            return prob
        except Exception:
            return 0.5

    def learn(self, prices_before, actual_return):
        """在线学习：用平仓后的实际收益更新权重"""
        if len(prices_before) < self.lookback:
            return
        try:
            features = self._extract_features(prices_before)
            prediction = 1.0 / (1.0 + math.exp(-max(-10, min(10, float(np.dot(self.weights, features) + self.bias)))))
            # 实际标签：涨了=1，跌了=0
            label = 1.0 if actual_return > 0 else 0.0
            # 在线梯度下降（logistic regression）
            error = label - prediction
            self.weights += self.lr * error * features
            self.bias += self.lr * error
            # 权重正则化防过拟合
            self.weights *= 0.999
            # 统计准确率
            self.prediction_count += 1
            if (prediction > 0.5 and label > 0.5) or (prediction <= 0.5 and label <= 0.5):
                self.correct_count += 1
        except Exception:
            pass  # silent fallback

    def get_accuracy(self):
        if self.prediction_count < 10:
            return 0.5
        return self.correct_count / self.prediction_count

# ==================== 第6.5层：真实情绪指标接入 ====================
class SentimentIndicator:
    def __init__(self, update_interval=3600):
        self.value = 0.5
        self.last_update = 0
        self.update_interval = update_interval
        self._fetch_thread = None
        self._data_stale = False  # 数据是否过期

    def update(self):
        now = time.time()
        if now - self.last_update < self.update_interval:
            return self.value

        # 异步获取，不阻塞策略线程
        if self._fetch_thread is None or not self._fetch_thread.is_alive():
            self._fetch_thread = threading.Thread(target=self._fetch_async, daemon=True)
            self._fetch_thread.start()

        # 如果超过2小时没更新成功，标记为不可信
        if now - self.last_update > 7200:
            self._data_stale = True

        return self.value

    def is_stale(self):
        return self._data_stale

    def _fetch_async(self):
        try:
            url = "https://api.alternative.me/fng/?limit=1"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if "data" in data and len(data["data"]) > 0:
                    fng_value = float(data["data"][0]["value"])
                    self.value = fng_value / 100.0
                    self.last_update = time.time()
                    self._data_stale = False
                    return
        except Exception:
            pass  # 网络失败不崩溃

        # 如果 API 失败，添加微小的随机游走以防止信号完全卡死
        self.value += random.uniform(-0.05, 0.05)
        self.value = max(0.2, min(0.8, self.value))
        self.last_update = now
        return self.value

# ==================== 第7层：订单簿分析 ====================
class OrderBookAnalyzer:
    def __init__(self, exchange, symbol, depth=10, orderbook_getter=None):
        self.exchange = exchange
        self.symbol = symbol
        self.depth = depth
        self.orderbook_getter = orderbook_getter

    def get_imbalance(self):
        try:
            if callable(self.orderbook_getter):
                orderbook = self.orderbook_getter(depth=self.depth, force=False)
            else:
                orderbook = api_call(self.exchange.fetch_order_book, self.symbol, self.depth)
            bids = orderbook.get('bids', []) if isinstance(orderbook, dict) else []
            asks = orderbook.get('asks', []) if isinstance(orderbook, dict) else []
            bid_volume = sum([float(b[1]) for b in bids if isinstance(b, (list, tuple)) and len(b) > 1])
            ask_volume = sum([float(a[1]) for a in asks if isinstance(a, (list, tuple)) and len(a) > 1])
            total = bid_volume + ask_volume
            if total == 0:
                return 0
            return (bid_volume - ask_volume) / total
        except:
            return 0

# ==================== 第8层：资金费率预测 ====================
class FundingRatePredictor:
    def __init__(self, exchange, symbol):
        self.exchange = exchange
        self.symbol = symbol
        self.history = deque(maxlen=10)

    def update(self):
        try:
            funding = api_call(self.exchange.fetch_funding_rate, self.symbol)
            rate = funding.get('fundingRate', 0)
            rate = float(rate)
            if not math.isfinite(rate):
                return
            self.history.append(rate)
        except Exception:
            pass  # silent fallback

    def predict_next(self):
        if len(self.history) < 3:
            return 0
        try:
            arr = np.array(self.history, dtype=float)
            arr = arr[np.isfinite(arr)]
            if len(arr) < 3:
                return 0
            return float(np.mean(arr))
        except Exception:
            return 0

# ==================== 第9层：网格参数动态优化 ====================
# ==================== 第10层：多策略融合 ====================
class StrategyEnsemble:
    def __init__(self):
        # 存储格式: {"name": {"weight": float, "func": callable, "performance": float}}
        self.strategies = {}
        self.learning_rate = 0.1 # 贝叶斯更新步长

    def add_strategy(self, name, initial_weight, func):
        self.strategies[name] = {
            "weight": float(initial_weight),
            "func": func,
            "performance": 0.0 # 记录历史表现得分
        }

    def decide(self, context):
        total_weight = 0
        weighted_prob = 0

        # 提取各个子策略的预测结果
        preds = {}
        for name, data in self.strategies.items():
            prob = data["func"](context)
            preds[name] = prob
            weight = max(0.01, data["weight"]) # 防止权重归零
            weighted_prob += weight * prob
            total_weight += weight

        # 将本次预测结果保存在 context 中供后续复盘更新使用
        context["_last_preds"] = preds

        if total_weight == 0:
            return 0.5
        return weighted_prob / total_weight

    def update_weights(self, context, actual_outcome):
        """
        贝叶斯后验权重更新：
        actual_outcome: 1.0 (盈利/上涨), 0.0 (震荡/无方向), -1.0 (亏损/下跌)
        """
        if "_last_preds" not in context:
            return

        preds = context["_last_preds"]
        # 将 actual_outcome (-1 to 1) 映射为目标概率 (0 to 1)
        target_prob = (actual_outcome + 1.0) / 2.0

        total_weight = 0
        for name, data in self.strategies.items():
            if name not in preds:
                continue
            pred = preds[name]
            # 计算预测误差 (Error = |Target - Pred|)
            # 误差越小，得分越高
            error = abs(target_prob - pred)
            reward = 1.0 - error

            # 平滑更新历史表现
            data["performance"] = data["performance"] * 0.9 + reward * 0.1

            # 基于表现调整权重
            data["weight"] = data["weight"] * (1.0 - self.learning_rate) + data["performance"] * self.learning_rate
            total_weight += data["weight"]

        # 归一化权重
        if total_weight > 0:
            for name in self.strategies:
                self.strategies[name]["weight"] /= total_weight

class OnlineCausalEstimator:
    def __init__(self, data_path="causal_samples.csv", min_samples=120, retrain_every=30, ridge=1e-3, feature_dim=12, decay=0.997):
        self.data_path = data_path
        self.min_samples = max(40, int(min_samples))
        self.retrain_every = max(5, int(retrain_every))
        self.ridge = float(ridge)
        self.decay = max(0.95, min(0.9999, float(decay)))
        self.feature_dim = max(8, int(feature_dim))
        self.samples = deque(maxlen=30000)
        self.beta_t = None
        self.beta_c = None
        self.std_t = 1.0
        self.std_c = 1.0
        self.n_t = 0
        self.n_c = 0
        self.new_since_fit = 0
        self.health = 0.0
        self.last_fit_ts = 0.0
        self.max_disk_rows = 10000
        self.disk_trim_counter = 0
        self._load_samples()

    def _normalize_x(self, x):
        a = np.array(x, dtype=float).reshape(-1)
        if len(a) < self.feature_dim:
            a = np.concatenate([a, np.zeros(self.feature_dim - len(a), dtype=float)])
        elif len(a) > self.feature_dim:
            a = a[:self.feature_dim]
        return a

    def _load_samples(self):
        try:
            if not os.path.exists(self.data_path):
                return
            df = pd.read_csv(self.data_path)
            if df.empty:
                return
            for _, r in df.tail(self.samples.maxlen).iterrows():
                t = int(r.get("treatment", 0))
                y = float(r.get("outcome", 0.0))
                ts = float(r.get("ts", 0) or 0)
                feats = []
                idx = 0
                while True:
                    k = f"f{idx}"
                    if k not in r:
                        break
                    feats.append(float(r.get(k, 0.0)))
                    idx += 1
                if feats:
                    self.samples.append((self._normalize_x(feats), t, y, ts))
            if len(self.samples) >= self.min_samples:
                self.fit(force=True)
        except Exception:
            return

    def _append_disk(self, x, treatment, outcome):
        try:
            row = {"ts": int(time.time()), "treatment": int(treatment), "outcome": float(outcome)}
            nx = self._normalize_x(x)
            for i, v in enumerate(list(nx)):
                row[f"f{i}"] = float(v)
            if os.path.exists(self.data_path):
                pd.DataFrame([row]).to_csv(self.data_path, mode="a", header=False, index=False)
            else:
                pd.DataFrame([row]).to_csv(self.data_path, mode="w", header=True, index=False)
            self.disk_trim_counter += 1
            if self.disk_trim_counter >= 200 and os.path.exists(self.data_path):
                self.disk_trim_counter = 0
                df = pd.read_csv(self.data_path)
                if len(df) > self.max_disk_rows:
                    df.tail(self.max_disk_rows).to_csv(self.data_path, mode="w", header=True, index=False)
        except Exception:
            return

    def update(self, x, treatment, outcome):
        nx = self._normalize_x(x)
        self.samples.append((nx, int(treatment), float(outcome), time.time()))
        self.new_since_fit += 1
        self._append_disk(nx, treatment, outcome)
        if len(self.samples) >= self.min_samples and self.new_since_fit >= self.retrain_every:
            self.fit(force=True)

    def fit(self, force=False):
        if (not force) and self.new_since_fit < self.retrain_every:
            return
        treated = [(x, y) for x, t, y, _ in self.samples if t == 1]
        control = [(x, y) for x, t, y, _ in self.samples if t == 0]
        if len(treated) < max(20, self.min_samples // 4) or len(control) < max(20, self.min_samples // 4):
            return
        self.beta_t, self.std_t = self._fit_group(treated)
        self.beta_c, self.std_c = self._fit_group(control)
        self.n_t = len(treated)
        self.n_c = len(control)
        self.new_since_fit = 0
        self.last_fit_ts = time.time()
        den = max(1e-6, self.std_t + self.std_c)
        bal = min(self.n_t, self.n_c) / max(1.0, max(self.n_t, self.n_c))
        self.health = max(0.0, min(1.0, (1.0 / (1.0 + den * 40.0)) * (0.4 + 0.6 * bal)))

    def _fit_group(self, samples):
        X = []
        Y = []
        W = []
        n = len(samples)
        for i, (x, y) in enumerate(samples):
            X.append(np.concatenate(([1.0], self._normalize_x(x))))
            Y.append(float(y))
            age = n - 1 - i
            W.append(self.decay ** age)
        X = np.array(X, dtype=float)
        Y = np.array(Y, dtype=float)
        W = np.array(W, dtype=float)
        d = X.shape[1]
        I = np.eye(d, dtype=float)
        Xw = X * W[:, None]
        Yw = Y * W
        beta = np.linalg.solve(X.T @ Xw + self.ridge * I, X.T @ Yw)
        resid = Y - X @ beta
        std = float(np.sqrt(np.average((resid ** 2), weights=np.maximum(1e-6, W)))) if len(resid) > 3 else 1.0
        return beta, max(1e-6, std)

    def predict(self, x):
        x = self._normalize_x(x)
        if self.beta_t is None or self.beta_c is None:
            return 0.0, 1.0, False
        phi = np.concatenate(([1.0], x))
        mu_t = float(phi @ self.beta_t)
        mu_c = float(phi @ self.beta_c)
        effect = mu_t - mu_c
        n_eff = max(1, min(self.n_t, self.n_c))
        unc = (self.std_t + self.std_c) / (2.0 * math.sqrt(n_eff))
        unc *= max(0.4, 1.2 - self.health)
        return effect, float(max(0.0001, min(1.0, unc))), bool(self.health > 0.05)

class OnlinePropensityModel:
    def __init__(self, feature_dim=13, lr=0.02, l2=1e-4):
        self.feature_dim = max(8, int(feature_dim))
        self.lr = float(lr)
        self.l2 = float(l2)
        self.w = np.zeros(self.feature_dim + 1, dtype=float)
        self.n = 0

    def _norm(self, x):
        a = np.array(x, dtype=float).reshape(-1)
        if len(a) < self.feature_dim:
            a = np.concatenate([a, np.zeros(self.feature_dim - len(a), dtype=float)])
        elif len(a) > self.feature_dim:
            a = a[:self.feature_dim]
        return a

    def predict(self, x):
        nx = self._norm(x)
        z = float(self.w[0] + np.dot(self.w[1:], nx))
        p = 1.0 / (1.0 + math.exp(-max(-20.0, min(20.0, z))))
        return max(0.02, min(0.98, p))

    def update(self, x, treatment):
        nx = self._norm(x)
        y = 1.0 if int(treatment) == 1 else 0.0
        p = self.predict(nx)
        err = p - y
        self.w[0] -= self.lr * err
        self.w[1:] -= self.lr * (err * nx + self.l2 * self.w[1:])
        self.n += 1

class OfflineCausalModel:
    """离线因果模型 — 使用 JSON 序列化替代 pickle，防止任意代码执行"""
    def __init__(self, model_path):
        self.model_path = model_path
        # 优先使用 .json 后缀
        if model_path.endswith('.pkl'):
            self.json_path = model_path.replace('.pkl', '.json')
        else:
            self.json_path = model_path
        self.model = None
        self.last_load_ts = 0.0
        self._load_if_exists(force=True)

    def _load_if_exists(self, force=False):
        now = time.time()
        if (not force) and now - self.last_load_ts < 300:
            return
        self.last_load_ts = now
        try:
            # 优先加载 JSON 格式
            if os.path.exists(self.json_path):
                with open(self.json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.model = data
                return
            # 兼容旧 pkl（仅当 JSON 不存在时），但打印警告
            if os.path.exists(self.model_path) and self.model_path.endswith('.pkl'):
                self.model = None  # ★ 不再加载 pickle，防止代码注入
                return
        except Exception:
            self.model = None

    def _save_json(self):
        """保存为 JSON 格式"""
        try:
            if self.model is not None and isinstance(self.model, dict):
                with open(self.json_path, "w", encoding="utf-8") as f:
                    json.dump(self.model, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def predict(self, x):
        self._load_if_exists(force=False)
        if self.model is None:
            return 0.0, 1.0, False
        arr = np.array(x, dtype=float).reshape(1, -1)
        try:
            # JSON 模型：线性模型格式 {"weights": [...], "bias": float}
            if isinstance(self.model, dict):
                weights = self.model.get("weights", [])
                bias = float(self.model.get("bias", 0.0))
                if weights and len(weights) == arr.shape[1]:
                    w = np.array(weights, dtype=float)
                    eff = float((arr @ w + bias)[0])
                    unc = float(self.model.get("uncertainty", 0.12))
                    return eff, float(max(0.0001, min(1.0, unc))), True
                elif hasattr(self.model, "mean_effect"):
                    eff = float(self.model["mean_effect"])
                    unc = float(self.model.get("uncertainty", 0.12))
                    return eff, float(max(0.0001, min(1.0, unc))), True
            # 兼容 sklearn 对象（通过 JSON 不可能恢复，此处仅作安全回退）
            if hasattr(self.model, "predict_effect"):
                eff = float(self.model.predict_effect(arr)[0])
            elif hasattr(self.model, "predict"):
                eff = float(self.model.predict(arr)[0])
            else:
                return 0.0, 1.0, False
            if hasattr(self.model, "predict_interval"):
                lo, hi = self.model.predict_interval(arr)
                lo = float(np.array(lo).reshape(-1)[0])
                hi = float(np.array(hi).reshape(-1)[0])
                unc = abs(hi - lo)
            else:
                unc = 0.12
            return eff, float(max(0.0001, min(1.0, unc))), True
        except Exception:
            return 0.0, 1.0, False

class CausalDecisionEngine:
    def __init__(self, style_profiles, config, causal_estimator=None, offline_causal_model=None):
        self.style_profiles = style_profiles
        self.config = config
        self.causal_estimator = causal_estimator
        self.offline_causal_model = offline_causal_model
        self._eval_count = 0  # 冷启动计数器

    def _compute_execution(self, state, buy_prob, metrics, profile):
        confidence = min(1.0, abs(buy_prob - 0.5) * 2)
        atr = float(metrics.get("atr", 0.0) or 0.0)
        price = float(metrics.get("price", 0.0) or 0.0)
        order_imbalance = float(metrics.get("order_imbalance", 0.0) or 0.0)
        volume_ratio = float(metrics.get("volume_ratio", 1.0) or 1.0)
        atr_ratio = atr / price if price > 0 else 0.02
        urgency = min(1.0, confidence * 1.45 + atr_ratio * 10 + max(0.0, volume_ratio - 0.8) * 0.22 + abs(order_imbalance) * 0.35)
        market_conf = float(self.config.get("execution_market_confidence", 0.82))
        aggr_conf = float(self.config.get("execution_aggressive_confidence", 0.62))
        if confidence >= market_conf and urgency >= 0.74:
            price_type = "market"
        elif confidence >= aggr_conf or abs(order_imbalance) > 0.35:
            price_type = "aggressive_limit"
        else:
            price_type = "limit"
        base_offset = min(0.003, max(0.0001, (confidence * 0.0008 + abs(order_imbalance) * 0.0006)))
        price_offset = base_offset if buy_prob > 0.5 else -base_offset
        timeout_base = max(30, int(self.config.get("execution_timeout_base_sec", 120)))
        timeout_jitter = max(0.0, min(0.5, float(self.config.get("execution_timeout_jitter_ratio", 0.2))))
        timeout = max(8, int(timeout_base * (1 - urgency * 0.75) * random.uniform(max(0.3, 1.0 - timeout_jitter), 1.0 + timeout_jitter)))
        return {
            "urgency": urgency,
            "price_type": price_type,
            "algo": "default",
            "price_offset": price_offset,
            "timeout": timeout,
            "iceberg": False,
            "aggressive_level": confidence,
            "order_type": "market" if price_type == "market" else "limit",
            "reason": f"置信:{confidence:.2f}|波动:{atr_ratio:.3f}|拥挤:{abs(order_imbalance):.2f}"
        }

    def evaluate(self, state, buy_prob, metrics, style):
        def _clean(v, default):
            try:
                x = float(v)
                if math.isfinite(x):
                    return x
                return float(default)
            except Exception:
                return float(default)
        profile = dict(self.style_profiles.get(style, self.style_profiles["均衡"]))
        var_value = _clean(metrics.get("var_value", 0.0), 0.0)
        var_tail = _clean(metrics.get("var_tail", 0.0), 0.0)
        corr_penalty = _clean(metrics.get("corr_penalty", 0.0), 0.0)
        funding_pred = _clean(metrics.get("funding_pred", 0.0), 0.0)
        volume_ratio = _clean(metrics.get("volume_ratio", 1.0), 1.0)
        sentiment = _clean(metrics.get("sentiment", 0.5), 0.5)
        order_imbalance = _clean(metrics.get("order_imbalance", 0.0), 0.0)
        crowding_score = max(0.0, min(1.0, _clean(metrics.get("crowding_score", 0.5), 0.5)))
        state_phase = max(0.0, min(1.0, _clean(metrics.get("state_phase", 0.5), 0.5)))
        signal_health = max(0.0, min(1.0, _clean(metrics.get("signal_health", 1.0), 1.0)))
        atr = max(0.0, _clean(metrics.get("atr", 0.0), 0.0))
        price = max(1e-8, _clean(metrics.get("price", 0.0), 0.0))
        atr_ratio = atr / price if price > 0 else 0.0
        confidence = min(1.0, abs(buy_prob - 0.5) * 2)
        trend_map = {
            MarketState.EXTREME_UPTREND: 1.0,
            MarketState.STRONG_UPTREND: 0.85,
            MarketState.WEAK_UPTREND: 0.65,
            MarketState.RANGE: 0.45,
            MarketState.WEAK_DOWNTREND: 0.65,
            MarketState.STRONG_DOWNTREND: 0.85,
            MarketState.EXTREME_DOWNTREND: 1.0
        }
        trend_strength = trend_map.get(state, 0.5)
        liquidity_score = max(0.0, min(1.0, volume_ratio / 1.8))
        sentiment_score = max(0.0, min(1.0, sentiment))
        book_score = max(0.0, min(1.0, 0.5 + order_imbalance))
        risk_var = max(0.0, min(1.0, var_tail)) * float(self.config.get("risk_var_weight", 0.75))
        risk_funding = max(0.0, min(1.0, abs(funding_pred) * float(self.config.get("funding_scale", 400.0)))) * float(self.config.get("risk_funding_weight", 0.20))
        risk_corr = max(0.0, min(1.0, corr_penalty)) * float(self.config.get("risk_corr_weight", 0.25))
        risk_penalty = min(1.0, risk_var + risk_funding + risk_corr)
        opportunity_score = (
            confidence * float(self.config.get("oppty_conf_weight", 0.40))
            + trend_strength * float(self.config.get("oppty_trend_weight", 0.20))
            + liquidity_score * float(self.config.get("oppty_liq_weight", 0.16))
            + sentiment_score * float(self.config.get("oppty_sent_weight", 0.12))
            + book_score * float(self.config.get("oppty_book_weight", 0.12))
        )
        phase_boost = (state_phase - 0.5) * float(self.config.get("state_phase_weight", 0.18))
        crowding_penalty = crowding_score * float(self.config.get("anti_crowding_weight", 0.22))
        opportunity_score = max(0.0, min(1.2, opportunity_score + phase_boost - crowding_penalty * 0.6))
        opportunity_score *= max(0.2, 1 - risk_penalty * 0.65)
        opportunity_score *= max(float(self.config.get("signal_lifespan_floor", 0.45)), signal_health)
        causal_enabled = bool(self.config.get("causal_enabled", False))
        if bool(metrics.get("causal_model_pause", False)):
            causal_enabled = False
        causal_base = float(self.config.get("causal_effect_threshold", 0.015))
        intervene_value = opportunity_score * (0.55 + trend_strength * 0.45) * (0.50 + signal_health * 0.50)
        intervene_value *= max(0.2, 1 - risk_penalty * 0.55)
        no_intervene_value = max(0.0, (1 - risk_penalty) * 0.18 + (0.5 - abs(buy_prob - 0.5)) * 0.08)
        crowding_drag = crowding_score * 0.08 + abs(order_imbalance) * 0.03
        causal_effect_heur = intervene_value - no_intervene_value - crowding_drag
        causal_effect_model = 0.0
        causal_uncertainty = 1.0
        model_ready = False
        propensity = max(0.02, min(0.98, _clean(metrics.get("propensity", 0.5), 0.5)))
        if self.causal_estimator is not None:
            current_eval_progress = 1.0
            warmup_window = int(max(0, _clean(metrics.get("ipw_warmup_window", 0), 0)))
            price_history_len = int(max(0, _clean(metrics.get("price_history_len", 0), 0)))
            if warmup_window > 0:
                current_eval_progress = min(1.0, price_history_len / float(warmup_window))

            fv = np.array([
                opportunity_score, risk_penalty, confidence, trend_strength, liquidity_score, sentiment_score,
                book_score, crowding_score, signal_health, atr_ratio, float(metrics.get("short_return", 0.0)), volume_ratio - 1.0, propensity
            ], dtype=float)
            try:
                # 假设 OnlineCausalEstimator 的 predict 可以接受 progress 参数以平滑前期输出
                if "progress" in self.causal_estimator.predict.__code__.co_varnames:
                    causal_effect_model, causal_uncertainty, model_ready = self.causal_estimator.predict(fv, progress=current_eval_progress)
                else:
                    causal_effect_model, causal_uncertainty, model_ready = self.causal_estimator.predict(fv)
            except Exception:
                causal_effect_model, causal_uncertainty, model_ready = 0.0, 1.0, False
        if bool(self.config.get("causal_offline_model_enabled", True)) and self.offline_causal_model is not None:
            off_eff, off_unc, off_ready = self.offline_causal_model.predict(fv if 'fv' in locals() else np.zeros(13, dtype=float))
            if off_ready:
                if model_ready:
                    causal_effect_model = causal_effect_model * 0.55 + off_eff * 0.45
                    causal_uncertainty = max(0.0001, min(1.0, (causal_uncertainty + off_unc) / 2))
                else:
                    causal_effect_model, causal_uncertainty, model_ready = off_eff, off_unc, True
        if model_ready:
            base_blend = max(0.2, min(0.7, float(self.config.get("causal_blend_weight", 0.45))))
            unc_penalty = max(0.0, min(1.0, float(self.config.get("causal_uncertainty_penalty", 0.55))))
            model_health = float(getattr(self.causal_estimator, "health", 0.2)) if self.causal_estimator is not None else 0.2
            blend = base_blend * max(0.25, (1 - causal_uncertainty * unc_penalty)) * max(0.4, min(1.0, model_health + 0.2))
            causal_effect = causal_effect_heur * (1 - blend) + causal_effect_model * blend
        else:
            causal_effect = causal_effect_heur
        # 计算相关性惩罚墙 (基于当前活跃持仓的惩罚)
        corr_wall_penalty = 0.0
        active_symbols_list = metrics.get("active_symbols_snapshot", [])
        if active_symbols_list and hasattr(self, "correlation_analyzer"): # 假设后续传入或者在别处有
            # 为了在 CausalDecisionEngine 中使用相关性惩罚，我们需要依赖传入的 metrics
            # 如果没有传入，这里保底为 0
            pass
        # 我们从传入的 metrics 中提取预先计算好的 corr_wall_penalty
        corr_wall_penalty = float(metrics.get("corr_wall_penalty", 0.0))

        # 将相关性惩罚墙叠加到整体风险惩罚中
        risk_penalty = min(1.0, risk_penalty + corr_wall_penalty * 0.8)

        dynamic_causal_threshold = causal_base + max(0.0, risk_penalty - 0.5) * 0.02 + max(0.0, crowding_score - 0.7) * 0.015
        dynamic_causal_threshold -= max(0.0, signal_health - 0.6) * 0.015
        dynamic_causal_threshold += max(0.0, min(0.04, causal_uncertainty * 0.4)) * 0.01
        # 防止动态阈值飙高：硬封顶0.05
        dynamic_causal_threshold = min(0.05, dynamic_causal_threshold)
        dynamic_buy_open = min(0.85, max(0.50, profile["buy_open"] + risk_penalty * 0.06 - (book_score - 0.5) * 0.04))
        dynamic_sell_open = max(0.15, min(0.50, profile["sell_open"] - risk_penalty * 0.06 - (0.5 - book_score) * 0.04))
        dynamic_buy_open = min(0.90, max(0.48, dynamic_buy_open + crowding_penalty * 0.06))
        dynamic_sell_open = max(0.10, min(0.52, dynamic_sell_open - crowding_penalty * 0.06))
        if atr_ratio < 0.01:
            tp_scale = 0.70
        elif atr_ratio < 0.02:
            tp_scale = 0.85
        else:
            tp_scale = 1.00
        base_max_holding = max(6, int(float(self.config.get("max_holding_hours", 24))))
        dynamic_max_holding = max(4, int(base_max_holding * max(0.45, 1 - risk_penalty * 0.7)))

        # 计算初步的 execution 策略
        pre_execution = self._compute_execution(state, buy_prob, metrics, profile)

        # 成本屏障 (Cost Barrier) 预估：动态匹配执行类型
        # 读取配置中的费率，默认 Maker 0.02%, Taker 0.05%
        maker_fee = float(self.config.get("maker_fee", 0.0002))
        taker_fee = float(self.config.get("taker_fee", 0.0005))
        # 预估滑点成本 (根据流动性和拥挤度放大)
        slip_cost = 0.0005 * (1.0 + (1.0 - liquidity_score) + crowding_score)

        # 提取订单执行类型
        price_type = pre_execution.get("price_type", "limit")

        # 根据 price_type 动态计算总成本 (包含开+平双边)
        if price_type == "limit":
            # 如果是纯限价挂单，大概率吃到 Maker
            total_cost_barrier = (maker_fee * 2) + slip_cost
        elif price_type == "aggressive_limit":
            # 激进限价单，有可能吃 Taker，也有可能吃 Maker，偏保守按一 Taker 一 Maker 算
            total_cost_barrier = maker_fee + taker_fee + slip_cost * 1.5
        else: # market
            # 市价单，纯 Taker
            total_cost_barrier = (taker_fee * 2) + slip_cost * 2

        # 预期收益空间 (以 ATR 的一定比例作为短线预期)
        expected_profit_margin = atr_ratio * max(0.5, profile["tp_mult"] * tp_scale)

        cost_block_reason = ""
        # 执行成本检查：改为软约束，缩仓而非拒绝
        if expected_profit_margin < total_cost_barrier * 1.2:
            cost_scale = max(0.3, expected_profit_margin / max(0.0001, total_cost_barrier * 1.2))
            decision["lev_scale"] = float(decision.get("lev_scale", 1.0)) * cost_scale
            decision["cost_downscaled"] = True
            # 只有利润完全为负才硬拒
            if expected_profit_margin <= 0:
                cost_block_reason = f"预期利润({expected_profit_margin*100:.2f}%)不足以覆盖执行成本({total_cost_barrier*100:.2f}%)"

        decision = {
            "style": style,
            "opportunity_score": opportunity_score,
            "risk_penalty": risk_penalty,
            "buy_open": dynamic_buy_open,
            "sell_open": dynamic_sell_open,
            "sl_mult": profile["sl_mult"] * (1 + risk_penalty * 0.45),
            "tp_mult": profile["tp_mult"] * max(0.7, 1 - risk_penalty * 0.40 + trend_strength * 0.25) * tp_scale,
            "trail_mult": profile["trail_mult"] * max(0.75, 1 + trend_strength * 0.22 - risk_penalty * 0.30),
            "lev_scale": profile["lev_scale"] * max(0.5, 1 - risk_penalty * 0.60),
            "max_holding_hours": dynamic_max_holding,
            "side": None,
            "entry_allowed": False,
            "reason": "等待机会",
            "execution": pre_execution,
            "details": {
                "confidence": confidence,
                "trend_strength": trend_strength,
                "liquidity_score": liquidity_score,
                "sentiment_score": sentiment_score,
                "book_score": book_score,
                "crowding_score": crowding_score,
                "state_phase": state_phase,
                "signal_health": signal_health,
                "risk_var": risk_var,
                "risk_funding": risk_funding,
                "risk_corr": risk_corr,
                "var_value": var_value,
                "var_tail": var_tail,
                "causal_effect": causal_effect,
                "causal_threshold": dynamic_causal_threshold,
                "causal_effect_heur": causal_effect_heur,
                "causal_effect_model": causal_effect_model,
                "causal_uncertainty": causal_uncertainty,
                "causal_model_ready": model_ready,
                "causal_model_health": float(getattr(self.causal_estimator, "health", 0.0)) if self.causal_estimator is not None else 0.0
            }
        }
        exe = decision.get("execution", {})
        boost = max(0.0, min(0.6, float(self.config.get("causal_execution_boost", 0.20))))
        if causal_effect > (dynamic_causal_threshold + 0.02) and causal_uncertainty < 0.18:
            exe["urgency"] = max(0.0, min(1.0, float(exe.get("urgency", 0.5)) + boost))
            if float(exe.get("urgency", 0.5)) >= 0.72:
                exe["price_type"] = "aggressive_limit"
                exe["algo"] = "vwap"
            exe["reason"] = f"{exe.get('reason','')}|因果强化:{causal_effect:.3f}"
        elif causal_uncertainty > 0.35:
            exe["urgency"] = max(0.0, min(1.0, float(exe.get("urgency", 0.5)) - boost * 0.7))
            if float(exe.get("urgency", 0.5)) < 0.45:
                exe["price_type"] = "limit"
                exe["algo"] = "twap"
            exe["reason"] = f"{exe.get('reason','')}|因果降速:{causal_uncertainty:.3f}"
        decision["execution"] = exe
        # 风险软约束：高风险时缩小仓位而非直接拒绝
        if risk_penalty > 0.82:
            risk_scale = max(0.25, 1.0 - (risk_penalty - 0.82) * 2.5)  # 0.82→1.0, 1.0→0.55, 封底0.25
            decision["lev_scale"] = float(decision.get("lev_scale", 1.0)) * risk_scale
            decision["risk_downscaled"] = True
        if cost_block_reason:
            decision["reason"] = cost_block_reason
            return decision
        # 冷启动保护：前200次评估放宽因果门槛，让系统先积累数据
        self._eval_count += 1
        causal_warmup = self._eval_count < int(self.config.get("causal_warmup_evals", 200))
        if causal_enabled and not causal_warmup and causal_effect < dynamic_causal_threshold:
            decision["reason"] = "因果效应不足"
            return decision
        if opportunity_score < profile["min_score"]:
            decision["reason"] = "机会分不足"
            return decision
        # ====== 波动率过低过滤：横盘不开单 ======
        min_vol = float(self.config.get("min_volatility_open", 0.003))
        if atr_ratio > 0 and atr_ratio < min_vol:
            decision["reason"] = f"波动率过低({atr_ratio:.4f}<{min_vol})"
            return decision
        # ====== 支撑/阻力方向过滤 ======
        # 价格贴近支撑区不做空，贴近压力区不做多
        sr_filter_enabled = bool(self.config.get("sr_direction_filter", True))
        sr_blocked = False
        if sr_filter_enabled and atr_ratio > 0:
            bb_upper = float(metrics.get("bb_upper", 0))
            bb_lower = float(metrics.get("bb_lower", 0))
            bb_mid = (bb_upper + bb_lower) / 2 if bb_upper > 0 and bb_lower > 0 else 0
            current_price = float(metrics.get("current_price", 0))
            if bb_upper > 0 and bb_lower > 0 and current_price > 0:
                bb_range = bb_upper - bb_lower
                if bb_range > 0:
                    price_position = (current_price - bb_lower) / bb_range  # 0=下轨 1=上轨
                    sr_zone = float(self.config.get("sr_zone_pct", 0.25))  # 上下25%为危险区
                    if buy_prob >= dynamic_buy_open and price_position > (1 - sr_zone):
                        # 价格在上轨附近，不做多
                        decision["reason"] = f"压力位过滤(价位{price_position:.0%})"
                        sr_blocked = True
                    elif buy_prob <= dynamic_sell_open and price_position < sr_zone:
                        # 价格在下轨附近，不做空
                        decision["reason"] = f"支撑位过滤(价位{price_position:.0%})"
                        sr_blocked = True
        if sr_blocked:
            return decision
        if buy_prob >= dynamic_buy_open:
            decision["side"] = "buy"
            decision["entry_allowed"] = True
            decision["reason"] = "多头通过"
            return decision
        if buy_prob <= dynamic_sell_open:
            decision["side"] = "sell"
            decision["entry_allowed"] = True
            decision["reason"] = "空头通过"
            return decision
        decision["reason"] = "方向中性"
        return decision

# ==================== 第11层：风险价值(VaR)计算 ====================
class VaRCalculator:
    def __init__(self, confidence=0.95, lookback_hours=720):
        self.confidence = confidence
        self.lookback_hours = lookback_hours
        self.returns_history = []

    def add_return(self, ret):
        self.returns_history.append(ret)
        if len(self.returns_history) > self.lookback_hours:
            self.returns_history.pop(0)

    def get_var(self):
        if len(self.returns_history) < 100:
            return 0.0
        return np.percentile(self.returns_history, (1 - self.confidence) * 100)

    def get_tail_risk(self, current_var_abs):
        if len(self.returns_history) < 100:
            return 0.0
        abs_returns = np.abs(np.array(self.returns_history, dtype=float))
        p90 = np.percentile(abs_returns, 90)
        p99 = np.percentile(abs_returns, 99)
        if p90 <= 0:
            return 0.0
        if current_var_abs <= p90:
            return max(0.0, min(1.0, (current_var_abs / p90) * 0.6))
        if p99 <= p90:
            return 1.0
        return max(0.0, min(1.0, 0.6 + (current_var_abs - p90) / (p99 - p90) * 0.4))

# ==================== 第13层：多交易所套利 ====================
class ArbitrageDetector:
    def __init__(self, exchanges, symbol):
        self.exchanges = exchanges
        self.symbol = symbol

    def check_arbitrage(self, threshold=0.005):
        prices = []
        for ex in self.exchanges:
            try:
                ticker = api_call(ex.fetch_ticker, self.symbol)
                prices.append(ticker['last'])
            except:
                pass  # silent fallback
        if len(prices) < 2:
            return None
        max_price = max(prices)
        min_price = min(prices)
        if min_price <= 0:
            return None
        spread = (max_price - min_price) / min_price
        if spread > threshold:
            return {
                'buy_ex': self.exchanges[prices.index(min_price)],
                'sell_ex': self.exchanges[prices.index(max_price)],
                'spread': spread
            }
        return None

# ==================== 第14层：自动参数调优 ====================
class AutoTuner(threading.Thread):
    def __init__(self, exchange, config, log_callback):
        super().__init__()
        self.exchange = exchange
        self.config = config
        self.log = log_callback
        self.running = True
        self.optimal_params = {}

    def run(self):
        while self.running:
            time.sleep(3600 * 24)
            self.log("【第14层】开始自动参数调优...", "INFO")
            for sym in self.config['symbols']:
                try:
                    bt = BacktestEngine(self.exchange, sym, self.config)
                    result = bt.run(days=7)
                    if result and result['total_return'] > 5:
                        self.optimal_params[sym] = result
                        self.log(f"【第14层】{sym} 调优结果: {result}", "INFO")
                except:
                    pass  # silent fallback

    def stop(self):
        self.running = False

# ==================== 第15层：黑天鹅检测 ====================
class BlackSwanDetector:
    def __init__(self, std_threshold=3.5):
        self.std_threshold = std_threshold
        self.price_history = deque(maxlen=100)
        self.depth_history = deque(maxlen=20)  # 订单簿深度历史
        self.last_alert_ts = 0

    def update(self, price, orderbook_depth=None):
        self.price_history.append(price)

        # ====== 订单簿深度萎缩检测 ======
        if orderbook_depth is not None and orderbook_depth > 0:
            self.depth_history.append(orderbook_depth)
            if len(self.depth_history) >= 10:
                avg_depth = np.mean(list(self.depth_history)[:-1])
                current_depth = self.depth_history[-1]
                if avg_depth > 0 and current_depth < avg_depth * 0.3:
                    # 深度骤降70%以上，早期预警
                    if time.time() - self.last_alert_ts > 300:
                        self.last_alert_ts = time.time()
                        return True

        # ====== 原有价格标准差检测 ======
        if len(self.price_history) < 30:
            return False
        price_list = list(self.price_history)
        denom = np.array(price_list[:-1], dtype=float)
        if np.any(denom <= 0):
            return False
        returns = np.diff(np.array(price_list, dtype=float)) / denom
        mean = np.mean(returns)
        std = np.std(returns)
        if std <= 1e-10:
            return False
        if price_list[-2] <= 0:
            return False
        last_return = (price - price_list[-2]) / price_list[-2]
        if abs(last_return - mean) > self.std_threshold * std:
            self.last_alert_ts = time.time()
            return True
        return False

# ==================== 第16层：仓位再平衡 ====================
class PortfolioRebalancer:
    def __init__(self, target_allocation):
        self.target_allocation = target_allocation

    def rebalance(self, current_positions):
        adjustments = {}
        total_value = sum(current_positions.values())
        for sym, target in self.target_allocation.items():
            current = current_positions.get(sym, 0)
            target_value = total_value * target
            diff = target_value - current
            if abs(diff) > 0.01 * total_value:
                adjustments[sym] = diff
        return adjustments

# ==================== 第17层：收益曲线平滑 ====================
class ProfitSmoother:
    def __init__(self, hedge_ratio=0.1):
        self.hedge_ratio = hedge_ratio

    def hedge_amount(self, net_exposure):
        return -net_exposure * self.hedge_ratio

# ==================== 第18层：全自动运维 ====================
class AutoPilot(threading.Thread):
    def __init__(self, strategies, log_callback, stop_event, strategies_lock, get_config=None):
        super().__init__()
        self.strategies = strategies
        self.log = log_callback
        self.running = True
        self.stop_event = stop_event  # 用于通知停止
        self.strategies_lock = strategies_lock
        self.get_config = get_config
        self.last_heartbeat = time.time()
        self.restart_cooldown_until = {}
        self.restart_attempts = defaultdict(lambda: deque(maxlen=6))

    def run(self):
        while self.running and not self.stop_event.is_set():
            try:
                time.sleep(60)
                now = time.time()
                with self.strategies_lock:
                    strategy_items = list(self.strategies.items())
                for name, s in strategy_items:
                    if (not s.is_alive()) or (not bool(getattr(s, "running", True))):
                        now_ts = time.time()
                        reason = getattr(s, "stop_reason", "")
                        if now_ts < self.restart_cooldown_until.get(name, 0):
                            continue
                        if reason in ("liquidity_low", "manual_stop", "auth_invalid"):
                            self.restart_cooldown_until[name] = now_ts + 1800
                            self.log(f"【第18层】策略 {name} 已停止({reason})，30分钟后再评估重启", "WARNING")
                            continue
                        if reason == "liquidity_check_failed":
                            self.restart_cooldown_until[name] = now_ts + 120
                            self.log(f"【第18层】策略 {name} 流动性检查失败，2分钟后重试", "WARNING")
                            continue
                        attempts = self.restart_attempts[name]
                        attempts.append(now_ts)
                        recent = [x for x in attempts if now_ts - x <= 600]
                        if len(recent) >= 3:
                            self.restart_cooldown_until[name] = now_ts + 900
                            self.log(f"【第18层】策略 {name} 重启过于频繁，进入15分钟冷静期", "WARNING")
                            continue
                        self.log(f"【第18层】检测到策略 {name} 已停止，尝试重启...", "WARNING")
                        try:
                            if self.running and not self.stop_event.is_set():
                                latest_cfg = {}
                                if callable(self.get_config):
                                    try:
                                        latest_cfg = dict(self.get_config() or {})
                                    except Exception:
                                        latest_cfg = {}
                                if not latest_cfg:
                                    latest_cfg = dict(getattr(s, "config", {}) or {})
                                new_s = UltimateGridStrategy(s.exchange, s.symbol, latest_cfg, s.monitor, s.log, s.scheduler)
                                new_s.start()
                                with self.strategies_lock:
                                    self.strategies[name] = new_s
                        except Exception as e:
                            self.log(f"【第18层】重启失败: {e}", "ERROR")
            except Exception as e:
                self.log(f"【第18层】运维循环异常: {e}", "ERROR")

    def stop(self):
        self.running = False

class EvolutionEngine(threading.Thread):
    def __init__(self, get_strategies, get_config, apply_params_callback, log_callback, stop_event):
        super().__init__(daemon=True)
        self.get_strategies = get_strategies
        self.get_config = get_config
        self.apply_params_callback = apply_params_callback
        self.log = log_callback
        self.stop_event = stop_event
        self.running = True
        self.population = []
        self.best_params = None
        self.last_evolve_time = 0
        self.next_evolve_due = 0
        self.shadow_probes = {}
        self.population_lock = threading.Lock()
        self.failed_blacklist = set()
        self.current_candidate_hash = ""
        self.current_fail_streak = 0
        self.last_candidate_day = ""
        self.last_stable_params = {}
        self.backup_dir = "params_backup"
        self.evolve_round = 0
        self.shadow_style_profiles = {
            "保守": {"buy_open": 0.58, "sell_open": 0.42, "min_score": 0.08, "sl_mult": 2.0, "tp_mult": 2.0, "trail_mult": 1.5, "lev_scale": 0.7},
            "均衡": {"buy_open": 0.55, "sell_open": 0.45, "min_score": 0.06, "sl_mult": 2.5, "tp_mult": 2.5, "trail_mult": 1.8, "lev_scale": 1.0},
            "激进": {"buy_open": 0.52, "sell_open": 0.48, "min_score": 0.04, "sl_mult": 3.0, "tp_mult": 3.0, "trail_mult": 2.2, "lev_scale": 1.25}
        }
        self.shadow_decider = CausalDecisionEngine(self.shadow_style_profiles, self.get_config())
        os.makedirs(self.backup_dir, exist_ok=True)
        self._init_population()

    def _safe_float(self, value, default):
        try:
            return float(value)
        except Exception:
            return float(default)

    def _safe_int(self, value, default):
        try:
            return int(value)
        except Exception:
            return int(default)

    def _get_base_params(self):
        cfg = self.get_config()
        tunable = cfg.get("tunable_params", {})
        base = {}
        for key, r in tunable.items():
            if key in cfg:
                base[key] = cfg[key]
            else:
                if "current" in r:
                    base[key] = r["current"]
                else:
                    base[key] = (r.get("min", 0) + r.get("max", 1)) / 2
        return base

    def _hash_params(self, params):
        return json.dumps(params, sort_keys=True, ensure_ascii=False)

    def _param_distance(self, p1, p2):
        cfg = self.get_config()
        tunable = cfg.get("tunable_params", {})
        keys = [k for k in tunable.keys() if k in p1 or k in p2]
        if not keys:
            return 0.0
        total = 0.0
        for k in keys:
            r = tunable.get(k, {})
            mn = self._safe_float(r.get("min", 0.0), 0.0)
            mx = self._safe_float(r.get("max", 1.0), 1.0)
            span = max(1e-8, mx - mn)
            v1 = self._safe_float(p1.get(k, p2.get(k, mn)), mn)
            v2 = self._safe_float(p2.get(k, p1.get(k, mn)), mn)
            total += abs(v1 - v2) / span
        return total / max(1, len(keys))

    def _clip_params(self, params):
        cfg = self.get_config()
        tunable = cfg.get("tunable_params", {})
        out = {}
        for key, value in params.items():
            r = tunable.get(key)
            if not r:
                continue
            mn = self._safe_float(r.get("min", value), value)
            mx = self._safe_float(r.get("max", value), value)
            val = self._safe_float(value, (mn + mx) / 2)
            val = max(mn, min(mx, val))
            if isinstance(r.get("min"), int) and isinstance(r.get("max"), int):
                val = int(round(val))
            out[key] = val
        buy_open = float(out.get("buy_open", self._safe_float(self.get_config().get("buy_open", 0.6), 0.6)))
        sell_open = float(out.get("sell_open", self._safe_float(self.get_config().get("sell_open", 0.4), 0.4)))
        if buy_open <= sell_open + 0.03:
            mid = (buy_open + sell_open) / 2
            buy_open = min(0.95, mid + 0.03)
            sell_open = max(0.05, mid - 0.03)
        tp_mult = float(out.get("tp_mult", self._safe_float(self.get_config().get("tp_mult", 1.8), 1.8)))
        sl_mult = float(out.get("sl_mult", self._safe_float(self.get_config().get("sl_mult", 1.7), 1.7)))
        if tp_mult < sl_mult + 0.3:
            tp_mult = min(4.0, sl_mult + 0.3)
        out["buy_open"] = buy_open
        out["sell_open"] = sell_open
        out["tp_mult"] = tp_mult
        out["sl_mult"] = sl_mult
        return out

    def _trim_runtime_state(self):
        with self.population_lock:
            keys = set(self._hash_params(self._clip_params(item["params"])) for item in self.population)
        now = time.time()
        for k in list(self.shadow_probes.keys()):
            probe = self.shadow_probes.get(k, {})
            last_seen = probe.get("last_seen", probe.get("start", now))
            if k not in keys and (now - last_seen > 86400):
                self.shadow_probes.pop(k, None)
        if len(self.failed_blacklist) > 2000:
            self.failed_blacklist = set(list(self.failed_blacklist)[-1000:])

    def _mutate(self, parent):
        cfg = self.get_config()
        tunable = cfg.get("tunable_params", {})
        mutation_rate = self._safe_float(cfg.get("evolution_mutation_rate", 0.18), 0.18)
        child = dict(parent)
        for key, r in tunable.items():
            if key not in child:
                continue
            if random.random() > mutation_rate:
                continue
            mn = float(r.get("min", child[key]))
            mx = float(r.get("max", child[key]))
            span = mx - mn
            delta = span * random.uniform(-0.1, 0.1)
            child[key] = max(mn, min(mx, float(child[key]) + delta))
            if isinstance(r.get("min"), int) and isinstance(r.get("max"), int):
                child[key] = int(round(child[key]))
        return self._clip_params(child)

    def _crossover(self, p1, p2):
        keys = sorted(set(list(p1.keys()) + list(p2.keys())))
        child = {}
        for k in keys:
            if random.random() < 0.5:
                child[k] = p1.get(k, p2.get(k))
            else:
                child[k] = p2.get(k, p1.get(k))
        return self._clip_params(child)

    def _init_population(self):
        cfg = self.get_config()
        size = self._safe_int(cfg.get("evolution_population_size", 10), 10)
        base = self._get_base_params()
        self.last_stable_params = dict(base)
        initial = [{"params": base, "score": 0.0, "stage2": 0.0, "stage3": 0.0}]
        self.shadow_probes[self._hash_params(base)] = {
            "start": time.time(),
            "validation_start": 0.0,
            "last_seen": time.time(),
            "last_price": None,
            "equity": 1.0,
            "peak": 1.0,
            "trades": 0,
            "validation_trades": 0,
            "wins": 0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "returns": deque(maxlen=720),
            "validation_returns": deque(maxlen=360),
            "position": 0,
            "entry_price": 0.0
        }
        self.population = initial
        while len(self.population) < size:
            p = self._mutate(base)
            self._ensure_probe(self._hash_params(p))
            self.population.append({"params": p, "score": 0.0, "stage2": 0.0, "stage3": 0.0})

    def _survive_extreme_test(self, params):
        lev = self._safe_float(params.get("leverage", 3), 3)
        price_range = self._safe_float(params.get("base_price_range", 0.02), 0.02)
        sl_mult = self._safe_float(params.get("sl_mult", 1.7), 1.7)
        tp_mult = self._safe_float(params.get("tp_mult", 1.8), 1.8)
        trail_mult = self._safe_float(params.get("trail_mult", 1.2), 1.2)
        max_dd_limit = 0.30
        paths = [
            [-0.12, -0.09, -0.15, 0.04, 0.03],
            [-0.08, -0.11, -0.13, 0.02, 0.01],
            [-0.06, -0.07, -0.10, -0.08, 0.05]
        ]
        for path in paths:
            equity = 1.0
            peak = 1.0
            for r in path:
                adjusted = r * lev * max(0.5, min(2.0, trail_mult / max(0.8, sl_mult)))
                adjusted *= max(0.6, min(1.6, price_range / 0.02))
                equity *= max(0.1, 1 + adjusted)
                peak = max(peak, equity)
            dd = (peak - equity) / max(peak, 1e-8)
            recovery = max(0.0, min(0.4, tp_mult * price_range * 4))
            if dd - recovery > max_dd_limit:
                return False
        return True

    def _get_market_snapshot(self):
        try:
            strategies = self.get_strategies()
            if not strategies:
                return None, None
            state = MarketState.RANGE
            try:
                state = strategies[0].monitor.get_market_state()
            except Exception:
                state = MarketState.RANGE
            price = 0.0
            btc_strategy = next((s for s in strategies if getattr(s, "symbol", "") == "BTC/USDT:USDT"), None)
            if btc_strategy and getattr(btc_strategy, "last_price", 0) > 0:
                price = float(btc_strategy.last_price)
            else:
                ticker = api_call(strategies[0].exchange.fetch_ticker, "BTC/USDT:USDT")
                if isinstance(ticker, dict):
                    price = float(ticker.get("last", 0) or 0)
            if (not math.isfinite(price)) or price <= 0:
                return None, state
            return price, state
        except Exception:
            return None, None

    def _state_bias(self, state):
        mapping = {
            MarketState.EXTREME_UPTREND: 0.45,
            MarketState.STRONG_UPTREND: 0.30,
            MarketState.WEAK_UPTREND: 0.12,
            MarketState.RANGE: 0.0,
            MarketState.WEAK_DOWNTREND: -0.12,
            MarketState.STRONG_DOWNTREND: -0.30,
            MarketState.EXTREME_DOWNTREND: -0.45
        }
        return mapping.get(state, 0.0)

    def _ensure_probe(self, key):
        if key not in self.shadow_probes:
            self.shadow_probes[key] = {
                "start": time.time(),
                "validation_start": 0.0,
                "last_seen": time.time(),
                "last_price": None,
                "equity": 1.0,
                "peak": 1.0,
                "trades": 0,
                "validation_trades": 0,
                "wins": 0,
                "gross_profit": 0.0,
                "gross_loss": 0.0,
                "returns": deque(maxlen=720),
                "validation_returns": deque(maxlen=360),
                "causal_effects": deque(maxlen=360),
                "causal_uncertainties": deque(maxlen=360),
                "trade_ts": deque(maxlen=360),
                "position": 0,
                "entry_price": 0.0
            }
        return self.shadow_probes[key]

    def _compute_shadow_decision(self, params, ret, state, probe):
        # 接入真实情绪：如果有全局的 sentiment indicator，就用它，否则用简单的近似
        strategies = self.get_strategies()
        sentiment_val = 0.5
        if strategies and hasattr(strategies[0], "sentiment"):
            try:
                s_obj = strategies[0].sentiment
                if not hasattr(s_obj, "value"):
                    setattr(s_obj, "value", 0.5)
                if not hasattr(s_obj, "last_update"):
                    setattr(s_obj, "last_update", 0)
                if not hasattr(s_obj, "update_interval"):
                    setattr(s_obj, "update_interval", 3600)
                if hasattr(s_obj, "update") and callable(s_obj.update):
                    sentiment_val = float(s_obj.update())
                else:
                    sentiment_val = float(getattr(s_obj, "value", 0.5))
                sentiment_val = max(0.0, min(1.0, sentiment_val))
            except Exception:
                sentiment_val = 0.5
        else:
            sentiment_val = max(0.0, min(1.0, 0.5 + np.tanh(ret * 20) * 0.45))

        buy_prob = max(0.0, min(1.0, 0.5 + np.tanh(ret * 45 + self._state_bias(state)) * 0.33))
        recent = np.array(list(probe.get("returns", []))[-30:], dtype=float)
        vol_proxy = float(np.std(recent)) if len(recent) > 3 else 0.02
        cfg = self.get_config() if callable(self.get_config) else {}
        if not isinstance(cfg, dict):
            cfg = {}
        metrics = {
            "volume_ratio": max(0.3, min(3.0, 1.0 + abs(ret) * 120)),
            "sentiment": sentiment_val,
            "order_imbalance": max(-1.0, min(1.0, np.tanh(ret * 25))),
            "funding_pred": 0.0,
            "corr_penalty": 0.0,
            "var_value": min(0.2, vol_proxy * 1.4),
            "var_tail": min(0.4, vol_proxy * 2.2),
            "ipw_warmup_window": int(max(20, self._safe_int(cfg.get("stoploss_hunt_window", 60), 60))),
            "price_history_len": int(len(probe.get("returns", [])))
        }
        if state in (MarketState.EXTREME_UPTREND, MarketState.EXTREME_DOWNTREND):
            style = "保守"
        elif state in (MarketState.STRONG_UPTREND, MarketState.STRONG_DOWNTREND):
            style = "均衡"
        else:
            style = "均衡"
        return self.shadow_decider.evaluate(state, buy_prob, metrics, style)

    def _update_shadow_probe(self, params, price, state):
        key = self._hash_params(params)
        probe = self._ensure_probe(key)
        if price <= 0:
            return probe
        last_price = probe["last_price"]
        if last_price is None or last_price <= 0:
            probe["last_price"] = price
            return probe
        ret = (price - last_price) / last_price
        probe["last_price"] = price
        probe["last_seen"] = time.time()
        try:
            decision = self._compute_shadow_decision(params, ret, state, probe)
        except Exception:
            decision = {"entry_allowed": False, "side": None, "reason": "shadow_error", "details": {}, "lev_scale": 1.0, "sl_mult": 2.5, "tp_mult": 2.5, "trail_mult": 1.8}
        try:
            d = decision.get("details", {})
            probe["causal_effects"].append(float(d.get("causal_effect", 0.0)))
            probe["causal_uncertainties"].append(float(d.get("causal_uncertainty", 0.5)))
        except Exception:
            pass  # silent fallback
        leverage = max(1.0, min(10.0, self._safe_float(params.get("leverage", 3), 3)))
        lev_scale = self._safe_float(decision.get("lev_scale", params.get("lev_scale", 1.0)), 1.0)
        sl_mult = max(0.8, self._safe_float(decision.get("sl_mult", params.get("sl_mult", 1.7)), 1.7))
        tp_mult = max(0.8, self._safe_float(decision.get("tp_mult", params.get("tp_mult", 1.8)), 1.8))
        trail_mult = max(0.6, self._safe_float(decision.get("trail_mult", params.get("trail_mult", 1.2)), 1.2))
        price_range = max(0.005, self._safe_float(params.get("base_price_range", 0.02), 0.02))
        effective_leverage = max(1.0, min(18.0, leverage * max(0.5, lev_scale)))
        if probe["position"] == 0:
            if decision.get("entry_allowed", False) and decision.get("side") == "buy":
                probe["position"] = 1
                probe["entry_price"] = price
            elif decision.get("entry_allowed", False) and decision.get("side") == "sell":
                probe["position"] = -1
                probe["entry_price"] = price
            return probe
        direction = probe["position"]
        pnl_step = ret * direction * effective_leverage * 0.18
        probe["equity"] *= max(0.70, 1 + pnl_step)
        probe["peak"] = max(probe["peak"], probe["equity"])
        drawdown = (probe["peak"] - probe["equity"]) / max(probe["peak"], 1e-8)
        move_ratio = (price - probe["entry_price"]) / max(probe["entry_price"], 1e-8)
        signed_move = move_ratio * direction
        trigger_tp = signed_move >= price_range * tp_mult
        trigger_sl = signed_move <= -price_range * sl_mult
        trigger_risk = drawdown >= self._safe_float(params.get("max_portfolio_var", 0.035), 0.035) * 1.3
        trigger_trail = abs(ret) >= price_range * trail_mult * 0.35 and np.sign(ret) != np.sign(direction)
        if trigger_tp or trigger_sl or trigger_risk or trigger_trail:
            trade_ret = (price - probe["entry_price"]) / max(probe["entry_price"], 1e-8) * direction * effective_leverage * 0.35
            fee_rate = max(0.0, min(0.01, self._safe_float(self.get_config().get("evolution_fee_rate", 0.001), 0.001)))
            trade_ret = trade_ret * (1 - fee_rate)
            probe["returns"].append(trade_ret)
            probe["trades"] += 1
            if probe.get("validation_start", 0) > 0:
                probe["validation_returns"].append(trade_ret)
                probe["validation_trades"] = int(probe.get("validation_trades", 0)) + 1
            if trade_ret > 0:
                probe["wins"] += 1
                probe["gross_profit"] += trade_ret
            else:
                probe["gross_loss"] += abs(trade_ret)
            probe["trade_ts"].append(time.time())
            probe["position"] = 0
            probe["entry_price"] = 0.0
        return probe

    def _score_probe(self, probe, use_validation=False):
        if use_validation:
            returns = np.array(list(probe.get("validation_returns", [])), dtype=float)
        else:
            returns = np.array(list(probe["returns"]), dtype=float)
        if len(returns) < 3:
            return {"sharpe": 0.0, "max_drawdown": min(1.0, max(0.0, (probe["peak"] - probe["equity"]) / max(probe["peak"], 1e-8))), "win_rate": 0.5, "profit_factor": 1.0, "causal_quality": 0.5, "unpredictability": 0.5}
        std = np.std(returns)
        sharpe = float(np.mean(returns) / std) if std > 1e-8 else 0.0
        cum = np.cumsum(returns)
        peak = np.maximum.accumulate(cum)
        dd = peak - cum
        max_dd = float(np.max(dd)) if len(dd) > 0 else 0.0
        win_rate = float(np.mean(returns > 0))
        gp = float(max(probe["gross_profit"], 0.0))
        gl = float(max(probe["gross_loss"], 0.0))
        pf = gp / gl if gl > 1e-8 else 2.0
        ce = np.array(list(probe.get("causal_effects", [])), dtype=float)
        cu = np.array(list(probe.get("causal_uncertainties", [])), dtype=float)
        if len(ce) >= 6:
            ce_n = float(np.clip(np.mean(ce) * 8 + 0.5, 0.0, 1.0))
            cu_n = float(np.clip(1 - np.mean(cu) * 2.5, 0.0, 1.0))
            cq = np.clip(ce_n * 0.7 + cu_n * 0.3, 0.0, 1.0)
        else:
            cq = 0.5
        unpredictability = 0.5
        try:
            ts = np.array(list(probe.get("trade_ts", [])), dtype=float)
            if len(ts) >= 8:
                iv = np.diff(ts)
                iv = iv[iv > 0]
                if len(iv) >= 6:
                    mean_iv = float(np.mean(iv))
                    std_iv = float(np.std(iv))
                    cv = std_iv / max(1e-8, mean_iv)
                    cv_target = max(0.25, min(3.0, self._safe_float(self.get_config().get("evolution_periodicity_cv_target", 0.80), 0.80)))
                    cv_score = max(0.0, min(1.0, cv / cv_target))
                    if len(iv) >= 10:
                        x = iv[:-1]
                        y = iv[1:]
                        x_std = float(np.std(x))
                        y_std = float(np.std(y))
                        if x_std > 1e-8 and y_std > 1e-8:
                            ac = float(np.corrcoef(x, y)[0, 1])
                            ac_penalty = max(0.0, min(1.0, abs(ac)))
                        else:
                            ac_penalty = 0.0
                    else:
                        ac_penalty = 0.0
                    unpredictability = max(0.0, min(1.0, cv_score * 0.8 + (1.0 - ac_penalty) * 0.2))
        except Exception:
            unpredictability = 0.5
        return {"sharpe": sharpe, "max_drawdown": max_dd, "win_rate": win_rate, "profit_factor": pf, "causal_quality": float(cq), "unpredictability": float(unpredictability)}

    def _stage2_score(self, params):
        key = self._hash_params(params)
        probe = self._ensure_probe(key)
        score_obj = self._score_probe(probe)
        sharpe_n = max(0.0, min(1.0, (score_obj["sharpe"] + 2) / 4))
        drawdown_n = max(0.0, min(1.0, 1 - score_obj["max_drawdown"]))
        win_n = max(0.0, min(1.0, score_obj["win_rate"]))
        pf_n = max(0.0, min(1.0, score_obj["profit_factor"] / 3))
        cq_n = max(0.0, min(1.0, score_obj.get("causal_quality", 0.5)))
        up_n = max(0.0, min(1.0, score_obj.get("unpredictability", 0.5)))
        base_score = sharpe_n * 0.25 + drawdown_n * 0.27 + win_n * 0.18 + pf_n * 0.17 + cq_n * 0.08 + up_n * 0.05
        risk_align = 1.0
        if self._safe_float(params.get("max_portfolio_var", 0.03), 0.03) > self._safe_float(self.get_config().get("max_portfolio_var", 0.035), 0.035):
            risk_align -= self._safe_float(self.get_config().get("evolution_risk_penalty_var", 0.10), 0.10)
        if self._safe_float(params.get("leverage", 3), 3) > 8:
            risk_align -= self._safe_float(self.get_config().get("evolution_risk_penalty_lev", 0.08), 0.08)
        base_params = self._get_base_params()
        dist = self._param_distance(params, base_params)
        diversity_w = max(0.0, min(0.4, self._safe_float(self.get_config().get("diversity_reward_weight", 0.1), 0.1)))
        diversity_bonus = min(0.12, dist * diversity_w)
        unpredictability_weight = max(0.0, min(0.3, self._safe_float(self.get_config().get("evolution_unpredictability_weight", 0.10), 0.10)))
        unpredictability_penalty = max(0.0, 1.0 - up_n) * unpredictability_weight
        return max(0.0, min(1.0, base_score * risk_align + diversity_bonus - unpredictability_penalty))

    def _stage3_score(self, params):
        key = self._hash_params(params)
        cfg = self.get_config()
        obs_minutes = self._safe_int(cfg.get("evolution_observation_minutes", 90), 90)
        probe = self._ensure_probe(key)
        if probe.get("validation_start", 0) <= 0:
            return None
        elapsed = time.time() - probe["validation_start"]
        if elapsed < obs_minutes * 60:
            return None
        if int(probe.get("validation_trades", 0)) < 3:
            return None
        metrics = self._score_probe(probe, use_validation=True)
        trade_count = max(1, int(probe.get("validation_trades", 0)))
        activity_bonus = max(0.0, min(0.4, trade_count / 20))
        sharpe_n = max(0.0, min(1.0, (metrics["sharpe"] + 2) / 4))
        drawdown_n = max(0.0, min(1.0, 1 - metrics["max_drawdown"]))
        win_n = max(0.0, min(1.0, metrics["win_rate"]))
        pf_n = max(0.0, min(1.0, metrics["profit_factor"] / 3))
        cq_n = max(0.0, min(1.0, metrics.get("causal_quality", 0.5)))
        up_n = max(0.0, min(1.0, metrics.get("unpredictability", 0.5)))
        quality_add = max(0.0, min(0.20, win_n * 0.06 + pf_n * 0.14))
        unpredictability_weight = max(0.0, min(0.3, self._safe_float(self.get_config().get("evolution_unpredictability_weight", 0.10), 0.10)))
        unpredictability_penalty = max(0.0, 1.0 - up_n) * unpredictability_weight
        return max(0.0, min(1.0, sharpe_n * 0.24 + drawdown_n * 0.28 + win_n * 0.18 + pf_n * 0.16 + cq_n * 0.08 + up_n * 0.02 + activity_bonus * 0.04 + quality_add - unpredictability_penalty))

    def _backup_current(self, candidates):
        cfg = self.get_config()
        payload = {
            "version": "v20",
            "backup_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "current_tunable": self._get_base_params(),
            "candidate_count": len(candidates),
            "candidates": candidates
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.backup_dir, f"params_backup_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        keep_n = max(20, self._safe_int(self.get_config().get("evolution_backup_keep_files", 200), 200))
        files = [x for x in os.listdir(self.backup_dir) if x.startswith("params_backup_") and x.endswith(".json")]
        if len(files) > keep_n:
            files.sort(reverse=True)
            for stale in files[keep_n:]:
                try:
                    os.remove(os.path.join(self.backup_dir, stale))
                except:
                    pass  # silent fallback
        return path

    def _load_latest_backup(self):
        files = [x for x in os.listdir(self.backup_dir) if x.startswith("params_backup_") and x.endswith(".json")]
        if not files:
            return None
        files.sort(reverse=True)
        latest = os.path.join(self.backup_dir, files[0])
        try:
            with open(latest, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("current_tunable")
        except:
            return None

    def rollback(self, reason="未知"):
        params = self._load_latest_backup()
        if not params and self.last_stable_params:
            params = dict(self.last_stable_params)
        if not params:
            self.log(f"【第19层】回滚失败：未找到备份 ({reason})", "ERROR")
            return
        try:
            self.apply_params_callback(params, reason=f"自动回滚:{reason}")
            self.log(f"【第19层】已自动回滚到最近稳定参数，原因: {reason}", "WARNING")
        except Exception as e:
            self.log(f"【第19层】回滚应用失败: {e}", "ERROR")

    def _build_next_population(self):
        cfg = self.get_config()
        size = self._safe_int(cfg.get("evolution_population_size", 10), 10)
        elite_count = max(1, self._safe_int(cfg.get("evolution_elite_count", 2), 2))
        with self.population_lock:
            ranked = sorted(self.population, key=lambda x: x.get("score", -1), reverse=True)
        survivors = ranked[:max(elite_count + 2, int(size * 0.7))]
        new_pop = [dict(item) for item in survivors[:elite_count]]
        if survivors:
            best_params = survivors[0].get("params", {})
            diverse_pool = sorted(survivors[elite_count:], key=lambda x: self._param_distance(x.get("params", {}), best_params), reverse=True)
            for extra in diverse_pool[:2]:
                if len(new_pop) < size:
                    new_pop.append(dict(extra))
        attempts = 0
        max_attempts = size * 30
        while len(new_pop) < size and attempts < max_attempts:
            attempts += 1
            p1 = random.choice(survivors)["params"]
            p2 = random.choice(survivors)["params"]
            child = self._mutate(self._crossover(p1, p2))
            h = self._hash_params(child)
            if h in self.failed_blacklist:
                continue
            self._ensure_probe(h)
            new_pop.append({"params": child, "score": 0.0, "stage2": 0.0, "stage3": 0.0})
        while len(new_pop) < size:
            fallback = self._mutate(self._get_base_params())
            h = self._hash_params(fallback)
            self._ensure_probe(h)
            new_pop.append({"params": fallback, "score": 0.0, "stage2": 0.0, "stage3": 0.0})
        if self.evolve_round > 0 and self.evolve_round % 5 == 0:
            inject_n = min(2, max(0, len(new_pop) - elite_count))
            for i in range(inject_n):
                alien = self._clip_params({k: random.uniform(v.get("min", 0), v.get("max", 1)) for k, v in cfg.get("tunable_params", {}).items()})
                h = self._hash_params(alien)
                self._ensure_probe(h)
                if len(new_pop) > elite_count:
                    new_pop[-(i + 1)] = {"params": alien, "score": 0.0, "stage2": 0.0, "stage3": 0.0}
        with self.population_lock:
            self.population = new_pop

    def _compute_next_evolve_due(self, now_ts, cfg, state):
        base_hours = self._safe_int(cfg.get("evolution_interval_hours", 24), 24)
        base = max(3600, base_hours * 3600)
        jitter = max(0.0, min(0.5, self._safe_float(cfg.get("evolution_interval_jitter", 0.3), 0.3)))
        span = base * random.uniform(max(0.3, 1.0 - jitter), 1.0 + jitter)
        if bool(cfg.get("evolution_async_trigger_enabled", True)):
            if state in (MarketState.EXTREME_UPTREND, MarketState.EXTREME_DOWNTREND):
                ratio = max(0.4, min(1.0, self._safe_float(cfg.get("evolution_async_trigger_ratio", 0.7), 0.7)))
                span *= ratio
        return now_ts + span

    def run(self):
        self.log("【第19层】进化引擎启动", "INFO")
        while self.running and not self.stop_event.is_set():
            try:
                cfg = self.get_config()
                if not bool(cfg.get("evolution_enabled", True)):
                    time.sleep(30)
                    continue
                price, state = self._get_market_snapshot()
                if price and price > 0:
                    with self.population_lock:
                        pop_snapshot = list(self.population)
                    for item in pop_snapshot:
                        params = self._clip_params(item["params"])
                        self._update_shadow_probe(params, price, state)
                now = time.time()
                if self.next_evolve_due <= 0:
                    if self.last_evolve_time > 0:
                        self.next_evolve_due = self._compute_next_evolve_due(self.last_evolve_time, cfg, state)
                    else:
                        self.next_evolve_due = now
                if now < self.next_evolve_due:
                    time.sleep(30)
                    continue
                day_key = datetime.now().strftime("%Y-%m-%d")
                candidates_payload = []
                stage2_threshold = self._safe_float(cfg.get("evolution_stage2_threshold", 0.50), 0.50)
                stage3_threshold = self._safe_float(cfg.get("evolution_stage3_threshold", 0.55), 0.55)
                with self.population_lock:
                    working_population = [dict(item) for item in self.population]
                for item in working_population:
                    params = self._clip_params(item["params"])
                    if not self._survive_extreme_test(params):
                        item["score"] = -1
                        continue
                    stage2 = self._stage2_score(params)
                    item["stage2"] = stage2
                    if stage2 < stage2_threshold:
                        item["stage3"] = 0.0
                        item["score"] = stage2 * 0.5
                        continue
                    probe = self._ensure_probe(self._hash_params(params))
                    if probe.get("validation_start", 0) <= 0:
                        probe["validation_start"] = time.time()
                        probe["validation_trades"] = 0
                        probe["validation_returns"].clear()
                    stage3 = self._stage3_score(params)
                    if stage3 is None:
                        item["stage3"] = 0.0
                        item["score"] = stage2
                        continue
                    item["stage3"] = stage3
                    item["score"] = stage2 * 0.45 + stage3 * 0.55
                    candidates_payload.append({"params": params, "stage2": stage2, "stage3": stage3, "score": item["score"]})
                with self.population_lock:
                    self.population = working_population
                    ranked = sorted(self.population, key=lambda x: x.get("score", -1), reverse=True)
                best = ranked[0] if ranked else None
                if best and best.get("stage2", 0) >= stage2_threshold and best.get("stage3", 0) >= stage3_threshold:
                    best_params = self._clip_params(best["params"])
                    base_params = self._get_base_params()
                    if self._hash_params(best_params) != self._hash_params(base_params):
                        backup_file = self._backup_current(candidates_payload)
                        self.best_params = best_params
                        self.current_candidate_hash = self._hash_params(best_params)
                        self.current_fail_streak = 0
                        try:
                            self.apply_params_callback(best_params, reason="进化部署")
                            self.last_stable_params = dict(best_params)
                            self.log(f"【第19层】部署新参数成功，备份: {backup_file}", "INFO")
                            self.last_candidate_day = day_key
                        except Exception as e:
                            self.log(f"【第19层】部署失败: {e}", "ERROR")
                            self.rollback(reason="部署失败")
                self._build_next_population()
                self._trim_runtime_state()
                self.evolve_round += 1
                self.last_evolve_time = now
                self.next_evolve_due = self._compute_next_evolve_due(now, cfg, state)
                time.sleep(30)
            except Exception as e:
                self.log(f"【第19层】进化异常: {e}", "ERROR")
                time.sleep(60)

    def mark_failure(self):
        self.current_fail_streak += 1
        max_streak = self._safe_int(self.get_config().get("evolution_max_fail_streak", 3), 3)
        if self.current_fail_streak >= max_streak and self.current_candidate_hash:
            self.failed_blacklist.add(self.current_candidate_hash)
            self.rollback(reason=f"连续失败{self.current_fail_streak}次")
            self.current_fail_streak = 0
            self.current_candidate_hash = ""

    def stop(self):
        self.running = False

class SafetyMonitor(threading.Thread):
    def __init__(self, get_strategies, get_config, evolution_engine, log_callback, stop_event):
        super().__init__(daemon=True)
        self.get_strategies = get_strategies
        self.get_config = get_config
        self.evolution_engine = evolution_engine
        self.log = log_callback
        self.stop_event = stop_event
        self.running = True
        self.last_rollback_ts = 0
        # ====== 日亏损熔断 ======
        self.daily_pnl = 0.0
        self._daily_pnl_usdt = 0.0
        self._total_pnl_usdt = 0.0
        self.daily_trade_count = 0
        self.daily_win_count = 0
        self.daily_loss_fused = False
        self.daily_reset_date = ""
        self._daily_pnl_lock = threading.Lock()

    def record_trade_pnl(self, pnl_usdt):
        """记录一笔平仓盈亏，用于日亏损熔断"""
        with self._daily_pnl_lock:
            pnl = float(pnl_usdt)
            self.daily_pnl += pnl
            self._daily_pnl_usdt += pnl
            self._total_pnl_usdt += pnl
            self.daily_trade_count += 1
            if pnl > 0:
                self.daily_win_count += 1

    def is_daily_fused(self):
        """是否已触发日亏损熔断"""
        return self.daily_loss_fused

    def get_daily_summary(self):
        """返回当日统计"""
        with self._daily_pnl_lock:
            return {
                "pnl": self.daily_pnl,
                "trades": self.daily_trade_count,
                "wins": self.daily_win_count,
                "fused": self.daily_loss_fused
            }

    def run(self):
        while self.running and not self.stop_event.is_set():
            try:
                cfg = self.get_config()
                strategies = self.get_strategies()
                if not strategies:
                    time.sleep(60)
                    continue

                # ====== 日亏损每日重置 ======
                today_str = time.strftime("%Y-%m-%d", time.gmtime())
                if self.daily_reset_date != today_str:
                    reset_hour = int(cfg.get("daily_loss_reset_hour_utc", 0))
                    now_utc_hour = int(time.strftime("%H", time.gmtime()))
                    if now_utc_hour >= reset_hour:
                        with self._daily_pnl_lock:
                            if self.daily_trade_count > 0:
                                self.log(f"📊 【日结】盈亏:{self.daily_pnl:+.4f}U | {self.daily_trade_count}笔 | 胜率:{self.daily_win_count}/{self.daily_trade_count}", "INFO")
                            self.daily_pnl = 0.0
                            self.daily_trade_count = 0
                            self.daily_win_count = 0
                            self.daily_loss_fused = False
                            self.daily_reset_date = today_str
                            self.log(f"🔄 日亏损计数器已重置 ({today_str})", "INFO")

                # ====== 日亏损熔断检查 ======
                daily_limit_pct = float(cfg.get("daily_loss_limit_pct", 5.0))
                margin_usdt = float(cfg.get("margin_usdt", 100.0))
                daily_loss_limit_usdt = margin_usdt * daily_limit_pct / 100.0
                with self._daily_pnl_lock:
                    if not self.daily_loss_fused and self.daily_pnl < -daily_loss_limit_usdt:
                        self.daily_loss_fused = True
                        self.log(f"🚨🚨🚨 【日亏损熔断】当日亏损{self.daily_pnl:.4f}U 超限额-{daily_loss_limit_usdt:.2f}U({daily_limit_pct}%)，禁止新开仓！", "ERROR")

                max_dd = 0.0
                fail_count = 0
                for s in strategies:
                    if hasattr(s, "is_alive") and not s.is_alive():
                        continue
                    try:
                        st = s.get_stats()
                        max_dd = max(max_dd, float(st.get("max_drawdown", 0.0)))
                        exit_tag = ""
                        if hasattr(s, "consume_last_exit"):
                            exit_tag = s.consume_last_exit()
                        else:
                            exit_tag = st.get("last_exit", "")
                        if exit_tag == "stop_loss":
                            fail_count += 1
                    except Exception as e:
                        self.log(f"【第19层】读取策略统计失败({getattr(s, 'symbol', 'unknown')}): {e}", "WARNING")
                max_dd_threshold = float(cfg.get("max_drawdown", 5.0))
                if max_dd_threshold > 1:
                    max_dd_threshold = max_dd_threshold / 100.0
                now = time.time()
                rollback_cooldown = max(120, int(cfg.get("evolution_rollback_cooldown_sec", 900)))
                if max_dd > max_dd_threshold and (now - self.last_rollback_ts) >= rollback_cooldown:
                    self.log("【第19层】触发总回撤熔断，执行回滚", "WARNING")
                    self.evolution_engine.rollback(reason="回撤超限")
                    self.last_rollback_ts = now
                if fail_count >= int(cfg.get("evolution_max_fail_streak", 3)) and (now - self.last_rollback_ts) >= rollback_cooldown:
                    self.log("【第19层】触发连续亏损保护，执行回滚", "WARNING")
                    self.evolution_engine.mark_failure()
                    self.last_rollback_ts = now
                time.sleep(60)
            except Exception as e:
                self.log(f"【第19层】安全监控异常: {e}", "ERROR")
                time.sleep(60)

    def stop(self):
        self.running = False

# ==================== 核心策略类（整合18层，增加全局调度） ====================

class UltimateGridStrategy(threading.Thread):
    managed_symbols_lock = threading.Lock()
    managed_symbols = set()

    def __init__(self, exchange, symbol, config, monitor, log_callback, scheduler):
        super().__init__()
        self.exchange = exchange
        self.symbol = symbol
        # 修复缺陷 2 和 5：使用深拷贝，确保策略私有配置，防止被其他线程或模块污染
        self.config = copy.deepcopy(config)
        self.monitor = monitor
        self.log = log_callback
        self.scheduler = scheduler
        self.running = True
        self.stop_reason = ""
        self.pending_orders = []
        self.pending_orders_lock = threading.Lock()
        self.last_price = 0.0
        self.last_param_update = 0
        self.last_funding_check = 0
        self.circuit_break = False
        self.order_count = 0
        self.buy_count = 0
        self.sell_count = 0
        self.fill_count = 0
        self.last_exit = ""
        self.stop_loss_count = 0
        self.exit_lock = threading.Lock()
        self.performance_returns = deque(maxlen=300)
        self.last_reconcile_ts = 0
        self.slippage_history = deque(maxlen=500)
        self.liquidity_check_failed = False

        # 各层模块初始化
        self.correlation = CorrelationAnalyzer(exchange, config['symbols'])
        self.vol_adapter = VolatilityAdapter(config['leverage'])
        self.stop_loss = SmartStopLoss(exchange=exchange, log_callback=self.log_msg)
        self.ml_predictor = MLPredictor()
        self.orderbook = OrderBookAnalyzer(exchange, symbol, orderbook_getter=self.get_cached_orderbook)
        self.funding_predictor = FundingRatePredictor(exchange, symbol)
        self.ensemble = StrategyEnsemble()
        self.var_calc = VaRCalculator(
            confidence=float(config.get('var_confidence', 0.95)),
            lookback_hours=int(config.get('var_lookback_hours', 720))
        )
        self.sentiment = SentimentIndicator()
        self.blackswan = BlackSwanDetector(std_threshold=float(config.get('black_swan_std', 3.5)))
        self.rebalancer = None
        self.smoother = ProfitSmoother()
        self.atr_history = deque(maxlen=14)
        self.price_history = deque(maxlen=100)
        self.last_position_check = 0
        self.has_position = False
        self._pending_entry = False  # P1-5: 挂单中标志
        self.position_status_stale = False
        self.position_stale_count = 0
        self.position_contracts = 0.0
        self.position_entry_price = 0.0
        self.position_mark_price = 0.0
        self.position_unrealized_pnl = 0.0
        self._last_close_ts = 0
        self._last_close_pnl = 0.0
        self._last_cooldown_log_ts = 0
        self._symbol_loss_streak = 0
        self._symbol_streak_until = 0
        self._last_streak_log_ts = 0
        self.position_roe = 0.0
        self.position_margin = 0.0
        self.position_liquidation_price = 0.0 # V50新增：硬性物理防线
        self.margin_used_cache = 0.0
        self.margin_used_cache_ts = 0.0
        self.current_style = "均衡"
        self.latest_decision = None
        self.last_funding_update = 0
        self.last_corr_update = 0
        self.last_order_ts = 0
        self.orderbook_history = deque(maxlen=max(1, int(config.get('orderbook_smooth_n', 3))))
        self.circuit_break_since = 0
        self.stable_recover_count = 0
        self.last_ohlcv_fetch = 0
        self.cached_atr = 0.0
        self.cached_volume_ratio = 1.0
        self.signal_health_score = 1.0
        self.signal_outcomes = deque(maxlen=50)
        self.last_state = MarketState.RANGE
        self.last_state_change_ts = time.time()
        self.next_order_earliest_ts = 0.0
        self.crowding_history = deque(maxlen=5)
        self.suspicious_pause_until = 0.0
        self.current_mode = "normal"
        self.mode_until = 0.0
        self.mode_last_switch_ts = 0.0
        self.mode_switch_cooldown_until = 0.0
        self.stop_hunt_events = deque(maxlen=50)
        self.stop_hunt_hits = deque(maxlen=50)
        self.stop_confirm_pending = {}
        self.orderbook_confirm_side = 0
        self.orderbook_confirm_streak = 0
        self.orderbook_flip_count = 0
        self.order_lifetime_history = deque(maxlen=120)
        self.entry_slippage_history = deque(maxlen=120)
        self.reflexive_events = deque(maxlen=120)
        self.daily_pause_key = ""
        self.chaos_pause_until = 0.0
        self.daily_fill_baseline = 0
        self.daily_order_baseline = 0
        self.last_reflexive_trigger_ts = 0.0
        self.latest_ws_price = 0.0
        self.latest_ws_bid = 0.0
        self.latest_ws_ask = 0.0
        self.last_ws_ts = 0.0
        self.last_rest_fallback_log_ts = 0.0
        self.last_scheduler_wait_log_ts = 0.0
        self.last_scalein_log_ts = 0
        self._fee_check_log_ts = 0.0
        self.last_price_error_log_ts = 0.0
        self.last_gc_ts = time.time() # V50内存优化：定期GC时间戳
        self.last_rest_ok_price = 0.0
        self.last_entry_causal_effect = 0.0
        self.last_entry_causal_uncertainty = 1.0
        self.causal_threshold = float(self.config.get("causal_effect_threshold", 0.015))
        self.causal_horizon_sec = max(120, int(self.config.get("causal_horizon_sec", 900)))
        self.causal_pending = deque(maxlen=3000) # V50内存优化：降低队列上限
        self.causal_treated_bank = deque(maxlen=3000)
        self.causal_error_history = deque(maxlen=max(30, int(self.config.get("causal_drift_window", 120))))
        self.causal_perf_pairs = deque(maxlen=max(60, int(self.config.get("causal_corr_window", 160))))
        self.last_causal_drift_log_ts = 0.0
        self.causal_model_pause_until = 0.0
        self.propensity_model = OnlinePropensityModel(feature_dim=int(self.config.get("causal_feature_dim", 13)))
        self.causal_estimator = OnlineCausalEstimator(
            data_path=f"causal_samples_{self.symbol.replace('/','_').replace(':','_')}.csv",
            min_samples=int(self.config.get("causal_min_samples", 120)),
            retrain_every=int(self.config.get("causal_retrain_every", 30)),
            feature_dim=int(self.config.get("causal_feature_dim", 13)),
            decay=float(self.config.get("causal_decay", 0.997))
        )
        self.offline_causal_model = OfflineCausalModel(str(self.config.get("causal_offline_model_path", "causal_offline_model.pkl")))
        self.ws_stream = None
        self.orderbook_cache = None
        self.orderbook_cache_ts = 0.0
        self.style_profiles = {
            "保守": {"buy_open": 0.58, "sell_open": 0.42, "min_score": 0.08, "sl_mult": 2.0, "tp_mult": 2.0, "trail_mult": 1.5, "lev_scale": 0.7},
            "均衡": {"buy_open": 0.55, "sell_open": 0.45, "min_score": 0.06, "sl_mult": 2.5, "tp_mult": 2.5, "trail_mult": 1.8, "lev_scale": 1.0},
            "激进": {"buy_open": 0.52, "sell_open": 0.48, "min_score": 0.04, "sl_mult": 3.0, "tp_mult": 3.0, "trail_mult": 2.2, "lev_scale": 1.25}
        }
        self.apply_tunable_params(config)
        self.causal_engine = CausalDecisionEngine(
            self.style_profiles,
            self.config,
            causal_estimator=self.causal_estimator,
            offline_causal_model=self.offline_causal_model
        )
        self.runtime_adapt_last_ts = time.time()
        self.runtime_fill_snapshot = self.fill_count
        self.runtime_window_stats = {
            "candidate": 0,
            "entry_allowed": 0,
            "causal_block": 0,
            "risk_block": 0,
            "scheduler_grant": 0,
            "orders_sent": 0,
            "fills": 0
        }
        self.runtime_recent_exits = deque(maxlen=120)
        self.slot_pressure_mode = False
        self.last_slot_relief_ts = 0.0
        self.prev_has_position = False
        self.position_open_ts = 0.0
        self.best_roe_seen = -9.9
        self.recovery_day_key = datetime.now().strftime("%Y-%m-%d")
        self.recovery_today_realized = 0.0
        self.recovery_prev_day_realized = 0.0
        self.recovery_today_boost_used = 0.0
        self.recovery_boost_level = 0.0
        self.recovery_fail_streak = 0
        self.recovery_cooldown_until = 0.0

        # 传递唯一的状态文件路径给止损模块
        self.stop_loss._state_file = f"stoploss_state_{self.symbol.replace('/', '_').replace(':', '_')}.json"

        # 自适应入场引擎
        self.adaptive_entry = AdaptiveEntryEngine(self.config, self.log_msg)
        self.last_entry_mode = -1
        self.last_entry_fill_ts = 0.0

        # BB缓存（供自适应入场使用）
        self.cached_bb_upper = 0.0
        self.cached_bb_lower = 0.0

        # 将配置的 stoploss_hunt_window 作为 IPW 冷启动优化的参数传递
        self.ipw_warmup_window = max(20, int(self.config.get('stoploss_hunt_window', 60)))

        # 冷却控制
        self.shockwave_cooldown_until = 0.0
        self.fast_prices_for_shockwave = deque(maxlen=60) # 记录最近60次（约1分钟）的价格

        # 并发锁：保护所有需要安全线程访问的本地数据
        # pending_orders_lock 已在上方初始化，此处避免重复定义
        self.ensemble.add_strategy("trend", 0.4, self.strategy_trend)
        self.ensemble.add_strategy("ml", 0.3, self.strategy_ml)
        self.ensemble.add_strategy("orderbook", 0.2, self.strategy_orderbook)
        self.ensemble.add_strategy("sentiment", 0.1, self.strategy_sentiment)
        self._register_managed_symbols()

    def _normalize_symbol(self, s):
        return str(s or "").upper().replace(":USDT", "").replace("/", "").replace("_", "")

    def _register_managed_symbols(self):
        local_symbols = set()
        symbols_cfg = self.config.get("symbols", [])
        if isinstance(symbols_cfg, (list, tuple)):
            for s in symbols_cfg:
                ns = self._normalize_symbol(s)
                if ns:
                    local_symbols.add(ns)
        me = self._normalize_symbol(self.symbol)
        if me:
            local_symbols.add(me)
        if not local_symbols:
            return
        with self.__class__.managed_symbols_lock:
            self.__class__.managed_symbols.update(local_symbols)

    def _get_managed_symbol_set(self):
        local_symbols = set()
        symbols_cfg = self.config.get("symbols", [])
        if isinstance(symbols_cfg, (list, tuple)):
            for s in symbols_cfg:
                ns = self._normalize_symbol(s)
                if ns:
                    local_symbols.add(ns)
        me = self._normalize_symbol(self.symbol)
        if me:
            local_symbols.add(me)
        with self.__class__.managed_symbols_lock:
            local_symbols.update(self.__class__.managed_symbols)
        return local_symbols

    def log_msg(self, msg, level="INFO"):
        degrade_keywords = ("运行异常", "下单失败", "平仓异常", "查询订单失败", "挂单核对失败", "资金费率检查异常")
        count_for_degrade = (level == "ERROR") and any(k in msg for k in degrade_keywords)
        self.log(f"[{self.symbol}] {msg}", level, count_for_degrade)

    def strategy_trend(self, context):
        state = context['state']
        if state in [MarketState.EXTREME_UPTREND, MarketState.STRONG_UPTREND]:
            return 0.8
        elif state in [MarketState.EXTREME_DOWNTREND, MarketState.STRONG_DOWNTREND]:
            return 0.2
        elif state in [MarketState.WEAK_UPTREND]:
            return 0.6
        elif state in [MarketState.WEAK_DOWNTREND]:
            return 0.4
        else:
            return 0.5

    def strategy_ml(self, context):
        prices = context.get('prices', [])
        return self.ml_predictor.predict(prices)

    def strategy_orderbook(self, context):
        imb = context.get('order_imbalance', self.orderbook.get_imbalance())
        self.orderbook_history.append(imb)
        if len(self.orderbook_history) > 0:
            imb = float(np.mean(self.orderbook_history))
        side = 1 if imb > 0.08 else (-1 if imb < -0.08 else 0)
        if side != 0 and side == self.orderbook_confirm_side:
            self.orderbook_confirm_streak += 1
        elif side != 0:
            if self.orderbook_confirm_side != 0:
                self.orderbook_flip_count += 1
            self.orderbook_confirm_side = side
            self.orderbook_confirm_streak = 1
        else:
            self.orderbook_confirm_streak = 0
        confirm_ticks = max(1, int(self.config.get("orderbook_confirm_ticks", 3)))
        if side != 0 and self.orderbook_confirm_streak < confirm_ticks:
            imb = imb * 0.35
        if self.orderbook_flip_count >= 2:
            imb = imb * 0.7
            self.orderbook_flip_count = 0
        return max(0.0, min(1.0, 0.5 + imb))

    def strategy_sentiment(self, context):
        return context.get('sentiment', self.sentiment.update())

    def classify_state_phase(self, state, atr, price, volume_ratio):
        atr_ratio = atr / price if price > 0 else 0.0
        phase = 0.5
        if state in (MarketState.EXTREME_UPTREND, MarketState.EXTREME_DOWNTREND):
            phase = 0.95
        elif state in (MarketState.STRONG_UPTREND, MarketState.STRONG_DOWNTREND):
            phase = 0.80
        elif state in (MarketState.WEAK_UPTREND, MarketState.WEAK_DOWNTREND):
            phase = 0.62
        elif state == MarketState.RANGE:
            phase = 0.42
        if atr_ratio > 0.04:
            phase -= 0.12
        if volume_ratio > 1.4:
            phase += 0.08
        return max(0.0, min(1.0, phase))

    def compute_crowding_score(self, buy_prob, order_imbalance, volume_ratio, state):
        directional_heat = abs(buy_prob - 0.5) * 2
        imbalance_heat = min(1.0, abs(order_imbalance) * 1.6)
        liquidity_heat = max(0.0, min(1.0, (volume_ratio - 1.0) / 1.5))
        trend_heat = 0.0
        if state in (MarketState.EXTREME_UPTREND, MarketState.EXTREME_DOWNTREND):
            trend_heat = 0.35
        elif state in (MarketState.STRONG_UPTREND, MarketState.STRONG_DOWNTREND):
            trend_heat = 0.22
        crowding = directional_heat * 0.45 + imbalance_heat * 0.25 + liquidity_heat * 0.20 + trend_heat * 0.10
        self.crowding_history.append(crowding)
        if len(self.crowding_history) > 1:
            crowding = crowding * 0.7 + float(np.mean(self.crowding_history)) * 0.3
        return max(0.0, min(1.0, crowding))

    def detect_suspicious_market(self, atr, price, volume_ratio, order_imbalance, crowding_score):
        atr_ratio = atr / price if price > 0 else 0.0
        now = time.time()
        if self.suspicious_pause_until > now:
            return False
        try:
            vol_th = float(self.config.get("suspicious_volatility_ratio", 0.03))
            vol_th = vol_th * random.uniform(0.9, 1.1)
        except Exception:
            vol_th = 0.03
        try:
            low_liq_th = float(self.config.get("suspicious_low_volume_ratio", 0.65))
        except Exception:
            low_liq_th = 0.65
        if atr_ratio > vol_th and volume_ratio < low_liq_th and abs(order_imbalance) > 0.35 and crowding_score > 0.7:
            try:
                pause_min = max(5, int(self.config.get("suspicious_pause_min_sec", 30)))
            except Exception:
                pause_min = 30
            try:
                pause_max = max(pause_min, int(self.config.get("suspicious_pause_max_sec", 90)))
            except Exception:
                pause_max = max(pause_min, 90)
            max_pause = max(600, int(self.config.get("max_suspicious_pause_sec", 21600)))
            self.suspicious_pause_until = min(now + max_pause, now + random.randint(pause_min, pause_max))
            return True
        return False

    def maybe_switch_mode(self, crowding_score):
        now = time.time()
        if self.mode_until > now:
            return
        if now < self.mode_switch_cooldown_until:
            return
        self.current_mode = "normal"
        try:
            p = max(0.0, min(0.2, float(self.config.get("mode_switch_probability", 0.02))))
        except Exception:
            p = 0.02
        p = max(0.01, min(0.05, random.uniform(0.01, 0.05) * (0.5 + p / 0.04)))
        if random.random() >= p:
            return
        min_dwell = max(120, int(self.config.get("mode_min_dwell_sec", 900)))
        switch_cd = max(60, int(self.config.get("mode_switch_cooldown_sec", 300)))
        if crowding_score > 0.72:
            self.current_mode = "stealth"
            self.mode_until = now + max(min_dwell, random.randint(1800, 7200))
        else:
            self.current_mode = "counter"
            self.mode_until = now + max(min_dwell, random.randint(900, 3600))
        self.mode_last_switch_ts = now
        self.mode_switch_cooldown_until = now + switch_cd
        self.log_msg(f"模式切换: {self.current_mode} 持续{int(self.mode_until-now)}s", "WARNING")

    def maybe_daily_random_pause(self):
        if not bool(self.config.get("antifragile_mode", True)):
            return
        now = time.time()
        if self.chaos_pause_until > now:
            return
        day_key = datetime.now().strftime("%Y-%m-%d")
        if day_key == self.daily_pause_key:
            return
        self.daily_pause_key = day_key
        day_fills = max(0, int(self.fill_count) - int(self.daily_fill_baseline))
        day_orders = max(0, int(self.order_count) - int(self.daily_order_baseline))
        activity = day_fills + day_orders * 0.2
        try:
            p = max(0.0, min(0.3, float(self.config.get("daily_random_pause_prob", 0.05))))
        except Exception:
            p = 0.05
        if activity < 4:
            p *= 0.45
        elif activity < 8:
            p *= 0.70
        elif activity > 18:
            p *= 1.25
        p = max(0.0, min(0.3, p))
        if random.random() < p:
            try:
                pmin = max(300, int(self.config.get("daily_random_pause_min_sec", 3600)))
                pmax = max(pmin, int(self.config.get("daily_random_pause_max_sec", 14400)))
            except Exception:
                pmin, pmax = 3600, 14400
            self.chaos_pause_until = now + random.randint(pmin, pmax)
            self.log_msg(f"混沌注入: 随机休眠{int(self.chaos_pause_until-now)}s", "WARNING")
        self.daily_fill_baseline = int(self.fill_count)
        self.daily_order_baseline = int(self.order_count)

    def evaluate_reflexive_risk(self):
        now = time.time()
        cooldown = max(60, int(self.config.get("reflexive_trigger_cooldown_sec", 600)))
        if now - self.last_reflexive_trigger_ts < cooldown:
            return
        score = 0.0
        if len(self.slippage_history) >= 20:
            all_slip = np.array(self.slippage_history, dtype=float)
            recent = np.array(list(self.slippage_history)[-8:], dtype=float)
            p95 = float(np.percentile(all_slip, 95))
            try:
                alarm_base = float(self.config.get("max_slippage_alarm", 0.002))
            except Exception:
                alarm_base = 0.002
            alarm_dyn = max(0.0015, min(0.0025, random.uniform(0.0015, 0.0025) * (alarm_base / 0.002)))
            if float(np.mean(recent)) > max(alarm_dyn, p95):
                score += 1.0
        if len(self.order_lifetime_history) >= 12:
            life_all = np.array(self.order_lifetime_history, dtype=float)
            life_recent = np.array(list(self.order_lifetime_history)[-6:], dtype=float)
            if float(np.mean(life_recent)) > float(np.percentile(life_all, 90)) * 1.15:
                score += 0.7
        hunt_window = max(20, int(self.config.get("stoploss_hunt_window", 60)))
        hunt_hits = [x for x in self.stop_hunt_hits if now - x <= hunt_window * 6]
        if len(hunt_hits) >= 2:
            score += 1.1
        if score >= 1.2:
            self.reflexive_events.append({"ts": now, "score": score})
            pause = random.randint(1800, 10800)
            max_pause = max(600, int(self.config.get("max_suspicious_pause_sec", 21600)))
            self.suspicious_pause_until = min(now + max_pause, max(self.suspicious_pause_until, now + pause))
            self.current_mode = "stealth"
            self.mode_until = min(now + max_pause, max(self.mode_until, now + pause))
            self.last_reflexive_trigger_ts = now
            self.log_msg(f"反身性感知触发(score={score:.2f})，进入隐身{pause}s", "WARNING")

    def process_stop_hunt_events(self, price):
        if price <= 0:
            return
        now = time.time()
        window = max(20, int(self.config.get("stoploss_hunt_window", 60)))
        remain = deque(maxlen=50)
        for ev in self.stop_hunt_events:
            if now - ev["ts"] < window:
                remain.append(ev)
                continue
            side_pos = ev.get("side", "")
            exit_price = float(ev.get("price", 0.0))
            if exit_price <= 0:
                continue
            if side_pos == "long" and price > exit_price * 1.001:
                self.stop_hunt_hits.append(now)
            if side_pos == "short" and price < exit_price * 0.999:
                self.stop_hunt_hits.append(now)
        self.stop_hunt_events = remain

    def update_signal_lifespan(self, decision):
        score = float(decision.get("opportunity_score", 0.0))
        threshold = float(self.config.get("signal_decay_threshold", 0.42))
        if score < threshold:
            self.signal_health_score = max(0.30, self.signal_health_score * 0.985)
        else:
            self.signal_health_score = min(1.05, self.signal_health_score * 1.005)

    def update_signal_outcome(self, action):
        mapping = {"take_profit": 1.0, "trail_exit": 0.6, "time_exit": -0.3, "stop_loss": -1.0}
        self.signal_outcomes.append(mapping.get(action, -0.1))
        if len(self.signal_outcomes) >= 6:
            mean_outcome = float(np.mean(self.signal_outcomes))
            if mean_outcome < -0.2:
                self.signal_health_score = max(0.30, self.signal_health_score * 0.93)
            elif mean_outcome > 0.15:
                self.signal_health_score = min(1.10, self.signal_health_score * 1.03)

    def _roll_recovery_day_if_needed(self):
        day = datetime.now().strftime("%Y-%m-%d")
        if day == self.recovery_day_key:
            return
        self.recovery_prev_day_realized = float(self.recovery_today_realized)
        self.recovery_today_realized = 0.0
        self.recovery_today_boost_used = 0.0
        self.recovery_boost_level = 0.0
        self.recovery_fail_streak = 0
        self.recovery_day_key = day

    def _record_realized_pnl(self, pnl_usdt):
        self._roll_recovery_day_if_needed()
        try:
            pnl = float(pnl_usdt)
        except Exception:
            return
        if (not math.isfinite(pnl)) or abs(pnl) <= 0:
            return
        # ====== 同币种连亏熔断 ======
        if not hasattr(self, '_symbol_loss_streak'):
            self._symbol_loss_streak = 0
        if pnl < 0:
            self._symbol_loss_streak += 1
            max_streak = max(2, int(self.config.get("symbol_max_loss_streak", 3)))
            if self._symbol_loss_streak >= max_streak:
                cooldown_hr = max(0.5, float(self.config.get("symbol_streak_cooldown_hr", 1.0)))
                self._symbol_streak_until = time.time() + cooldown_hr * 3600
                self.log_msg(f"🚨 连亏{self._symbol_loss_streak}笔，暂停{cooldown_hr}小时", "WARNING")
        else:
            self._symbol_loss_streak = 0
        # ====== 上报日亏损统计 ======
        if hasattr(self, 'safety_monitor') and self.safety_monitor:
            self.safety_monitor.record_trade_pnl(pnl)
        self.recovery_today_realized += pnl
        if pnl > 0:
            step_down = max(0.05, min(0.5, float(self.config.get("recovery_step_down", 0.15))))
            self.recovery_boost_level = max(0.0, self.recovery_boost_level - step_down)
            self.recovery_fail_streak = 0
        else:
            self.recovery_fail_streak += 1
            fail_limit = max(1, int(self.config.get("recovery_fail_limit", 2)))
            if self.recovery_fail_streak >= fail_limit:
                cooldown = max(120, int(self.config.get("recovery_cooldown_sec", 3600)))
                self.recovery_cooldown_until = max(self.recovery_cooldown_until, time.time() + cooldown)
                self.recovery_boost_level = 0.0

    def _get_recovery_overlay(self):
        self._roll_recovery_day_if_needed()
        if not bool(self.config.get("recovery_enabled", True)):
            return {"active": False, "boost": 0.0}
        now = time.time()
        if now < self.recovery_cooldown_until:
            return {"active": False, "boost": 0.0}
        margin_total = max(1.0, float(self.config.get("margin_usdt", 10.0)))
        prev_loss = max(0.0, -float(self.recovery_prev_day_realized))
        daily_target = margin_total * max(0.0, min(0.03, float(self.config.get("recovery_daily_target_pct", 0.004))))
        recovered = max(0.0, float(self.recovery_today_realized))
        shortfall = max(0.0, prev_loss + daily_target - recovered)
        if shortfall <= 0:
            return {"active": False, "boost": 0.0}
        budget_ratio = max(0.0, min(0.4, float(self.config.get("recovery_budget_ratio", 0.15))))
        budget_total = margin_total * budget_ratio
        budget_left = max(0.0, budget_total - self.recovery_today_boost_used)
        if budget_left <= 0:
            return {"active": False, "boost": 0.0}
        step_up = max(0.02, min(0.5, float(self.config.get("recovery_step_up", 0.12))))
        max_boost = max(0.0, min(0.8, float(self.config.get("recovery_max_boost", 0.45))))
        target_level = max(0.0, min(max_boost, shortfall / margin_total))
        self.recovery_boost_level = min(max_boost, max(self.recovery_boost_level, target_level * 0.5))
        boost = min(max_boost, self.recovery_boost_level + step_up * 0.5)
        return {"active": boost > 0, "boost": float(max(0.0, min(max_boost, boost))), "budget_left": float(budget_left), "shortfall": float(shortfall)}

    def _apply_recovery_to_decision(self, decision):
        overlay = self._get_recovery_overlay()
        if not overlay.get("active"):
            return decision, overlay
        d = decision if isinstance(decision, dict) else {}
        boost = float(overlay.get("boost", 0.0))
        d["recovery_overlay"] = overlay
        d["opportunity_score"] = max(0.0, min(1.0, float(d.get("opportunity_score", 0.0)) + boost * 0.10))
        exe = d.get("execution", {}) if isinstance(d.get("execution", {}), dict) else {}
        exe["urgency"] = max(0.0, min(1.0, float(exe.get("urgency", 0.5)) + boost * 0.12))
        d["execution"] = exe
        return d, overlay

    def _anti_hunt_gate(self, crowding_score, order_imbalance):
        if not bool(self.config.get("anti_hunt_enabled", True)):
            return False
        now = time.time()
        hit_window = max(60, int(self.config.get("stoploss_hunt_window", 60)) * 6)
        recent_hits = len([x for x in self.stop_hunt_hits if now - x <= hit_window])
        hit_th = max(1, int(self.config.get("anti_hunt_hit_threshold", 2)))
        if recent_hits < hit_th:
            return False
        if crowding_score < 0.62 and abs(order_imbalance) < 0.25:
            return False
        pause = max(60, int(self.config.get("anti_hunt_pause_sec", 900)))
        max_pause = max(600, int(self.config.get("max_suspicious_pause_sec", 21600)))
        self.suspicious_pause_until = min(now + max_pause, max(self.suspicious_pause_until, now + pause))
        self.current_mode = "stealth"
        self.mode_until = min(now + max_pause, max(self.mode_until, now + pause))
        self.log_msg(f"反猎杀门控触发，近期止损狩猎命中{recent_hits}次，暂停开单{pause}s", "WARNING")
        return True

    def _confirm_stop_action(self, stop_action, current_price):
        if stop_action not in ("stop_loss", "trail_exit"):
            self.stop_confirm_pending.pop(self.symbol, None)
            return True
        now = time.time()
        try:
            base_window = max(0.0, min(30.0, float(self.config.get("stop_confirm_window_sec", 2))))
        except Exception:
            base_window = 6.0
        try:
            j = max(0.0, min(0.4, float(self.config.get("stop_trigger_jitter_ratio", 0.08))))
        except Exception:
            j = 0.08
        confirm_window = base_window * random.uniform(max(0.3, 1.0 - j), 1.0 + j)
        key = self.symbol
        rec = self.stop_confirm_pending.get(key)
        if (not rec) or rec.get("action") != stop_action:
            self.stop_confirm_pending[key] = {"action": stop_action, "ts": now, "price": float(current_price)}
            self.log_msg(f"止损确认中({stop_action})，等待{confirm_window:.1f}s二次确认", "WARNING")
            return False
        first_ts = float(rec.get("ts", now))
        first_price = float(rec.get("price", current_price))
        long_pos = float(self.position_contracts) >= 0
        rebound_tol = max(0.0003, j * 0.002)
        if long_pos and current_price > first_price * (1.0 + rebound_tol):
            self.stop_confirm_pending.pop(key, None)
            return False
        if (not long_pos) and current_price < first_price * (1.0 - rebound_tol):
            self.stop_confirm_pending.pop(key, None)
            return False
        if now - first_ts >= confirm_window:
            self.stop_confirm_pending.pop(key, None)
            return True
        return False

    def on_ws_tick(self, price, bid=0.0, ask=0.0):
        try:
            touched = False
            p = float(price)
            if math.isfinite(p) and p > 0:
                self.latest_ws_price = p
                touched = True
            b = float(bid)
            if math.isfinite(b) and b > 0:
                self.latest_ws_bid = b
                touched = True
            a = float(ask)
            if math.isfinite(a) and a > 0:
                self.latest_ws_ask = a
                touched = True
            if touched:
                self.last_ws_ts = time.time()
        except Exception:
            return

    def get_latest_price(self):
        ws_enabled = bool(self.config.get("ws_enabled", True))
        ws_stale_sec = max(0.5, float(self.config.get("ws_stale_sec", 1.2)))
        if ws_enabled and (time.time() - self.last_ws_ts) <= ws_stale_sec:
            if self.latest_ws_price > 0:
                return self.latest_ws_price, "ws"
            if self.latest_ws_bid > 0 and self.latest_ws_ask > 0:
                return (self.latest_ws_bid + self.latest_ws_ask) / 2.0, "ws"
        try:
            ticker = api_call(self.exchange.fetch_ticker, self.symbol)
            px = float(ticker.get('last', 0) or 0)
            if math.isfinite(px) and px > 0:
                self.last_rest_ok_price = px
                return px, "rest"
        except Exception:
            return
        if self.last_rest_ok_price > 0:
            return self.last_rest_ok_price, "cache"
        if self.last_price > 0:
            return float(self.last_price), "cache"
        return 0.0, "rest_err"

    def get_cached_orderbook(self, depth=5, force=False):
        now = time.time()
        interval = max(0.05, float(self.config.get("orderbook_cache_interval_sec", 0.25)))
        if (not force) and self.orderbook_cache is not None and (now - self.orderbook_cache_ts) <= interval:
            return self.orderbook_cache
        ws_enabled = bool(self.config.get("ws_enabled", True))
        ws_book_stale = max(0.3, float(self.config.get("ws_book_stale_sec", 1.0)))
        if ws_enabled and self.last_ws_ts > 0 and (now - self.last_ws_ts) <= ws_book_stale:
            bids = [[self.latest_ws_bid, 0.0]] if self.latest_ws_bid > 0 else []
            asks = [[self.latest_ws_ask, 0.0]] if self.latest_ws_ask > 0 else []
            if bids or asks:
                self.orderbook_cache = {"bids": bids, "asks": asks}
                self.orderbook_cache_ts = now
                return self.orderbook_cache
        try:
            ob = api_call(self.exchange.fetch_order_book, self.symbol, int(depth))
            if isinstance(ob, dict):
                self.orderbook_cache = ob
                self.orderbook_cache_ts = now
        except Exception:
            pass  # silent fallback
        if self.orderbook_cache is None:
            bids = [[self.latest_ws_bid, 0.0]] if self.latest_ws_bid > 0 else []
            asks = [[self.latest_ws_ask, 0.0]] if self.latest_ws_ask > 0 else []
            self.orderbook_cache = {"bids": bids, "asks": asks}
            self.orderbook_cache_ts = now
        return self.orderbook_cache

    def _causal_feature_vector(self, decision, metrics):
        d = decision.get("details", {}) if isinstance(decision, dict) else {}
        prev_ret = 0.0
        if len(self.price_history) >= 2 and self.price_history[-2] > 0:
            prev_ret = (self.price_history[-1] - self.price_history[-2]) / self.price_history[-2]
        vol_shock = float(metrics.get("volume_ratio", 1.0)) - 1.0
        return np.array([
            float(decision.get("opportunity_score", 0.0)),
            float(decision.get("risk_penalty", 0.0)),
            float(d.get("confidence", 0.0)),
            float(d.get("trend_strength", 0.0)),
            float(d.get("liquidity_score", 0.0)),
            float(d.get("sentiment_score", 0.0)),
            float(d.get("book_score", 0.0)),
            float(metrics.get("crowding_score", 0.0)),
            float(metrics.get("signal_health", self.signal_health_score)),
            float(metrics.get("atr", 0.0)) / max(1e-8, float(metrics.get("price", self.last_price if self.last_price > 0 else 1.0))),
            float(prev_ret),
            float(vol_shock)
        ], dtype=float)

    def _record_causal_decision(self, decision, metrics, treatment, price, buy_prob):
        if not bool(self.config.get("causal_enabled", False)):
            return
        try:
            base_fv = self._causal_feature_vector(decision, metrics)
            side_sign = 1.0 if buy_prob >= 0.5 else -1.0
            d = decision.get("details", {}) if isinstance(decision, dict) else {}
            propensity = max(0.05, min(0.95, float(metrics.get("propensity", 0.5))))
            fv = np.concatenate([np.array(base_fv, dtype=float), np.array([propensity], dtype=float)])
            self.causal_pending.append({
                "ts": time.time(),
                "fv": fv,
                "treatment": int(1 if treatment else 0),
                "price": float(price),
                "side_sign": side_sign,
                "risk": float(decision.get("risk_penalty", 0.0)),
                "propensity": propensity,
                "pred_effect": float(d.get("causal_effect", 0.0))
            })
        except Exception:
            return

    def _causal_drift_adapt(self):
        if len(self.causal_error_history) < max(20, int(self.config.get("causal_drift_window", 120)) // 3):
            return
        err_mean = float(np.mean(self.causal_error_history))
        drift_thr = max(0.002, float(self.config.get("causal_drift_threshold", 0.012)))
        corr_floor = float(self.config.get("causal_corr_floor", -0.05))
        corr_val = 0.0
        if len(self.causal_perf_pairs) >= 24:
            arr = np.array(self.causal_perf_pairs, dtype=float)
            if arr.ndim == 2 and arr.shape[1] == 2:
                a = arr[:, 0]
                b = arr[:, 1]
                if np.std(a) > 1e-8 and np.std(b) > 1e-8:
                    corr_val = float(np.corrcoef(a, b)[0, 1])
        blend = float(self.config.get("causal_blend_weight", 0.45))
        unc_pen = float(self.config.get("causal_uncertainty_penalty", 0.55))
        changed = False
        if err_mean > drift_thr or corr_val < corr_floor:
            new_blend = max(0.1, blend * 0.98)
            new_unc = min(1.0, unc_pen + 0.01)
            changed = (abs(new_blend - blend) > 1e-6) or (abs(new_unc - unc_pen) > 1e-6)
            self.config["causal_blend_weight"] = new_blend
            self.config["causal_uncertainty_penalty"] = new_unc
            if corr_val < corr_floor:
                self.causal_model_pause_until = time.time() + max(300, int(self.config.get("causal_model_pause_sec", 1800)))
        elif err_mean < drift_thr * 0.6:
            new_blend = min(0.8, blend * 1.01)
            new_unc = max(0.0, unc_pen - 0.005)
            changed = (abs(new_blend - blend) > 1e-6) or (abs(new_unc - unc_pen) > 1e-6)
            self.config["causal_blend_weight"] = new_blend
            self.config["causal_uncertainty_penalty"] = new_unc
        if changed:
            now = time.time()
            if now - self.last_causal_drift_log_ts >= 300:
                pause_left = max(0, int(self.causal_model_pause_until - now))
                self.log_msg(f"因果漂移自适应: err={err_mean:.4f} corr={corr_val:.3f} blend={self.config.get('causal_blend_weight',0.45):.3f} unc_pen={self.config.get('causal_uncertainty_penalty',0.55):.3f} pause={pause_left}s", "WARNING")
                self.last_causal_drift_log_ts = now

    def _update_runtime_funnel(self, decision, order_token, placed):
        st = self.runtime_window_stats
        st["candidate"] += 1
        if bool(decision.get("entry_allowed", False)):
            st["entry_allowed"] += 1
        reason = str(decision.get("reason", ""))
        if reason == "因果效应不足":
            st["causal_block"] += 1
        if ("风险" in reason) or ("VaR" in reason):
            st["risk_block"] += 1
        if order_token:
            st["scheduler_grant"] += 1
        if placed:
            st["orders_sent"] += 1

    def _runtime_adapt(self):
        if not bool(self.config.get("runtime_adapt_enabled", True)):
            return
        now = time.time()
        elapsed = max(1.0, now - float(self.runtime_adapt_last_ts))
        adapt_interval = max(60.0, float(self.config.get("runtime_adapt_interval_sec", 300)))
        if elapsed < adapt_interval:
            return
        st = self.runtime_window_stats
        fill_delta = max(0, int(self.fill_count - self.runtime_fill_snapshot))
        st["fills"] += fill_delta
        self.runtime_fill_snapshot = self.fill_count
        cand = max(1, int(st.get("candidate", 0)))
        entry_allowed = int(st.get("entry_allowed", 0))
        scheduler_grant = int(st.get("scheduler_grant", 0))
        orders_sent = int(st.get("orders_sent", 0))
        causal_block = int(st.get("causal_block", 0))
        causal_block_ratio = causal_block / cand
        scheduler_pass_rate = scheduler_grant / max(1, entry_allowed)
        placed_rate = orders_sent / max(1, scheduler_grant)
        orders_per_hour = orders_sent * 3600.0 / elapsed
        target_orders = max(1.0, float(self.config.get("runtime_target_orders_per_hour", 6.0)))
        target_causal_block = max(0.15, min(0.75, float(self.config.get("runtime_target_causal_block_ratio", 0.35))))
        thr_step = max(0.0005, float(self.config.get("runtime_threshold_step", 0.0015)))
        unc_step = max(0.005, float(self.config.get("runtime_uncertainty_step", 0.02)))
        int_step = max(5, int(self.config.get("runtime_interval_step_sec", 15)))
        min_interval = max(10, int(self.config.get("runtime_min_order_interval_sec", 10)))
        max_interval = max(min_interval, int(self.config.get("runtime_max_order_interval_sec", 240)))
        sl_guard = max(0.10, min(0.95, float(self.config.get("runtime_stoploss_guard_ratio", 0.55))))
        exits = list(self.runtime_recent_exits)
        exit_count = len(exits)
        bad_exits = sum(1 for x in exits if x in ("stop_loss", "time_exit"))
        bad_exit_ratio = (bad_exits / exit_count) if exit_count > 0 else 0.0
        thr = float(self.config.get("causal_effect_threshold", 0.006))
        base_thr = float(self.config.get("causal_effect_threshold_base", 0.008))
        unc_pen = float(self.config.get("causal_uncertainty_penalty", 0.40))
        g_interval = int(self.config.get("global_order_interval", 180))
        if orders_per_hour < target_orders * 0.65 and causal_block_ratio > target_causal_block:
            thr = max(-0.02, thr - thr_step)
            base_thr = max(-0.02, base_thr - thr_step * 0.7)
            unc_pen = max(0.0, unc_pen - unc_step)
        if orders_per_hour < target_orders * 0.55 and scheduler_pass_rate < 0.40:
            g_interval = max(min_interval, g_interval - int_step)
        if orders_per_hour > target_orders * 1.35 and bad_exit_ratio > sl_guard and exit_count >= 4:
            thr = min(0.12, thr + thr_step * 1.4)
            base_thr = min(0.12, base_thr + thr_step)
            unc_pen = min(1.0, unc_pen + unc_step)
            g_interval = min(max_interval, g_interval + int_step)
        self.config["causal_effect_threshold"] = float(max(-0.50, min(0.12, thr)))
        self.config["causal_effect_threshold_base"] = float(max(-0.50, min(0.12, base_thr)))

        # 狂暴模式物理破防：如果目标单量>=20，彻底瘫痪不确定性惩罚
        if target_orders >= 20.0:
            unc_pen = 0.0

        self.config["causal_uncertainty_penalty"] = float(max(0.0, min(1.0, unc_pen)))
        self.config["global_order_interval"] = int(max(min_interval, min(max_interval, g_interval)))
        if hasattr(self, "causal_engine"):
            self.causal_engine.config = self.config
        self.log_msg(
            f"实盘迭代: cand={cand} allow={entry_allowed} causal_blk={causal_block_ratio:.2f} sch={scheduler_pass_rate:.2f} "
            f"place={placed_rate:.2f} ord/h={orders_per_hour:.2f} fills={st.get('fills',0)} bad_exit={bad_exit_ratio:.2f} "
            f"thr={self.config.get('causal_effect_threshold',0):.4f} unc_pen={self.config.get('causal_uncertainty_penalty',0):.3f} "
            f"g_int={self.config.get('global_order_interval',0)}s"
        )
        self.runtime_adapt_last_ts = now
        self.runtime_window_stats = {
            "candidate": 0,
            "entry_allowed": 0,
            "causal_block": 0,
            "risk_block": 0,
            "scheduler_grant": 0,
            "orders_sent": 0,
            "fills": 0
        }
        self.runtime_recent_exits.clear()

    def _estimate_active_occupancy(self):
        max_active = max(1, int(self.config.get("max_active_symbols", 2)))
        active_est = 0
        try:
            sched = self.scheduler
            if hasattr(sched, "get_active_symbols_snapshot"):
                active_est = len(sched.get_active_symbols_snapshot())
            elif hasattr(sched, "lock") and hasattr(sched, "active_symbols"):
                with sched.lock:
                    active_est = len(set(getattr(sched, "active_symbols", set())))
            elif hasattr(sched, "active_symbols"):
                active_est = len(set(getattr(sched, "active_symbols", set())))
        except Exception:
            active_est = 0
        if self.has_position and active_est <= 0:
            active_est = 1
        ratio = max(0.0, min(2.0, active_est / max_active))
        return active_est, max_active, ratio

    def _refresh_position_lifecycle(self):
        now = time.time()
        if self.has_position and (not self.prev_has_position):
            self.position_open_ts = now
            self.best_roe_seen = float(self.position_roe)
            # 修复缺陷 10：记录真正的建仓上下文，用于后续平仓时的贝叶斯归因
            self.position_open_context = getattr(self, "last_decision_context", None)
        elif self.has_position:
            self.best_roe_seen = max(float(self.best_roe_seen), float(self.position_roe))
        elif (not self.has_position) and self.prev_has_position:
            self.position_open_ts = 0.0
            self.best_roe_seen = -9.9
            self.position_open_context = None
        self.prev_has_position = bool(self.has_position)

    def _get_dynamic_slip_estimate(self, atr, price):
        base_factor = float(self.config.get("slippage_factor", 0.1))
        if len(self.slippage_history) >= 5:
            recent_slip = float(np.mean(list(self.slippage_history)[-20:]))
            if price > 0:
                atr_ratio = atr / price
                if atr_ratio > 1e-8:
                    implied_factor = recent_slip / atr_ratio
                    # 平滑调整系数，但不偏离基础系数太远 (0.05 ~ 0.3)
                    base_factor = max(0.05, min(0.3, base_factor * 0.5 + implied_factor * 0.5))
        return max(0.0, float(atr) * base_factor)

    def _apply_slot_pressure_policy(self, decision, price, atr):
        active_est, max_active, occ_ratio = self._estimate_active_occupancy()

        # 计算线性压力因子 (0.0 到 1.0)
        pressure_start = 0.50  # 从50%占用率开始施加轻微压力
        pressure_full = 0.90   # 到90%占用率时压力拉满

        if occ_ratio <= pressure_start:
            pressure_factor = 0.0
            self.slot_pressure_mode = False
        elif occ_ratio >= pressure_full:
            pressure_factor = 1.0
            self.slot_pressure_mode = True
        else:
            pressure_factor = (occ_ratio - pressure_start) / (pressure_full - pressure_start)
            self.slot_pressure_mode = True

        if pressure_factor > 0:
            # 线性缩减参数
            orig_holding = float(decision.get("max_holding_hours", 24))
            decision["max_holding_hours"] = max(4, int(orig_holding * (1.0 - 0.40 * pressure_factor)))

            orig_trail = float(decision.get("trail_mult", 1.2))
            decision["trail_mult"] = max(0.5, orig_trail * (1.0 - 0.25 * pressure_factor))

            orig_tp = float(decision.get("tp_mult", 1.8))
            decision["tp_mult"] = max(0.8, orig_tp * (1.0 - 0.15 * pressure_factor))

            decision["reason"] = f"{decision.get('reason','')}|槽位压力:{active_est}/{max_active}({pressure_factor:.2f})"

        if (not self.has_position) or (pressure_factor == 0):
            return False
        now = time.time()
        cooldown = max(10, int(self.config.get("slot_pressure_cooldown_sec", 120)))
        if now - self.last_slot_relief_ts < cooldown:
            return False

        # 波动率加权的到手即走
        atr_ratio = atr / price if price > 0 else 0.0
        volatility_multiplier = 1.0
        if atr_ratio > 0.015:  # 波动率偏高
            volatility_multiplier = 1.5  # 放宽容忍度，让利润奔跑
        elif atr_ratio > 0.03: # 极高波动率
            volatility_multiplier = 2.0

        take_roe = float(self.config.get("slot_pressure_take_roe", 0.05)) * volatility_multiplier
        retrace_roe = float(self.config.get("slot_pressure_retrace_roe", 0.02)) * volatility_multiplier

        max_hold_sec = max(600, int(self.config.get("slot_pressure_max_hold_sec", 5400)))
        min_roe_for_time = float(self.config.get("slot_pressure_time_relief_min_roe", -0.0015))
        roe = float(self.position_roe)
        best = max(float(self.best_roe_seen), roe)
        hold_sec = (now - self.position_open_ts) if self.position_open_ts > 0 else 0.0
        slip_estimate = self._get_dynamic_slip_estimate(atr, price)
        # 只有当持仓时间超过2分钟且 ROE 显著回撤时才触发（防止刚开仓就被平）
        if best >= take_roe and (best - roe) >= retrace_roe and hold_sec > 120:
            self.log_msg(f"槽位压力触发到手即走: occ={occ_ratio:.2f} roe={roe:.4f} best={best:.4f} hold={int(hold_sec)}s", "WARNING")
            closed = self.close_all_positions(expected_price=price, reason="slot_take_and_run", slippage_estimate=slip_estimate)
            if closed:
                self.record_exit_action("trail_exit", pnl_actual=getattr(self, "_last_close_pnl", None))
            self.last_slot_relief_ts = now
            return True
        if hold_sec >= max_hold_sec and roe >= min_roe_for_time:
            self.log_msg(f"槽位压力触发限时腾仓: occ={occ_ratio:.2f} hold={int(hold_sec)}s roe={roe:.4f}", "WARNING")
            closed = self.close_all_positions(expected_price=price, reason="slot_time_relief", slippage_estimate=slip_estimate)
            if closed:
                self.record_exit_action("time_exit", pnl_actual=getattr(self, "_last_close_pnl", None))
            self.last_slot_relief_ts = now
            return True
        return False

    def _flush_causal_labels(self, current_price):
        if not bool(self.config.get("causal_enabled", False)):
            return
        if current_price <= 0 or len(self.causal_pending) == 0:
            return
        now = time.time()
        while self.causal_pending and (now - float(self.causal_pending[0].get("ts", now))) >= self.causal_horizon_sec:
            s = self.causal_pending.popleft()
            px0 = max(1e-8, float(s.get("price", 0.0)))
            side_sign = float(s.get("side_sign", 1.0))
            raw_ret = side_sign * (float(current_price) - px0) / px0
            treatment = int(s.get("treatment", 0))
            risk = float(s.get("risk", 0.0))
            propensity = max(0.05, min(0.95, float(s.get("propensity", 0.5))))
            fee_penalty = 0.0008 if treatment == 1 else 0.0
            risk_penalty = risk * (0.002 if treatment == 1 else 0.0003)
            if treatment == 1:
                outcome = raw_ret - fee_penalty - risk_penalty
                self.causal_treated_bank.append((s.get("fv"), float(outcome)))
            else:
                ocw = max(0.0, min(1.0, float(self.config.get("causal_opportunity_cost_weight", 0.35))))
                if len(self.causal_treated_bank) >= 8:
                    fv = np.array(s.get("fv"), dtype=float)
                    k = min(8, len(self.causal_treated_bank))

                    bank_fvs = np.array([x[0] for x in self.causal_treated_bank])
                    bank_outs = np.array([x[1] for x in self.causal_treated_bank])
                    diffs = bank_fvs - fv
                    dists = np.sum(diffs ** 2, axis=1)

                    if k < len(dists):
                        idx = np.argpartition(dists, k)[:k]
                    else:
                        idx = np.arange(len(dists))

                    near_outs = bank_outs[idx]
                    opp = float(np.mean(near_outs)) if len(near_outs) > 0 else 0.0
                    outcome = -max(0.0, opp) * ocw
                else:
                    outcome = 0.0
            ipw = (1.0 / propensity) if treatment == 1 else (1.0 / (1.0 - propensity))
            ipw = max(0.7, min(2.5, ipw))
            weighted_outcome = float(outcome) * ipw
            self.causal_estimator.update(s.get("fv"), treatment, weighted_outcome)
            self.propensity_model.update(s.get("fv"), treatment)
            pred_eff = float(s.get("pred_effect", 0.0))
            self.causal_error_history.append(abs(pred_eff - weighted_outcome))
            self.causal_perf_pairs.append((pred_eff, weighted_outcome))
        self._causal_drift_adapt()

    def run(self):
        if not self.check_liquidity():
            if self.liquidity_check_failed:
                self.stop_reason = "liquidity_check_failed"
                self.log_msg("流动性检查失败，等待自动恢复", "WARNING")
            else:
                self.stop_reason = "liquidity_low"
                self.log_msg("流动性过低，策略停止", "WARNING")
                # 不再直接退出，防止频繁重启，只是保持不活跃状态
                time.sleep(30)

        if not self.init_settings():
            # 只有当停止原因是明确的鉴权失败时，才真正退出策略
            if getattr(self, "stop_reason", "") == "auth_invalid":
                self.running = False
                self.log_msg("初始化配置因鉴权失败被阻断，策略线程安全退出", "ERROR")
                return
            self.log_msg("初始化配置失败或降级，继续执行", "WARNING")
        self.log_msg("【第18层+全局调度】终极网格策略启动")
        if bool(self.config.get("ws_enabled", True)):
            if websocket is None:
                self.log_msg("WS模块不可用，自动降级REST", "WARNING")
            else:
                try:
                    reconnect_sec = float(self.config.get("ws_reconnect_sec", 3.0))
                    ssl_verify = bool(self.config.get("ws_ssl_verify", True))
                    disable_on_ssl_error = bool(self.config.get("ws_disable_on_ssl_error", True))
                    ws_url = str(self.config.get("ws_url", "wss://fx-ws.gateio.ws/v4/ws/usdt"))
                    ws_channel = str(self.config.get("ws_channel", "futures.tickers"))
                    ws_trace = bool(self.config.get("ws_trace", False))
                    self.ws_stream = GateTickerStream(
                        self.symbol,
                        self.on_ws_tick,
                        self.log_msg,
                        reconnect_sec=reconnect_sec,
                        ssl_verify=ssl_verify,
                        disable_on_ssl_error=disable_on_ssl_error,
                        ws_url=ws_url,
                        ws_channel=ws_channel,
                        trace_enabled=ws_trace
                    )
                    self.ws_stream.start()
                    self.log_msg("WS行情订阅已启动")
                except Exception as e:
                    self.log_msg(f"WS行情启动失败，降级REST: {e}", "WARNING")
        else:
            try:
                self.log_msg("WS已关闭，使用REST行情")
            except Exception:
                pass  # silent fallback
        try:
            self.update_position_status(force=True)
            if self.has_position:
                self.log_msg(f"启动继承持仓: 数量{self.position_contracts:.4f} 开仓价{self.position_entry_price:.6f}", "WARNING")
                # 校验继承持仓是否超过单币保证金上限
                try:
                    max_active_limit = max(1, int(self.config.get('max_active_symbols', 4)))
                    max_margin_per = float(self.config.get('margin_usdt', 100)) / max_active_limit
                    pos_margin = float(self.position_margin) if hasattr(self, 'position_margin') and self.position_margin else 0.0
                    if pos_margin > max_margin_per * 1.1:
                        self.log_msg(f"⚠️ 继承持仓保证金({pos_margin:.2f}U)超过单币上限({max_margin_per:.2f}U)！建议手动减仓或调整max_active_symbols", "ERROR")
                except Exception:
                    pass
        except Exception as e:
            if is_auth_error(e):
                self.running = False
                self.log_msg(f"启动持仓状态检查遭遇鉴权失败，策略线程安全退出", "ERROR")
                return
            self.log_msg(f"启动持仓状态检查失败: {e}", "WARNING")

        try:
            self.correlation.update()
        except Exception as e:
            self.log_msg(f"相关性矩阵初始更新失败: {e}", "WARNING")

        try:
            self.reconcile_orders(startup=True)
        except Exception as e:
            if is_auth_error(e):
                self.running = False
                self.log_msg(f"启动订单核对遭遇鉴权失败，策略线程安全退出", "ERROR")
                return
            self.log_msg(f"订单对账初始失败: {e}", "WARNING")

        while self.running:
            try:
                self.maybe_daily_random_pause()
                if self.circuit_break and bool(self.config.get('black_swan_enabled', False)):
                    cooldown = int(self.config.get('black_swan_cooldown_sec', 300))
                    recover_need = int(self.config.get('black_swan_recover_checks', 3))
                    if self.circuit_break_since == 0:
                        self.circuit_break_since = time.time()
                    if time.time() - self.circuit_break_since >= cooldown:
                        if self.last_price > 0 and not self.blackswan.update(self.last_price):
                            self.stable_recover_count += 1
                        else:
                            self.stable_recover_count = 0
                    if self.stable_recover_count >= recover_need:
                        self.circuit_break = False
                        self.circuit_break_since = 0
                        self.stable_recover_count = 0
                        self.log_msg("【第15层】市场恢复稳定，解除黑天鹅暂停", "WARNING")
                        continue
                    time.sleep(10)
                    continue

                # 获取最新数据（WS优先，REST回退）
                price, price_src = self.get_latest_price()
                try:
                    price = float(price)
                except Exception:
                    self.log_msg("ticker价格无效，跳过本轮")
                    time.sleep(5)
                    continue
                if not math.isfinite(price) or price <= 0:
                    now_ts = time.time()
                    if now_ts - self.last_price_error_log_ts >= 10:
                        self.log_msg("ticker价格异常，跳过本轮", "WARNING")
                        self.last_price_error_log_ts = now_ts
                    time.sleep(5)
                    continue
                if price_src == "rest":
                    log_interval = max(5.0, float(self.config.get("ws_fallback_log_interval_sec", 30.0)))
                    now_ts = time.time()
                    if now_ts - self.last_rest_fallback_log_ts >= log_interval:
                        self.log_msg("行情源回退REST", "WARNING")
                        self.last_rest_fallback_log_ts = now_ts
                elif price_src in ("cache", "rest_err"):
                    now_ts = time.time()
                    if now_ts - self.last_price_error_log_ts >= 10:
                        self.log_msg("REST行情暂不可用，使用缓存价", "WARNING")
                        self.last_price_error_log_ts = now_ts
                    time.sleep(0.2)
                if self.chaos_pause_until > time.time():
                    self.log_msg(f"混沌暂停中，剩余{int(self.chaos_pause_until-time.time())}s", "WARNING")
                    self.check_filled_orders(0.5, self.config.get('base_price_range', 0.02), self.config.get('leverage', 3), self.latest_decision or {})
                    self.reconcile_orders()
                    time.sleep(2)
                    continue
                self.last_price = price
                self._flush_causal_labels(price)
                self.price_history.append(price)
                self.process_stop_hunt_events(price)

                # 记录高频价格，用于 Shockwave 计算
                self.fast_prices_for_shockwave.append((time.time(), price))

                # 计算大盘斜率冷却 (Shockwave Cooldown)
                # 如果在最近 60 秒内，价格发生了极端断崖式暴跌或暴涨（超过 ATR 的 2 倍），则全局挂起所有新开仓
                if len(self.fast_prices_for_shockwave) >= 10 and self.cached_atr > 0:
                    oldest_ts, oldest_px = self.fast_prices_for_shockwave[0]
                    current_ts, current_px = self.fast_prices_for_shockwave[-1]
                    if current_ts - oldest_ts <= 90: # 确保数据是新鲜的
                        price_change_ratio = abs(current_px - oldest_px) / oldest_px
                        atr_ratio = self.cached_atr / price
                        if price_change_ratio > atr_ratio * 2.0: # 极端行情
                            if self.shockwave_cooldown_until < time.time():
                                self.shockwave_cooldown_until = time.time() + 180 # 强制冷却 3 分钟
                                self.log_msg(f"【风控墙】检测到极端瞬时波动 (变化率 {price_change_ratio*100:.2f}%)，触发全局斜率冷却 3 分钟", "WARNING")
                if self.shockwave_cooldown_until > time.time():
                    remain = int(self.shockwave_cooldown_until - time.time())
                    self.log_msg(f"【风控墙】震波冷却中，剩余 {remain}s，暂缓所有操作", "WARNING")
                    self.check_filled_orders(0.5, self.config.get('base_price_range', 0.02), self.config.get('leverage', 3), self.latest_decision or {})
                    self.reconcile_orders()
                    time.sleep(2)
                    continue

                # 成本屏障：获取最新的配置费率，用于决策引擎
                maker_fee = float(self.config.get("maker_fee", 0.0002))
                taker_fee = float(self.config.get("taker_fee", 0.0005))

                # 更新ATR（节流请求，避免触发限频）
                now_ts = time.time()
                ohlcv_interval = max(15, int(self.config.get("strategy_ohlcv_interval_sec", 45)))
                if (now_ts - self.last_ohlcv_fetch >= ohlcv_interval) or self.cached_atr <= 0:
                    try:
                        ohlcv = api_call(self.exchange.fetch_ohlcv, self.symbol, '1h', limit=20)
                        df = pd.DataFrame(ohlcv, columns=['ts','o','h','l','c','v'])
                        atr = calculate_atr(df['h'], df['l'], df['c'])
                        if atr <= 0 or not math.isfinite(atr):
                            self.log_msg(f"⚠️ ATR计算异常({atr})，使用上次缓存值{self.cached_atr}", "WARNING")
                            atr = self.cached_atr if self.cached_atr > 0 else self.last_price * 0.005
                        volume = df['v'].iloc[-1]
                        avg_volume = df['v'].rolling(20).mean().iloc[-1]
                        volume_ratio = volume / avg_volume if avg_volume > 0 else 1.0
                        self.cached_atr = atr
                        self.cached_volume_ratio = volume_ratio
                        # 缓存布林带供自适应入场使用
                        _, bb_up, bb_low = calculate_bb_width(df['c'])
                        self.cached_bb_upper = float(bb_up) if bb_up and math.isfinite(float(bb_up)) else 0.0
                        self.cached_bb_lower = float(bb_low) if bb_low and math.isfinite(float(bb_low)) else 0.0
                        self.last_ohlcv_fetch = now_ts
                        self._data_trust = True
                    except Exception as e:
                        self.log_msg(f"⚠️ OHLCV获取失败: {e}，数据标记不可信", "WARNING")
                        atr = self.cached_atr if self.cached_atr > 0 else self.last_price * 0.005
                        volume_ratio = self.cached_volume_ratio if hasattr(self, 'cached_volume_ratio') else 1.0
                        self._data_trust = False
                else:
                    atr = self.cached_atr
                    volume_ratio = self.cached_volume_ratio
                self.atr_history.append(atr)

                # 更新波动率自适应杠杆
                dynamic_leverage = self.vol_adapter.update(atr, price)
                if dynamic_leverage != self.config['leverage']:
                    self.log_msg(f"【第3层】杠杆调整: {self.config['leverage']} -> {dynamic_leverage}")
                self.config['leverage'] = int(max(1, dynamic_leverage))

                # 获取市场状态
                state = self.monitor.get_market_state()
                if state != self.last_state:
                    self.last_state = state
                    self.last_state_change_ts = now_ts
                short_ret = 0.0
                if len(self.price_history) > 1 and self.price_history[-2] > 0:
                    short_ret = (self.price_history[-1] - self.price_history[-2]) / self.price_history[-2]
                    self.var_calc.add_return(short_ret)
                funding_interval = int(self.config.get('funding_update_interval_sec', 1800))
                corr_interval = int(self.config.get('corr_update_interval_sec', 1800))
                funding_gate = max(180, int(max(300, funding_interval) * random.uniform(0.8, 1.2)))
                corr_gate = max(180, int(max(300, corr_interval) * random.uniform(0.8, 1.2)))
                if now_ts - self.last_funding_update > funding_gate:
                    self.funding_predictor.update()
                    self.last_funding_update = now_ts
                if now_ts - self.last_corr_update > corr_gate:
                    self.correlation.update()
                    self.last_corr_update = now_ts
                order_imbalance = self.orderbook.get_imbalance()
                sentiment_value = self.sentiment.update()
                funding_pred = self.funding_predictor.predict_next()
                corr_penalty = self.get_correlation_penalty()

                # 计算针对当前标的与活跃持仓的惩罚墙
                active_symbols_snapshot = []
                if hasattr(self.scheduler, "get_active_symbols_snapshot"):
                    active_symbols_snapshot = self.scheduler.get_active_symbols_snapshot()
                corr_wall_penalty = self.correlation.get_correlation_penalty(self.symbol, active_symbols_snapshot)

                var_value = abs(self.var_calc.get_var())
                var_tail = self.var_calc.get_tail_risk(var_value)
                var_block = var_value > float(self.config.get('max_portfolio_var', 0.035))

                # 全局市场斜率熔断 (Global Shockwave)
                global_shockwave_active = False
                if hasattr(self.monitor, "global_shockwave_cooldown_until"):
                    if time.time() < self.monitor.global_shockwave_cooldown_until:
                        global_shockwave_active = True
                        self.log_msg(f"【全局风控】检测到大盘(BTC+ETH)系统性崩盘/暴涨，进入全局静默期，暂缓开单", "WARNING")
                if global_shockwave_active:
                    self.check_filled_orders(0.5, self.config.get('base_price_range', 0.02), self.config.get('leverage', 3), self.latest_decision or {})
                    self.reconcile_orders()
                    time.sleep(2)
                    continue

                # 构建上下文
                context = {
                    'state': state,
                    'prices': list(self.price_history),
                    'atr': atr,
                    'volume_ratio': volume_ratio,
                    'order_imbalance': order_imbalance,
                    'sentiment': sentiment_value
                }
                buy_prob = max(0.0, min(1.0, self.ensemble.decide(context)))
                self.last_decision_context = context  # 保存上下文，供贝叶斯更新使用

                crowding_score = self.compute_crowding_score(buy_prob, order_imbalance, volume_ratio, state)
                if self.suspicious_pause_until > time.time():
                    remain = int(self.suspicious_pause_until - time.time())
                    self.log_msg(f"检测到可疑盘口，暂停开单 {remain}s", "WARNING")
                elif self.detect_suspicious_market(atr, price, volume_ratio, order_imbalance, crowding_score):
                    remain = int(self.suspicious_pause_until - time.time())
                    self.log_msg(f"触发反诱饵暂停，暂缓开单 {remain}s", "WARNING")
                self.maybe_switch_mode(crowding_score)
                if self._anti_hunt_gate(crowding_score, order_imbalance):
                    self.check_filled_orders(buy_prob, self.config.get('base_price_range', 0.02), dynamic_leverage, self.latest_decision or {})
                    self.reconcile_orders()
                    time.sleep(2)
                    continue
                state_phase = self.classify_state_phase(state, atr, price, volume_ratio)
                self.current_style = self.resolve_style(state, atr, price, volume_ratio, var_value, funding_pred, corr_penalty)
                metrics = {
                    "atr": atr,
                    "price": price,
                    "volume_ratio": volume_ratio,
                    "sentiment": sentiment_value,
                    "order_imbalance": order_imbalance,
                    "funding_pred": funding_pred,
                    "corr_penalty": corr_penalty,
                    "corr_wall_penalty": corr_wall_penalty,
                    "active_symbols_snapshot": active_symbols_snapshot,
                    "var_value": var_value,
                    "var_tail": var_tail,
                    "crowding_score": crowding_score,
                    "state_phase": state_phase,
                    "signal_health": self.signal_health_score,
                    "short_return": short_ret,
                    "causal_model_pause": bool(time.time() < self.causal_model_pause_until),
                    "ipw_warmup_window": int(self.ipw_warmup_window),
                    "price_history_len": int(len(self.price_history)),
                    "bb_upper": getattr(self, 'cached_bb_upper', 0.0),
                    "bb_lower": getattr(self, 'cached_bb_lower', 0.0),
                    "current_price": price
                }
                try:
                    prop_features = np.array([
                        0.5, 0.5, min(1.0, abs(buy_prob - 0.5) * 2), 0.5, max(0.0, min(1.0, volume_ratio / 1.8)),
                        max(0.0, min(1.0, sentiment_value)), max(0.0, min(1.0, 0.5 + order_imbalance)),
                        crowding_score, self.signal_health_score, atr / max(1e-8, price), short_ret, volume_ratio - 1.0, 0.5
                    ], dtype=float)
                    metrics["propensity"] = float(self.propensity_model.predict(prop_features))
                except Exception:
                    metrics["propensity"] = 0.5
                decision = self.causal_engine.evaluate(state, buy_prob, metrics, self.current_style)
                decision, recovery_info = self._apply_recovery_to_decision(decision)

                # 动态收网检查（接入因果引擎实时判定）
                if self.current_mode == "stealth":
                    exe = decision.get("execution", {})
                    exe["price_type"] = "limit"
                    exe["urgency"] = max(0.2, float(exe.get("urgency", 0.5)) * 0.85)
                    decision["execution"] = exe
                elif self.current_mode == "counter":
                    exe = decision.get("execution", {})
                    exe["price_type"] = "aggressive_limit"
                    decision["execution"] = exe
                if var_block:
                    decision["entry_allowed"] = False
                    decision["reason"] = f"VaR超阈值({var_value:.4f})"
                self.update_signal_lifespan(decision)
                self.latest_decision = decision
                self.evaluate_reflexive_risk()
                self.log_msg(
                    f"【第10层】开多概率:{buy_prob*100:.1f}% 风格:{decision['style']} 模式:{self.current_mode} "
                    f"机会:{decision['opportunity_score']:.2f} 风险:{decision['risk_penalty']:.2f} 因果:{float(decision.get('details',{}).get('causal_effect',0)):.3f} "
                    f"拥挤:{crowding_score:.2f} 寿命:{self.signal_health_score:.2f} 恢复:{float(recovery_info.get('boost',0.0)):.2f} 结论:{decision['reason']}"
                )
                if bool(self.config.get('decision_verbose', False)):
                    d = decision.get("details", {})
                    self.log_msg(
                        f"【决策明细】conf:{d.get('confidence',0):.2f} trend:{d.get('trend_strength',0):.2f} "
                        f"liq:{d.get('liquidity_score',0):.2f} sent:{d.get('sentiment_score',0):.2f} book:{d.get('book_score',0):.2f} "
                        f"risk(var:{d.get('risk_var',0):.2f},fund:{d.get('risk_funding',0):.2f},corr:{d.get('risk_corr',0):.2f}) "
                        f"causal(h:{d.get('causal_effect_heur',0):.3f},m:{d.get('causal_effect_model',0):.3f},u:{d.get('causal_uncertainty',0):.3f})"
                    )
                self.update_position_status()
                self._refresh_position_lifecycle()
                if self.position_status_stale:
                    self.update_position_status(force=True)
                    if self.position_status_stale:
                        self.log_msg("持仓状态未知，快速重试后仍未恢复，暂缓调度开仓", "WARNING")
                        time.sleep(1)
                        continue
                if self._apply_slot_pressure_policy(decision, price, atr):
                    self.check_filled_orders(buy_prob, self.config.get('base_price_range', 0.02), dynamic_leverage, decision)
                    self.reconcile_orders()
                    time.sleep(1.2)
                    continue
                if self.suspicious_pause_until > time.time():
                    self.log_msg("反诱饵暂停窗口内，跳过本轮开仓请求", "WARNING")
                    self.check_filled_orders(buy_prob, self.config.get('base_price_range', 0.02), dynamic_leverage, decision)
                    self.reconcile_orders()
                    time.sleep(2)
                    continue
                placed = False
                order_token = self.scheduler.request_order(
                    self.symbol,
                    decision["opportunity_score"],
                    self.has_position,
                    self.config.get('max_active_symbols', 2),
                    self.config.get('scheduler_best_window_sec', 10),
                    self.config.get('scheduler_interval_jitter', 0.2),
                    self.config.get('scheduler_skip_prob', 0.08)
                )
                # 增加 _order_token 以便防爆仓拦截时可以释放锁
                decision["_order_token"] = order_token

                if order_token:
                    # 直接下单（网格已移除）
                    placed = self.place_one_order(buy_prob, self.config.get('base_price_range', 0.02), dynamic_leverage, decision)
                    if placed:
                        self.scheduler.confirm_order(order_token, self.symbol, self.config.get('scheduler_interval_jitter', 0.2))
                    else:
                        self.scheduler.cancel_token(order_token)
                else:
                    wait_sec = self.scheduler.get_wait_seconds() if hasattr(self.scheduler, "get_wait_seconds") else 0
                    if wait_sec <= 0:
                        wait_msg = "等待全局调度，候选竞争中"
                    elif wait_sec >= 3600:
                        wait_msg = f"等待全局调度，约{wait_sec // 3600}小时后再试"
                    elif wait_sec >= 60:
                        wait_msg = f"等待全局调度，约{max(1, wait_sec // 60)}分钟后再试"
                    else:
                        wait_msg = f"等待全局调度，约{wait_sec}秒后再试"
                    wait_log_interval = max(1.0, float(self.config.get("scheduler_wait_log_interval_sec", 3.0)))
                    now_ts = time.time()
                    if now_ts - self.last_scheduler_wait_log_ts >= wait_log_interval:
                        self.log_msg(wait_msg)
                        self.last_scheduler_wait_log_ts = now_ts
                self._update_runtime_funnel(decision, order_token, placed)
                self._record_causal_decision(decision, metrics, placed, price, buy_prob)

                # 检查成交
                self.check_filled_orders(buy_prob, self.config.get('base_price_range', 0.02), dynamic_leverage, decision)
                self.reconcile_orders()

                # 检查止损（含自适应trail+ML退出+保本线+Chandelier Exit动态止盈）
                _vol_ratio = getattr(self, 'cached_volume_ratio', 1.0)
                _recent_prices = list(self.price_history) if hasattr(self, 'price_history') else []
                _market_state = self.monitor.get_market_state() if hasattr(self, 'monitor') and self.monitor else None
                stop_action = self.stop_loss.check_stop(
                    self.symbol,
                    price,
                    atr_multiplier=decision["sl_mult"],
                    max_holding_hours=decision["max_holding_hours"],
                    trail_multiplier=decision["trail_mult"],
                    liquidation_price=self.position_liquidation_price,
                    ml_predictor=getattr(self, 'ml_predictor', None),
                    recent_prices=_recent_prices,
                    volatility_ratio=_vol_ratio,
                    market_state=_market_state
                )
                if stop_action:
                    if not self._confirm_stop_action(stop_action, price):
                        stop_action = None
                if stop_action:
                    self.log_msg(f"【第4层】触发{stop_action}，平仓")
                    slip_estimate = self._get_dynamic_slip_estimate(atr, price)
                    if self.close_all_positions(expected_price=price, reason=stop_action, slippage_estimate=slip_estimate):
                        self.record_exit_action(stop_action, pnl_actual=getattr(self, "_last_close_pnl", None))
                    else:
                        self.log_msg(f"【第4层】平仓未成功，跳过退出记录", "WARNING")

                # 黑天鹅检测（价格+订单簿深度双维度）
                ob_depth = 0.0
                try:
                    ob = self.get_cached_orderbook(depth=5, force=False)
                    if isinstance(ob, dict):
                        bids = ob.get("bids", [])
                        asks = ob.get("asks", [])
                        bid_vol = sum(float(b[1]) for b in bids[:5]) if bids else 0
                        ask_vol = sum(float(a[1]) for a in asks[:5]) if asks else 0
                        ob_depth = bid_vol + ask_vol
                except Exception:
                    pass
                if self.blackswan.update(price, orderbook_depth=ob_depth if ob_depth > 0 else None):
                    self.log_msg("【第15层】检测到异常波动（已忽略，止损保护中）", "INFO")
                    # 黑天鹅暂停已关闭：有止损保护，大波动是机会不是风险

                # 每小时更新参数
                param_gate = int(3600 * random.uniform(0.8, 1.2))
                if time.time() - self.last_param_update > param_gate:
                    self.update_params()

                # 每4小时检查资金费率
                funding_gate2 = int(14400 * random.uniform(0.8, 1.2))
                if time.time() - self.last_funding_check > funding_gate2:
                    self.check_funding_rate()
                # 内存深度垃圾回收 (GC) - 每隔约 1 小时触发一次
                if time.time() - self.last_gc_ts > 3600:
                    self.log_msg("【系统维护】执行深度内存垃圾回收...", "INFO")
                    import gc
                    # 清理过期的因果样本
                    if len(self.causal_pending) > 2000:
                        self.causal_pending = deque(list(self.causal_pending)[-1000:], maxlen=3000)
                    if len(self.causal_treated_bank) > 2000:
                        self.causal_treated_bank = deque(list(self.causal_treated_bank)[-1000:], maxlen=3000)
                    # 清理过期的订单簿缓存
                    if hasattr(self, "orderbook_history") and len(self.orderbook_history) > 10:
                        self.orderbook_history = deque(list(self.orderbook_history)[-5:], maxlen=int(self.config.get('orderbook_smooth_n', 3)))
                    gc.collect()
                    self.last_gc_ts = time.time()
                    self.log_msg("【系统维护】内存垃圾回收完成", "INFO")

                self._runtime_adapt()

                try:
                    urgency = max(0.0, min(1.0, float(decision.get("execution", {}).get("urgency", 0.5))))
                except Exception:
                    urgency = 0.5
                sleep_min = max(0.3, float(self.config.get("loop_sleep_min_sec", 1.0)))
                sleep_max = max(sleep_min, float(self.config.get("loop_sleep_max_sec", 6.0)))
                loop_sleep = sleep_max - (sleep_max - sleep_min) * urgency
                if bool(self.config.get("antifragile_mode", True)):
                    loop_sleep = loop_sleep * random.uniform(0.9, 1.15)
                time.sleep(loop_sleep)
            except Exception as e:
                self.log_msg(f"运行异常: {e}", "ERROR")
                if is_auth_error(e):
                    self.log_msg("运行中检测到签名或鉴权失败，策略线程挂起等待恢复", "ERROR")
                    time.sleep(60)
                elif is_rate_limit_error(e):
                    time.sleep(20)
                else:
                    time.sleep(10)

        if not self.stop_reason:
            self.stop_reason = "stopped"
        self.cleanup()

    def place_market_order(self, side, qty, reason="market", decision=None, expected_price=None):
        try:
            order = api_call(self.exchange.create_order, self.symbol, 'market', side, qty, params={'hedged': True})
            self.last_order_ts = time.time()
            self.order_count += 1
            self.fill_count += 1
            if side == 'buy':
                self.buy_count += 1
            else:
                self.sell_count += 1
            avg_price = self.last_price
            if isinstance(order, dict):
                try:
                    avg_price = float(order.get("average") or order.get("price") or self.last_price)
                except Exception:
                    avg_price = self.last_price
            if expected_price and float(expected_price) > 0 and avg_price > 0:
                slip = abs(avg_price - float(expected_price)) / float(expected_price)
                self.slippage_history.append(slip)
                self.entry_slippage_history.append(slip)
            atr_value = np.mean(self.atr_history) if len(self.atr_history) > 0 else max(1e-8, self.last_price * 0.005)
            tp_mult = float((decision or {}).get("tp_mult", 1.8))
            self.stop_loss.add_position(self.symbol, avg_price, atr_value, side, tp_mult)
            self.log_msg(f"【执行层】市价单成交 {side} {qty:.4f} 原因:{reason}")
            return isinstance(order, dict) or order is not None
        except Exception as e:
            self.log_msg(f"市价补单失败: {e}", "ERROR")
            return False

    def place_one_order(self, buy_prob, price_range, leverage, decision):
        try:
            decision = decision if isinstance(decision, dict) else {}
            reason = str(decision.get("reason", ""))
            if bool(self.config.get("execution_degraded", False)):
                self.log_msg("执行容灾开启中，暂缓新开仓", "WARNING")
                return False
            # ====== 数据可信检查 ======
            if hasattr(self, '_data_trust') and not self._data_trust:
                self.log_msg("⚠️ 数据不可信（OHLCV获取失败），暂缓开仓", "WARNING")
                return False
            # ====== 日亏损熔断检查 ======
            if hasattr(self, 'safety_monitor') and self.safety_monitor and self.safety_monitor.is_daily_fused():
                self.log_msg("🚨 日亏损熔断中，禁止新开仓", "WARNING")
                return False
            # ====== 平仓冷却检查 ======
            cooldown_min = max(5, int(self.config.get("symbol_close_cooldown_min", 10)))
            last_close = getattr(self, '_last_close_ts', 0)
            if last_close > 0 and (time.time() - last_close) < cooldown_min * 60:
                remaining = int(cooldown_min * 60 - (time.time() - last_close))
                now_ts = time.time()
                if now_ts - getattr(self, '_last_cooldown_log_ts', 0) >= 30:
                    self.log_msg(f"平仓冷却中，还需{remaining}秒", "INFO")
                    self._last_cooldown_log_ts = now_ts
                return False
            # ====== 同币种连亏冷却 ======
            streak_until = getattr(self, '_symbol_streak_until', 0)
            if streak_until > 0 and time.time() < streak_until:
                remaining_min = int((streak_until - time.time()) / 60)
                now_ts = time.time()
                if now_ts - getattr(self, '_last_streak_log_ts', 0) >= 60:
                    self.log_msg(f"连亏冷却中，还需{remaining_min}分钟", "WARNING")
                    self._last_streak_log_ts = now_ts
                return False
            if self.has_position and (not bool(self.config.get("allow_scale_in", False))):
                now_ts = time.time()
                if now_ts - self.last_scalein_log_ts >= 10:
                    self.log_msg("已有持仓，禁止重复加仓（可在allow_scale_in开启后允许）", "WARNING")
                    self.last_scalein_log_ts = now_ts
                return False
            pending_notional = 0.0
            price = self.last_price
            if price <= 0:
                self.log_msg("价格异常，跳过本次下单")
                return False
            market = self.exchange.market(self.symbol)
            now_ts_for_pos = time.time()
            if self.has_position and (now_ts_for_pos - float(self.last_position_check)) >= max(3.0, float(self.config.get("position_refresh_interval_sec", 5))):
                self.update_position_status(force=True)
            symbols_cfg = self.config.get('symbols', [])
            symbols_count = len(symbols_cfg) if isinstance(symbols_cfg, (list, tuple)) else 1
            max_active_limit = int(self.config.get('max_active_symbols', DEFAULT_CONFIG.get('max_active_symbols', 6)))
            max_active_limit = max(1, max_active_limit)
            max_margin_per_symbol = float(self.config['margin_usdt']) / max_active_limit
            with self.pending_orders_lock:
                for o in self.pending_orders:
                    try:
                        p_qty = float(o.get('amount', 0))
                        p_price = float(o.get('price', price))
                        if p_qty > 0 and p_price > 0:
                            p_contract_size = float(market.get('contractSize', market.get('info', {}).get('quanto_multiplier', 1)) or 1)
                            is_contract_p = market.get('contract', False)
                            p_notional = p_qty * p_price * (p_contract_size if is_contract_p and p_contract_size > 0 else 1.0)
                            pending_notional += (p_notional / max(1e-8, leverage))
                    except:
                        pass  # silent fallback
            total_exposure = max(0.0, float(self.position_margin)) + max(0.0, float(pending_notional))
            if total_exposure >= max_margin_per_symbol * 0.95:
                now_ts = time.time()
                if now_ts - self.last_scalein_log_ts >= 30:
                    self.log_msg(f"该币种当前总敞口({total_exposure:.2f}U, 含挂单{pending_notional:.2f}U)已达到单币上限({max_margin_per_symbol:.2f}U)，强制拒绝任何加仓请求！", "WARNING")
                    self.last_scalein_log_ts = now_ts
                return False
            if not decision.get("entry_allowed", False):
                self.log_msg(f"开仓门禁未通过({decision.get('reason', 'unknown')})")
                return False

            # ====== 手续费覆盖检查：预期利润不够付手续费就不开仓 ======
            try:
                _atr_val = getattr(self, 'cached_atr', 0.0)
                if _atr_val > 0 and safe_price > 0:
                    # 预期利润 = ATR × tp_mult 的约一半（保守估计）
                    maker_fee = float(self.config.get("maker_fee", 0.0002))
                    taker_fee = float(self.config.get("taker_fee", 0.0005))
                    fee_rate = maker_fee + taker_fee  # 开仓限价+平仓市价
                    # 最小名义价值
                    min_notional = max_amount if min_amount > 0 else 0
                    if is_contract and contract_size > 0:
                        min_notional = max(min_notional, float(market.get('limits', {}).get('cost', {}).get('min', 0) or 0))
                    if min_notional > 0:
                        round_trip_fee = min_notional * fee_rate
                        # 保守预期利润 = ATR × tp_mult × qty × 0.5（只吃一半行情）
                        tp_mult_val = float(decision.get("tp_mult", 2.5))
                        expected_profit = _atr_val * tp_mult_val * qty * contract_size if (is_contract and contract_size > 0) else _atr_val * tp_mult_val * qty
                        if expected_profit < round_trip_fee * 1.5:
                            now_ts = time.time()
                            if now_ts - getattr(self, '_fee_check_log_ts', 0) >= 30:
                                self.log_msg(f"⚠️ 预期利润({expected_profit:.4f}U)不足以覆盖手续费({round_trip_fee:.4f}U×1.5)，跳过开仓", "WARNING")
                                self._fee_check_log_ts = now_ts
                            return False
            except Exception:
                pass  # fee check 失败不阻止开仓
            side = decision.get("side")
            if side not in ("buy", "sell"):
                return False
            try:
                base_skip = max(0.0, min(0.2, float(self.config.get("random_skip_rate", 0.05))))
            except Exception:
                base_skip = 0.05
            base_skip = max(0.02, min(0.08, random.uniform(0.02, 0.08) * (0.5 + base_skip / 0.1)))
            crowding_now = float(decision.get("details", {}).get("crowding_score", 0.5)) if isinstance(decision, dict) else 0.5
            mode_boost = 0.05 if self.current_mode == "stealth" else 0.0
            skip_rate = max(0.0, min(0.35, base_skip + max(0.0, crowding_now - 0.6) * 0.2 + mode_boost))
            overlay = decision.get("recovery_overlay", {}) if isinstance(decision.get("recovery_overlay", {}), dict) else {}
            if overlay.get("active"):
                skip_rate = max(0.0, skip_rate * (1.0 - min(0.6, float(overlay.get("boost", 0.0)))))
            if bool(self.config.get("antifragile_mode", True)) and random.random() < skip_rate:
                self.log_msg(f"混沌注入：随机跳过本次开单(skip={skip_rate:.3f})", "WARNING")
                return False

            lev_scale = float(decision.get("lev_scale", 1.0))
            if not math.isfinite(lev_scale):
                lev_scale = 1.0
            effective_lev = float(leverage) * lev_scale
            if not math.isfinite(effective_lev):
                effective_lev = float(self.config.get('leverage', 3))
            max_lev = max(1, min(20, int(self.config.get('leverage', 10))))
            max_lev = min(10, max_lev)
            effective_lev = max(1.0, min(float(max_lev), effective_lev))
            leverage = max(1, int(effective_lev))
            leverage_for_sizing = self._get_confirmed_leverage_for_sizing(leverage)
            min_amount = market.get('limits', {}).get('amount', {}).get('min')
            if min_amount is None:
                min_amount = 0.0
            min_amount = float(min_amount)
            margin_total = float(self.config['margin_usdt'])
            used_margin = self._estimate_used_margin_total(refresh_sec=8.0)
            available_margin = max(0.0, margin_total - used_margin)
            symbols_cfg = self.config.get('symbols', [])
            symbols_count = len(symbols_cfg) if isinstance(symbols_cfg, (list, tuple)) else 0
            max_active = int(self.config.get('max_active_symbols', DEFAULT_CONFIG.get('max_active_symbols', 6)))
            max_active = max(1, max_active)
            active_est = 0
            try:
                if hasattr(self.scheduler, "get_active_symbols_snapshot"):
                    active_est = len(self.scheduler.get_active_symbols_snapshot())
                elif hasattr(self.scheduler, "lock") and hasattr(self.scheduler, "active_symbols"):
                    with self.scheduler.lock:
                        active_est = len(set(self.scheduler.active_symbols))
                elif hasattr(self.scheduler, "active_symbols"):
                    active_est = len(set(self.scheduler.active_symbols))
            except Exception:
                active_est = 0
            if self.has_position and active_est <= 0:
                active_est = 1
            slots_left = max(1, max_active - max(0, active_est))

            # === 这里修复单币种开仓量的问题 ===
            # 如果当前是空仓，或者我们允许加仓，那么我们只能使用 margin_per_symbol (比如25U)
            # 而不是把剩余的 available_margin 全部用掉
            margin_per_symbol = available_margin / slots_left
            if margin_per_symbol <= 0:
                self.log_msg(f"可用保证金不足: total={margin_total:.2f} used={used_margin:.2f}", "WARNING")
                return False

            safe_price = max(1e-8, price)
            # 计算开仓数量：注意这里的合约价值。
            # 如果是币本位或以张数计价的 U本位，我们需要将面值纳入计算，否则可能导致算出 1.0 等整数。
            contract_size = float(market.get('contractSize', market.get('info', {}).get('quanto_multiplier', 1)) or 1)

            # 针对 Gate.io 等交易所，如果 API 要求传入的是"张数"（contracts），则需要除以 contract_size
            # 这里的判断逻辑优化：只要是衍生品合约，且 contract_size > 0 且不等于 1，就默认除以它
            is_contract = market.get('contract', False)
            effective_margin_cap = min(margin_per_symbol, max_margin_per_symbol)
            if overlay.get("active"):
                try:
                    boost = max(0.0, min(float(self.config.get("recovery_max_boost", 0.45)), float(overlay.get("boost", 0.0))))
                except Exception:
                    boost = 0.0
                if boost > 0:
                    boosted = min(max_margin_per_symbol, effective_margin_cap * (1.0 + boost))
                    extra = max(0.0, boosted - effective_margin_cap)
                    budget_left = max(0.0, float(overlay.get("budget_left", 0.0)))
                    if extra > 0 and budget_left > 0:
                        use_extra = min(extra, budget_left)
                        effective_margin_cap = min(max_margin_per_symbol, effective_margin_cap + use_extra)
                        self.recovery_today_boost_used += use_extra
            if is_contract and contract_size > 0:
                total_contracts = (effective_margin_cap * leverage_for_sizing) / (safe_price * contract_size)
            else:
                total_contracts = (effective_margin_cap * leverage_for_sizing) / safe_price

            # ====== ATR 仓位管理：根据波动率动态调整仓位 ======
            # 核心逻辑：单笔最大亏损 = effective_margin_cap × risk_per_trade_pct
            # 止损距离 = atr × sl_mult
            # ATR限制仓位 = 最大亏损金额 / 止损距离
            _atr_for_sizing = getattr(self, 'cached_atr', 0.0)
            _sl_mult_for_sizing = float(decision.get("sl_mult", 1.7))
            if _atr_for_sizing > 0 and safe_price > 0:
                try:
                    _risk_pct = max(0.5, min(3.0, float(self.config.get("risk_per_trade_pct", 1.0))))
                    _risk_amount = effective_margin_cap * _risk_pct / 100.0
                    _stop_distance_price = _atr_for_sizing / safe_price  # ATR占价格比例
                    if _stop_distance_price > 1e-8:
                        if is_contract and contract_size > 0:
                            _atr_contracts = (_risk_amount * leverage_for_sizing) / (_atr_for_sizing * _sl_mult_for_sizing * contract_size)
                        else:
                            _atr_contracts = (_risk_amount * leverage_for_sizing) / (_atr_for_sizing * _sl_mult_for_sizing)
                        # 取原仓位和ATR限制仓位的较小值（取小不取大，确保不超风险）
                        if _atr_contracts < total_contracts:
                            total_contracts = _atr_contracts
                            if hasattr(self, 'log_msg'):
                                self.log_msg(f"【ATR仓位管理】波动率偏高，ATR限制仓位缩至 {_atr_contracts:.4f}", "INFO")
                except Exception:
                    pass  # ATR仓位计算失败时使用原始仓位，不影响正常运行

            # 处理精度问题，防止下单量太大或不符合交易所步长
            precision_amount = market.get('precision', {}).get('amount')
            if precision_amount is not None:
                try:
                    # 统一将精度转化为 step size
                    if isinstance(precision_amount, float) and precision_amount < 1.0:
                        step = precision_amount
                    elif isinstance(precision_amount, int) and precision_amount >= 0:
                        # 如果是 0，step 就是 1.0；如果是 4，step 就是 0.0001
                        step = 10 ** -precision_amount
                    elif precision_amount == 1.0 or precision_amount == 1:
                        step = 1.0
                    else:
                        step = 1.0

                    # 使用 step size 进行安全向下取整
                    qty = math.floor(total_contracts / step) * step
                    # 如果 step 是整数，则把 qty 转成 int，防止 160.0 被某些严格的交易所拒绝
                    if step >= 1.0:
                        qty = int(qty)
                    else:
                        # 截断多余的浮点误差
                        decimals = len(str(step).rstrip('0').split('.')[-1])
                        qty = round(qty, decimals)
                except:
                    qty = total_contracts
            else:
                qty = total_contracts

            if qty < min_amount:
                qty = min_amount

            # 终极安全断言：硬性限制名义价值绝对不能超过分配额度的杠杆倍数
            # 如果计算出的 qty 因为 min_amount 或各种意外导致其价值过大，直接拒绝下单
            notional_value = qty * safe_price * (contract_size if is_contract and contract_size > 0 else 1.0)

            # 将已成交持仓保证金 + 挂单冻结保证金 进行合并计算
            total_exposure = self.position_margin + pending_notional

            # 这里修复一个隐患：如果当前是加仓，且剩余额度不足以满足 min_amount 的强制要求，也必须拒绝
            remaining_margin = max(0.0, max_margin_per_symbol - total_exposure)

            max_allowed_notional = remaining_margin * leverage_for_sizing * 1.05 # 允许5%的误差容限

            if notional_value > max_allowed_notional:
                self.log_msg(f"【硬性防爆仓拦截】尝试开仓的名义价值({notional_value:.2f}U)超过了当前剩余额度允许上限({max_allowed_notional:.2f}U)，取消下单！(当前总敞口:{total_exposure:.2f}U)", "ERROR")
                if self.scheduler:
                    self.scheduler.cancel_token(decision.get("_order_token", ""))
                return False

            try:
                reverse_rate = max(0.0, min(0.02, float(self.config.get("reverse_probe_rate", 0.005))))
            except Exception:
                reverse_rate = 0.005
            if bool(self.config.get("antifragile_mode", True)) and random.random() < reverse_rate:
                side = "sell" if side == "buy" else "buy"
                try:
                    chaos_scale = max(0.05, min(0.3, float(self.config.get("chaos_injection_rate", 0.1))))
                except Exception:
                    chaos_scale = 0.1
                qty = max(min_amount, qty * chaos_scale)
                self.log_msg("混沌注入：触发极小反向试探单", "WARNING")

            exec_cfg = decision.get("execution", {}) if isinstance(decision, dict) else {}
            price_type = str(exec_cfg.get("price_type", "limit"))
            exec_algo = str(exec_cfg.get("algo", "default"))
            if self.current_mode == "stealth":
                price_type = "limit"
                exec_algo = "twap"
            try:
                price_offset = float(exec_cfg.get("price_offset", 0.0))
            except Exception:
                price_offset = 0.0
            try:
                offset_jitter = max(0.0, min(0.4, float(self.config.get("execution_offset_jitter_ratio", 0.2))))
            except Exception:
                offset_jitter = 0.2
            price_offset = price_offset * random.uniform(max(0.2, 1.0 - offset_jitter), 1.0 + offset_jitter)
            try:
                timeout_sec = max(0, int(exec_cfg.get("timeout", 0)))
            except Exception:
                timeout_sec = 0
            try:
                urgency = max(0.0, min(1.0, float(exec_cfg.get("urgency", 0.5))))
            except Exception:
                urgency = 0.5
            exec_reason = str(exec_cfg.get("reason", decision.get("reason", "默认执行")))
            try:
                jitter_sec = max(0.0, min(3.0, float(self.config.get("execution_jitter_sec", 2))))
            except Exception:
                jitter_sec = 2.0
            if jitter_sec > 0:
                now_ts = time.monotonic()
                if now_ts < self.next_order_earliest_ts:
                    self.log_msg("执行指纹节流，暂缓本次下单", "WARNING")
                    return False

            best_bid = 0.0
            best_ask = 0.0
            try:
                ob = self.get_cached_orderbook(depth=5, force=False)
                bids = ob.get("bids", []) if isinstance(ob, dict) else []
                asks = ob.get("asks", []) if isinstance(ob, dict) else []
                if bids and len(bids[0]) > 0:
                    best_bid = float(bids[0][0])
                if asks and len(asks[0]) > 0:
                    best_ask = float(asks[0][0])
            except Exception:
                pass  # silent fallback

            # ==================== 自适应入场引擎接入 ====================
            atr_ratio_for_entry = self.cached_atr / price if price > 0 and self.cached_atr > 0 else 0.02
            confidence_for_entry = min(1.0, abs(buy_prob - 0.5) * 2)
            state_for_entry = self.monitor.get_market_state() if self.monitor else MarketState.RANGE
            vol_ratio_for_entry = self.cached_volume_ratio if hasattr(self, 'cached_volume_ratio') else 1.0
            ob_imb_for_entry = float(decision.get("details", {}).get("book_score", 0.5)) - 0.5 if isinstance(decision, dict) else 0.0

            entry_mode = self.adaptive_entry.select_mode(
                state_for_entry, confidence_for_entry, atr_ratio_for_entry, vol_ratio_for_entry, ob_imb_for_entry
            )
            self.last_entry_mode = entry_mode

            entry_params = self.adaptive_entry.compute_entry_params(
                entry_mode, side, price, self.cached_atr,
                getattr(self, 'cached_bb_upper', 0.0),
                getattr(self, 'cached_bb_lower', 0.0),
                price_range
            )

            adaptive_order_type = entry_params.get("order_type", "limit")
            adaptive_price = entry_params.get("price", price)
            adaptive_timeout = entry_params.get("timeout_sec", 0)
            adaptive_split = entry_params.get("split_parts", 1)
            adaptive_reason = entry_params.get("reason", "自适应入场")

            # 如果自适应引擎选择了市价，直接走市价通道
            if adaptive_order_type == "market":
                expected_mkt = best_ask if side == "buy" else best_bid
                if expected_mkt <= 0:
                    expected_mkt = price
                # 分批入场
                if adaptive_split > 1:
                    per_qty = max(min_amount, qty / adaptive_split)
                    all_ok = True
                    for part_i in range(adaptive_split):
                        ok = self.place_market_order(side, per_qty, f"{adaptive_reason}(第{part_i+1}/{adaptive_split}批)", decision, expected_price=expected_mkt)
                        if not ok:
                            all_ok = False
                            break
                        if part_i < adaptive_split - 1:
                            time.sleep(random.uniform(0.5, 2.0))
                            # 刷新价格
                            p2, _ = self.get_latest_price()
                            if p2 > 0:
                                expected_mkt = p2
                    if all_ok:
                        self.position_open_context = getattr(self, "last_decision_context", None)
                        # P1-5: 确认成交才设 has_position，防止未成交就标记
                        self.has_position = True
                        self._pending_entry = False
                        self.last_entry_fill_ts = time.time()
                        if jitter_sec > 0:
                            self.next_order_earliest_ts = time.monotonic() + random.uniform(0.2, jitter_sec)
                    return all_ok
                else:
                    placed = self.place_market_order(side, qty, adaptive_reason, decision, expected_price=expected_mkt)
                    if placed:
                        self.position_open_context = getattr(self, "last_decision_context", None)
                        # P1-5: 确认成交才设 has_position
                        self.has_position = True
                        self._pending_entry = False
                        self.last_entry_fill_ts = time.time()
                        if jitter_sec > 0:
                            self.next_order_earliest_ts = time.monotonic() + random.uniform(0.2, jitter_sec)
                    return placed

            # 限价单通道：使用自适应引擎计算的价格
            if adaptive_timeout > 0:
                timeout_sec = adaptive_timeout
            order_price = adaptive_price

            # 应用价格抖动
            try:
                price_jitter = max(0.0, min(0.002, float(self.config.get("execution_price_jitter_ratio", 0.0005))))
            except Exception:
                price_jitter = 0.0005
            if price_jitter > 0:
                order_price = max(1e-8, order_price * random.uniform(1.0 - price_jitter, 1.0 + price_jitter))

            order = api_call(
                self.exchange.create_order,
                self.symbol, 'limit', side, qty, order_price,
                {'hedged': True}
            )
            if isinstance(order, dict) and order.get("id"):
                order["_created_ts"] = time.time()
                order["_timeout"] = timeout_sec
                order["_fallback_market"] = bool(timeout_sec > 0 and price_type in ("aggressive_limit", "limit"))
                with self.pending_orders_lock:
                    self.pending_orders.append(order)
            else:
                self.log_msg("下单返回缺少订单ID，已跳过本地挂单跟踪", "WARNING")
            self.last_order_ts = time.time()
            self.order_count += 1
            if side == 'buy':
                self.buy_count += 1
            else:
                self.sell_count += 1

            self.log_msg(
                f"【全局调度】下单 #{self.order_count} {side} {qty:.4f} @ {order_price:.4f} "
                f"(入场:{adaptive_reason}|风格:{decision['style']}|机会:{decision['opportunity_score']:.2f}|风险:{decision['risk_penalty']:.2f}|总额度:{margin_total:.2f}U|已用:{used_margin:.2f}U|可用:{available_margin:.2f}U|单币:{margin_per_symbol:.2f}U|槽位:{slots_left}|多概率:{buy_prob*100:.1f}%)"
            )
            try:
                self.last_entry_causal_effect = float(decision.get("details", {}).get("causal_effect", 0.0))
                self.last_entry_causal_uncertainty = float(decision.get("details", {}).get("causal_uncertainty", 1.0))
            except Exception:
                self.last_entry_causal_effect = 0.0
                self.last_entry_causal_uncertainty = 1.0
            if jitter_sec > 0:
                self.next_order_earliest_ts = time.monotonic() + random.uniform(0.2, jitter_sec)
            self.position_open_context = getattr(self, "last_decision_context", None)
            # P1-5: 限价单不立即设 has_position，标记 _pending_entry
            self._pending_entry = True
            # ML学习：保存开仓时的价格快照
            try:
                self._entry_prices_snapshot = list(self.price_history) if hasattr(self, 'price_history') else None
            except Exception:
                self._entry_prices_snapshot = None
            return True
        except Exception as e:
            self.log_msg(f"下单失败: {e}", "ERROR")
            return False

    def update_position_status(self, force=False):
        def _norm_symbol(s):
            s = str(s or "").upper().replace(":USDT", "").replace("/", "").replace("_", "")
            return s
        now = time.time()
        refresh_sec = max(3.0, float(self.config.get("position_refresh_interval_sec", 5)))
        if (not force) and now - self.last_position_check < refresh_sec:
            return
        self.last_position_check = now
        try:
            positions = api_call(self.exchange.fetch_positions)
            if not isinstance(positions, list):
                self.position_status_stale = True
                return
            me = _norm_symbol(self.symbol)
            pos = None
            for p in positions:
                ps = _norm_symbol(p.get('symbol', p.get('info', {}).get('contract', p.get('info', {}).get('symbol', ''))))
                if ps == me:
                    pos = p
                    break
            if pos is None and self.has_position:
                self.position_stale_count += 1
                if self.position_stale_count >= 3:
                    self.has_position = False
                    self.position_contracts = 0.0
                    self.position_entry_price = 0.0
                    self.position_mark_price = 0.0
                    self.position_unrealized_pnl = 0.0
                    self.position_roe = 0.0
                    self.position_margin = 0.0
                    self.position_status_stale = False
                    self.position_stale_count = 0
                else:
                    self.position_status_stale = True
                    return
            contracts = float(pos.get('contracts', 0)) if pos else 0.0
            # 兼容 Gate.io 返回 size 字段的情况
            if pos and contracts == 0 and pos.get('info', {}).get('size'):
                contracts = float(pos.get('info', {}).get('size'))

            self.has_position = bool(pos and abs(contracts) > 0)
            if self.has_position:
                if self.scheduler:
                    self.scheduler.keep_alive(self.symbol)
                self.position_contracts = contracts
                self.position_entry_price = float(pos.get('entryPrice', pos.get('entry_price', 0)) or 0)
                self.position_mark_price = float(pos.get('markPrice', pos.get('mark_price', 0)) or 0)

                # 兼容 Gate.io 未实现盈亏可能存在于 info 里的情况
                upnl = pos.get('unrealizedPnl', pos.get('unrealized_pnl'))
                if upnl is None:
                    upnl = pos.get('info', {}).get('unrealised_pnl', 0)
                self.position_unrealized_pnl = float(upnl or 0)

                # ==== 提取真实杠杆倍数 ====
                real_leverage = float(pos.get('leverage', self.config.get('leverage', 3)) or self.config.get('leverage', 3))
                if real_leverage <= 0:
                    real_leverage = float(pos.get('info', {}).get('leverage', self.config.get('leverage', 3)) or self.config.get('leverage', 3))

                # ==== 修复 Gate.io 等交易所 ROE 为 0 的问题 ====
                roe = float(pos.get('percentage', pos.get('roe', 0)) or 0)
                if roe == 0 and self.position_entry_price > 0:
                    # 如果 API 没返回 ROE，我们自己算: (标记价 - 开仓价) / 开仓价 * 杠杆 * 方向
                    # 修复正负号 Bug：做空时如果标记价 < 开仓价，应该是赚的。
                    # direction = 1 (多单), direction = -1 (空单)
                    side = pos.get('side', 'long').lower()
                    if 'short' in side or 'sell' in side:
                        direction = -1
                    else:
                        direction = 1 if self.position_contracts > 0 else -1
                    roe = (self.position_mark_price - self.position_entry_price) / self.position_entry_price * max(1e-8, real_leverage) * direction * 100
                self.position_roe = roe

                # ==== 修复 Gate.io 保证金显示为 0 的问题 ====
                margin = float(pos.get('initialMargin', pos.get('margin', pos.get('collateral', 0))) or 0)
                if margin == 0:
                    margin = float(pos.get('info', {}).get('margin', 0))
                if margin == 0 and self.position_mark_price > 0 and abs(self.position_contracts) > 0:
                    # 如果 API 没返回保证金，用名义价值除以杠杆估算
                    # 注意：Gate.io 合约面值(contractSize)可能不是 1，但为简化且统一，这里基于名义价值近似
                    contract_size = float(pos.get('contractSize', pos.get('info', {}).get('quanto_multiplier', 1)) or 1)
                    notional = abs(self.position_contracts) * contract_size * self.position_mark_price
                    margin = notional / max(1e-8, real_leverage)
                self.position_margin = margin

                # 尝试获取强平价，不同交易所字段不同
                liq_price = pos.get('liquidationPrice', pos.get('liquidation_price'))
                if liq_price is None:
                    info = pos.get('info', {})
                    liq_price = info.get('liquidationPrice', info.get('liqPrice', info.get('estimatedLiquidationPrice', 0.0)))
                self.position_liquidation_price = float(liq_price) if liq_price else 0.0

                # 确保止损模块同步了该持仓（防止因为本地 JSON 丢失导致永远无法平仓）
                self.stop_loss._load_state()
                if self.symbol not in self.stop_loss.positions or not self.stop_loss.positions[self.symbol]:
                    dummy_atr = self.position_entry_price * 0.005
                    self.stop_loss.add_position(self.symbol, self.position_entry_price, dummy_atr, 'buy' if direction > 0 else 'sell', 1.8)
                    self.log_msg(f"从交易所恢复丢失的止损状态记录", "WARNING")

            else:
                self.position_contracts = 0.0
                self.position_entry_price = 0.0
                self.position_mark_price = 0.0
                self.position_unrealized_pnl = 0.0
                self.position_roe = 0.0
                self.position_margin = 0.0
            self.position_status_stale = False
            self.position_stale_count = 0
        except Exception as e:
            if is_auth_error(e):
                self.log_msg(f"获取持仓状态时发现API密钥或签名无效: {e}，策略暂时挂起，等待恢复", "ERROR")
                # 移除 self.stop_reason = "auth_invalid"，防止被外层认为是致命错误而退出
                self.suspicious_pause_until = time.time() + 60
                self.position_status_stale = True
                return
            self.log_msg(f"获取持仓状态异常: {e}", "DEBUG")
            self.position_status_stale = True

    def _estimate_used_margin_total(self, refresh_sec=8.0):
        now = time.time()
        if now - float(self.margin_used_cache_ts) < max(1.0, float(refresh_sec)):
            return max(0.0, float(self.margin_used_cache))
        symbol_set = self._get_managed_symbol_set()
        try:
            positions = api_call(self.exchange.fetch_positions)
            used = 0.0
            if isinstance(positions, list):
                for p in positions:
                    ps = self._normalize_symbol(p.get('symbol', p.get('info', {}).get('contract', p.get('info', {}).get('symbol', ''))))
                    if ps not in symbol_set:
                        continue
                    contracts = abs(float(p.get('contracts', p.get('positionAmt', p.get('size', 0))) or 0))
                    # 兼容 Gate.io 返回 size 的情况
                    if contracts == 0 and p.get('info', {}).get('size'):
                        contracts = abs(float(p.get('info', {}).get('size')))

                    if contracts <= 0:
                        continue

                    # 统一的三层兜底逻辑
                    m = float(p.get('initialMargin', p.get('margin', p.get('collateral', 0))) or 0)
                    if m == 0:
                        m = float(p.get('info', {}).get('margin', 0))

                    if m > 0:
                        used += m
                    else:
                        ep = float(p.get('entryPrice', p.get('entry_price', 0)) or 0)
                        real_leverage = float(p.get('leverage', self.config.get('leverage', 4)) or self.config.get('leverage', 4))
                        if real_leverage <= 0:
                            real_leverage = float(p.get('info', {}).get('leverage', self.config.get('leverage', 4)) or self.config.get('leverage', 4))

                        # 如果没有明文保证金，用名义价值除以当前杠杆作为预估
                        if ep > 0 and real_leverage > 0:
                            contract_size = float(p.get('contractSize', p.get('info', {}).get('quanto_multiplier', 1)) or 1)
                            used += (contracts * contract_size * ep) / max(1e-8, real_leverage)
            self.margin_used_cache = max(0.0, float(used))
            self.margin_used_cache_ts = now
        except Exception:
            pass  # silent fallback
        return max(0.0, float(self.margin_used_cache))

    def resolve_style(self, state, atr, price, volume_ratio, var_value, funding_pred, corr_penalty):
        configured = str(self.config.get('strategy_style', '自动'))
        if configured != "自动":
            return configured if configured in self.style_profiles else "均衡"
        if price <= 0:
            return "保守"
        atr_ratio = atr / price if price > 0 else 0.0
        high_vol = atr_ratio > 0.035
        low_vol = atr_ratio < 0.012
        strong_state = state in [MarketState.STRONG_UPTREND, MarketState.STRONG_DOWNTREND, MarketState.EXTREME_UPTREND, MarketState.EXTREME_DOWNTREND]
        weak_state = state in [MarketState.RANGE, MarketState.WEAK_UPTREND, MarketState.WEAK_DOWNTREND]
        var_scale = max(0.5, min(5.0, float(self.config.get("style_var_scale", 2.0))))
        risk_pressure = (abs(funding_pred) * 400 + corr_penalty * 0.8 + var_value * var_scale)
        conservative_thr = max(0.3, min(3.0, float(self.config.get("style_risk_conservative_threshold", 1.2))))
        aggressive_thr = max(0.1, min(conservative_thr, float(self.config.get("style_risk_aggressive_threshold", 0.75))))
        if high_vol or risk_pressure > conservative_thr:
            return "保守"
        if strong_state and volume_ratio >= 1.2 and not high_vol and risk_pressure < aggressive_thr:
            return "激进"
        if low_vol and weak_state:
            return "保守"
        return "均衡"

    def get_correlation_penalty(self):
        try:
            matrix = self.correlation.correlation_matrix
            if matrix is None or self.symbol not in matrix.index:
                return 0.0
            if hasattr(self.scheduler, "get_active_symbols_snapshot"):
                active_symbols = list(self.scheduler.get_active_symbols_snapshot())
            else:
                s_active = getattr(self.scheduler, "active_symbols", [])
                if isinstance(s_active, dict):
                    active_symbols = list(s_active.keys())
                else:
                    active_symbols = list(s_active)
            peers = [s for s in active_symbols if s != self.symbol and s in matrix.columns]
            if not peers:
                return 0.0
            positive = []
            for s in peers:
                c = float(matrix.loc[self.symbol, s])
                if c > 0:
                    positive.append(c)
            if not positive:
                return 0.0
            return float(np.mean(positive))
        except:
            return 0.0

    def check_filled_orders(self, buy_prob, price_range, leverage, decision):
        open_map = {}
        try:
            open_orders = api_call(self.exchange.fetch_open_orders, self.symbol)
            if isinstance(open_orders, list):
                for o in open_orders:
                    oid = str(o.get("id")) if isinstance(o, dict) else ""
                    if oid:
                        open_map[oid] = o
        except Exception as e:
            self.log_msg(f"批量查询挂单失败，回退逐单查询: {e}", "WARNING")
        for order in list(self.pending_orders):
            try:
                order_id = order.get("id")
                if not order_id:
                    with self.pending_orders_lock:
                        if order in self.pending_orders:
                            self.pending_orders.remove(order)
                    continue
                oid = str(order_id)
                fetched = open_map.get(oid)
                if fetched is None:
                    fetched = api_call(self.exchange.fetch_order, order_id, self.symbol)
                    if not isinstance(fetched, dict):
                        continue
                status = str(fetched.get('status', '')).lower()
                if status == 'closed':
                    side = fetched.get('side', 'unknown')
                    amount = fetched.get('amount', 0)
                    avg_price = fetched.get('average', fetched.get('price', self.last_price))
                    atr_value = np.mean(self.atr_history) if len(self.atr_history) > 0 else max(1e-8, self.last_price * 0.005)
                    tp_mult = float((decision or {}).get("tp_mult", 1.8))
                    self.stop_loss.add_position(self.symbol, avg_price, atr_value, side, tp_mult)
                    self.fill_count += 1
                    with self.pending_orders_lock:
                        if order in self.pending_orders:
                            self.pending_orders.remove(order)
                    self.log_msg(f"订单已成交: {side} {amount}")
                    if self.scheduler:
                        self.scheduler.record_fill(self.symbol)

                    # === 补充原先缺失的成交后处理逻辑 ===
                    created_ts = float(order.get("_created_ts", 0) or 0)
                    if created_ts > 0:
                        self.order_lifetime_history.append(max(0.0, time.time() - created_ts))
                    try:
                        placed_price = float(order.get("price", 0) or 0)
                        fill_price = float(avg_price or 0)
                        if placed_price > 0 and fill_price > 0:
                            eslip = abs(fill_price - placed_price) / placed_price
                            self.entry_slippage_history.append(eslip)
                            self.slippage_history.append(eslip)
                    except Exception:
                        pass  # silent fallback
                    try:
                        cooldown = int(self.config.get('fill_reentry_cooldown_sec', 1800))
                    except Exception:
                        cooldown = 1800
                    try:
                        cooldown_jitter = max(0.0, min(0.5, float(self.config.get('fill_reentry_jitter_ratio', 0.2))))
                    except Exception:
                        cooldown_jitter = 0.2
                    cooldown = max(60, int(cooldown * random.uniform(max(0.3, 1.0 - cooldown_jitter), 1.0 + cooldown_jitter)))
                    if time.time() - self.last_order_ts >= cooldown:
                        self.log_msg(f"成交后补单触发（冷却{cooldown}s已满足）")
                        self.place_one_order(buy_prob, price_range, leverage, decision)
                elif status in ('open', 'new', 'partially_filled'):
                    timeout_sec = int(order.get("_timeout", 0) or 0)
                    created_ts = float(order.get("_created_ts", 0) or 0)
                    if timeout_sec > 0 and created_ts > 0 and (time.time() - created_ts >= timeout_sec):
                        cancelled = False
                        try:
                            api_call(self.exchange.cancel_order, order_id, self.symbol)
                            cancelled = True
                        except Exception:
                            cancelled = False
                        if bool(order.get("_fallback_market", False)):
                            side = str(order.get("side", fetched.get("side", ""))).lower()
                            amount = float(fetched.get("remaining", order.get("remaining", order.get("amount", fetched.get("amount", 0)))) or 0)
                            if cancelled:
                                try:
                                    post = api_call(self.exchange.fetch_order, order_id, self.symbol)
                                    if isinstance(post, dict):
                                        amount = float(post.get("remaining", amount) or amount)
                                except Exception:
                                    pass  # silent fallback
                            if side in ("buy", "sell") and amount > 0 and cancelled:
                                self.place_market_order(side, amount, reason="timeout_convert", decision=decision)
                        if cancelled:
                            with self.pending_orders_lock:
                                if order in self.pending_orders:
                                    self.pending_orders.remove(order)
                        if (not cancelled) and isinstance(order, dict):
                            retry_n = int(order.get("_cancel_retry", 0) or 0) + 1
                            order["_cancel_retry"] = retry_n
                            max_retry = max(1, int(self.config.get("timeout_cancel_max_retries", 4)))
                            if retry_n >= max_retry:
                                self.log_msg(f"超时撤单连续失败({retry_n})，移出本地跟踪: {order_id}", "ERROR")
                                with self.pending_orders_lock:
                                    if order in self.pending_orders:
                                        self.pending_orders.remove(order)
                                continue
                            order["_created_ts"] = time.time()
                            self.log_msg(f"超时撤单失败，稍后重试: {order_id}", "WARNING")
                elif status in ['canceled', 'rejected', 'expired', 'cancelled']:
                    with self.pending_orders_lock:
                        if order in self.pending_orders:
                            self.pending_orders.remove(order)
                    self.log_msg(f"订单已失效/取消 ({status})")
                    # ==== 触发重进逻辑 ====
                    try:
                        timeout_limit = order.get("_timeout", 0)
                        fallback_mkt = order.get("_fallback_market", False)
                        if timeout_limit > 0 and fallback_mkt and time.time() - order.get("_created_ts", 0) > timeout_limit:
                            side = order.get("side", "buy")
                            qty = order.get("amount", 0)
                            if qty > 0:
                                self.log_msg(f"挂单超时，转为市价重进 {side} {qty}")
                                self.place_market_order(side, qty, reason="timeout_convert", decision=decision)
                    except Exception as e:
                        self.log_msg(f"处理超时转市价异常: {e}", "WARNING")
            except Exception as e:
                self.log_msg(f"核对订单状态异常: {e}", "ERROR")

    def reconcile_orders(self, startup=False):
        now = time.time()
        interval = max(30, int(self.config.get("order_reconcile_interval_sec", 120)))
        if (not startup) and (now - self.last_reconcile_ts < interval):
            return
        self.last_reconcile_ts = now
        try:
            open_orders = api_call(self.exchange.fetch_open_orders, self.symbol)
            if not isinstance(open_orders, list):
                return
            open_ids = {str(o.get('id')) for o in open_orders if isinstance(o, dict) and o.get('id')}
            removed = 0
            with self.pending_orders_lock:
                for order in list(self.pending_orders):
                    oid = str(order.get("id"))
                    if oid in open_ids:
                        continue
                    try:
                        order_id = order.get("id")
                        if not order_id:
                            continue
                        fetched = api_call(self.exchange.fetch_order, order_id, self.symbol)
                        if not isinstance(fetched, dict):
                            continue
                        status = str(fetched.get("status", "")).lower()
                        if status in ("closed", "canceled", "expired", "rejected"):
                            if order in self.pending_orders:
                                self.pending_orders.remove(order)
                            removed += 1
                            if status == "closed":
                                self.fill_count += 1
                    except Exception as e:
                        if startup:
                            self.log_msg(f"启动核对查询挂单失败({order.get('id', 'unknown')}): {e}", "WARNING")
            if removed > 0:
                if startup:
                    self.log_msg(f"启动核对已修正挂单状态({removed})", "WARNING")
                else:
                    self.log_msg(f"挂单核对已修正状态({removed})", "INFO")

            if len(open_orders) > max(3, int(self.config.get("max_stale_orders", 5))):
                for o in open_orders:
                    if time.time() - (o.get('timestamp', 0) / 1000.0) > max(300, int(self.config.get("stale_order_cancel_sec", 600))):
                        try:
                            api_call(self.exchange.cancel_order, o['id'], self.symbol)
                        except:
                            pass  # silent fallback
        except Exception as e:
            self.log_msg(f"定期对账异常: {e}", "WARNING")

    def check_funding_rate(self):
        try:
            funding = api_call(self.exchange.fetch_funding_rate, self.symbol)
            rate_raw = funding.get('fundingRate', 0)
            next_time_raw = funding.get('nextFundingTime')
            rate = float(rate_raw) if rate_raw is not None else 0.0
            next_time = float(next_time_raw) if next_time_raw is not None else 0.0
            if (not math.isfinite(rate)) or (not math.isfinite(next_time)):
                self.last_funding_check = time.time()
                return
            now_ms = time.time() * 1000
            if next_time > now_ms and abs(rate) > float(self.config['funding_rate_threshold']) and (next_time - now_ms) < 3600000:
                positions = api_call(self.exchange.fetch_positions)
                me = self._normalize_symbol(self.symbol)
                pos = None
                if isinstance(positions, list):
                    for p in positions:
                        ps = self._normalize_symbol(p.get('symbol', p.get('info', {}).get('contract', p.get('info', {}).get('symbol', ''))))
                        if ps == me:
                            pos = p
                            break
                contracts = float(pos.get('contracts', 0)) if pos else 0.0
                if pos and abs(contracts) > 0:
                    side_pos = str(pos.get('side', '')).lower()
                    if side_pos not in ('long', 'short'):
                        side_pos = 'long' if contracts > 0 else 'short'
                    if (rate > 0 and side_pos == 'long') or (rate < 0 and side_pos == 'short'):
                        self.log_msg(f"【第8层】资金费率 {rate:.6f} 过高，平仓")
                        side = 'sell' if side_pos == 'long' else 'buy'
                        closed = False
                        try:
                            api_call(
                                self.exchange.create_order,
                                self.symbol, 'market', side, abs(contracts),
                                params={'reduceOnly': True}
                            )
                            closed = True
                        except Exception as e:
                            self.log_msg(f"资金费率平仓失败: {e}", "ERROR")

                        if closed:
                            ep = float(pos.get('entryPrice', pos.get('entry_price', 0)) or 0)
                            self.stop_loss.remove_position(self.symbol, entry_price=ep, side='buy' if side_pos == 'long' else 'sell')
                            self.record_exit_action("time_exit", pnl_actual=getattr(self, "_last_close_pnl", None))
            self.last_funding_check = time.time()
        except Exception as e:
            self.log_msg(f"资金费率检查异常: {e}", "ERROR")

    def update_params(self, new_params=None):
        self.log_msg("【第14层】更新参数与风险阈值")
        if isinstance(new_params, dict):
            self.apply_tunable_params(new_params)
        self.var_calc.confidence = float(self.config.get('var_confidence', self.var_calc.confidence))
        self.var_calc.lookback_hours = int(self.config.get('var_lookback_hours', self.var_calc.lookback_hours))
        self.last_param_update = time.time()

    def close_all_positions(self, expected_price=None, reason="manual", slippage_estimate=0.0):
        self.log_msg(f"【安全清理】正在执行全局平仓({reason})", "WARNING")
        all_closed = True
        try:
            positions = api_call(self.exchange.fetch_positions)
            if not isinstance(positions, list):
                return False
            me = self._normalize_symbol(self.symbol)
            for pos in positions:
                try:
                    ps = self._normalize_symbol(pos.get('symbol', pos.get('info', {}).get('contract', pos.get('info', {}).get('symbol', ''))))
                    if ps != me:
                        continue
                    contracts = float(pos.get('contracts', 0))
                    if abs(contracts) <= 0:
                        continue
                    side_pos = pos.get('side', '')
                    side = 'sell' if side_pos == 'long' else 'buy'
                    expected_exec = float(expected_price) if expected_price else float(self.last_price)
                    if expected_exec > 0 and slippage_estimate > 0:
                        if side == "sell":
                            expected_exec = max(1e-8, expected_exec - slippage_estimate)
                        else:
                            expected_exec = expected_exec + slippage_estimate
                    order = api_call(
                        self.exchange.create_order,
                        self.symbol, 'market', side, abs(contracts),
                        params={'reduceOnly': True}
                    )
                    try:
                        actual_exec = float(order.get("average") or order.get("price") or expected_exec)
                    except Exception:
                        actual_exec = expected_exec
                    if expected_exec > 0 and actual_exec > 0:
                        slip = abs(actual_exec - expected_exec) / expected_exec
                        self.slippage_history.append(slip)
                        if reason in ("stop_loss", "trail_exit", "time_exit"):
                            self.log_msg(f"滑点记录({reason}) 预期:{expected_exec:.4f} 实际:{actual_exec:.4f} 比例:{slip*100:.3f}%", "WARNING")
                    if reason in ("stop_loss", "trail_exit", "time_exit") and actual_exec > 0:
                        self.stop_hunt_events.append({"ts": time.time(), "side": str(side_pos), "price": float(actual_exec)})
                    try:
                        ep = float(pos.get('entryPrice', pos.get('entry_price', 0)) or 0)
                        lev_used = float(pos.get('leverage', pos.get('info', {}).get('leverage', self.config.get("leverage", 3))) or self.config.get("leverage", 3))
                        side_tag = str(side_pos).lower()
                        if side_tag not in ("long", "short"):
                            side_tag = "long" if contracts > 0 else "short"
                        pnl_quote = 0.0
                        contract_size_pos = float(pos.get('contractSize', pos.get('info', {}).get('quanto_multiplier', 1)) or 1)
                        if contract_size_pos <= 0:
                            contract_size_pos = 1.0
                        notional_entry = abs(contracts) * ep * contract_size_pos
                        if side_tag == "long":
                            pnl_quote = (actual_exec - ep) * abs(contracts) * contract_size_pos
                        else:
                            pnl_quote = (ep - actual_exec) * abs(contracts) * contract_size_pos
                        fee_rate = float(self.config.get("taker_fee", 0.0005))
                        # 双边手续费: 开仓 + 平仓
                        pnl_quote -= abs(notional_entry) * fee_rate * 2
                        self._record_realized_pnl(float(pnl_quote))
                        self._last_close_pnl = float(pnl_quote)  # 记录供 record_exit_action 使用
                    except Exception:
                        pass  # silent fallback
                    ep = float(pos.get('entryPrice', pos.get('entry_price', 0)) or 0)
                    self.stop_loss.remove_position(self.symbol, entry_price=ep, side='buy' if side_pos == 'long' else 'sell')
                except Exception as e:
                    self.log_msg(f"单持仓平仓失败: {e}", "ERROR")
                    all_closed = False

            if all_closed:
                self.has_position = False
                # 平仓冷却：记录该币种平仓时间，防止立刻重新开单
                self._last_close_ts = time.time()
                cooldown_min = max(5, int(self.config.get("symbol_close_cooldown_min", 10)))
                self.log_msg(f"平仓完成，{cooldown_min}分钟内不再开仓", "INFO")
            return all_closed
        except Exception as e:
            self.log_msg(f"平仓异常: {e}", "ERROR")
            return False

    def check_liquidity(self):
        try:
            vol = 0.0
            cache = self.config.get("_startup_quote_volume", {})
            cache_ts = float(self.config.get("_startup_quote_volume_ts", 0) or 0)
            if isinstance(cache, dict) and (time.time() - cache_ts) <= 180 and self.symbol in cache:
                vol = float(cache.get(self.symbol, 0) or 0)
            if vol <= 0:
                ticker = api_call(self.exchange.fetch_ticker, self.symbol)
                vol = float(ticker.get('quoteVolume', 0) or 0)
            if vol < self.config['min_24h_volume']:
                self.liquidity_check_failed = False
                self.log_msg(f"成交量 {vol:.0f} < 阈值")
                return False
            self.liquidity_check_failed = False
            return True
        except Exception as e:
            # 增加详细日志，如果是因为限频等异常，不能静默失败
            self.log_msg(f"流动性检查API调用失败: {e}", "WARNING")
            self.liquidity_check_failed = True
            return False

    def init_settings(self):
        lev = int(max(1, min(10, int(self.config.get('leverage', 3)))))
        try:
            api_call(self.exchange.set_leverage, lev, self.symbol)
        except Exception as e:
            if is_auth_error(e):
                self.log_msg(f"初始化杠杆时发现API密钥无效或签名错误: {e}，将使用当前杠杆并继续运行", "WARNING")
            else:
                self.log_msg(f"初始化杠杆失败: {e}，将使用交易所当前杠杆设置", "WARNING")
        try:
            positions = api_call(self.exchange.fetch_positions)
            me = self._normalize_symbol(self.symbol)
            if isinstance(positions, list):
                for p in positions:
                    ps = self._normalize_symbol(p.get('symbol', p.get('info', {}).get('contract', p.get('info', {}).get('symbol', ''))))
                    if ps != me:
                        continue
                    lv = float(p.get('leverage', p.get('info', {}).get('leverage', lev)) or lev)
                    if math.isfinite(lv) and lv > 0:
                        self.config['leverage'] = int(max(1, min(10, lv)))
                    break
        except Exception:
            pass  # silent fallback
        try:
            has_margin_mode = False
            has_map = getattr(self.exchange, "has", {})
            if isinstance(has_map, dict):
                has_margin_mode = bool(has_map.get("setMarginMode", False))
            can_call = hasattr(self.exchange, "set_margin_mode")
            if has_margin_mode and can_call:
                api_call(self.exchange.set_margin_mode, self.config.get('margin_mode', 'cross'), self.symbol)
        except Exception as e:
            if is_auth_error(e):
                self.log_msg(f"初始化保证金模式时发现API密钥无效或签名错误: {e}，将使用当前模式并继续运行", "WARNING")
                pass
            elif "not supported" not in str(e).lower():
                self.log_msg(f"初始化保证金模式失败: {e}", "WARNING")
        return True

    def _get_confirmed_leverage_for_sizing(self, target_leverage):
        target = int(max(1, min(10, int(target_leverage or self.config.get('leverage', 3)))))
        confirmed = 0.0
        try:
            api_call(self.exchange.set_leverage, target, self.symbol)
        except Exception as e:
            self.log_msg(f"下单前杠杆设置失败，转保守模式: {e}", "WARNING")
        try:
            positions = api_call(self.exchange.fetch_positions)
            me = self._normalize_symbol(self.symbol)
            if isinstance(positions, list):
                for p in positions:
                    ps = self._normalize_symbol(p.get('symbol', p.get('info', {}).get('contract', p.get('info', {}).get('symbol', ''))))
                    if ps != me:
                        continue
                    lv = float(p.get('leverage', p.get('info', {}).get('leverage', 0)) or 0)
                    if math.isfinite(lv) and lv > 0:
                        confirmed = lv
                    break
        except Exception:
            confirmed = 0.0
        if confirmed > 0:
            self.config['leverage'] = int(max(1, min(10, confirmed)))
            if abs(confirmed - target) > 0.5:
                self.log_msg(f"杠杆确认值与目标不一致(target={target}x, confirmed={confirmed:.2f}x)，按确认值计算仓位", "WARNING")
            return float(max(1.0, min(10.0, confirmed)))
        self.log_msg(f"无法确认实际杠杆(target={target}x)，按1x保守计算仓位", "WARNING")
        return 1.0

    def cleanup(self):
        self.log_msg("【清理】策略停止，撤销所有挂单")
        try:
            if self.ws_stream is not None:
                self.ws_stream.stop()
                if self.ws_stream.is_alive():
                    self.ws_stream.join(timeout=2)
        except Exception:
            pass  # silent fallback
        try:
            api_call(self.exchange.cancel_all_orders, self.symbol)
        except Exception as e:
            self.log_msg(f"清理挂单失败: {e}", "ERROR")

    def get_stats(self):
        returns = np.array(self.performance_returns, dtype=float) if len(self.performance_returns) > 0 else np.array([])
        slippages = np.array(self.slippage_history, dtype=float) if len(self.slippage_history) > 0 else np.array([])
        sharpe = 0.0
        max_drawdown = 0.0
        win_rate = 0.5
        profit_factor = 1.0
        slippage_mean = 0.0
        slippage_p95 = 0.0
        if len(returns) > 5:
            std = np.std(returns)
            sharpe = float(np.mean(returns) / std) if std > 1e-8 else 0.0
            equity_curve = np.cumprod(1 + np.clip(returns * 0.1, -0.9, 1.5))
            peak = np.maximum.accumulate(equity_curve)
            dd = (peak - equity_curve) / np.maximum(peak, 1e-8)
            max_drawdown = float(np.max(dd)) if len(dd) > 0 else 0.0
            win_rate = float(np.mean(returns > 0))
            pos = returns[returns > 0].sum()
            neg = np.abs(returns[returns < 0].sum())
            profit_factor = float(pos / neg) if neg > 1e-8 else 2.0
        if len(slippages) > 0:
            slippage_mean = float(np.mean(slippages))
            slippage_p95 = float(np.percentile(slippages, 95))
        reflexive_alert = 1 if (self.suspicious_pause_until > time.time() or self.current_mode != "normal") else 0
        avg_lifetime = float(np.mean(self.order_lifetime_history)) if len(self.order_lifetime_history) > 0 else 0.0
        stop_hunt_freq = float(len([x for x in self.stop_hunt_hits if time.time() - x <= max(60, int(self.config.get("stoploss_hunt_window", 60))*6)]))
        return {
            'order_count': self.order_count,
            'buy_count': self.buy_count,
            'sell_count': self.sell_count,
            'pending_orders': len(self.pending_orders),
            'fill_count': self.fill_count,
            'last_exit': self.last_exit,
            'stop_loss_count': self.stop_loss_count,
            'sharpe': sharpe,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'var_value': abs(self.var_calc.get_var()),
            'slippage_mean': slippage_mean,
            'slippage_p95': slippage_p95,
            'signal_health': self.signal_health_score,
            'order_lifetime_mean': avg_lifetime,
            'stop_hunt_hits': stop_hunt_freq,
            'mode': self.current_mode,
            'reflexive_alert': reflexive_alert,
            'has_position': self.has_position,
            'position_contracts': self.position_contracts,
            'entry_price': self.position_entry_price,
            'mark_price': self.position_mark_price,
            'unrealized_pnl': self.position_unrealized_pnl,
            'position_roe': self.position_roe,
            'position_margin': self.position_margin,
            'used_margin_total': float(self._estimate_used_margin_total(refresh_sec=30.0)),
            'available_margin_total': max(0.0, float(self.config.get("margin_usdt", 0.0)) - float(self._estimate_used_margin_total(refresh_sec=30.0))),
            'rt_candidate': int(self.runtime_window_stats.get("candidate", 0)),
            'rt_entry_allowed': int(self.runtime_window_stats.get("entry_allowed", 0)),
            'rt_causal_block': int(self.runtime_window_stats.get("causal_block", 0)),
            'rt_scheduler_grant': int(self.runtime_window_stats.get("scheduler_grant", 0)),
            'rt_orders_sent': int(self.runtime_window_stats.get("orders_sent", 0)),
            'rt_fills': int(self.runtime_window_stats.get("fills", 0)),
            'rt_global_order_interval': int(self.config.get("global_order_interval", 180)),
            'rt_causal_threshold': float(self.config.get("causal_effect_threshold", 0.006)),
            'rt_uncertainty_penalty': float(self.config.get("causal_uncertainty_penalty", 0.40)),
            'slot_pressure_mode': int(1 if self.slot_pressure_mode else 0),
            'slot_best_roe_seen': float(self.best_roe_seen if self.best_roe_seen > -9 else 0.0)
        }

    def stop(self):
        self.running = False
        self.stop_reason = "manual_stop"

    def record_exit_action(self, action, pnl_actual=None):
        with self.exit_lock:
            self.last_exit = action
            if action == "stop_loss":
                self.stop_loss_count += 1
        self.runtime_recent_exits.append(str(action))
        # 根据实际盈亏判断胜负（优先使用传入值），回退到退出类型
        if pnl_actual is not None and pnl_actual != 0:
            outcome = 1.0 if pnl_actual > 0 else -1.0
        else:
            mapping = {
                "take_profit": 1.0,
                "trail_exit": 1.0,
                "ml_trend_exit": 1.0,
                "breakeven_exit": 1.0,
                "time_exit": -1.0,
                "stop_loss": -1.0
            }
            outcome = float(mapping.get(action, -1.0))
        self.performance_returns.append(outcome)

        # ML在线学习：用平仓结果反馈给ML预测器
        try:
            if hasattr(self, 'ml_predictor') and hasattr(self.ml_predictor, 'learn'):
                actual_ret = 1.0 if outcome > 0 else -1.0
                if hasattr(self, '_entry_prices_snapshot') and self._entry_prices_snapshot is not None:
                    self.ml_predictor.learn(self._entry_prices_snapshot, actual_ret)
        except Exception:
            pass  # silent fallback

        # MAB入场模式反馈：根据平仓结果更新入场引擎
        if hasattr(self, 'adaptive_entry') and hasattr(self, 'last_entry_mode') and self.last_entry_mode >= 0:
            mab_reward = outcome * 0.5  # 缩放到合理范围
            # 考虑成交速度：如果入场到平仓时间很短且盈利，说明入场时机好
            if hasattr(self, 'last_entry_fill_ts') and self.last_entry_fill_ts > 0:
                hold_hours = (time.time() - self.last_entry_fill_ts) / 3600
                if hold_hours < 2 and outcome > 0:
                    mab_reward += 0.2  # 快速盈利加分
                elif hold_hours > 12 and outcome < 0:
                    mab_reward -= 0.1  # 长时间持仓亏损扣分
            self.adaptive_entry.record_outcome(self.last_entry_mode, mab_reward)

        # 贝叶斯策略权重更新：根据平仓结果的胜负，动态调整各个子策略的权重
        # outcome > 0 视为胜利 (1.0), outcome < 0 视为失败 (-1.0)
        actual_outcome = 1.0 if outcome > 0 else -1.0

        # 为了让权重更新平滑且合理，我们将最新的上下文状态传给 ensemble
        # 修复缺陷 10：传入建仓时的真实 context（如果存在），否则回退到最近的 context
        try:
            if hasattr(self.ensemble, "update_weights"):
                ctx_to_use = getattr(self, "position_open_context", None) or getattr(self, "last_decision_context", None)
                if ctx_to_use:
                    self.ensemble.update_weights(ctx_to_use, actual_outcome)
                    # 记录融合样本数
                    if not hasattr(self.ensemble, 'update_count'):
                        self.ensemble.update_count = 0
                    self.ensemble.update_count += 1
        except Exception as e:
            self.log_msg(f"贝叶斯权重更新异常: {e}", "WARNING")

        try:
            alpha = max(0.01, min(0.5, float(self.config.get("causal_feedback_alpha", 0.15))))
        except Exception:
            alpha = 0.15
        u_factor = max(0.25, min(1.0, 1 - float(self.last_entry_causal_uncertainty) * 0.7))
        if action in ("stop_loss", "time_exit"):
            self.causal_threshold = min(0.12, self.causal_threshold + 0.02 * alpha * u_factor)
        elif action in ("take_profit", "trail_exit"):
            self.causal_threshold = max(-0.02, self.causal_threshold - 0.015 * alpha * u_factor)
        if self.last_entry_causal_effect > 0 and outcome < 0:
            self.causal_threshold = min(0.12, self.causal_threshold + min(0.03, self.last_entry_causal_effect * 0.05) * u_factor)
        base_thr = float(self.config.get("causal_effect_threshold_base", self.config.get("causal_effect_threshold", self.causal_threshold)))
        self.causal_threshold = max(-0.02, min(0.12, self.causal_threshold * 0.88 + base_thr * 0.12))
        self.config["causal_effect_threshold"] = float(self.causal_threshold)
        self.update_signal_outcome(action)

    def consume_last_exit(self):
        with self.exit_lock:
            tag = self.last_exit
            self.last_exit = ""
        return tag

    def apply_tunable_params(self, params):
        for key, value in params.items():
            if key in self.config:
                self.config[key] = value
        def _clean_float(value, fallback):
            try:
                v = float(value)
                if math.isfinite(v):
                    return v
                return float(fallback)
            except Exception:
                return float(fallback)
        self.style_profiles["均衡"]["buy_open"] = _clean_float(self.config.get("buy_open", self.style_profiles["均衡"]["buy_open"]), self.style_profiles["均衡"]["buy_open"])
        self.style_profiles["均衡"]["sell_open"] = _clean_float(self.config.get("sell_open", self.style_profiles["均衡"]["sell_open"]), self.style_profiles["均衡"]["sell_open"])
        self.style_profiles["均衡"]["min_score"] = _clean_float(self.config.get("min_score", self.style_profiles["均衡"]["min_score"]), self.style_profiles["均衡"]["min_score"])
        self.style_profiles["均衡"]["sl_mult"] = _clean_float(self.config.get("sl_mult", self.style_profiles["均衡"]["sl_mult"]), self.style_profiles["均衡"]["sl_mult"])
        self.style_profiles["均衡"]["tp_mult"] = _clean_float(self.config.get("tp_mult", self.style_profiles["均衡"]["tp_mult"]), self.style_profiles["均衡"]["tp_mult"])
        self.style_profiles["均衡"]["trail_mult"] = _clean_float(self.config.get("trail_mult", self.style_profiles["均衡"]["trail_mult"]), self.style_profiles["均衡"]["trail_mult"])
        self.style_profiles["均衡"]["lev_scale"] = _clean_float(self.config.get("lev_scale", self.style_profiles["均衡"]["lev_scale"]), self.style_profiles["均衡"]["lev_scale"])
        self.config["max_holding_hours"] = max(4, min(72, int(_clean_float(self.config.get("max_holding_hours", 24), 24))))
        self.config["causal_effect_threshold"] = max(-0.02, min(0.12, _clean_float(self.config.get("causal_effect_threshold", 0.015), 0.015)))
        self.config["causal_effect_threshold_base"] = max(-0.02, min(0.12, _clean_float(self.config.get("causal_effect_threshold_base", 0.015), 0.015)))
        self.config["causal_blend_weight"] = max(0.1, min(0.8, _clean_float(self.config.get("causal_blend_weight", 0.45), 0.45)))
        self.config["causal_uncertainty_penalty"] = max(0.0, min(1.0, _clean_float(self.config.get("causal_uncertainty_penalty", 0.55), 0.55)))
        self.config["causal_opportunity_cost_weight"] = max(0.0, min(1.0, _clean_float(self.config.get("causal_opportunity_cost_weight", 0.35), 0.35)))
        self.config["causal_execution_boost"] = max(0.0, min(0.6, _clean_float(self.config.get("causal_execution_boost", 0.20), 0.20)))
        self.config["causal_drift_threshold"] = max(0.002, min(0.05, _clean_float(self.config.get("causal_drift_threshold", 0.012), 0.012)))
        self.config["causal_corr_floor"] = max(-0.5, min(0.5, _clean_float(self.config.get("causal_corr_floor", -0.05), -0.05)))
        self.config["runtime_target_orders_per_hour"] = max(0.5, min(40.0, _clean_float(self.config.get("runtime_target_orders_per_hour", 6.0), 6.0)))
        self.config["runtime_target_causal_block_ratio"] = max(0.1, min(0.95, _clean_float(self.config.get("runtime_target_causal_block_ratio", 0.55), 0.55)))
        self.config["runtime_threshold_step"] = max(0.0002, min(0.01, _clean_float(self.config.get("runtime_threshold_step", 0.0015), 0.0015)))
        self.config["runtime_uncertainty_step"] = max(0.001, min(0.2, _clean_float(self.config.get("runtime_uncertainty_step", 0.02), 0.02)))
        self.config["runtime_stoploss_guard_ratio"] = max(0.1, min(0.95, _clean_float(self.config.get("runtime_stoploss_guard_ratio", 0.55), 0.55)))
        self.config["runtime_adapt_interval_sec"] = max(60, min(3600, int(_clean_float(self.config.get("runtime_adapt_interval_sec", 300), 300))))
        self.config["runtime_interval_step_sec"] = max(5, min(120, int(_clean_float(self.config.get("runtime_interval_step_sec", 15), 15))))
        self.config["runtime_min_order_interval_sec"] = max(10, min(900, int(_clean_float(self.config.get("runtime_min_order_interval_sec", 10), 60))))
        self.config["runtime_max_order_interval_sec"] = max(int(self.config["runtime_min_order_interval_sec"]), min(3600, int(_clean_float(self.config.get("runtime_max_order_interval_sec", 240), 240))))
        self.config["slot_pressure_on_ratio"] = max(0.3, min(1.2, _clean_float(self.config.get("slot_pressure_on_ratio", 0.80), 0.80)))
        self.config["slot_pressure_off_ratio"] = max(0.2, min(self.config["slot_pressure_on_ratio"], _clean_float(self.config.get("slot_pressure_off_ratio", 0.55), 0.55)))
        self.config["slot_pressure_take_roe"] = max(0.0001, min(0.05, _clean_float(self.config.get("slot_pressure_take_roe", 0.0045), 0.0045)))
        self.config["slot_pressure_retrace_roe"] = max(0.0001, min(0.05, _clean_float(self.config.get("slot_pressure_retrace_roe", 0.0025), 0.0025)))
        self.config["slot_pressure_max_hold_sec"] = max(300, min(43200, int(_clean_float(self.config.get("slot_pressure_max_hold_sec", 5400), 5400))))
        self.config["slot_pressure_time_relief_min_roe"] = max(-0.05, min(0.05, _clean_float(self.config.get("slot_pressure_time_relief_min_roe", -0.0015), -0.0015)))
        self.config["slot_pressure_cooldown_sec"] = max(10, min(3600, int(_clean_float(self.config.get("slot_pressure_cooldown_sec", 120), 120))))
        self.config["leverage"] = max(1, min(10, int(_clean_float(self.config.get("leverage", 3), 3))))
        self.config["random_skip_rate"] = max(0.0, min(0.25, _clean_float(self.config.get("random_skip_rate", 0.05), 0.05)))
        self.config["mode_switch_probability"] = max(0.0, min(0.2, _clean_float(self.config.get("mode_switch_probability", 0.02), 0.02)))
        self.config["max_slippage_alarm"] = max(0.0001, min(0.02, _clean_float(self.config.get("max_slippage_alarm", 0.002), 0.002)))
        self.config["orderbook_confirm_ticks"] = max(1, min(12, int(_clean_float(self.config.get("orderbook_confirm_ticks", 3), 3))))
        self.config["mode_min_dwell_sec"] = max(60, min(21600, int(_clean_float(self.config.get("mode_min_dwell_sec", 900), 900))))
        self.config["mode_switch_cooldown_sec"] = max(30, min(7200, int(_clean_float(self.config.get("mode_switch_cooldown_sec", 300), 300))))
        self.config["stop_confirm_window_sec"] = max(0.0, min(30.0, _clean_float(self.config.get("stop_confirm_window_sec", 2), 6)))
        self.config["stop_trigger_jitter_ratio"] = max(0.0, min(0.4, _clean_float(self.config.get("stop_trigger_jitter_ratio", 0.08), 0.08)))
        self.config["anti_hunt_hit_threshold"] = max(1, min(8, int(_clean_float(self.config.get("anti_hunt_hit_threshold", 2), 2))))
        self.config["anti_hunt_pause_sec"] = max(60, min(21600, int(_clean_float(self.config.get("anti_hunt_pause_sec", 900), 900))))
        self.config["recovery_budget_ratio"] = max(0.0, min(0.4, _clean_float(self.config.get("recovery_budget_ratio", 0.15), 0.15)))
        self.config["recovery_daily_target_pct"] = max(0.0, min(0.03, _clean_float(self.config.get("recovery_daily_target_pct", 0.004), 0.004)))
        self.config["recovery_step_up"] = max(0.01, min(0.5, _clean_float(self.config.get("recovery_step_up", 0.12), 0.12)))
        self.config["recovery_step_down"] = max(0.01, min(0.5, _clean_float(self.config.get("recovery_step_down", 0.15), 0.15)))
        self.config["recovery_max_boost"] = max(0.0, min(0.8, _clean_float(self.config.get("recovery_max_boost", 0.45), 0.45)))
        self.config["recovery_fail_limit"] = max(1, min(8, int(_clean_float(self.config.get("recovery_fail_limit", 2), 2))))
        self.config["recovery_cooldown_sec"] = max(60, min(86400, int(_clean_float(self.config.get("recovery_cooldown_sec", 3600), 3600))))
        self.vol_adapter.base_leverage = int(self.config.get("leverage", self.vol_adapter.base_leverage))
        if hasattr(self, "causal_engine"):
            self.causal_engine.config = self.config

# ==================== GUI界面 ====================
class BotGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("PhoenixQ V1.2.0 // 凤凰量化交易系统")
        self.root.geometry("1200x900")
        # PhoenixQ 主题 - 暖金+深灰，凤凰涅槃感
        self.colors = {
            "bg": "#1a1a2e",       # 深靛蓝底色
            "panel": "#16213e",    # 深海蓝面板
            "fg": "#e8e8e8",       # 柔白文字
            "accent": "#f0a500",   # 凤凰金（主强调色）
            "danger": "#e74c3c",   # 柔红警告
            "success": "#2ecc71",  # 翡翠绿
            "warning": "#f39c12"   # 琥珀黄
        }
        self.root.configure(bg=self.colors["bg"])
        try:
            self._blank_icon = tk.PhotoImage(width=1, height=1)
            self.root.iconphoto(True, self._blank_icon)
        except:
            pass  # silent fallback
        self.load_config()
        self.setup_ui()
        self.running = False
        self.monitor = None
        self.strategies = {}
        self.exchange = None
        self.autopilot = None
        self.evolution_engine = None
        self.safety_monitor = None
        self.scheduler = None
        self.strategies_lock = threading.Lock()
        self.stop_event = threading.Event()  # 用于通知所有线程停止
        self.backend_thread = None
        self.last_balance_refresh = 0
        self.error_events = deque(maxlen=200)
        self.warn_count = 0
        self.error_count = 0
        self.runtime_degrade_until = 0
        self.last_report_day = ""
        self.health_lock = threading.Lock()
        self._degrade_lock = threading.Lock()  # P0-4: execution_degraded 竞态修复
        self.daily_report_retry_ts = 0

    def load_config(self):
        global API_RUNTIME_SETTINGS
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    loaded = json.load(f)
                    self.config = DEFAULT_CONFIG.copy()
                    self.config.update(loaded)
                    if isinstance(DEFAULT_CONFIG.get("tunable_params"), dict):
                        merged_tunable = dict(DEFAULT_CONFIG.get("tunable_params", {}))
                        loaded_tunable = loaded.get("tunable_params", {})
                        if isinstance(loaded_tunable, dict):
                            for k, v in loaded_tunable.items():
                                if isinstance(v, dict) and k in merged_tunable and isinstance(merged_tunable[k], dict):
                                    tmp = dict(merged_tunable[k])
                                    tmp.update(v)
                                    merged_tunable[k] = tmp
                                else:
                                    merged_tunable[k] = v
                        self.config["tunable_params"] = merged_tunable
            except:
                self.config = DEFAULT_CONFIG.copy()
        else:
            self.config = DEFAULT_CONFIG.copy()
        symbol_pool = self.config.get('symbol_pool', self.config.get('symbols', []))
        if not isinstance(symbol_pool, list):
            symbol_pool = self.config.get('smart_symbol_fallback', DEFAULT_CONFIG.get('smart_symbol_fallback', []))
        cleaned_pool = []
        seen = set()
        for sym in symbol_pool:
            if not isinstance(sym, str):
                continue
            s = sym.strip().upper()
            if not s or s == "RNDR/USDT:USDT":
                continue
            if s not in seen:
                cleaned_pool.append(s)
                seen.add(s)
        if not cleaned_pool:
            cleaned_pool = self.config.get('smart_symbol_fallback', DEFAULT_CONFIG.get('smart_symbol_fallback', [
                "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT",
                "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
                "DOT/USDT:USDT", "LINK/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT"
            ]))
        selected_symbols = self.config.get('symbols', cleaned_pool[:])
        if not isinstance(selected_symbols, list):
            selected_symbols = cleaned_pool[:]
        # 不再过滤：允许symbols包含pool外的币种（智能池动态获取的）
        if not selected_symbols:
            selected_symbols = cleaned_pool[:]
        self.available_symbols = cleaned_pool
        self.config['symbol_pool'] = cleaned_pool
        self.config['symbols'] = selected_symbols
        try:
            self.config['leverage'] = max(1, min(10, int(self.config.get('leverage', DEFAULT_CONFIG['leverage']))))
        except Exception:
            self.config['leverage'] = int(DEFAULT_CONFIG['leverage'])
        try:
            self.config['api_retry_count'] = max(1, int(self.config.get('api_retry_count', DEFAULT_CONFIG.get('api_retry_count', 5))))
        except Exception:
            self.config['api_retry_count'] = int(DEFAULT_CONFIG.get('api_retry_count', 5))
        try:
            self.config['api_base_retry_sec'] = max(0.3, float(self.config.get('api_base_retry_sec', DEFAULT_CONFIG.get('api_base_retry_sec', 1.5))))
        except Exception:
            self.config['api_base_retry_sec'] = float(DEFAULT_CONFIG.get('api_base_retry_sec', 1.5))
        try:
            timeout_raw = float(self.config.get('execution_timeout_base_sec', DEFAULT_CONFIG.get('execution_timeout_base_sec', 120)))
            if timeout_raw > 600:
                timeout_raw = timeout_raw / 1000.0
            self.config['execution_timeout_base_sec'] = max(15, min(600, int(timeout_raw)))
        except Exception:
            self.config['execution_timeout_base_sec'] = int(DEFAULT_CONFIG.get('execution_timeout_base_sec', 120))
        try:
            self.config['scheduler_interval_jitter'] = max(0.0, min(0.5, float(self.config.get('scheduler_interval_jitter', DEFAULT_CONFIG.get('scheduler_interval_jitter', 0.2)))))
        except Exception:
            self.config['scheduler_interval_jitter'] = float(DEFAULT_CONFIG.get('scheduler_interval_jitter', 0.2))
        try:
            self.config['scheduler_skip_prob'] = max(0.0, min(0.3, float(self.config.get('scheduler_skip_prob', DEFAULT_CONFIG.get('scheduler_skip_prob', 0.08)))))
        except Exception:
            self.config['scheduler_skip_prob'] = float(DEFAULT_CONFIG.get('scheduler_skip_prob', 0.08))
        try:
            self.config['fill_reentry_jitter_ratio'] = max(0.0, min(0.5, float(self.config.get('fill_reentry_jitter_ratio', DEFAULT_CONFIG.get('fill_reentry_jitter_ratio', 0.2)))))
        except Exception:
            self.config['fill_reentry_jitter_ratio'] = float(DEFAULT_CONFIG.get('fill_reentry_jitter_ratio', 0.2))
        try:
            self.config['execution_timeout_jitter_ratio'] = max(0.0, min(0.5, float(self.config.get('execution_timeout_jitter_ratio', DEFAULT_CONFIG.get('execution_timeout_jitter_ratio', 0.2)))))
        except Exception:
            self.config['execution_timeout_jitter_ratio'] = float(DEFAULT_CONFIG.get('execution_timeout_jitter_ratio', 0.2))
        try:
            self.config['execution_offset_jitter_ratio'] = max(0.0, min(0.4, float(self.config.get('execution_offset_jitter_ratio', DEFAULT_CONFIG.get('execution_offset_jitter_ratio', 0.2)))))
        except Exception:
            self.config['execution_offset_jitter_ratio'] = float(DEFAULT_CONFIG.get('execution_offset_jitter_ratio', 0.2))
        try:
            self.config['execution_price_jitter_ratio'] = max(0.0, min(0.002, float(self.config.get('execution_price_jitter_ratio', DEFAULT_CONFIG.get('execution_price_jitter_ratio', 0.0005)))))
        except Exception:
            self.config['execution_price_jitter_ratio'] = float(DEFAULT_CONFIG.get('execution_price_jitter_ratio', 0.0005))
        try:
            self.config['evolution_interval_jitter'] = max(0.0, min(0.5, float(self.config.get('evolution_interval_jitter', DEFAULT_CONFIG.get('evolution_interval_jitter', 0.3)))))
        except Exception:
            self.config['evolution_interval_jitter'] = float(DEFAULT_CONFIG.get('evolution_interval_jitter', 0.3))
        try:
            self.config['evolution_async_trigger_ratio'] = max(0.4, min(1.0, float(self.config.get('evolution_async_trigger_ratio', DEFAULT_CONFIG.get('evolution_async_trigger_ratio', 0.7)))))
        except Exception:
            self.config['evolution_async_trigger_ratio'] = float(DEFAULT_CONFIG.get('evolution_async_trigger_ratio', 0.7))
        try:
            self.config['random_skip_rate'] = max(0.0, min(0.25, float(self.config.get('random_skip_rate', DEFAULT_CONFIG.get('random_skip_rate', 0.05)))))
        except Exception:
            self.config['random_skip_rate'] = float(DEFAULT_CONFIG.get('random_skip_rate', 0.05))
        try:
            self.config['chaos_injection_rate'] = max(0.01, min(0.3, float(self.config.get('chaos_injection_rate', DEFAULT_CONFIG.get('chaos_injection_rate', 0.1)))))
        except Exception:
            self.config['chaos_injection_rate'] = float(DEFAULT_CONFIG.get('chaos_injection_rate', 0.1))
        try:
            self.config['mode_switch_probability'] = max(0.0, min(0.2, float(self.config.get('mode_switch_probability', DEFAULT_CONFIG.get('mode_switch_probability', 0.02)))))
        except Exception:
            self.config['mode_switch_probability'] = float(DEFAULT_CONFIG.get('mode_switch_probability', 0.02))
        try:
            self.config['max_slippage_alarm'] = max(0.0001, min(0.02, float(self.config.get('max_slippage_alarm', DEFAULT_CONFIG.get('max_slippage_alarm', 0.002)))))
        except Exception:
            self.config['max_slippage_alarm'] = float(DEFAULT_CONFIG.get('max_slippage_alarm', 0.002))
        try:
            self.config['stoploss_hunt_window'] = max(20, min(600, int(self.config.get('stoploss_hunt_window', DEFAULT_CONFIG.get('stoploss_hunt_window', 60)))))
        except Exception:
            self.config['stoploss_hunt_window'] = int(DEFAULT_CONFIG.get('stoploss_hunt_window', 60))
        try:
            self.config['orderbook_confirm_ticks'] = max(1, min(12, int(self.config.get('orderbook_confirm_ticks', DEFAULT_CONFIG.get('orderbook_confirm_ticks', 3)))))
        except Exception:
            self.config['orderbook_confirm_ticks'] = int(DEFAULT_CONFIG.get('orderbook_confirm_ticks', 3))
        try:
            self.config['stop_confirm_window_sec'] = max(0.0, min(30.0, float(self.config.get('stop_confirm_window_sec', DEFAULT_CONFIG.get('stop_confirm_window_sec', 6)))))
        except Exception:
            self.config['stop_confirm_window_sec'] = float(DEFAULT_CONFIG.get('stop_confirm_window_sec', 6))
        try:
            self.config['mode_min_dwell_sec'] = max(60, min(21600, int(self.config.get('mode_min_dwell_sec', DEFAULT_CONFIG.get('mode_min_dwell_sec', 900)))))
        except Exception:
            self.config['mode_min_dwell_sec'] = int(DEFAULT_CONFIG.get('mode_min_dwell_sec', 900))
        try:
            self.config['mode_switch_cooldown_sec'] = max(30, min(7200, int(self.config.get('mode_switch_cooldown_sec', DEFAULT_CONFIG.get('mode_switch_cooldown_sec', 300)))))
        except Exception:
            self.config['mode_switch_cooldown_sec'] = int(DEFAULT_CONFIG.get('mode_switch_cooldown_sec', 300))
        try:
            self.config['recovery_budget_ratio'] = max(0.0, min(0.4, float(self.config.get('recovery_budget_ratio', DEFAULT_CONFIG.get('recovery_budget_ratio', 0.15)))))
        except Exception:
            self.config['recovery_budget_ratio'] = float(DEFAULT_CONFIG.get('recovery_budget_ratio', 0.15))
        try:
            self.config['recovery_daily_target_pct'] = max(0.0, min(0.03, float(self.config.get('recovery_daily_target_pct', DEFAULT_CONFIG.get('recovery_daily_target_pct', 0.004)))))
        except Exception:
            self.config['recovery_daily_target_pct'] = float(DEFAULT_CONFIG.get('recovery_daily_target_pct', 0.004))
        try:
            self.config['recovery_max_boost'] = max(0.0, min(0.8, float(self.config.get('recovery_max_boost', DEFAULT_CONFIG.get('recovery_max_boost', 0.45)))))
        except Exception:
            self.config['recovery_max_boost'] = float(DEFAULT_CONFIG.get('recovery_max_boost', 0.45))
        API_RUNTIME_SETTINGS["api_retry_count"] = self.config['api_retry_count']
        API_RUNTIME_SETTINGS["api_base_retry_sec"] = self.config['api_base_retry_sec']

    def save_config(self):
        global API_RUNTIME_SETTINGS
        self.config['api_key'] = self.ent_key.get().strip()
        self.config['api_secret'] = self.ent_sec.get().strip()
        self.config['proxy_host'] = self.ent_proxy_host.get().strip()
        try:
            self.config['proxy_port'] = int(self.ent_proxy_port.get().strip())
        except:
            self.config['proxy_port'] = 10808
        try:
            self.config['margin_usdt'] = max(0.1, float(self.ent_margin.get().strip()))
        except:
            self.config['margin_usdt'] = float(DEFAULT_CONFIG['margin_usdt'])
        try:
            self.config['leverage'] = max(1, min(10, int(self.ent_lev.get().strip())))
        except:
            self.config['leverage'] = int(DEFAULT_CONFIG['leverage'])
        try:
            self.config['max_active_symbols'] = max(1, int(self.ent_max_hold.get().strip()))
        except:
            self.config['max_active_symbols'] = int(DEFAULT_CONFIG.get('max_active_symbols', 2))
        try:
            self.config['max_portfolio_var'] = float(DEFAULT_CONFIG.get('max_portfolio_var', 0.04))
        except:
            pass
        try:
            self.config['funding_rate_threshold'] = float(DEFAULT_CONFIG.get('funding_rate_threshold', 0.00025))
        except:
            pass
        try:
            self.config['style_var_scale'] = float(DEFAULT_CONFIG.get('style_var_scale', 1.8))
        except:
            pass
        try:
            self.config['runtime_target_orders_per_hour'] = float(DEFAULT_CONFIG.get('runtime_target_orders_per_hour', 4.5))
        except:
            pass
        try:
            self.config['execution_market_confidence'] = float(DEFAULT_CONFIG.get('execution_market_confidence', 0.86))
        except:
            pass
        try:
            self.config['execution_aggressive_confidence'] = float(DEFAULT_CONFIG.get('execution_aggressive_confidence', 0.67))
        except:
            pass
        try:
            self.config['random_skip_rate'] = float(DEFAULT_CONFIG.get('random_skip_rate', 0.03))
        except:
            pass
        try:
            self.config['scheduler_skip_prob'] = float(DEFAULT_CONFIG.get('scheduler_skip_prob', 0.03))
        except:
            pass
        try:
            self.config['max_slippage_alarm'] = float(DEFAULT_CONFIG.get('max_slippage_alarm', 0.0018))
        except:
            pass
        try:
            self.config['recovery_budget_ratio'] = float(DEFAULT_CONFIG.get('recovery_budget_ratio', 0.15))
        except:
            pass
        try:
            self.config['recovery_daily_target_pct'] = float(DEFAULT_CONFIG.get('recovery_daily_target_pct', 0.004))
        except:
            pass
        try:
            self.config['recovery_max_boost'] = float(DEFAULT_CONFIG.get('recovery_max_boost', 0.45))
        except:
            pass
        try:
            self.config['orderbook_confirm_ticks'] = int(DEFAULT_CONFIG.get('orderbook_confirm_ticks', 3))
        except:
            pass
        try:
            self.config['stop_confirm_window_sec'] = float(DEFAULT_CONFIG.get('stop_confirm_window_sec', 6))
        except:
            pass
        try:
            self.config['mode_min_dwell_sec'] = int(DEFAULT_CONFIG.get('mode_min_dwell_sec', 900))
        except:
            pass
        style_choice = self.cmb_style.get().strip()
        if style_choice in ["自动", "保守", "均衡", "激进"]:
            self.config['strategy_style'] = style_choice
        else:
            self.config['strategy_style'] = "自动"

        op_mode_choice = self.cmb_op_mode.get().strip()
        if op_mode_choice in ["自动", "生存优先", "效率优先"]:
            self.config['operation_mode'] = op_mode_choice
        else:
            self.config['operation_mode'] = "自动"

        self.config['evolution_enabled'] = bool(self.var_evolution.get())
        # 新增：智能引擎开关
        if hasattr(self, 'var_smart_symbol_enabled'):
            self.config['smart_symbol_enabled'] = bool(self.var_smart_symbol_enabled.get())
        if hasattr(self, 'var_adaptive_entry_enabled'):
            self.config['adaptive_entry_enabled'] = bool(self.var_adaptive_entry_enabled.get())
        if hasattr(self, 'var_causal_enabled'):
            self.config['causal_enabled'] = bool(self.var_causal_enabled.get())
        try:
            self.config['evolution_population_size'] = max(4, int(self.ent_evo_pop.get().strip()))
        except:
            self.config['evolution_population_size'] = int(DEFAULT_CONFIG.get('evolution_population_size', 10))
        try:
            self.config['evolution_mutation_rate'] = max(0.01, min(0.8, float(self.ent_evo_mut.get().strip())))
        except:
            self.config['evolution_mutation_rate'] = float(DEFAULT_CONFIG.get('evolution_mutation_rate', 0.18))
        try:
            self.config['evolution_interval_hours'] = max(4, int(self.ent_evo_interval.get().strip()))
        except:
            self.config['evolution_interval_hours'] = int(DEFAULT_CONFIG.get('evolution_interval_hours', 24))
        selected = self.get_selected_symbols_from_ui()
        if selected:
            self.config['symbols'] = selected
        else:
            self.config['symbols'] = self.available_symbols[:]
        self.config['symbol_pool'] = self.available_symbols[:]
        try:
            interval_sec = int(float(self.cmb_rotate_seconds.get().strip()))
        except:
            interval_sec = int(self.config.get('global_order_interval', 600))
        interval_sec = max(10, min(86400, interval_sec))
        self.config['global_order_interval'] = int(interval_sec)
        try:
            self.config['api_retry_count'] = max(1, int(self.config.get('api_retry_count', DEFAULT_CONFIG.get('api_retry_count', 5))))
        except Exception:
            self.config['api_retry_count'] = int(DEFAULT_CONFIG.get('api_retry_count', 5))
        try:
            self.config['api_base_retry_sec'] = max(0.3, float(self.config.get('api_base_retry_sec', DEFAULT_CONFIG.get('api_base_retry_sec', 1.5))))
        except Exception:
            self.config['api_base_retry_sec'] = float(DEFAULT_CONFIG.get('api_base_retry_sec', 1.5))
        try:
            timeout_raw = float(self.config.get('execution_timeout_base_sec', DEFAULT_CONFIG.get('execution_timeout_base_sec', 120)))
            if timeout_raw > 600:
                timeout_raw = timeout_raw / 1000.0
            self.config['execution_timeout_base_sec'] = max(15, min(600, int(timeout_raw)))
        except Exception:
            self.config['execution_timeout_base_sec'] = int(DEFAULT_CONFIG.get('execution_timeout_base_sec', 120))
        try:
            self.config['scheduler_interval_jitter'] = max(0.0, min(0.5, float(self.config.get('scheduler_interval_jitter', DEFAULT_CONFIG.get('scheduler_interval_jitter', 0.2)))))
        except Exception:
            self.config['scheduler_interval_jitter'] = float(DEFAULT_CONFIG.get('scheduler_interval_jitter', 0.2))
        try:
            self.config['scheduler_skip_prob'] = max(0.0, min(0.3, float(self.config.get('scheduler_skip_prob', DEFAULT_CONFIG.get('scheduler_skip_prob', 0.08)))))
        except Exception:
            self.config['scheduler_skip_prob'] = float(DEFAULT_CONFIG.get('scheduler_skip_prob', 0.08))
        try:
            self.config['fill_reentry_jitter_ratio'] = max(0.0, min(0.5, float(self.config.get('fill_reentry_jitter_ratio', DEFAULT_CONFIG.get('fill_reentry_jitter_ratio', 0.2)))))
        except Exception:
            self.config['fill_reentry_jitter_ratio'] = float(DEFAULT_CONFIG.get('fill_reentry_jitter_ratio', 0.2))
        try:
            self.config['execution_timeout_jitter_ratio'] = max(0.0, min(0.5, float(self.config.get('execution_timeout_jitter_ratio', DEFAULT_CONFIG.get('execution_timeout_jitter_ratio', 0.2)))))
        except Exception:
            self.config['execution_timeout_jitter_ratio'] = float(DEFAULT_CONFIG.get('execution_timeout_jitter_ratio', 0.2))
        try:
            self.config['execution_offset_jitter_ratio'] = max(0.0, min(0.4, float(self.config.get('execution_offset_jitter_ratio', DEFAULT_CONFIG.get('execution_offset_jitter_ratio', 0.2)))))
        except Exception:
            self.config['execution_offset_jitter_ratio'] = float(DEFAULT_CONFIG.get('execution_offset_jitter_ratio', 0.2))
        try:
            self.config['execution_price_jitter_ratio'] = max(0.0, min(0.002, float(self.config.get('execution_price_jitter_ratio', DEFAULT_CONFIG.get('execution_price_jitter_ratio', 0.0005)))))
        except Exception:
            self.config['execution_price_jitter_ratio'] = float(DEFAULT_CONFIG.get('execution_price_jitter_ratio', 0.0005))
        try:
            self.config['evolution_interval_jitter'] = max(0.0, min(0.5, float(self.config.get('evolution_interval_jitter', DEFAULT_CONFIG.get('evolution_interval_jitter', 0.3)))))
        except Exception:
            self.config['evolution_interval_jitter'] = float(DEFAULT_CONFIG.get('evolution_interval_jitter', 0.3))
        try:
            self.config['evolution_async_trigger_ratio'] = max(0.4, min(1.0, float(self.config.get('evolution_async_trigger_ratio', DEFAULT_CONFIG.get('evolution_async_trigger_ratio', 0.7)))))
        except Exception:
            self.config['evolution_async_trigger_ratio'] = float(DEFAULT_CONFIG.get('evolution_async_trigger_ratio', 0.7))
        try:
            self.config['random_skip_rate'] = max(0.0, min(0.25, float(self.config.get('random_skip_rate', DEFAULT_CONFIG.get('random_skip_rate', 0.05)))))
        except Exception:
            self.config['random_skip_rate'] = float(DEFAULT_CONFIG.get('random_skip_rate', 0.05))
        try:
            self.config['chaos_injection_rate'] = max(0.01, min(0.3, float(self.config.get('chaos_injection_rate', DEFAULT_CONFIG.get('chaos_injection_rate', 0.1)))))
        except Exception:
            self.config['chaos_injection_rate'] = float(DEFAULT_CONFIG.get('chaos_injection_rate', 0.1))
        try:
            self.config['mode_switch_probability'] = max(0.0, min(0.2, float(self.config.get('mode_switch_probability', DEFAULT_CONFIG.get('mode_switch_probability', 0.02)))))
        except Exception:
            self.config['mode_switch_probability'] = float(DEFAULT_CONFIG.get('mode_switch_probability', 0.02))
        try:
            self.config['max_slippage_alarm'] = max(0.0001, min(0.02, float(self.config.get('max_slippage_alarm', DEFAULT_CONFIG.get('max_slippage_alarm', 0.002)))))
        except Exception:
            self.config['max_slippage_alarm'] = float(DEFAULT_CONFIG.get('max_slippage_alarm', 0.002))
        try:
            self.config['stoploss_hunt_window'] = max(20, min(600, int(self.config.get('stoploss_hunt_window', DEFAULT_CONFIG.get('stoploss_hunt_window', 60)))))
        except Exception:
            self.config['stoploss_hunt_window'] = int(DEFAULT_CONFIG.get('stoploss_hunt_window', 60))
        try:
            self.config['orderbook_confirm_ticks'] = max(1, min(12, int(self.config.get('orderbook_confirm_ticks', DEFAULT_CONFIG.get('orderbook_confirm_ticks', 3)))))
        except Exception:
            self.config['orderbook_confirm_ticks'] = int(DEFAULT_CONFIG.get('orderbook_confirm_ticks', 3))
        try:
            self.config['stop_confirm_window_sec'] = max(0.0, min(30.0, float(self.config.get('stop_confirm_window_sec', DEFAULT_CONFIG.get('stop_confirm_window_sec', 6)))))
        except Exception:
            self.config['stop_confirm_window_sec'] = float(DEFAULT_CONFIG.get('stop_confirm_window_sec', 6))
        try:
            self.config['mode_min_dwell_sec'] = max(60, min(21600, int(self.config.get('mode_min_dwell_sec', DEFAULT_CONFIG.get('mode_min_dwell_sec', 900)))))
        except Exception:
            self.config['mode_min_dwell_sec'] = int(DEFAULT_CONFIG.get('mode_min_dwell_sec', 900))
        try:
            self.config['mode_switch_cooldown_sec'] = max(30, min(7200, int(self.config.get('mode_switch_cooldown_sec', DEFAULT_CONFIG.get('mode_switch_cooldown_sec', 300)))))
        except Exception:
            self.config['mode_switch_cooldown_sec'] = int(DEFAULT_CONFIG.get('mode_switch_cooldown_sec', 300))
        try:
            self.config['recovery_budget_ratio'] = max(0.0, min(0.4, float(self.config.get('recovery_budget_ratio', DEFAULT_CONFIG.get('recovery_budget_ratio', 0.15)))))
        except Exception:
            self.config['recovery_budget_ratio'] = float(DEFAULT_CONFIG.get('recovery_budget_ratio', 0.15))
        try:
            self.config['recovery_daily_target_pct'] = max(0.0, min(0.03, float(self.config.get('recovery_daily_target_pct', DEFAULT_CONFIG.get('recovery_daily_target_pct', 0.004)))))
        except Exception:
            self.config['recovery_daily_target_pct'] = float(DEFAULT_CONFIG.get('recovery_daily_target_pct', 0.004))
        try:
            self.config['recovery_max_boost'] = max(0.0, min(0.8, float(self.config.get('recovery_max_boost', DEFAULT_CONFIG.get('recovery_max_boost', 0.45)))))
        except Exception:
            self.config['recovery_max_boost'] = float(DEFAULT_CONFIG.get('recovery_max_boost', 0.45))
        API_RUNTIME_SETTINGS["api_retry_count"] = self.config['api_retry_count']
        API_RUNTIME_SETTINGS["api_base_retry_sec"] = self.config['api_base_retry_sec']
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            self.log(f"保存配置失败: {e}", "ERROR")

    def setup_ui(self):
        BG = self.colors["bg"]
        PANEL = self.colors["panel"]
        FG = self.colors["fg"]
        ACCENT = self.colors["accent"]
        DANGER = self.colors["danger"]
        SUCCESS = self.colors["success"]
        WARNING = self.colors["warning"]
        BORDER = "#2a3050"

        style = ttk.Style()
        style.theme_use('clam')
        style.configure(".", background=BG, foreground=FG, font=("Consolas", 9))
        style.configure("TNotebook", background=BG, bordercolor=BORDER, tabmargins=[2,2,0,0])
        style.configure("TNotebook.Tab", background=PANEL, foreground=FG, padding=[14,5], font=("Consolas", 9, "bold"), bordercolor=BORDER)
        style.map("TNotebook.Tab", background=[("selected","#1e2d50"),("active","#1a2845")], foreground=[("selected",ACCENT),("active",FG)])
        style.configure("TFrame", background=BG)
        style.configure("TLabel", background=BG, foreground=FG, font=("Consolas", 9))
        style.configure("TEntry", fieldbackground="#0f1a30", foreground="#f5f5f5", insertcolor=ACCENT, bordercolor=BORDER, font=("Consolas", 9))
        style.configure("TCombobox", fieldbackground="#0f1a30", foreground="#f5f5f5", selectbackground="#0f1a30", selectforeground=ACCENT, background=PANEL, arrowcolor=ACCENT, bordercolor=BORDER, font=("Consolas", 9))
        style.map("TCombobox", fieldbackground=[("readonly","#0f1a30")], foreground=[("readonly","#f5f5f5")])
        style.configure("TCheckbutton", background=BG, foreground=FG, font=("Consolas", 9))
        style.configure("TLabelframe", background=BG, foreground=ACCENT, bordercolor=BORDER, borderwidth=1, padding=8)
        style.configure("TLabelframe.Label", background=BG, foreground=ACCENT, font=("Consolas", 10, "bold"))
        style.configure("TButton", background=PANEL, foreground=FG, borderwidth=1, bordercolor=ACCENT, focuscolor=ACCENT, padding=(10,5), font=("Consolas", 9, "bold"))
        style.map("TButton", background=[("active","#2a3a5a")], foreground=[("active",ACCENT)])
        style.configure("Start.TButton", background="#1a7a3a", foreground="#ffffff", padding=(12,6), font=("Consolas", 12, "bold"))
        style.map("Start.TButton", background=[("active",SUCCESS)])
        style.configure("Stop.TButton", background="#8a2020", foreground="#ffffff", padding=(12,6), font=("Consolas", 12, "bold"))
        style.map("Stop.TButton", background=[("active",DANGER)])
        style.configure("Treeview", background=PANEL, fieldbackground=PANEL, foreground=FG, rowheight=28, borderwidth=0)
        style.map("Treeview", background=[('selected','#2a3a5a')], foreground=[('selected',ACCENT)])
        style.configure("Treeview.Heading", background=BG, foreground=ACCENT, font=("Consolas", 9, "bold"), borderwidth=1)

        # ====== 顶部标题栏 ======
        header = tk.Frame(self.root, bg="#0f1528", height=55)
        header.pack(fill="x")
        header.pack_propagate(False)
        tk.Label(header, text="🔥 PhoenixQ V1.2.0 // 凤凰量化交易系统", font=("Consolas", 15, "bold"), bg="#0f1528", fg=ACCENT).pack(side="left", padx=16)
        self.lbl_status = tk.Label(header, text="SYSTEM READY", font=("Consolas", 11, "bold"), bg="#0f1528", fg=SUCCESS)
        self.lbl_status.pack(side="right", padx=16)
        self.lbl_health = tk.Label(header, text="WARN:0 ERR:0", font=("Consolas", 10), bg="#0f1528", fg=WARNING)
        self.lbl_health.pack(side="right", padx=16)
        self.lbl_balance = tk.Label(header, text="余额: --- USDT", font=("Consolas", 10, "bold"), bg="#0f1528", fg=SUCCESS)
        self.lbl_balance.pack(side="right", padx=16)
        tk.Frame(self.root, bg=ACCENT, height=2).pack(fill="x")

        # ====== 启动/停止/更新按钮栏 ======
        cmd_bar = tk.Frame(self.root, bg=BG)
        cmd_bar.pack(fill="x", pady=4, padx=8)
        self.btn_start = ttk.Button(cmd_bar, text="▶ 启动引擎", command=self.start, style="Start.TButton")
        self.btn_start.pack(side="left", padx=6, ipady=4)
        self.btn_stop = ttk.Button(cmd_bar, text="⏹ 紧急熔断", command=self.stop, style="Stop.TButton")
        self.btn_stop.pack(side="left", padx=6, ipady=4)
        self.btn_stop["state"] = "disabled"
        self.btn_save_config = ttk.Button(cmd_bar, text="💾 保存配置", command=self._gui_save_config)
        self.btn_save_config.pack(side="left", padx=6, ipady=4)
        self.btn_update = ttk.Button(cmd_bar, text="🔄 一键更新", command=self._auto_update)
        self.btn_update.pack(side="right", padx=6, ipady=4)
        self.btn_upload_log = ttk.Button(cmd_bar, text="📤 上传日志", command=self._upload_log)
        self.btn_upload_log.pack(side="right", padx=6, ipady=4)
        self.lbl_version = tk.Label(cmd_bar, text="", bg=BG, fg="#7a7ea0", font=("Consolas", 8))
        self.lbl_version.pack(side="right", padx=4)
        tk.Frame(self.root, bg=BORDER, height=1).pack(fill="x")
        # ====== Notebook 主体 ======
        nb = ttk.Notebook(self.root)
        nb.pack(fill="both", expand=True, padx=6, pady=4)

        # ========== Tab1: 交易参数 ==========
        tab1 = tk.Frame(nb, bg=BG)
        nb.add(tab1, text=" 📊 交易参数 ")
        t1_canvas = tk.Canvas(tab1, bg=BG, highlightthickness=0)
        t1_sb = ttk.Scrollbar(tab1, orient="vertical", command=t1_canvas.yview)
        t1_inner = tk.Frame(t1_canvas, bg=BG)
        t1_canvas.create_window((0,0), window=t1_inner, anchor="nw", tags="inner")
        t1_inner.bind("<Configure>", lambda e: t1_canvas.configure(scrollregion=t1_canvas.bbox("all")))
        t1_canvas.bind("<Configure>", lambda e: t1_canvas.itemconfig("inner", width=e.width))
        t1_canvas.configure(yscrollcommand=t1_sb.set)
        t1_canvas.pack(side="left", fill="both", expand=True)
        t1_sb.pack(side="right", fill="y")

        # API配置
        fr_api = ttk.LabelFrame(t1_inner, text="API 配置")
        fr_api.pack(fill="x", pady=5, padx=8)
        self.ent_key = self.create_entry(fr_api, "API Key:", self.config.get('api_key',''), show="*")
        self.ent_sec = self.create_entry(fr_api, "Secret:", self.config.get('api_secret',''), show="*")

        # 代理配置
        fr_proxy = ttk.LabelFrame(t1_inner, text="代理配置")
        fr_proxy.pack(fill="x", pady=5, padx=8)
        self.ent_proxy_host = self.create_entry(fr_proxy, "代理IP:", str(self.config.get('proxy_host','127.0.0.1')))
        self.ent_proxy_port = self.create_entry(fr_proxy, "代理端口:", str(self.config.get('proxy_port',10808)))

        # 基础交易参数
        fr_base = ttk.LabelFrame(t1_inner, text="基础交易参数")
        fr_base.pack(fill="x", pady=5, padx=8)
        self.ent_margin = self.create_entry(fr_base, "总仓位额度(U):", str(self.config.get('margin_usdt',10)))
        self.ent_lev = self.create_entry(fr_base, "杠杆倍数:", str(self.config.get('leverage',3)))
        self.ent_max_hold = self.create_entry(fr_base, "持币数量上限:", str(self.config.get('max_active_symbols',2)))

        # 运行模式
        fr_mode = ttk.LabelFrame(t1_inner, text="运行模式")
        fr_mode.pack(fill="x", pady=5, padx=8)
        mode_row1 = tk.Frame(fr_mode, bg=BG)
        mode_row1.pack(fill="x", padx=5, pady=3)
        tk.Label(mode_row1, text="运行模式:", width=13, anchor="w", bg=BG, fg=FG).pack(side="left")
        self.cmb_op_mode = ttk.Combobox(mode_row1, state="readonly", values=["自动","生存优先","效率优先"], width=12)
        op_mode_default = str(self.config.get('operation_mode','自动'))
        if op_mode_default not in ["自动","生存优先","效率优先"]: op_mode_default = "自动"
        self.cmb_op_mode.set(op_mode_default)
        self.cmb_op_mode.pack(side="right", fill="x", expand=True)

        mode_row2 = tk.Frame(fr_mode, bg=BG)
        mode_row2.pack(fill="x", padx=5, pady=3)
        tk.Label(mode_row2, text="策略风格:", width=13, anchor="w", bg=BG, fg=FG).pack(side="left")
        self.cmb_style = ttk.Combobox(mode_row2, state="readonly", values=["自动","保守","均衡","激进"], width=12)
        style_default = str(self.config.get('strategy_style','自动'))
        if style_default not in ["自动","保守","均衡","激进"]: style_default = "自动"
        self.cmb_style.set(style_default)
        self.cmb_style.pack(side="right", fill="x", expand=True)

        mode_row3 = tk.Frame(fr_mode, bg=BG)
        mode_row3.pack(fill="x", padx=5, pady=3)
        tk.Label(mode_row3, text="防并发节流(秒):", width=13, anchor="w", bg=BG, fg=FG).pack(side="left")
        self.cmb_rotate_seconds = ttk.Combobox(mode_row3, state="readonly", values=["10","30","60","120","300"], width=12)
        rotate_default = str(int(self.config.get('global_order_interval',60)))
        if rotate_default not in ["10","30","60","120","300"]: rotate_default = "60"
        self.cmb_rotate_seconds.set(rotate_default)
        self.cmb_rotate_seconds.pack(side="right", fill="x", expand=True)

        # 进化引擎
        fr_evo = ttk.LabelFrame(t1_inner, text="进化引擎 (第19层)")
        fr_evo.pack(fill="x", pady=5, padx=8)
        evo_sw = tk.Frame(fr_evo, bg=BG)
        evo_sw.pack(fill="x", padx=5, pady=3)
        self.var_evolution = tk.BooleanVar(value=bool(self.config.get('evolution_enabled', True)))
        tk.Checkbutton(evo_sw, text="启用进化引擎", variable=self.var_evolution, bg=BG, fg=FG, selectcolor="#0f1a30", activebackground=BG, font=("Consolas",9)).pack(side="left")
        self.ent_evo_pop = self.create_entry(fr_evo, "进化种群数:", str(self.config.get('evolution_population_size',10)))
        self.ent_evo_mut = self.create_entry(fr_evo, "变异率:", str(self.config.get('evolution_mutation_rate',0.18)))
        self.ent_evo_interval = self.create_entry(fr_evo, "进化间隔(h):", str(self.config.get('evolution_interval_hours',24)))

        # 币种选择（隐藏，由智能池自动管理）
        self.fr_sym = tk.Frame(t1_inner, bg=BG)  # 隐藏的容器，不pack
        self.sym_list_frame = tk.Frame(self.fr_sym, bg=BG)
        self.sym_vars = {}
        self.ent_symbol_input = ttk.Entry(self.fr_sym)  # 保留引用防报错
        self.refresh_symbol_checkboxes()
        # ========== Tab2: 智能引擎 ==========
        tab2 = tk.Frame(nb, bg=BG)
        nb.add(tab2, text=" 🧠 智能引擎 ")

        # 智能币种池
        fr_pool = ttk.LabelFrame(tab2, text="智能币种池")
        fr_pool.pack(fill="x", pady=8, padx=8)
        pool_sw = tk.Frame(fr_pool, bg=BG)
        pool_sw.pack(fill="x", padx=5, pady=3)
        self.var_smart_symbol_enabled = tk.BooleanVar(value=bool(self.config.get('smart_symbol_enabled', True)))
        tk.Checkbutton(pool_sw, text="启用智能币种池（自动获取成交量前50）", variable=self.var_smart_symbol_enabled, bg=BG, fg=FG, selectcolor="#0f1a30", activebackground=BG, font=("Consolas",9)).pack(side="left")
        pool_info = tk.Frame(fr_pool, bg=BG)
        pool_info.pack(fill="x", padx=5, pady=3)
        tk.Label(pool_info, text="当前池:", bg=BG, fg=FG, font=("Consolas",9)).pack(side="left")
        self.lbl_smart_pool_count = tk.Label(pool_info, text=f"{len(self.available_symbols)} 个币种", bg=BG, fg=SUCCESS, font=("Consolas",10,"bold"))
        self.lbl_smart_pool_count.pack(side="left", padx=8)
        self.btn_refresh_pool = ttk.Button(pool_info, text="🔄 手动刷新", command=self._gui_refresh_pool)
        self.btn_refresh_pool.pack(side="right", padx=5)
        pool_note = tk.Label(fr_pool, text="关闭后使用固定回退列表 | 开启后每4小时自动刷新", bg=BG, fg="#6a7090", font=("Consolas",8))
        pool_note.pack(anchor="w", padx=10, pady=(0,5))

        # 自适应入场引擎
        fr_entry = ttk.LabelFrame(tab2, text="自适应入场引擎 (MAB强化学习)")
        fr_entry.pack(fill="x", pady=8, padx=8)
        entry_sw = tk.Frame(fr_entry, bg=BG)
        entry_sw.pack(fill="x", padx=5, pady=3)
        self.var_adaptive_entry_enabled = tk.BooleanVar(value=bool(self.config.get('adaptive_entry_enabled', True)))
        tk.Checkbutton(entry_sw, text="启用自适应入场（根据市场状态智能选择入场方式）", variable=self.var_adaptive_entry_enabled, bg=BG, fg=FG, selectcolor="#0f1a30", activebackground=BG, font=("Consolas",9)).pack(side="left")
        entry_info = tk.Frame(fr_entry, bg=BG)
        entry_info.pack(fill="x", padx=5, pady=3)
        tk.Label(entry_info, text="4种模式: 市价即入 | ATR回撤限价 | 支撑阻力限价 | 分批蜡烛确认", bg=BG, fg="#7a7ea0", font=("Consolas",8)).pack(anchor="w")
        mab_frame = tk.Frame(fr_entry, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        mab_frame.pack(fill="x", padx=8, pady=5)
        tk.Label(mab_frame, text="MAB 学习统计", bg=PANEL, fg=ACCENT, font=("Consolas",9,"bold")).pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_mab_stats = tk.Label(mab_frame, text="市价即入: 0次 | ATR回撤: 0次 | 支撑阻力: 0次 | 分批确认: 0次", bg=PANEL, fg=FG, font=("Consolas",9), anchor="w", justify="left")
        self.lbl_mab_stats.pack(fill="x", padx=8, pady=(0,5))

        # 因果推理引擎
        fr_causal = ttk.LabelFrame(tab2, text="因果推理引擎 (第10层)")
        fr_causal.pack(fill="x", pady=8, padx=8)
        causal_sw = tk.Frame(fr_causal, bg=BG)
        causal_sw.pack(fill="x", padx=5, pady=3)
        self.var_causal_enabled = tk.BooleanVar(value=bool(self.config.get('causal_enabled', False)))
        tk.Checkbutton(causal_sw, text="启用因果推理门控（关闭后跳过因果效应检查）", variable=self.var_causal_enabled, bg=BG, fg=FG, selectcolor="#0f1a30", activebackground=BG, font=("Consolas",9)).pack(side="left")
        causal_info = tk.Frame(fr_causal, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        causal_info.pack(fill="x", padx=8, pady=5)
        tk.Label(causal_info, text="当前因果阈值", bg=PANEL, fg=ACCENT, font=("Consolas",9,"bold")).pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_causal_threshold = tk.Label(causal_info, text="基础: -0.01 | 动态: 待启动后更新", bg=PANEL, fg=FG, font=("Consolas",9))
        self.lbl_causal_threshold.pack(fill="x", padx=8, pady=(0,5))
        causal_note = tk.Label(fr_causal, text="阈值已从0.05降至-0.01，允许轻微负因果也能开单", bg=BG, fg="#6a7090", font=("Consolas",8))
        causal_note.pack(anchor="w", padx=10, pady=(0,5))
        # ========== Tab3: 学习进度 ==========
        tab3 = tk.Frame(nb, bg=BG)
        nb.add(tab3, text=" 📊 学习进度 ")
        t3_canvas = tk.Canvas(tab3, bg=BG, highlightthickness=0)
        t3_sb = ttk.Scrollbar(tab3, orient="vertical", command=t3_canvas.yview)
        t3_inner = tk.Frame(t3_canvas, bg=BG)
        t3_canvas.create_window((0,0), window=t3_inner, anchor="nw", tags="t3inner")
        t3_inner.bind("<Configure>", lambda e: t3_canvas.configure(scrollregion=t3_canvas.bbox("all")))
        t3_canvas.bind("<Configure>", lambda e: t3_canvas.itemconfig("t3inner", width=e.width))
        t3_canvas.configure(yscrollcommand=t3_sb.set)
        t3_canvas.pack(side="left", fill="both", expand=True)
        t3_sb.pack(side="right", fill="y")

        # ---- MAB入场学习 ----
        fr_mab_progress = ttk.LabelFrame(t3_inner, text="🎰 MAB入场引擎 (强化学习)")
        fr_mab_progress.pack(fill="x", pady=5, padx=8)
        mab_inner = tk.Frame(fr_mab_progress, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        mab_inner.pack(fill="x", padx=5, pady=5)
        self.lbl_mab_total = tk.Label(mab_inner, text="总样本: 0/100单  [阶段: 探索期]", bg=PANEL, fg=ACCENT, font=("Consolas", 10, "bold"))
        self.lbl_mab_total.pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_mab_mode0 = tk.Label(mab_inner, text="  市价即入:    0次 | 奖励: 0.00 | UCB: 0.00", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_mab_mode0.pack(anchor="w", padx=8)
        self.lbl_mab_mode1 = tk.Label(mab_inner, text="  ATR回撤:    0次 | 奖励: 0.00 | UCB: 0.00", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_mab_mode1.pack(anchor="w", padx=8)
        self.lbl_mab_mode2 = tk.Label(mab_inner, text="  支撑阻力:   0次 | 奖励: 0.00 | UCB: 0.00", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_mab_mode2.pack(anchor="w", padx=8)
        self.lbl_mab_mode3 = tk.Label(mab_inner, text="  分批确认:   0次 | 奖励: 0.00 | UCB: 0.00", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_mab_mode3.pack(anchor="w", padx=8, pady=(0,5))

        # ---- ML预测器 ----
        fr_ml_progress = ttk.LabelFrame(t3_inner, text="🧠 ML预测器 (12维在线学习)")
        fr_ml_progress.pack(fill="x", pady=5, padx=8)
        ml_inner = tk.Frame(fr_ml_progress, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        ml_inner.pack(fill="x", padx=5, pady=5)
        self.lbl_ml_accuracy = tk.Label(ml_inner, text="准确率: --% | 样本: 0/100 | 阶段: 未启动", bg=PANEL, fg=ACCENT, font=("Consolas", 10, "bold"))
        self.lbl_ml_accuracy.pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_ml_weights = tk.Label(ml_inner, text="  特征权重: 等待学习数据...", bg=PANEL, fg=FG, font=("Consolas", 9), wraplength=600, justify="left")
        self.lbl_ml_weights.pack(anchor="w", padx=8, pady=(0,5))

        # ---- 因果引擎 ----
        fr_causal_progress = ttk.LabelFrame(t3_inner, text="⚖️ 因果推理引擎")
        fr_causal_progress.pack(fill="x", pady=5, padx=8)
        causal_p_inner = tk.Frame(fr_causal_progress, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        causal_p_inner.pack(fill="x", padx=5, pady=5)
        self.lbl_causal_stats = tk.Label(causal_p_inner, text="通过率: --% | 通过: 0 | 拦截: 0 | 当前阈值: -0.01", bg=PANEL, fg=ACCENT, font=("Consolas", 10, "bold"))
        self.lbl_causal_stats.pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_causal_recent = tk.Label(causal_p_inner, text="  最近因果值: 等待数据...", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_causal_recent.pack(anchor="w", padx=8, pady=(0,5))

        # ---- 贝叶斯融合 ----
        fr_bayes_progress = ttk.LabelFrame(t3_inner, text="🔮 贝叶斯策略融合")
        fr_bayes_progress.pack(fill="x", pady=5, padx=8)
        bayes_inner = tk.Frame(fr_bayes_progress, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        bayes_inner.pack(fill="x", padx=5, pady=5)
        self.lbl_bayes_weights = tk.Label(bayes_inner, text="趋势: 30% | ML: 25% | 订单簿: 25% | 情绪: 20%", bg=PANEL, fg=ACCENT, font=("Consolas", 10, "bold"))
        self.lbl_bayes_weights.pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_bayes_samples = tk.Label(bayes_inner, text="  融合样本: 0 | 阶段: 初始化", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_bayes_samples.pack(anchor="w", padx=8, pady=(0,5))

        # ---- 进化引擎 ----
        fr_evo_progress = ttk.LabelFrame(t3_inner, text="🧬 进化引擎 (遗传算法)")
        fr_evo_progress.pack(fill="x", pady=5, padx=8)
        evo_inner = tk.Frame(fr_evo_progress, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        evo_inner.pack(fill="x", padx=5, pady=5)
        self.lbl_evo_gen = tk.Label(evo_inner, text="当前代数: 0 | 最优适应度: -- | 下次进化: --", bg=PANEL, fg=ACCENT, font=("Consolas", 10, "bold"))
        self.lbl_evo_gen.pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_evo_params = tk.Label(evo_inner, text="  最优参数: 等待进化...", bg=PANEL, fg=FG, font=("Consolas", 9), wraplength=600, justify="left")
        self.lbl_evo_params.pack(anchor="w", padx=8, pady=(0,5))

        # ---- 交易统计 ----
        fr_trade_stats = ttk.LabelFrame(t3_inner, text="📈 实时交易统计")
        fr_trade_stats.pack(fill="x", pady=5, padx=8)
        stats_inner = tk.Frame(fr_trade_stats, bg=PANEL, highlightthickness=1, highlightbackground=BORDER)
        stats_inner.pack(fill="x", padx=5, pady=5)
        self.lbl_trade_total = tk.Label(stats_inner, text="总交易: 0 | 盈利: 0 | 亏损: 0 | 胜率: --%", bg=PANEL, fg=ACCENT, font=("Consolas", 10, "bold"))
        self.lbl_trade_total.pack(anchor="w", padx=8, pady=(5,2))
        self.lbl_trade_exits = tk.Label(stats_inner, text="  止盈:0 | trail:0 | ML退出:0 | 保本:0 | 止损:0 | 超时:0", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_trade_exits.pack(anchor="w", padx=8)
        self.lbl_trade_pnl = tk.Label(stats_inner, text="  累计盈亏: 0.00 USDT | 今日: 0.00 USDT", bg=PANEL, fg=FG, font=("Consolas", 9))
        self.lbl_trade_pnl.pack(anchor="w", padx=8, pady=(0,5))
        # ========== Tab4: 监控日志 ==========
        tab4 = tk.Frame(nb, bg=BG)
        nb.add(tab4, text=" 📈 监控日志 ")

        # 日志区
        log_top = tk.Frame(tab4, bg=BG)
        log_top.pack(fill="x", padx=4, pady=(4,0))
        self.var_log_autoscroll = tk.BooleanVar(value=True)
        tk.Checkbutton(log_top, text="自动滚动", variable=self.var_log_autoscroll, bg=BG, fg=FG, selectcolor="#0f1a30", activebackground=BG).pack(side="left", padx=4)

        self.txt_log = scrolledtext.ScrolledText(tab4, bg=PANEL, fg=FG, font=("Consolas", 10), borderwidth=0, highlightthickness=1, highlightbackground=BG)
        self.txt_log.pack(fill="both", expand=True, padx=4, pady=4)
        self.txt_log.tag_config("ERROR", foreground=DANGER)
        self.txt_log.tag_config("INFO", foreground=SUCCESS)
        self.txt_log.tag_config("WARNING", foreground=WARNING)

        # 持仓面板
        hold_frame = ttk.LabelFrame(tab4, text="实时持仓")
        hold_frame.pack(fill="x", padx=4, pady=(0,4))
        self.tree_holdings = ttk.Treeview(hold_frame, columns=("symbol","status","qty","entry","mark","pnl","roe","mode"), show="headings", height=6)
        self.tree_holdings.heading("symbol", text="币种")
        self.tree_holdings.heading("status", text="持仓")
        self.tree_holdings.heading("qty", text="数量")
        self.tree_holdings.heading("entry", text="开仓价")
        self.tree_holdings.heading("mark", text="标记价")
        self.tree_holdings.heading("pnl", text="未实现盈亏")
        self.tree_holdings.heading("roe", text="收益率")
        self.tree_holdings.heading("mode", text="模式")
        self.tree_holdings.column("symbol", width=120, anchor="w")
        self.tree_holdings.column("status", width=60, anchor="center")
        self.tree_holdings.column("qty", width=70, anchor="e")
        self.tree_holdings.column("entry", width=90, anchor="e")
        self.tree_holdings.column("mark", width=90, anchor="e")
        self.tree_holdings.column("pnl", width=90, anchor="e")
        self.tree_holdings.column("roe", width=70, anchor="e")
        self.tree_holdings.column("mode", width=60, anchor="center")
        self.tree_holdings.pack(fill="x", padx=4, pady=4)
        self.lbl_holdings_summary = tk.Label(hold_frame, text="汇总: 持仓币种 0 | 总未实现盈亏 +0.0000U", anchor="w", bg=BG, fg=FG)
        self.lbl_holdings_summary.pack(fill="x", padx=6, pady=(0,4))

    def _start_learning_refresh(self):
        """每5秒刷新学习进度面板"""
        if not self.running:
            return
        try:
            self._update_learning_panel()
        except Exception:
            pass
        self.root.after(5000, self._start_learning_refresh)

    def _update_learning_panel(self):
        """更新Tab3学习进度面板"""
        # ---- MAB ----
        try:
            with self.strategies_lock:
                strategies = list(self.strategies.values())
            mab_counts = [0, 0, 0, 0]
            mab_rewards = [0.0, 0.0, 0.0, 0.0]
            for s in strategies:
                if hasattr(s, 'adaptive_entry') and s.adaptive_entry:
                    ae = s.adaptive_entry
                    for i in range(min(4, len(ae.counts))):
                        mab_counts[i] += ae.counts[i]
                        mab_rewards[i] += ae.rewards[i]
            total_mab = sum(mab_counts)
            phase = "探索期" if total_mab < 30 else ("学习中" if total_mab < 100 else "✅ 已稳定")
            self.lbl_mab_total.config(text=f"总样本: {total_mab}/100单  [阶段: {phase}]")
            mode_names = ["市价即入", "ATR回撤", "支撑阻力", "分批确认"]
            labels = [self.lbl_mab_mode0, self.lbl_mab_mode1, self.lbl_mab_mode2, self.lbl_mab_mode3]
            for i, (lbl, name) in enumerate(zip(labels, mode_names)):
                avg_r = mab_rewards[i] / max(1, mab_counts[i])
                ucb = avg_r + (2 * math.log(max(1, total_mab)) / max(1, mab_counts[i])) ** 0.5 if total_mab > 0 else 0
                lbl.config(text=f"  {name}:  {mab_counts[i]:>3}次 | 奖励: {avg_r:>+.2f} | UCB: {ucb:.2f}")
        except Exception:
            pass

        # ---- ML ----
        try:
            total_samples = 0
            total_correct = 0
            agg_weights = None
            feat_names = ["动量短","动量中","动量差","波动比","ATR归一","布林","MA交叉","偏度","连胜","RSI","ROC","MACD"]
            for s in strategies:
                if hasattr(s, 'ml_predictor') and s.ml_predictor:
                    ml = s.ml_predictor
                    samples = getattr(ml, 'sample_count', 0)
                    total_samples += samples
                    acc = ml.get_accuracy() if hasattr(ml, 'get_accuracy') else 0
                    total_correct += int(acc * samples)
                    if hasattr(ml, 'weights') and ml.weights is not None:
                        if agg_weights is None:
                            agg_weights = [0.0] * len(ml.weights)
                        for i in range(min(len(agg_weights), len(ml.weights))):
                            agg_weights[i] += ml.weights[i]
            if total_samples > 0 and agg_weights:
                for i in range(len(agg_weights)):
                    agg_weights[i] /= len(strategies)
                acc = total_correct / total_samples
                phase = "未启动" if total_samples == 0 else ("学习中" if total_samples < 100 else "✅ 已稳定")
                self.lbl_ml_accuracy.config(text=f"准确率: {acc:.0%} | 样本: {total_samples}/100 | 阶段: {phase}")
                w_str = " | ".join(f"{feat_names[i]}:{agg_weights[i]:+.2f}" for i in range(min(len(feat_names), len(agg_weights))))
                self.lbl_ml_weights.config(text=f"  权重: {w_str}")
            else:
                self.lbl_ml_accuracy.config(text="准确率: --% | 样本: 0/100 | 阶段: 未启动")
                self.lbl_ml_weights.config(text="  权重: 等待学习数据...")
        except Exception:
            pass

        # ---- 因果 ----
        try:
            causal_pass = 0
            causal_block = 0
            recent_causals = []
            for s in strategies:
                if hasattr(s, 'runtime_causal_pass_count'):
                    causal_pass += getattr(s, 'runtime_causal_pass_count', 0)
                    causal_block += getattr(s, 'runtime_causal_block_count', 0)
                if hasattr(s, 'last_causal_effect'):
                    recent_causals.append(getattr(s, 'last_causal_effect', 0))
            total_causal = causal_pass + causal_block
            rate = causal_pass / max(1, total_causal)
            threshold = self.config.get('causal_effect_threshold', -0.01)
            self.lbl_causal_stats.config(text=f"通过率: {rate:.1%} | 通过: {causal_pass} | 拦截: {causal_block} | 阈值: {threshold}")
            if recent_causals:
                avg_c = sum(recent_causals) / len(recent_causals)
                max_c = max(recent_causals)
                min_c = min(recent_causals)
                self.lbl_causal_recent.config(text=f"  最近因果值: 均值{avg_c:+.3f} | 最高{max_c:+.3f} | 最低{min_c:+.3f}")
        except Exception:
            pass

        # ---- 贝叶斯 ----
        try:
            agg_weights = None
            total_updates = 0
            for s in strategies:
                if hasattr(s, 'ensemble') and s.ensemble:
                    ens = s.ensemble
                    if hasattr(ens, 'weights') and ens.weights:
                        if agg_weights is None:
                            agg_weights = [0.0] * len(ens.weights)
                        for i in range(min(len(agg_weights), len(ens.weights))):
                            agg_weights[i] += ens.weights[i]
                    # update_weights 被调用次数 = 融合样本数
                    total_updates += getattr(ens, 'update_count', 0)
            if agg_weights:
                for i in range(len(agg_weights)):
                    agg_weights[i] /= max(1, len(strategies))
                names = ["趋势", "ML", "订单簿", "情绪"]
                w_str = " | ".join(f"{names[i]}: {agg_weights[i]:.0%}" for i in range(min(len(names), len(agg_weights))))
                self.lbl_bayes_weights.config(text=w_str)
            phase = "初始化" if total_updates < 10 else ("学习中" if total_updates < 50 else "✅ 已稳定")
            self.lbl_bayes_samples.config(text=f"  融合样本: {total_updates} | 阶段: {phase}")
        except Exception:
            pass

        # ---- 进化引擎 ----
        try:
            if hasattr(self, 'evolution_engine') and self.evolution_engine:
                evo = self.evolution_engine
                gen = getattr(evo, 'evolve_round', 0)
                # 最优适应度从 population 中获取
                best_fit = None
                with getattr(evo, 'population_lock', threading.Lock()):
                    pop = getattr(evo, 'population', [])
                    if pop:
                        ranked = sorted(pop, key=lambda x: x.get("score", -1), reverse=True)
                        best_fit = ranked[0].get("score", None)
                fit_str = f"{best_fit:.4f}" if best_fit is not None else "--"
                next_evo = getattr(evo, 'next_evolve_due', 0)
                if next_evo > 0:
                    remain = max(0, next_evo - time.time())
                    next_str = f"{remain/3600:.1f}小时后"
                else:
                    next_str = "首次进化中..."
                self.lbl_evo_gen.config(text=f"当前代数: {gen} | 最优适应度: {fit_str} | 下次进化: {next_str}")
                if hasattr(evo, 'best_params') and evo.best_params:
                    bp = evo.best_params
                    p_str = " | ".join(f"{k}:{v:.3f}" if isinstance(v, float) else f"{k}:{v}" for k, v in list(bp.items())[:6])
                    self.lbl_evo_params.config(text=f"  最优参数: {p_str}")
        except Exception:
            pass

        # ---- 交易统计 ----
        try:
            exits = {"take_profit": 0, "trail_exit": 0, "ml_trend_exit": 0, "breakeven_exit": 0, "stop_loss": 0, "time_exit": 0}
            total_trades = 0
            wins = 0
            for s in strategies:
                if hasattr(s, 'runtime_recent_exits'):
                    for ex in s.runtime_recent_exits:
                        total_trades += 1
                        if ex in exits:
                            exits[ex] += 1
                        if ex in ("take_profit", "trail_exit", "ml_trend_exit", "breakeven_exit"):
                            wins += 1
            losses = total_trades - wins
            wr = wins / max(1, total_trades)
            self.lbl_trade_total.config(text=f"总交易: {total_trades} | 盈利: {wins} | 亏损: {losses} | 胜率: {wr:.0%}")
            self.lbl_trade_exits.config(text=f"  止盈:{exits['take_profit']} | trail:{exits['trail_exit']} | ML退出:{exits['ml_trend_exit']} | 保本:{exits['breakeven_exit']} | 止损:{exits['stop_loss']} | 超时:{exits['time_exit']}")
            # 累计盈亏
            if hasattr(self, 'safety_monitor') and self.safety_monitor:
                sm = self.safety_monitor
                daily = getattr(sm, '_daily_pnl_usdt', 0)
                total_pnl = getattr(sm, '_total_pnl_usdt', 0)
                self.lbl_trade_pnl.config(text=f"  累计盈亏: {total_pnl:+.2f} USDT | 今日: {daily:+.2f} USDT")
        except Exception:
            pass

    def _gui_refresh_pool(self):
        """GUI按钮：手动刷新智能币种池"""
        if hasattr(self, 'smart_pool') and self.smart_pool:
            self.log("手动刷新智能币种池...", "INFO")
            try:
                new_symbols = self.smart_pool.refresh(force=True)
                if new_symbols:
                    self.available_symbols = new_symbols
                    self.config['symbol_pool'] = new_symbols
                    if not self.config.get('symbols'):
                        self.config['symbols'] = new_symbols
                    self.refresh_symbol_checkboxes()
                    self.lbl_smart_pool_count.config(text=f"{len(new_symbols)} 个币种")
                    self.log(f"刷新成功：{len(new_symbols)} 个币种", "INFO")
                else:
                    self.log("刷新失败，保持当前列表", "WARNING")
            except Exception as e:
                self.log(f"刷新异常: {e}", "ERROR")
        else:
            self.log("引擎未启动，无法刷新。请先启动引擎。", "WARNING")

    def create_entry(self, parent, label, value, show=None):
        f = tk.Frame(parent, bg=self.colors["bg"])
        f.pack(fill="x", padx=5, pady=3)
        tk.Label(f, text=label, width=13, anchor="w", bg=self.colors["bg"], fg=self.colors["fg"]).pack(side="left")
        e = ttk.Entry(f, show=show)
        e.insert(0, value)
        e.pack(side="right", fill="x", expand=True)
        return e

    def _set_entry_value(self, entry, value):
        try:
            entry.delete(0, tk.END)
            entry.insert(0, str(value))
        except Exception:
            return

    def apply_risk_preset(self, preset):
        if str(preset).lower() == "furious":
            if not messagebox.askyesno(
                "⚠️ 极端风险警告",
                "狂暴模式关闭了几乎所有风控保护！\n可能导致重大资金损失！\n\n确定要启用吗？"
            ):
                self.log("用户取消了狂暴预设", "WARNING")
                return
        profiles = {
            "conservative": {
                "max_portfolio_var": 0.038,
                "funding_rate_threshold": 0.00030,
                "style_var_scale": 1.6,
                "runtime_target_orders_per_hour": 3.6,
                "execution_market_confidence": 0.88,
                "execution_aggressive_confidence": 0.70,
                "random_skip_rate": 0.04,
                "scheduler_skip_prob": 0.04,
                "max_slippage_alarm": 0.0016,
                "recovery_budget_ratio": 0.12,
                "recovery_daily_target_pct": 0.003,
                "recovery_max_boost": 0.30,
                "orderbook_confirm_ticks": 4,
                "stop_confirm_window_sec": 8,
                "mode_min_dwell_sec": 1200
            },
            "balanced": {
                "max_portfolio_var": 0.040,
                "funding_rate_threshold": 0.00025,
                "style_var_scale": 1.8,
                "runtime_target_orders_per_hour": 4.5,
                "execution_market_confidence": 0.86,
                "execution_aggressive_confidence": 0.67,
                "random_skip_rate": 0.03,
                "scheduler_skip_prob": 0.03,
                "max_slippage_alarm": 0.0018,
                "recovery_budget_ratio": 0.15,
                "recovery_daily_target_pct": 0.004,
                "recovery_max_boost": 0.45,
                "orderbook_confirm_ticks": 3,
                "stop_confirm_window_sec": 6,
                "mode_min_dwell_sec": 900
            },
            "aggressive": {
                "max_portfolio_var": 0.048,
                "funding_rate_threshold": 0.00018,
                "style_var_scale": 2.3,
                "runtime_target_orders_per_hour": 7.0,
                "execution_market_confidence": 0.80,
                "execution_aggressive_confidence": 0.60,
                "random_skip_rate": 0.015,
                "scheduler_skip_prob": 0.015,
                "max_slippage_alarm": 0.0022,
                "recovery_budget_ratio": 0.20,
                "recovery_daily_target_pct": 0.006,
                "recovery_max_boost": 0.55,
                "orderbook_confirm_ticks": 2,
                "stop_confirm_window_sec": 4,
                "mode_min_dwell_sec": 600
            },
            "furious": {
                "max_portfolio_var": 0.080,
                "funding_rate_threshold": 0.05000,
                "style_var_scale": 4.0,
                "runtime_target_orders_per_hour": 30.0,
                "execution_market_confidence": 0.50,
                "execution_aggressive_confidence": 0.30,
                "random_skip_rate": 0.0,
                "scheduler_skip_prob": 0.0,
                "max_slippage_alarm": 0.0500,
                "recovery_budget_ratio": 0.50,
                "recovery_daily_target_pct": 0.020,
                "recovery_max_boost": 0.90,
                "orderbook_confirm_ticks": 1,
                "stop_confirm_window_sec": 2,
                "mode_min_dwell_sec": 60
            }
        }
        cfg = profiles.get(str(preset), profiles["balanced"])
        self._set_entry_value(self.ent_max_var, cfg["max_portfolio_var"])
        self._set_entry_value(self.ent_funding_th, cfg["funding_rate_threshold"])
        self._set_entry_value(self.ent_style_var_scale, cfg["style_var_scale"])
        self._set_entry_value(self.ent_runtime_orders, cfg["runtime_target_orders_per_hour"])
        self._set_entry_value(self.ent_market_conf, cfg["execution_market_confidence"])
        self._set_entry_value(self.ent_aggr_conf, cfg["execution_aggressive_confidence"])
        self._set_entry_value(self.ent_random_skip, cfg["random_skip_rate"])
        self._set_entry_value(self.ent_scheduler_skip, cfg["scheduler_skip_prob"])
        self._set_entry_value(self.ent_slip_alarm, cfg["max_slippage_alarm"])
        self._set_entry_value(self.ent_recovery_budget, cfg["recovery_budget_ratio"])
        self._set_entry_value(self.ent_recovery_target, cfg["recovery_daily_target_pct"])
        self._set_entry_value(self.ent_recovery_boost, cfg["recovery_max_boost"])
        self._set_entry_value(self.ent_ob_confirm_ticks, cfg["orderbook_confirm_ticks"])
        self._set_entry_value(self.ent_stop_confirm_sec, cfg["stop_confirm_window_sec"])
        self._set_entry_value(self.ent_mode_dwell_sec, cfg["mode_min_dwell_sec"])
        preset_name = {
            'conservative': '保守',
            'balanced': '均衡',
            'aggressive': '激进',
            'furious': '狂暴'
        }.get(preset, '均衡')
        self.log(f"已应用[{preset_name}]参数预设，点击启动后生效", "INFO")

    def _refresh_symbol_manager_listbox(self):
        if not hasattr(self, "sym_mgr_listbox"):
            return
        selected = set(self.config.get("symbols", []))
        self.sym_mgr_listbox.delete(0, tk.END)
        for idx, sym in enumerate(self.available_symbols):
            self.sym_mgr_listbox.insert(tk.END, sym)
            if sym in selected:
                self.sym_mgr_listbox.selection_set(idx)

    def _symbol_manager_add(self):
        if not hasattr(self, "sym_mgr_entry"):
            return
        sym = self.normalize_symbol(self.sym_mgr_entry.get())
        if not sym:
            self.log("请输入有效币种，例如：BTC/USDT:USDT 或 BTCUSDT", "WARNING")
            return
        if sym not in self.available_symbols:
            self.available_symbols.append(sym)
        selected = self.config.get("symbols", [])
        if sym not in selected:
            selected = selected + [sym]
        self.config["symbols"] = selected
        self.sym_mgr_entry.delete(0, tk.END)
        self._refresh_symbol_manager_listbox()
        self.refresh_symbol_checkboxes()
        self.log(f"已添加币种：{sym}", "INFO")

    def _symbol_manager_delete(self):
        if not hasattr(self, "sym_mgr_entry"):
            return
        sym = self.normalize_symbol(self.sym_mgr_entry.get())
        if not sym:
            sel = list(getattr(self, "sym_mgr_listbox").curselection()) if hasattr(self, "sym_mgr_listbox") else []
            if sel:
                idx = sel[0]
                if idx < len(self.available_symbols):
                    sym = self.available_symbols[idx]
        if not sym:
            self.log("请先输入或选择要删除的币种", "WARNING")
            return
        if sym in self.available_symbols:
            self.available_symbols = [s for s in self.available_symbols if s != sym]
        self.config["symbols"] = [s for s in self.config.get("symbols", []) if s != sym]
        if not self.available_symbols:
            self.available_symbols = self.config.get("smart_symbol_fallback", ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"])
        if not self.config["symbols"]:
            self.config["symbols"] = self.available_symbols[:]
        self.sym_mgr_entry.delete(0, tk.END)
        self._refresh_symbol_manager_listbox()
        self.refresh_symbol_checkboxes()
        self.log(f"已删除币种：{sym}", "INFO")

    def _symbol_manager_select_all(self):
        if not hasattr(self, "sym_mgr_listbox"):
            return
        self.sym_mgr_listbox.selection_set(0, tk.END)

    def _symbol_manager_clear_select(self):
        if not hasattr(self, "sym_mgr_listbox"):
            return
        self.sym_mgr_listbox.selection_clear(0, tk.END)

    def _symbol_manager_apply(self):
        if not hasattr(self, "sym_mgr_listbox"):
            return
        idxs = list(self.sym_mgr_listbox.curselection())
        if idxs:
            selected = []
            for i in idxs:
                if i < len(self.available_symbols):
                    selected.append(self.available_symbols[i])
            self.config["symbols"] = selected if selected else self.available_symbols[:]
        else:
            self.config["symbols"] = self.available_symbols[:]
        self.config["symbol_pool"] = self.available_symbols[:]
        self.refresh_symbol_checkboxes()
        self.log(f"已应用币种选择：{len(self.config.get('symbols', []))}个", "INFO")

    def open_symbol_manager(self):
        if hasattr(self, "sym_mgr_win") and self.sym_mgr_win and self.sym_mgr_win.winfo_exists():
            self.sym_mgr_win.lift()
            self.sym_mgr_win.focus_force()
            return
        win = tk.Toplevel(self.root)
        self.sym_mgr_win = win
        win.title("币种管理")
        win.geometry("520x560")
        win.configure(bg=self.colors["bg"])
        top = tk.Frame(win, bg=self.colors["bg"])
        top.pack(fill="x", padx=8, pady=8)
        self.sym_mgr_entry = ttk.Entry(top)
        self.sym_mgr_entry.pack(side="left", fill="x", expand=True, padx=(0, 4))
        ttk.Button(top, text="添加", command=self._symbol_manager_add).pack(side="left", padx=2)
        ttk.Button(top, text="删除", command=self._symbol_manager_delete).pack(side="left", padx=2)
        list_wrap = tk.Frame(win, bg=self.colors["bg"])
        list_wrap.pack(fill="both", expand=True, padx=8, pady=(0, 8))
        self.sym_mgr_listbox = tk.Listbox(
            list_wrap,
            selectmode=tk.MULTIPLE,
            bg=self.colors["panel"],
            fg=self.colors["fg"],
            selectbackground=self.colors["accent"],
            activestyle="none"
        )
        sb = ttk.Scrollbar(list_wrap, orient="vertical", command=self.sym_mgr_listbox.yview)
        self.sym_mgr_listbox.configure(yscrollcommand=sb.set)
        self.sym_mgr_listbox.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")
        btn_row = tk.Frame(win, bg=self.colors["bg"])
        btn_row.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btn_row, text="全选", command=self._symbol_manager_select_all).pack(side="left", padx=2)
        ttk.Button(btn_row, text="清空选择", command=self._symbol_manager_clear_select).pack(side="left", padx=2)
        ttk.Button(btn_row, text="应用选择", command=self._symbol_manager_apply).pack(side="right", padx=2)
        ttk.Button(btn_row, text="关闭", command=win.destroy).pack(side="right", padx=2)
        self._refresh_symbol_manager_listbox()

    def normalize_symbol(self, raw):
        s = str(raw).strip().upper().replace(" ", "")
        if not s:
            return ""
        if "/" not in s and s.endswith("USDT"):
            base = s[:-4]
            if base:
                s = f"{base}/USDT:USDT"
        elif "/" in s and ":" not in s:
            s = f"{s}:USDT"
        return s

    def get_selected_symbols_from_ui(self):
        if not hasattr(self, "sym_vars"):
            return []
        return [k for k, v in self.sym_vars.items() if v.get()]

    def refresh_symbol_checkboxes(self):
        selected_symbols = set(self.get_selected_symbols_from_ui() or self.config.get("symbols", []))
        old_vars = dict(getattr(self, "sym_vars", {}))
        for w in self.sym_list_frame.winfo_children():
            w.destroy()
        self.sym_vars = {}
        # fr_sym可能是普通Frame（无text属性），安全跳过
        try:
            self.fr_sym.configure(text=f"币种选择（{len(self.available_symbols)}个）")
        except Exception:
            pass  # silent fallback
        for sym in self.available_symbols:
            checked = old_vars[sym].get() if sym in old_vars else (sym in selected_symbols)
            v = tk.BooleanVar(value=checked)
            self.sym_vars[sym] = v
            chk = tk.Checkbutton(
                self.sym_list_frame,
                text=sym,
                variable=v,
                bg=self.colors["bg"],
                fg=self.colors["fg"],
                selectcolor=self.colors["panel"],
                activebackground=self.colors["bg"]
            )
            chk.pack(anchor="w", padx=5, pady=1)

    def add_symbol(self):
        sym = self.normalize_symbol(self.ent_symbol_input.get())
        if not sym:
            self.log("请输入有效币种，例如：BTC/USDT:USDT 或 BTCUSDT", "WARNING")
            return
        if sym in self.available_symbols:
            if sym in self.sym_vars:
                self.sym_vars[sym].set(True)
            self.log(f"币种已存在：{sym}", "WARNING")
            return
        self.available_symbols.append(sym)
        selected = self.config.get('symbols', [])
        if sym not in selected:
            selected = selected + [sym]
        self.config['symbols'] = selected
        self.refresh_symbol_checkboxes()
        self.ent_symbol_input.delete(0, tk.END)
        self.log(f"已添加币种：{sym}", "INFO")

    def delete_symbol(self):
        sym = self.normalize_symbol(self.ent_symbol_input.get())
        if not sym:
            self.log("请先输入要删除的币种", "WARNING")
            return
        if sym not in self.available_symbols:
            self.log(f"币种不存在：{sym}", "WARNING")
            return
        self.available_symbols = [s for s in self.available_symbols if s != sym]
        self.config['symbols'] = [s for s in self.config.get('symbols', []) if s != sym]
        if not self.available_symbols:
            self.available_symbols = self.config.get('smart_symbol_fallback', ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT'])
        if not self.config['symbols']:
            self.config['symbols'] = self.available_symbols[:]
        self.refresh_symbol_checkboxes()
        self.ent_symbol_input.delete(0, tk.END)
        self.log(f"已删除币种：{sym}", "INFO")

    def log(self, msg, level="INFO", count_for_degrade=False):
        ts = datetime.now().strftime("%H:%M:%S")
        with self.health_lock:
            if level == "WARNING":
                self.warn_count += 1
            if level == "ERROR":
                self.error_count += 1
                if count_for_degrade:
                    self.error_events.append(time.time())
            health_text = f"告警:{self.warn_count} 错误:{self.error_count}"
        if level == "ERROR" and count_for_degrade:
            self.evaluate_degrade_mode()
        def _append():
            self.txt_log.insert(tk.END, f"[{ts}] {msg}\n", level)
            if getattr(self, "var_log_autoscroll", None) is None or self.var_log_autoscroll.get():
                self.txt_log.see(tk.END)
            if hasattr(self, "lbl_health"):
                self.lbl_health.config(text=health_text)
        self.root.after(0, _append)

    def evaluate_degrade_mode(self):
        now = time.time()
        window_sec = max(30, int(self.config.get("execution_degrade_window_sec", 120)))
        threshold = max(3, int(self.config.get("execution_degrade_error_threshold", 8)))
        cooldown = max(60, int(self.config.get("execution_degrade_cooldown_sec", 300)))
        with self.health_lock:
            recent = [x for x in list(self.error_events) if now - x <= window_sec]
        if len(recent) >= threshold:
            with self._degrade_lock:
                if self.config.get("execution_degraded", False) and now < self.runtime_degrade_until:
                    return
                self.runtime_degrade_until = max(self.runtime_degrade_until, now + cooldown)
                self.config["execution_degraded"] = True
                self.log(f"执行容灾已开启，{cooldown}s 内暂停新开仓", "WARNING")

    def maybe_recover_degrade_mode(self):
        with self._degrade_lock:
            if self.config.get("execution_degraded", False) and time.time() >= self.runtime_degrade_until:
                self.config["execution_degraded"] = False
                self.log("执行容灾已恢复，允许继续开仓", "INFO")

    def maybe_generate_daily_report(self):
        try:
            if not bool(self.config.get("daily_report_enabled", True)):
                return
            now_dt = datetime.now()
            if time.time() < self.daily_report_retry_ts:
                return
            day_key = now_dt.strftime("%Y-%m-%d")
            target_hour = max(0, min(23, int(self.config.get("daily_report_hour", 23))))
            if day_key == self.last_report_day or now_dt.hour < target_hour:
                return
            os.makedirs("reports", exist_ok=True)
            snapshot = self.get_strategy_snapshot()
            total_orders = 0
            total_fills = 0
            total_slip = []
            running = 0
            for s in snapshot:
                if hasattr(s, "is_alive") and s.is_alive():
                    running += 1
                try:
                    st = s.get_stats()
                    total_orders += int(st.get("order_count", 0))
                    total_fills += int(st.get("fill_count", 0))
                    if st.get("slippage_mean", 0) > 0:
                        total_slip.append(float(st.get("slippage_mean", 0)))
                except Exception:
                    pass  # silent fallback
            slip_avg = float(np.mean(total_slip)) if total_slip else 0.0
            report_path = os.path.join("reports", f"daily_report_{day_key}.txt")
            with self.health_lock:
                warn_count = self.warn_count
                error_count = self.error_count
            lines = [
                f"日期: {day_key}",
                f"运行策略数: {running}",
                f"总下单数: {total_orders}",
                f"总成交数: {total_fills}",
                f"平均滑点: {slip_avg*100:.4f}%",
                f"告警计数: {warn_count}",
                f"错误计数: {error_count}",
                f"进化启用: {bool(self.config.get('evolution_enabled', True))}"
            ]
            with open(report_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            self.last_report_day = day_key
            self.log(f"运营日报已生成: {report_path}", "INFO")
        except Exception as e:
            self.daily_report_retry_ts = time.time() + 300
            self.log(f"运营日报生成失败: {e}", "ERROR")

    def start(self):
        self.save_config()
        if not self.config.get('api_key') or not self.config.get('api_secret'):
            messagebox.showerror("Error", "请先配置 API Key 和 Secret")
            return
        self.running = True
        self.stop_event.clear()
        self.btn_start["state"] = "disabled"
        self.btn_stop["state"] = "normal"
        self.lbl_status.config(text="ENGINE RUNNING", fg=self.colors["success"])
        self.backend_thread = threading.Thread(target=self.run_backend, daemon=True)
        self.backend_thread.start()
        self._start_learning_refresh()

    def stop(self):
        self.log("紧急停止中...", "ERROR")
        self.running = False
        self.stop_event.set()  # 通知所有线程停止
        if self.monitor:
            self.monitor.stop()
        with self.strategies_lock:
            strategy_snapshot = list(self.strategies.values())
        for s in strategy_snapshot:
            s.stop()
        if self.autopilot:
            self.autopilot.stop()
        if self.evolution_engine:
            self.evolution_engine.stop()
        if self.safety_monitor:
            self.safety_monitor.stop()
        self.join_running_threads()
        self.root.after(0, lambda: self.btn_start.config(state="normal"))
        self.root.after(0, lambda: self.btn_stop.config(state="disabled"))
        self.log("系统已停止", "ERROR")

    def _get_ssl_context(self):
        """获取SSL上下文，兼容证书问题"""
        import ssl
        try:
            import certifi
            return ssl.create_default_context(cafile=certifi.where())
        except ImportError:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx

    def _gui_save_config(self):
        """GUI保存配置按钮"""
        try:
            self.save_config()
            self.log("💾 配置已保存到 " + CONFIG_FILE, "INFO")
            from tkinter import messagebox
            messagebox.showinfo("保存成功", "配置已保存！下次启动自动加载。")
        except Exception as e:
            self.log(f"❌ 保存配置失败: {e}", "ERROR")

    def _auto_update(self):
        """一键从GitHub下载最新版本并重启（含SHA256签名校验）"""
        import urllib.request
        REPO_URL = "https://raw.githubusercontent.com/alwayslivinginadream-lgtm/HUI/main/2.py"
        # ★ 每次发版时更新此哈希值，防止供应链攻击
        EXPECTED_SHA256 = "C20DEDA4FDCFB1DC9F3408F07279A3C6BA0752DA3BABEA5DB684A7B2821F08B4"

        def _do_update():
            try:
                self.log("🔄 正在检查更新...", "INFO")
                self.btn_update.config(state="disabled", text="⏳ 更新中...")

                # 下载最新代码
                req = urllib.request.Request(REPO_URL, headers={"Cache-Control": "no-cache"})
                with urllib.request.urlopen(req, timeout=30, context=self._get_ssl_context()) as resp:
                    new_code = resp.read()

                if len(new_code) < 1000:
                    self.log("❌ 下载失败：文件太小", "ERROR")
                    self.root.after(0, lambda: self.btn_update.config(state="normal", text="🔄 一键更新"))
                    return

                # ★ SHA256 签名校验
                actual_hash = hashlib.sha256(new_code).hexdigest()
                if EXPECTED_SHA256:
                    if actual_hash != EXPECTED_SHA256:
                        self.log(f"❌ 哈希校验失败！预期: {EXPECTED_SHA256[:16]}... 实际: {actual_hash[:16]}...", "ERROR")
                        self.log("⚠️ 可能遭遇供应链攻击，更新已中止！", "ERROR")
                        self.root.after(0, lambda: self.btn_update.config(state="normal", text="🔄 一键更新"))
                        return
                    self.log(f"✅ SHA256 校验通过: {actual_hash[:16]}...", "INFO")
                else:
                    self.log(f"⚠️ 未配置 EXPECTED_SHA256，跳过签名校验（{actual_hash[:16]}...）", "WARNING")
                    self.log("💡 请在代码中设置 EXPECTED_SHA256 以启用安全校验", "INFO")

                # 获取当前脚本/exe路径
                if getattr(sys, 'frozen', False):
                    current_file = os.path.abspath(sys.executable)
                    update_dir = os.path.dirname(current_file)
                    target_file = os.path.join(update_dir, "2.py")
                else:
                    current_file = os.path.abspath(sys.argv[0])
                    update_dir = os.path.dirname(current_file)
                    target_file = current_file

                # 备份旧文件
                backup_file = target_file + ".bak"
                if os.path.exists(target_file):
                    try:
                        import shutil
                        shutil.copy2(target_file, backup_file)
                    except Exception:
                        pass

                # 写入新文件
                with open(target_file, "wb") as f:
                    f.write(new_code)

                new_size = len(new_code)
                self.log(f"✅ 更新成功！({new_size} bytes)", "INFO")
                self.log(f"📁 已保存到: {target_file}", "INFO")
                self.log(f"💾 旧版备份: {backup_file}", "INFO")

                if getattr(sys, 'frozen', False):
                    # exe模式：下载2.py后自动打包成新exe
                    self.log("📦 正在自动打包exe...", "INFO")
                    update_dir = os.path.dirname(current_file)
                    py_file = os.path.join(update_dir, "2.py")
                    exe_name = os.path.splitext(os.path.basename(current_file))[0]
                    try:
                        import subprocess
                        # 备份旧exe
                        exe_bak = current_file + ".bak"
                        try:
                            if os.path.exists(exe_bak):
                                os.remove(exe_bak)
                            import shutil
                            shutil.copy2(current_file, exe_bak)
                        except Exception:
                            pass
                        # 智能查找python解释器
                        python_cmd = None
                        for candidate in [
                            shutil.which("python"),
                            shutil.which("python3"),
                            os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "Python", "Python313", "python.exe"),
                            os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "Python", "Python312", "python.exe"),
                            os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "Python", "Python311", "python.exe"),
                            os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "Python", "Python310", "python.exe"),
                            r"C:\Python313\python.exe", r"C:\Python312\python.exe",
                            r"C:\Python311\python.exe", r"C:\Python310\python.exe",
                        ]:
                            if candidate and os.path.isfile(candidate):
                                python_cmd = candidate
                                break
                        if not python_cmd:
                            raise FileNotFoundError("未找到python解释器")
                        self.log(f"🔧 使用Python: {python_cmd}", "INFO")
                        # 用bat脚本打包（确保环境变量与手动打包一致）
                        build_bat = os.path.join(update_dir, "_build.bat")
                        with open(build_bat, "w", encoding="utf-8") as bat:
                            bat.write(f'@echo off\n')
                            bat.write(f'cd /d "{update_dir}"\n')
                            bat.write(f'"{python_cmd}" -m pip install --quiet pyinstaller pysocks 2>nul\n')
                            bat.write(f'"{python_cmd}" -m PyInstaller --onefile --windowed --name {exe_name} --hidden-import pysocks --hidden-import socks "{py_file}"\n')
                        self.log("📦 正在执行打包脚本...", "INFO")
                        result = subprocess.run(
                            ["cmd", "/c", build_bat],
                            capture_output=True, text=True, timeout=300
                        )
                        dist_exe = os.path.join(update_dir, "dist", f"{exe_name}.exe")
                        if os.path.exists(dist_exe):
                            # 把新exe复制到原位置（当前exe在运行中无法直接覆盖）
                            new_exe_path = current_file + ".new"
                            shutil.copy2(dist_exe, new_exe_path)
                            # 写一个bat脚本：等当前exe退出后替换
                            bat_path = os.path.join(update_dir, "_update.bat")
                            with open(bat_path, "w") as bat:
                                bat.write(f'@echo off\n')
                                bat.write(f'ping 127.0.0.1 -n 3 >nul\n')
                                bat.write(f'move /Y "{new_exe_path}" "{current_file}"\n')
                                bat.write(f'start "" "{current_file}"\n')
                                bat.write(f'del "%~f0"\n')
                            self.log("✅ 打包成功！即将重启...", "INFO")
                            self.root.after(0, lambda: messagebox.showinfo("更新完成", "新版本打包成功，即将自动重启"))
                            # 启动bat并退出当前程序
                            subprocess.Popen(["cmd", "/c", bat_path], cwd=update_dir)
                            self.root.after(1000, lambda: os._exit(0))
                        else:
                            self.log(f"❌ 打包失败，请手动运行: pyinstaller --onefile --windowed --name {exe_name} 2.py", "ERROR")
                            self.log(f"pyinstaller输出: {result.stderr[-200:] if result.stderr else '无'}", "WARNING")
                            self.root.after(0, lambda: messagebox.showwarning("打包失败", f"自动打包失败，请手动打包\n或直接用 python 2.py 运行"))
                    except FileNotFoundError:
                        self.log("❌ 未找到python/pyinstaller，请先安装", "ERROR")
                        self.root.after(0, lambda: messagebox.showwarning("打包失败", "未找到python或pyinstaller\n请手动打包或用 python 2.py 运行"))
                    except Exception as e2:
                        self.log(f"❌ 自动打包异常: {e2}", "ERROR")
                        self.root.after(0, lambda: messagebox.showwarning("打包失败", f"自动打包异常: {e2}\n请手动打包或用 python 2.py 运行"))
                else:
                    self.log("🔄 即将重启...", "INFO")
                    self.root.after(0, lambda: messagebox.showinfo("更新完成", "即将重启应用"))
                    self.root.after(1000, lambda: self._restart_app())

            except Exception as e:
                self.log(f"❌ 更新失败: {e}", "ERROR")
            finally:
                self.root.after(0, lambda: self.btn_update.config(state="normal", text="🔄 一键更新"))

        threading.Thread(target=_do_update, daemon=True).start()

    def _restart_app(self):
        """重启应用"""
        try:
            self.root.destroy()
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception:
            pass

    def _upload_log(self):
        """一键上传日志到GitHub"""
        import urllib.request
        # token从配置文件读取
        token = self.config.get("github_token", "").strip()
        if not token:
            # 弹窗让用户输入
            token = tk.simpledialog.askstring("GitHub Token", "请输入GitHub Token:", show="*")
            if not token:
                self.log("❌ 未输入Token，取消上传", "WARNING")
                return
            self.config["github_token"] = token.strip()
            self.save_config()
        # 确保token是纯ASCII
        token = token.encode("ascii", errors="ignore").decode("ascii")
        REPO = "alwayslivinginadream-lgtm/HUI"

        def _do_upload():
            try:
                self.log("📤 正在上传日志...", "INFO")
                self.btn_upload_log.config(state="disabled", text="⏳ 上传中...")

                # 获取日志内容
                log_content = self.txt_log.get("1.0", tk.END).strip()
                if not log_content or len(log_content) < 50:
                    self.log("❌ 日志内容太少，不上传", "WARNING")
                    self.root.after(0, lambda: self.btn_upload_log.config(state="normal", text="📤 上传日志"))
                    return

                # 文件名：log_日期时间.txt
                import base64
                ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
                filename = f"log_{ts}.txt"
                content_b64 = base64.b64encode(log_content.encode("utf-8")).decode("utf-8")

                # GitHub API 创建文件
                url = f"https://api.github.com/repos/{REPO}/contents/{filename}"
                payload = json.dumps({
                    "message": f"上传日志 {filename}",
                    "content": content_b64
                }, ensure_ascii=True).encode("utf-8")

                req = urllib.request.Request(url, data=payload, method="PUT")
                req.add_header("Authorization", f"token {token}")
                req.add_header("Content-Type", "application/json")
                req.add_header("Accept", "application/vnd.github.v3+json")

                with urllib.request.urlopen(req, timeout=30, context=self._get_ssl_context()) as resp:
                    result = json.loads(resp.read().decode("utf-8"))

                if result.get("content", {}).get("name"):
                    self.log(f"✅ 日志上传成功: {filename} ({len(log_content)} bytes)", "INFO")
                    self.root.after(0, lambda: messagebox.showinfo("上传成功", f"日志已上传到GitHub:\n{filename}"))
                else:
                    self.log("❌ 上传返回异常", "ERROR")

            except Exception as e:
                self.log(f"❌ 日志上传失败: {e}", "ERROR")
            finally:
                self.root.after(0, lambda: self.btn_upload_log.config(state="normal", text="📤 上传日志"))

        threading.Thread(target=_do_upload, daemon=True).start()

    def run_backend(self):
        try:
            proxy_host = self.config.get('proxy_host', '127.0.0.1')
            proxy_port = self.config.get('proxy_port', 10808)
            proxies = {
                'http': f'socks5h://{proxy_host}:{proxy_port}',
                'https': f'socks5h://{proxy_host}:{proxy_port}'
            }
            raw_exchange = ccxt.gateio({
                'apiKey': self.config['api_key'],
                'secret': self.config['api_secret'],
                'proxies': proxies,
                'options': {'defaultType': 'swap', 'hedgeMode': True},
                'timeout': 30000,
                'enableRateLimit': True
            })
            exchange = ExchangeInterface(raw_exchange, self.log)
            exchange.call("load_markets")
            try:
                exchange.call("fetch_balance")
            except Exception as auth_e:
                if is_auth_error(auth_e):
                    self.log("API密钥无效，策略停止", "ERROR")
                    self.root.after(0, self.stop)
                    return
                self.log(f"测试 API 连接时遇到网络错误: {auth_e}，将继续尝试", "WARNING")
            self.log("Gate.io API 连接成功", "INFO")

            self.refresh_balance(exchange)
        except Exception as e:
            self.log(f"API 连接失败: {e}", "ERROR")
            self.root.after(0, self.stop)
            return

        self.exchange = exchange

        # 创建全局调度器
        self.scheduler = GlobalScheduler(interval=self.config.get('global_order_interval', 3600))

        # ==================== 智能币种池初始化 ====================
        self.smart_pool = SmartSymbolPool(exchange, self.config, self.log)
        if bool(self.config.get("smart_symbol_enabled", True)):
            self.log("【智能币种池】正在自动获取币种...", "INFO")
            auto_symbols = self.smart_pool.refresh(force=True)
            if auto_symbols:
                # 保底：确保BTC和ETH始终在列表最前面
                must_have = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
                final = list(must_have)
                for s in auto_symbols:
                    if s not in final:
                        final.append(s)
                # 智能池获取的全部选上
                self.config['symbols'] = final
                self.config['symbol_pool'] = final
                self.log(f"【智能币种池】最终运行 {len(final)} 个币种（含BTC/ETH保底）", "INFO")
            else:
                self.log("【智能币种池】获取失败，使用回退列表", "WARNING")
                fallback = self.smart_pool._fallback()
                if not self.config.get('symbols'):
                    self.config['symbols'] = fallback
                    self.config['symbol_pool'] = fallback
        else:
            # 手动模式：如果symbols为空，使用回退列表
            if not self.config.get('symbols'):
                self.config['symbols'] = self.config.get('smart_symbol_fallback', [
                    "BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT", "SOL/USDT:USDT",
                    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
                    "DOT/USDT:USDT", "LINK/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT"
                ])

        try:
            startup_tickers = api_call(exchange.fetch_tickers, self.config.get('symbols', []))
            qv = {}
            if isinstance(startup_tickers, dict):
                for sym, tk in startup_tickers.items():
                    if isinstance(tk, dict):
                        qv[str(sym)] = float(tk.get("quoteVolume", 0) or 0)
            self.config["_startup_quote_volume"] = qv
            self.config["_startup_quote_volume_ts"] = time.time()
        except Exception:
            self.config["_startup_quote_volume"] = {}
            self.config["_startup_quote_volume_ts"] = 0.0

        # 启动市场监控
        self.monitor = MarketMonitor(exchange, self.config, self.log, self.update_status)
        self.monitor.start()

        # 启动策略
        with self.strategies_lock:
            self.strategies = {}
        for sym in self.config['symbols']:
            if not self.running:
                break
            if sym not in exchange.markets:
                self.log(f"币种 {sym} 不存在，跳过", "ERROR")
                continue
            try:
                s = UltimateGridStrategy(exchange, sym, self.config, self.monitor, self.log, self.scheduler)
                s.safety_monitor = self.safety_monitor  # 传递日亏损熔断引用
                with self.strategies_lock:
                    self.strategies[sym] = s
                s.start()
                self.log(f"策略启动: {sym}", "INFO")
            except Exception as e:
                self.log(f"策略启动失败({sym}): {e}", "ERROR")
            time.sleep(max(0.02, float(self.config.get("startup_symbol_interval_sec", 2.0))))

        # 启动自动运维（传入stop_event）
        self.autopilot = AutoPilot(self.strategies, self.log, self.stop_event, self.strategies_lock, get_config=lambda: self.config)
        self.autopilot.start()
        self.evolution_engine = EvolutionEngine(
            get_strategies=self.get_strategy_snapshot,
            get_config=lambda: self.config,
            apply_params_callback=self.apply_evolution_params,
            log_callback=self.log,
            stop_event=self.stop_event
        )
        self.evolution_engine.start()
        self.safety_monitor = SafetyMonitor(
            get_strategies=self.get_strategy_snapshot,
            get_config=lambda: self.config,
            evolution_engine=self.evolution_engine,
            log_callback=self.log,
            stop_event=self.stop_event
        )
        self.safety_monitor.start()

        # 补传safety_monitor引用给所有策略线程
        with self.strategies_lock:
            for sym, s in self.strategies.items():
                s.safety_monitor = self.safety_monitor

        # 等待停止信号
        while self.running and not self.stop_event.is_set():
            try:
                now = time.time()
                interval = max(5, int(self.config.get("balance_refresh_interval_sec", 10)))
                if now - self.last_balance_refresh >= interval:
                    self.refresh_balance(exchange)
                    self.last_balance_refresh = now
                self.refresh_holdings_panel()
                self.maybe_recover_degrade_mode()
                self.maybe_generate_daily_report()
            except Exception as e:
                self.log(f"后台循环异常: {e}", "ERROR")
                if is_auth_error(e):
                    self.log("检测到签名失效或API密钥错误，停止系统", "ERROR")
                    self.root.after(0, self.stop)
                    break
            time.sleep(1)

        # 清理
        self.log("正在清理资源...", "INFO")
        if self.monitor:
            self.monitor.stop()
        with self.strategies_lock:
            strategy_snapshot = list(self.strategies.values())
        for s in strategy_snapshot:
            s.stop()
        if self.autopilot:
            self.autopilot.stop()
        if self.evolution_engine:
            self.evolution_engine.stop()
        if self.safety_monitor:
            self.safety_monitor.stop()
        self.join_running_threads()
        self.log("所有策略已停止", "INFO")

    def update_status(self, status, text):
        self.root.after(0, lambda: self.lbl_status.config(text=f"{status} | {text}"))

    def refresh_balance(self, exchange):
        try:
            balance = api_call(exchange.fetch_balance)
            usdt = 0.0
            if 'USDT' in balance:
                usdt = float(balance['USDT'].get('free', 0))
            self.root.after(0, lambda: self.lbl_balance.config(text=f"{usdt:.2f} USDT"))
        except Exception as e:
            if is_auth_error(e):
                self.root.after(0, lambda: self.lbl_balance.config(text="API无效"))
            else:
                self.root.after(0, lambda: self.lbl_balance.config(text="获取失败"))

    def refresh_holdings_panel(self):
        def _norm_symbol(s):
            return str(s or "").upper().replace(":USDT", "").replace("/", "").replace("_", "")
        rows_map = {}
        metrics_map = {}
        snapshot = self.get_strategy_snapshot()
        for s in snapshot:
            try:
                if hasattr(s, "update_position_status"):
                    s.update_position_status(force=False)
                st = s.get_stats()
                has_pos = bool(getattr(s, "has_position", False))
                status = "持仓中" if has_pos else "空仓"
                mode = str(getattr(s, "current_mode", "normal"))
                qty = float(st.get("position_contracts", 0) or 0)
                entry = float(st.get("entry_price", 0) or 0)
                mark = float(st.get("mark_price", 0) or 0)
                pnl = float(st.get("unrealized_pnl", 0) or 0)
                roe = float(st.get("position_roe", 0) or 0)
                margin = float(st.get("position_margin", 0) or 0)
                key = _norm_symbol(getattr(s, "symbol", ""))
                pnl_text = f"{pnl:+.4f}U" if has_pos else "--"
                roe_text = f"{roe:+.2f}%" if has_pos else "--"
                rows_map[key] = (
                    str(getattr(s, "symbol", "")),
                    status,
                    f"{qty:.4f}" if has_pos else "--",
                    f"{entry:.6f}" if has_pos and entry > 0 else "--",
                    f"{mark:.6f}" if has_pos and mark > 0 else "--",
                    pnl_text,
                    roe_text,
                    mode,
                )
                if has_pos:
                    metrics_map[key] = {"margin": max(0.0, margin), "roe": roe}
            except Exception:
                continue
        try:
            if self.exchange is not None:
                positions = api_call(self.exchange.fetch_positions)
                if isinstance(positions, list):
                    for p in positions:
                        if not isinstance(p, dict):
                            continue
                        contracts = float(p.get('contracts', 0) or 0)
                        if abs(contracts) <= 0:
                            continue
                        symbol_raw = p.get('symbol', p.get('info', {}).get('contract', p.get('info', {}).get('symbol', '')))
                        symbol = str(symbol_raw or "")
                        if not symbol:
                            continue
                        key = _norm_symbol(symbol)
                        entry = float(p.get('entryPrice', p.get('entry_price', 0)) or 0)
                        mark = float(p.get('markPrice', p.get('mark_price', 0)) or 0)

                        # 兼容 Gate.io 未实现盈亏可能存在于 info 里的情况
                        upnl = p.get('unrealizedPnl', p.get('unrealized_pnl'))
                        if upnl is None:
                            upnl = p.get('info', {}).get('unrealised_pnl', 0)
                        pnl = float(upnl or 0)

                        # ==== 提取真实杠杆倍数 ====
                        real_leverage = float(p.get('leverage', self.config.get('leverage', 3)) or self.config.get('leverage', 3))
                        if real_leverage <= 0:
                            real_leverage = float(p.get('info', {}).get('leverage', self.config.get('leverage', 3)) or self.config.get('leverage', 3))

                        # 获取方向
                        side = p.get('side', 'long').lower()
                        if 'short' in side or 'sell' in side:
                            direction = -1
                        else:
                            direction = 1 if p.get('contracts', 0) > 0 else -1

                        # ==== 修复 Gate.io 等交易所 ROE 为 0 的问题 ====
                        roe = float(p.get('percentage', p.get('roe', 0)) or 0)
                        if roe == 0 and entry > 0:
                            roe = (mark - entry) / entry * max(1e-8, real_leverage) * direction * 100

                        # 如果有了未实现盈亏，但还是算不出保证金（比如强行兜底），可以通过 pnl 反推，但这里用名义价值除以杠杆更稳
                        # ==== 修复 Gate.io 保证金显示为 0 的问题 ====
                        margin = float(p.get('initialMargin', p.get('margin', p.get('collateral', 0))) or 0)
                        if margin == 0:
                            margin = float(p.get('info', {}).get('margin', 0))
                        if margin == 0 and mark > 0 and abs(contracts) > 0:
                            contract_size = float(p.get('contractSize', p.get('info', {}).get('quanto_multiplier', 1)) or 1)
                            notional = abs(contracts) * contract_size * mark
                            margin = notional / max(1e-8, real_leverage)

                        # 二次校准：如果自己算的 roe 和真实盈亏方向相反，或者差得离谱，以真实 pnl 和 margin 反推 roe
                        if margin > 0 and pnl != 0 and (roe == 0 or np.sign(roe) != np.sign(pnl)):
                            roe = (pnl / margin) * 100

                        mode = "未托管"
                        old = rows_map.get(key)
                        if old is not None:
                            mode = old[7]
                            symbol = old[0]
                        rows_map[key] = (
                            symbol,
                            "持仓中",
                            f"{contracts:.4f}",
                            f"{entry:.6f}" if entry > 0 else "--",
                            f"{mark:.6f}" if mark > 0 else "--",
                            f"{pnl:+.4f}U",
                            f"{roe:+.2f}%",
                            mode
                        )
                        metrics_map[key] = {"margin": max(0.0, margin), "roe": roe}
        except Exception as e:
            self.log(f"刷新持仓列表异常: {e}", "WARNING")
        rows = list(rows_map.values())
        rows.sort(key=lambda x: (0 if x[1] == "持仓中" else 1, x[0]))
        pos_count = 0
        total_qty = 0.0
        total_pnl = 0.0
        total_margin = 0.0
        weighted_roe_num = 0.0
        for r in rows:
            if r[1] != "持仓中":
                continue
            pos_count += 1
            try:
                total_qty += abs(float(r[2]))
            except Exception:
                pass  # silent fallback
            try:
                total_pnl += float(str(r[5]).replace("U", ""))
            except Exception:
                pass  # silent fallback
            key = _norm_symbol(r[0])
            m = metrics_map.get(key, {})
            margin = float(m.get("margin", 0) or 0)
            roe = float(m.get("roe", 0) or 0)
            total_margin += margin
            weighted_roe_num += roe * margin
        def _render():
            if not hasattr(self, "tree_holdings"):
                return
            for iid in self.tree_holdings.get_children():
                self.tree_holdings.delete(iid)
            for row in rows:
                self.tree_holdings.insert("", "end", values=row)
            if hasattr(self, "lbl_holdings_summary"):
                weighted_roe = (weighted_roe_num / total_margin) if total_margin > 0 else 0.0
                self.lbl_holdings_summary.config(
                    text=f"汇总: 持仓币种 {pos_count} | 总数量 {total_qty:.4f} | 总未实现盈亏 {total_pnl:+.4f}U | 总保证金 {total_margin:.4f}U | 加权收益率 {weighted_roe:+.2f}%"
                )
        self.root.after(0, _render)

    def get_strategy_snapshot(self):
        with self.strategies_lock:
            return list(self.strategies.values())

    def apply_evolution_params(self, params, reason="进化更新"):
        for k, v in params.items():
            if k in self.config:
                self.config[k] = v
        try:
            API_RUNTIME_SETTINGS["api_retry_count"] = max(1, int(self.config.get('api_retry_count', DEFAULT_CONFIG.get('api_retry_count', 5))))
        except Exception:
            API_RUNTIME_SETTINGS["api_retry_count"] = int(DEFAULT_CONFIG.get('api_retry_count', 5))
        try:
            API_RUNTIME_SETTINGS["api_base_retry_sec"] = max(0.3, float(self.config.get('api_base_retry_sec', DEFAULT_CONFIG.get('api_base_retry_sec', 1.5))))
        except Exception:
            API_RUNTIME_SETTINGS["api_base_retry_sec"] = float(DEFAULT_CONFIG.get('api_base_retry_sec', 1.5))
        with self.strategies_lock:
            strategies = list(self.strategies.values())
        for s in strategies:
            try:
                s.update_params(params)
            except Exception as e:
                self.log(f"应用参数到 {s.symbol} 失败: {e}", "ERROR")
        self.log(f"【第19层】参数已应用 ({reason})", "INFO")
        try:
            with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            self.log(f"保存进化参数失败: {e}", "ERROR")

    def join_running_threads(self):
        with self.strategies_lock:
            strategy_snapshot = list(self.strategies.values())
        for s in strategy_snapshot:
            if s.is_alive():
                s.join(timeout=2)
        workers = [self.monitor, self.autopilot, self.evolution_engine, self.safety_monitor]
        for w in workers:
            if w and hasattr(w, "is_alive") and w.is_alive():
                w.join(timeout=2)
        if self.backend_thread and self.backend_thread.is_alive():
            if threading.current_thread() is not self.backend_thread:
                self.backend_thread.join(timeout=3)

if __name__ == "__main__":
    try:
        root = tk.Tk()
        app = BotGUI(root)
        root.mainloop()
    except Exception as e:
        with open("error_ultimate.log", "w") as f:
            traceback.print_exc(file=f)




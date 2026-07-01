"""
=============================================================================
Strategy 08: Bollinger Bands + RSI Mean Reversion (Step Index)
=============================================================================
ASSET:      Step Index (Deriv synthetic)
TIMEFRAME:  5m candles (configurable via timeframe_sec)
ENTRY BUY:  Close < Lower Bollinger Band AND RSI < 30  (oversold)
ENTRY SELL: Close > Upper Bollinger Band AND RSI > 70  (overbought)
EXIT:       Opposite band touch OR RSI crosses 50      (mid-band)
CONTRACT:   Binary option, 5-minute duration (matches analysis timeframe)
RISK:       Qty = 0.001 (minimum stake), position persisted in Redis
=============================================================================
"""

import os
import sys
import json
import time
import asyncio
import numpy as np
import logging
import psycopg2
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

sys.path.append(os.path.join(os.path.dirname(__file__), "../../common"))
from models import Tick, OrderSubmit
from strategy_base import StrategyBase
from runner_v3 import GenericRunnerV3


class BollingerRsiStrategy(StrategyBase):
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)

        self.timeframe_sec  = int(params.get("timeframe_sec", 300))   # 5m default
        self.bb_period      = int(params.get("bb_period", 20))
        self.bb_std         = float(params.get("bb_std", 2.0))
        self.rsi_period     = int(params.get("rsi_period", 14))
        self.rsi_ob         = float(params.get("rsi_ob", 70.0))       # Overbought
        self.rsi_os         = float(params.get("rsi_os", 30.0))       # Oversold
        self.rsi_exit       = float(params.get("rsi_exit", 50.0))     # Exit level

        self.lookback_candles = max(self.bb_period, self.rsi_period) + 10

        self.candles: List[Dict[str, float]] = []
        self.current_candle: Optional[Dict[str, Any]] = None
        self.position = None  # 'buy', 'sell', or None

        # DB warm-up credentials
        self.db_params = {
            "host":     os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port":     os.getenv("POSTGRES_PORT", "5432"),
            "dbname":   os.getenv("POSTGRES_DB", "trading"),
            "user":     os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"]
        }
        self._warm_up()

        # Restore position from Redis (expiry-aware)
        self.position = self.restore_position(contract_duration_sec=self.timeframe_sec)

    # ── WARM-UP ────────────────────────────────────────────────
    def _warm_up(self):
        if not self.db_params["host"] or not self.db_params["password"]:
            self.logger.warning("⚠️ Warm-up skipped: missing DB credentials")
            return
        try:
            self.logger.info(f"🧱 Warm-up (Bollinger/RSI 5m) for {self.params.get('symbol')}...")
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ts, last FROM market_ticks
                    WHERE symbol = %s ORDER BY ts DESC LIMIT 15000
                """, (self.params.get("symbol"),))
                rows = cur.fetchall()
                rows.reverse()
                for ts_dt, last in rows:
                    self._process_tick_internal(ts_dt.timestamp(), float(last))
            conn.close()
            self.logger.info(f"✅ Warm-up done. Candles: {len(self.candles)}")
        except Exception as e:
            self.logger.error(f"❌ Warm-up error: {e}")

    # ── CANDLE AGGREGATION ──────────────────────────────────────
    def _process_tick_internal(self, ts: float, last: float) -> bool:
        candle_ts = int(ts // self.timeframe_sec) * self.timeframe_sec
        closed = False
        if self.current_candle is None:
            self.current_candle = {'ts': candle_ts, 'open': last, 'high': last, 'low': last, 'close': last}
        elif candle_ts > self.current_candle['ts']:
            self.candles.append(self.current_candle)
            if len(self.candles) > self.lookback_candles:
                self.candles.pop(0)
            self.current_candle = {'ts': candle_ts, 'open': last, 'high': last, 'low': last, 'close': last}
            closed = True
        else:
            self.current_candle['high']  = max(self.current_candle['high'], last)
            self.current_candle['low']   = min(self.current_candle['low'], last)
            self.current_candle['close'] = last
        return closed

    # ── INDICATORS ──────────────────────────────────────────────
    def calculate_bollinger(self, prices: np.ndarray):
        """Returns (upper, middle, lower) Bollinger Bands."""
        n = self.bb_period
        if len(prices) < n:
            return None, None, None
        sma   = np.mean(prices[-n:])
        std   = np.std(prices[-n:], ddof=1)
        return sma + self.bb_std * std, sma, sma - self.bb_std * std

    def calculate_rsi(self, prices: np.ndarray) -> float:
        """Wilder RSI."""
        n = self.rsi_period
        if len(prices) < n + 1:
            return 50.0
        deltas = np.diff(prices[-(n + 1):])
        gains  = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_gain = np.mean(gains) if np.mean(gains) > 0 else 1e-10
        avg_loss = np.mean(losses) if np.mean(losses) > 0 else 1e-10
        rs  = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    # ── MAIN LOOP ───────────────────────────────────────────────
    def on_tick(self, tick: Tick) -> Optional[OrderSubmit]:
        try:
            ts_dt  = datetime.fromisoformat(tick.ts.replace("Z", "+00:00"))
            ts_val = ts_dt.timestamp()
        except Exception:
            ts_val = datetime.now().timestamp()

        candle_closed = self._process_tick_internal(ts_val, tick.last)
        if not candle_closed:
            return None
        if len(self.candles) < self.lookback_candles:
            return None

        prices = np.array([c['close'] for c in self.candles])
        upper, middle, lower = self.calculate_bollinger(prices)
        rsi   = self.calculate_rsi(prices)
        last_px = tick.last

        if upper is None:
            return None

        # Telemetry every 5m candle
        self.emit_telemetry({
            "rsi":    float(rsi),
            "bb_upper": float(upper),
            "bb_lower": float(lower),
            "bb_middle": float(middle)
        })

        ts_str = datetime.fromtimestamp(self.candles[-1]['ts']).strftime('%H:%M')
        self.logger.info(
            f"🕯️ [{ts_str}] Px={last_px:.3f} | RSI={rsi:.1f} | "
            f"BB=[{lower:.3f} — {upper:.3f}]"
        )

        qty = self.params.get('qty', 1.0)

        # ── ENTRY LOGIC ─────────────────────────────────────────
        if self.position is None:
            if last_px < lower and rsi < self.rsi_os:
                self.position = 'buy'
                self.save_position(self.position)
                self.logger.info(f"🟢 BUY SIGNAL | RSI={rsi:.1f} | Price={last_px} < Lower={lower:.3f}")
                self._publish_signal(tick.symbol, 'buy', rsi, qty)
                return self.create_order(tick.symbol, 'buy', qty)

            if last_px > upper and rsi > self.rsi_ob:
                self.position = 'sell'
                self.save_position(self.position)
                self.logger.info(f"🔴 SELL SIGNAL | RSI={rsi:.1f} | Price={last_px} > Upper={upper:.3f}")
                self._publish_signal(tick.symbol, 'sell', rsi, qty)
                return self.create_order(tick.symbol, 'sell', qty)

        # ── EXIT LOGIC ──────────────────────────────────────────
        else:
            if self.position == 'buy':
                # Exit: price returns above middle band OR RSI > exit level
                if last_px >= middle or rsi >= self.rsi_exit:
                    self.position = None
                    self.save_position(self.position)
                    self.logger.info(f"❄️ EXIT BUY | RSI={rsi:.1f} | Price={last_px} >= Mid={middle:.3f}")
                    return self.create_order(tick.symbol, 'sell', qty, meta={"reason": "exit_mean_rev"})

            elif self.position == 'sell':
                # Exit: price returns below middle band OR RSI < exit level
                if last_px <= middle or rsi <= self.rsi_exit:
                    self.position = None
                    self.save_position(self.position)
                    self.logger.info(f"❄️ EXIT SELL | RSI={rsi:.1f} | Price={last_px} <= Mid={middle:.3f}")
                    return self.create_order(tick.symbol, 'buy', qty, meta={"reason": "exit_mean_rev"})

        return None

    def _publish_signal(self, symbol: str, side: str, rsi: float, qty: float):
        """Publish signal to NATS for AlertManager → Telegram."""
        if not self.nc:
            return
        try:
            payload = json.dumps({
                "strategy": self.__class__.__name__,
                "symbol":   symbol,
                "side":     side,
                "rsi":      round(rsi, 2),
                "qty":      qty,
                "ts":       datetime.now(timezone.utc).isoformat()
            }).encode()
            asyncio.create_task(self.nc.publish("alerts.signal", payload))
        except Exception as e:
            self.logger.warning(f"⚠️ Signal publish failed: {e}")


if __name__ == "__main__":
    NATS_URL = os.environ["NATS_URL"]
    runner = GenericRunnerV3(BollingerRsiStrategy, NATS_URL)
    asyncio.run(runner.run())

import os
import sys
import asyncio
import numpy as np
import logging
import psycopg2
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

# Ensure base classes are in path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../common"))
from models import Tick, OrderSubmit
from strategy_base import StrategyBase
from runner_v3 import GenericRunnerV3

class HurstDonchianStrategy(StrategyBase):
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)
        self.timeframe_sec = int(params.get("timeframe_sec", 300)) # Default 5m
        self.hurst_window = int(params.get("hurst_window", 100))
        self.donchian_p = int(params.get("donchian_p", 40))
        self.hurst_threshold = float(params.get("hurst_threshold", 0.60))
        
        # Necesitamos suficientes velas para Hurst y Donchian
        self.lookback_candles = max(self.hurst_window, self.donchian_p) + 5
        
        self.candles: List[Dict[str, float]] = []
        self.current_candle: Optional[Dict[str, Any]] = None
        self.position = None # 'buy', 'sell' or None
        
        # DB connection for warm-up
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "dbname": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"]
        }
        
        self._warm_up()
        
        # Restaurar posición desde Redis con verificación de expiración automática
        self.position = self.restore_position(contract_duration_sec=self.timeframe_sec)

    def _warm_up(self):
        """Carga datos históricos desde la DB y los remuestrea en velas"""
        if not self.db_params["host"] or not self.db_params["password"]:
            self.logger.warning("⚠️ Saltando warm-up: Faltan credenciales de DB")
            return

        try:
            self.logger.info(f"🧱 Iniciando warm-up (Resampling 5m) para {self.params.get('symbol')}...")
            conn = psycopg2.connect(**self.db_params)
            with conn.cursor() as cur:
                # Traemos suficientes ticks para llenar el lookback de velas
                # 10,000 ticks a 2s/tick ~= 5.5 horas. Para 100 velas de 5m necesitamos 100*300 = 30,000s = 8.3h.
                # Pediremos 15,000 ticks.
                cur.execute("""
                    SELECT ts, last FROM market_ticks 
                    WHERE symbol = %s 
                    ORDER BY ts DESC LIMIT 15000
                """, (self.params.get("symbol"),))
                rows = cur.fetchall()
                
                # Invertir para orden cronológico
                rows.reverse()
                
                for ts_dt, last in rows:
                    ts = ts_dt.timestamp()
                    self._process_tick_internal(ts, float(last))
                    
            conn.close()
            self.logger.info(f"✅ Warm-up completado. Velas generadas: {len(self.candles)}")
        except Exception as e:
            self.logger.error(f"❌ Error en warm-up: {e}")

    def calculate_hurst(self, prices: np.ndarray) -> float:
        """Calcula el Exponente de Hurst (H)"""
        try:
            if len(prices) < 20: return 0.5
            lags = range(2, len(prices) // 2)
            tau = [np.std(np.subtract(prices[lag:], prices[:-lag])) for lag in lags]
            poly = np.polyfit(np.log(lags), np.log(tau), 1)
            return poly[0]
        except Exception as e:
            self.logger.error(f"Error calculating Hurst: {e}")
            return 0.5

    def _process_tick_internal(self, ts: float, last: float) -> bool:
        """Agrega un tick a la vela actual. Retorna True si se cerró una vela."""
        candle_ts = int(ts // self.timeframe_sec) * self.timeframe_sec
        closed = False

        if self.current_candle is None:
            self.current_candle = {'ts': candle_ts, 'open': last, 'high': last, 'low': last, 'close': last}
        elif candle_ts > self.current_candle['ts']:
            # Cerrar vela anterior
            self.candles.append(self.current_candle)
            if len(self.candles) > self.lookback_candles:
                self.candles.pop(0)
            
            # Iniciar nueva
            self.current_candle = {'ts': candle_ts, 'open': last, 'high': last, 'low': last, 'close': last}
            closed = True
        else:
            # Actualizar vela actual
            self.current_candle['high'] = max(self.current_candle['high'], last)
            self.current_candle['low'] = min(self.current_candle['low'], last)
            self.current_candle['close'] = last
            
        return closed

    def on_tick(self, tick: Tick) -> Optional[OrderSubmit]:
        # El framework llama a on_tick en tiempo real
        # tick.ts suele venir como string ISO desde NATS
        try:
            ts_dt = datetime.fromisoformat(tick.ts.replace("Z", "+00:00"))
            ts_val = ts_dt.timestamp()
        except Exception:
            # Fallback si ya es un objeto o tiene otro formato
            ts_val = tick.ts.timestamp() if hasattr(tick.ts, 'timestamp') else datetime.now().timestamp()
            
        candle_closed = self._process_tick_internal(ts_val, tick.last)
        
        if not candle_closed:
            return None
            
        # Solo operamos cuando se CIERRA una vela de 5m
        if len(self.candles) < self.lookback_candles:
            return None

        # 1. Extraer precios de CIERRE de las velas
        prices = np.array([c['close'] for c in self.candles])
        
        # 2. Calcular Filtro de Hurst
        current_hurst = self.calculate_hurst(prices[-self.hurst_window:])
        
        # 3. Lógica Donchian (usamos velas cerradas)
        donchian_slice = prices[-self.donchian_p:]
        upper_band = np.max(donchian_slice)
        lower_band = np.min(donchian_slice)
        
        # 4. Telemetría (Cada 5m)
        self.emit_telemetry({
            "hurst": float(current_hurst),
            "upper": float(upper_band),
            "lower": float(lower_band)
        })
        
        last_px = tick.last
        self.logger.info(f"🕯️ Candle Closed [{datetime.fromtimestamp(self.candles[-1]['ts']).strftime('%H:%M')}]: Px={last_px} | Hurst={current_hurst:.2f}")

        # --- Lógica de Señales ---
        if self.position is None:
            if current_hurst > self.hurst_threshold:
                if last_px > upper_band:
                    self.position = 'buy'
                    self.save_state("position_state", {"position": self.position, "ts": time.time()})
                    self.logger.info(f"🔥 SEÑAL COMPRA | Hurst {current_hurst:.2f} | Breakout Upper {upper_band}")
                    self._publish_signal(tick.symbol, 'buy', current_hurst, self.params.get('qty', 1.0))
                    return self.create_order(tick.symbol, 'buy', self.params.get('qty', 1.0))
                elif last_px < lower_band:
                    self.position = 'sell'
                    self.save_state("position_state", {"position": self.position, "ts": time.time()})
                    self.logger.info(f"🔥 SEÑAL VENTA | Hurst {current_hurst:.2f} | Breakout Lower {lower_band}")
                    self._publish_signal(tick.symbol, 'sell', current_hurst, self.params.get('qty', 1.0))
                    return self.create_order(tick.symbol, 'sell', self.params.get('qty', 1.0))
        
        else:
            if self.position == 'buy':
                if last_px < lower_band or current_hurst < 0.45:
                    self.position = None
                    self.save_state("position_state", {"position": None, "ts": time.time()})  # 💾 Con timestamp
                    self.logger.info(f"❄️ CIERRE COMPRA | Hurst {current_hurst:.2f} | Stop/Reverse {lower_band}")
                    return self.create_order(tick.symbol, 'sell', self.params.get('qty', 1.0), meta={"reason": "exit_signal"})
            elif self.position == 'sell':
                if last_px > upper_band or current_hurst < 0.45:
                    self.position = None
                    self.save_state("position_state", {"position": None, "ts": time.time()})  # 💾 Con timestamp
                    self.logger.info(f"❄️ CIERRE VENTA | Hurst {current_hurst:.2f} | Stop/Reverse {upper_band}")
                    return self.create_order(tick.symbol, 'buy', self.params.get('qty', 1.0), meta={"reason": "exit_signal"})

        return None

    def _publish_signal(self, symbol: str, side: str, hurst: float, qty: float):
        """Publica señal de trading a NATS para que AlertManager la reenvíe por Telegram."""
        if not self.nc:
            return
        try:
            payload = json.dumps({
                "strategy": self.__class__.__name__,
                "symbol": symbol,
                "side": side,
                "hurst": round(hurst, 4),
                "qty": qty,
                "ts": datetime.now(timezone.utc).isoformat()
            }).encode()
            asyncio.create_task(self.nc.publish("alerts.signal", payload))
        except Exception as e:
            self.logger.warning(f"⚠️ No se pudo publicar señal a NATS: {e}")

if __name__ == "__main__":
    NATS_URL = os.environ["NATS_URL"]
    runner = GenericRunnerV3(HurstDonchianStrategy, NATS_URL)
    asyncio.run(runner.run())

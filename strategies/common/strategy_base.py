import json
import asyncio
import time
import os
import redis as redis_lib
from models import Tick, OrderSubmit
from uuid import UUID, uuid4
from datetime import datetime, timezone
import logging
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod

class StrategyBase(ABC):
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        self.instance_id = UUID(instance_id)
        self.params = params
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        
        # NATS client (injected by runner)
        self.nc = None
        
        # Buffer de historial genérico
        self.lookback = int(params.get("lookback", 100))
        self.history: Dict[str, List[Tick]] = {}
        
        # Telemetry throttling
        self._last_telemetry_ts = 0

        # Redis client para persistencia de estado inter-reinicios
        redis_url = os.getenv("REDIS_URL")
        try:
            self._redis = redis_lib.from_url(redis_url) if redis_url else None
            if self._redis:
                self._redis.ping()
        except Exception:
            self._redis = None

    def _add_to_history(self, tick: Tick):
        symbol = tick.symbol
        if symbol not in self.history:
            self.history[symbol] = []
        
        self.history[symbol].append(tick)
        if len(self.history[symbol]) > self.lookback:
            self.history[symbol].pop(0)

    @abstractmethod
    def on_tick(self, tick: Tick) -> Optional[OrderSubmit]:
        """
        Método principal que debe implementar la estrategia.
        Retorna un objeto OrderSubmit si decide operar, de lo contrario None.
        """
        pass

    def create_order(self, symbol: str, side: str, qty: float, meta: Dict[str, Any] = None) -> OrderSubmit:
        """Helper para crear una orden estandarizada"""
        actual_meta = self.params.copy() if self.params else {}
        if meta:
            actual_meta.update(meta)
            
        return OrderSubmit(
            instance_id=self.instance_id,
            correlation_id=uuid4(),
            side=side,
            qty=qty,
            symbol=symbol,
            ts=datetime.now(timezone.utc).isoformat(),
            meta=actual_meta
        )

    def emit_telemetry(self, metrics: Dict[str, float], throttle_sec: float = 5.0):
        """
        Publica métricas internas en el subject strategy.metrics.{instance_id}
        con un mecanismo de throttling para evitar saturación.
        """
        if not self.nc:
            self.logger.warning("⚠️ Telemetría abortada: NATS client (self.nc) no inyectado")
            return

        now = time.time()
        if now - self._last_telemetry_ts < throttle_sec:
            return

        self._last_telemetry_ts = now
        
        # El servicio Vector espera este formato para el Bulk Insert
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "instance_id": str(self.instance_id),
            "metrics": metrics
        }
        
        subject = f"strategy.metrics.{self.instance_id}"
        self.logger.info(f"📤 Publicando telemetria: {subject} | {metrics}")
        
        try:
            # En nats-py (asyncio), publish es una coroutine y debe ser programada o awaited.
            # Como emit_telemetry se llama desde on_tick (sync), usamos create_task.
            asyncio.create_task(self.nc.publish(subject, json.dumps(payload).encode()))
        except Exception as e:
            self.logger.error(f"❌ Error programando publicación de telemetría: {e}")

    def save_state(self, key: str, value: Any, ttl_seconds: int = 86400):
        """
        Persiste un valor en Redis bajo 'strategy:{instance_id}:{key}'.
        TTL por defecto: 24 horas. Silencioso si Redis no está disponible.
        """
        if not self._redis:
            return
        try:
            redis_key = f"strategy:{self.instance_id}:{key}"
            self._redis.set(redis_key, json.dumps(value), ex=ttl_seconds)
            self.logger.debug(f"💾 State saved: {redis_key} = {value}")
        except Exception as e:
            self.logger.warning(f"⚠️ save_state failed: {e}")

    def load_state(self, key: str, default: Any = None) -> Any:
        """
        Recupera un valor previamente guardado en Redis.
        Retorna 'default' si no existe o si Redis no está disponible.
        """
        if not self._redis:
            return default
        try:
            redis_key = f"strategy:{self.instance_id}:{key}"
            raw = self._redis.get(redis_key)
            if raw:
                value = json.loads(raw)
                self.logger.info(f"🔄 State restored: {redis_key} = {value}")
                return value
        except Exception as e:
            self.logger.warning(f"⚠️ load_state failed: {e}")
        return default

    # ------------------------------------------------------------------
    # POSITION PERSISTENCE (convenience wrappers for all strategies)
    # ------------------------------------------------------------------

    def save_position(self, position: Any):
        """
        Guarda la posición actual + timestamp en Redis.
        Úsalo cada vez que la posición cambie:
            self.save_position('buy')
            self.save_position(None)
        """
        self.save_state("position_state", {
            "position": position,
            "ts": time.time()
        })

    def restore_position(self, contract_duration_sec: float = 300) -> Any:
        """
        Recupera la posición desde Redis con verificación de expiración.
        - Si el contrato ya expiró (edad > contract_duration_sec + 30s), retorna None.
        - Si sigue vigente, retorna la posición guardada.
        Úsalo al final de __init__:
            self.position = self.restore_position(contract_duration_sec=300)
        """
        saved = self.load_state("position_state", default=None)
        if saved and isinstance(saved, dict):
            saved_at = saved.get("ts", 0)
            age_sec = time.time() - saved_at
            grace = contract_duration_sec + 30  # 30s de margen extra
            if age_sec < grace:
                pos = saved.get("position")
                self.logger.info(f"🔄 Posición restaurada: {pos} (edad: {age_sec:.0f}s)")
                return pos
            else:
                self.logger.info(f"⏰ Posición EXPIRADA (edad: {age_sec:.0f}s > {grace:.0f}s). Neutral.")
                return None
        self.logger.info("ℹ️ Sin estado previo. Posición neutra.")
        return None

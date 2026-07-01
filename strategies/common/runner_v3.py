import os
import json
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Type
from uuid import UUID

from nats.aio.client import Client as NATS
from models import Tick, OrderSubmit

class GenericRunnerV3:
    def __init__(self, strategy_class, nats_url=None, instance_id=None):
        self.strategy_class = strategy_class
        self.nats_url = (nats_url or os.getenv("NATS_URL", "nats://localhost:4222")).strip()
        self.nc = NATS()
        self.strategy = None
        
        # Configuración desde entorno (inyectado por Factory Agent o Ansible)
        self.instance_id = os.getenv("INSTANCE_ID")
        self.symbol = os.getenv("SYMBOL") 
        self.broker = os.getenv("BROKER", "paper")
        self.qty = float(os.getenv("QTY", "0.0"))
        self.orders_subject = os.getenv("ORDERS_SUBJECT", "orders.intent")
        
        # Construir params desde entorno
        self.params = self._build_params()
        
        self.md_subjects = os.getenv("MARKET_SUBJECT", "").split(",")
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"RunnerV3-{self.instance_id[:8] if self.instance_id else 'None'}")

    def _build_params(self) -> Dict[str, Any]:
        params = {}
        # 1. Cargar desde STRATEGY_PARAMS (JSON) si existe
        params_raw = os.getenv("STRATEGY_PARAMS")
        if params_raw:
            try:
                params.update(json.loads(params_raw))
            except Exception:
                pass
        
        # 2. Cargar todas las variables de entorno que no son las estándar
        # Consideramos parámetros a las que son snake_case o que fueron inyectadas por Ansible
        # Para simplificar, tomamos todas las que CAPITALIZED y no son del sistema core
        omit = ["NATS_URL", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "PATH", "HOME"]
        for k, v in os.environ.items():
            if k not in omit and k.isupper():
                # Intentar convertir a float o int
                try:
                    if "." in v:
                        params[k.lower()] = float(v)
                    else:
                        params[k.lower()] = int(v)
                except ValueError:
                    params[k.lower()] = v
        return params



    async def run(self):
        try:
            if not self.instance_id:
                self.instance_id = os.getenv("INSTANCE_ID", "00000000-0000-0000-0000-000000000000")
                
            self.logger.info(f"🚀 Iniciando Runner V3 para {self.strategy_class.__name__}")
            self.logger.info(f"Parametros: {self.params}")
            
            # Inicializar la estrategia
            self.strategy = self.strategy_class(self.instance_id, self.params)
            
            # Conectar a NATS
            await self.nc.connect(
                servers=[self.nats_url],
                max_reconnect_attempts=20,
                reconnect_time_wait=2,
                error_cb=self.on_nats_error,
                reconnected_cb=self.on_nats_reconnected,
                disconnected_cb=self.on_nats_disconnected
            )

            self.strategy.nc = self.nc 
            self.logger.info(f"✅ Conectado a NATS: {self.nats_url}")
            
            # Emitir señal de vida inicial
            self.strategy.emit_telemetry({"heartbeat": 1.0}, throttle_sec=0)
            
            # Suscripciones
            await self.nc.subscribe(f"factory.control.params.{self.instance_id}", cb=self.on_control_params)
            await self.nc.subscribe("factory.control.ping", cb=self.on_ping)

            for subject in self.md_subjects:
                if subject.strip():
                    await self.nc.subscribe(subject.strip(), cb=self.on_market_data)
                    self.logger.info(f"📡 Suscrito a Market Data: {subject}")
                    
            asyncio.create_task(self.heartbeat_loop())

            self.logger.info("🟢 Runner V3 iniciado correctamente. Entrando en modo persistente (Event.wait)...")
            
            # Usamos un Event para que el loop sea eterno y no dependa de sleeps
            stop_event = asyncio.Event()
            await stop_event.wait()
            
            self.logger.critical("🚨 ADVERTENCIA: El Event.wait() ha terminado. El Runner se está cerrando.")

        except BaseException as e:
            self.logger.critical(f"❌ ERROR CRÍTICO EN RUNNER: {e}")
            self.logger.exception(e)
            raise

    async def heartbeat_loop(self):
        """Misión 3 - Asegura que el bot siempre reporte su estado aunque no haya ticks."""
        while True:
            try:
                if self.nc.is_connected:
                    self.strategy.emit_telemetry({"heartbeat": 1.0})
            except Exception:
                pass
            await asyncio.sleep(60)

    async def on_nats_error(self, e):
        self.logger.error(f"🌐 NATS Error: {e}")

    async def on_nats_disconnected(self):
        self.logger.warning("🌐 NATS Desconectado. Reintentando automáticamente...")

    async def on_nats_reconnected(self):
        self.logger.info("🌐 NATS RECONECTADO. Restaurando flujo...")
        # Al reconectar, nats-py restaura las suscripciones automáticamente.
        self.strategy.emit_telemetry({"reconnect": 1.0}, throttle_sec=0)

    async def on_market_data(self, msg):
        try:
            self.logger.info(f"📥 NATS Recv [{msg.subject}]: {msg.data.decode()[:100]}")
            data = json.loads(msg.data.decode())
            
            # Misión 2 - Resiliencia: Soportar ticks con campos parciales (ej: solo 'price')
            price = data.get("price") or data.get("last") or 0.0
            
            tick = Tick(
                ts=data.get("ts") or datetime.now().isoformat(),
                broker=data.get("broker", "unknown"),
                symbol=data.get("symbol", "unknown"),
                bid=float(data.get("bid") or price),
                ask=float(data.get("ask") or price),
                last=float(price),
                meta=data.get("meta", {})
            )
            
            # Misión 5 - Compatibilidad Dual: Soportar estrategias Async y Sync
            res = self.strategy.on_tick(tick)
            if asyncio.iscoroutine(res):
                orders = await res
            else:
                orders = res
            
            if orders:
                if isinstance(orders, list):
                    for o in orders:
                        await self.publish_order(o)
                else:
                    await self.publish_order(orders)
                
        except Exception:
            self.logger.exception("Error procesando tick")

    async def publish_order(self, order: OrderSubmit):
        payload = {
            "schema": "orders.submit.v1",
            "instance_id": str(order.instance_id),
            "correlation_id": str(order.correlation_id),
            "side": order.side,
            "qty": order.qty,
            "symbol": order.symbol,
            "ts": order.ts,
            "meta": order.meta
        }
        await self.nc.publish(self.orders_subject, json.dumps(payload).encode())
        self.logger.info(f"📤 Orden enviada: {order.side} {order.qty} {order.symbol}")

    async def on_control_params(self, msg):
        """Maneja la actualización de parámetros en caliente."""
        try:
            new_params = json.loads(msg.data.decode())
            self.logger.info(f"⚙️ Solicitantes de Hot-Swap recibidos: {new_params}")
            self.params.update(new_params)
            
            # Notificar a la estrategia si tiene el método implementado
            if hasattr(self.strategy, 'update_params'):
                self.strategy.update_params(new_params)
                self.logger.info("✅ Parámetros actualizados en la estrategia")
            else:
                # Intentar actualizar el atributo params directamente si existe
                if hasattr(self.strategy, 'params'):
                    self.strategy.params.update(new_params)
                    self.logger.info("⚠️ Atributo 'params' actualizado directamente (sin método update_params)")
                    
        except Exception as e:
            self.logger.error(f"❌ Error en Hot-Swap: {e}")

    async def on_ping(self, msg):
        """Responde a pings de salud para monitoreo."""
        status = {
            "instance_id": str(self.instance_id or "00000000"),
            "strategy": str(self.strategy_class.__name__),
            "symbol": str(self.symbol or "N/A"),
            "status": "RUNNING",
            "ts": datetime.now().isoformat()
        }
        await self.nc.publish(msg.reply, json.dumps(status).encode())

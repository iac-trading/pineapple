import os
import json
import asyncio
import logging
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
from nats.aio.client import Client as NATS
import ray

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [MICRO-MICRO] %(message)s")
logger = logging.getLogger("MicrostructureNLP")

NATS_URL = os.getenv("NATS_URL", "nats://192.168.100.200:4222")

DB = {
    "host":            os.getenv("POSTGRES_HOST", "192.168.100.201"),
    "port":            int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":          os.getenv("POSTGRES_DB", "trading"),
    "user":            os.getenv("POSTGRES_USER", "tsdb"),
    "password":        os.environ["POSTGRES_PASSWORD"],
    "connect_timeout": 5,
}

@ray.remote
class OFICalculator:
    def __init__(self):
        self.last_bid_price = 0.0
        self.last_bid_size = 0.0
        self.last_ask_price = 0.0
        self.last_ask_size = 0.0

    def update(self, best_bid_px, best_bid_sz, best_ask_px, best_ask_sz):
        # Calculation of Order Flow Imbalance (OFI)
        # Contis et al (2010), Cartea et al (2015)
        
        # Bid component
        if best_bid_px > self.last_bid_price:
            ib = best_bid_sz
        elif best_bid_px == self.last_bid_price:
            ib = best_bid_sz - self.last_bid_size
        else:
            ib = -self.last_bid_size

        # Ask component
        if best_ask_px < self.last_ask_price:
            ia = best_ask_sz
        elif best_ask_px == self.last_ask_price:
            ia = best_ask_sz - self.last_ask_size
        else:
            ia = -self.last_ask_size

        ofi = ib - ia
        
        self.last_bid_price = best_bid_px
        self.last_bid_size = best_bid_sz
        self.last_ask_price = best_ask_px
        self.last_ask_size = best_ask_sz
        
        return ofi

@ray.remote
class SentimentAnalyzer:
    def __init__(self):
        # En producción real, cargaríamos FinBERT aquí
        # from transformers import pipeline
        # self.nlp = pipeline("sentiment-analysis", model="ProsusAI/finbert")
        logger.info("SentimentAnalyzer (FinBERT placeholder) initialized.")

    def analyze(self, text):
        # Simulación de Score de Sentimiento (-1 a 1)
        # En el caso real: return self.nlp(text)[0]['score']
        score = np.random.uniform(-1, 1) # Placeholder
        return score

class MicrostructureService:
    def __init__(self):
        self.nc = NATS()
        self.ofi_calc = OFICalculator.remote()
        self.sentiment_node = SentimentAnalyzer.remote()
        self.vpin_buckets = []
        self.vpin_window = 50
        self.db_lock = asyncio.Lock()

    def _conn(self):
        return psycopg2.connect(**DB)

    async def save_signal(self, symbol, ofi=None, vpin=None, sentiment=None, is_toxic=False):
        """Persiste la señal calculada en la base de datos TimescaleDB."""
        try:
            async with self.db_lock:
                # Corremos el bloqueo de IO en un thread para no bloquear el loop asíncrono
                await asyncio.to_thread(self._execute_save, symbol, ofi, vpin, sentiment, is_toxic)
        except Exception as e:
            logger.error(f"Error persisting signal: {e}")

    def _execute_save(self, symbol, ofi, vpin, sentiment, is_toxic):
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO intelligence_signals (ts, symbol, ofi, vpin, sentiment, is_toxic)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (datetime.now(timezone.utc), symbol, ofi, vpin, sentiment, is_toxic)
            )
            conn.commit()

    async def start(self):
        await self.nc.connect(servers=[NATS_URL])
        logger.info(f"Connected to NATS at {NATS_URL}")

        # Suscribirse a Ticks L2 (Simulado o real si existe el stream)
        await self.nc.subscribe("md.*.*.l2", cb=self.on_l2_data)
        
        # Suscribirse a Noticias
        await self.nc.subscribe("news.stream", cb=self.on_news)

        while True:
            await asyncio.sleep(1)

    async def on_l2_data(self, msg):
        try:
            data = json.loads(msg.data.decode())
            symbol = data.get("symbol")
            
            # Extraer Best Bid/Ask y Tamaños
            b_px = data.get("bids")[0][0]
            b_sz = data.get("bids")[0][1]
            a_px = data.get("asks")[0][0]
            a_sz = data.get("asks")[0][1]

            # Calcular OFI vía Ray
            ofi = await self.ofi_calc.update.remote(b_px, b_sz, a_px, a_sz)
            
            # VPIN Simplified (Volume-synchronized Probability of Informed Trading)
            # Acumulamos el ratio en una ventana para suavizar la toxicidad
            vol = b_sz + a_sz + 1e-9
            current_toxic_ratio = abs(ofi) / vol
            self.vpin_buckets.append(current_toxic_ratio)
            if len(self.vpin_buckets) > self.vpin_window:
                self.vpin_buckets.pop(0)
            
            vpin_score = np.mean(self.vpin_buckets)
            
            # Publicar Score de Toxicidad y OFI
            payload = {
                "symbol": symbol,
                "ofi": ofi,
                "vpin": vpin_score,
                "is_toxic": vpin_score > 0.7, # Umbral de alerta
                "ts": datetime.now(timezone.utc).isoformat()
            }
            await self.nc.publish(f"intelligence.vpin.{symbol}", json.dumps(payload).encode())

            # PERSISTENCIA en base de datos para Backtest
            await self.save_signal(symbol, ofi=ofi, vpin=vpin_score, is_toxic=payload["is_toxic"])

        except Exception as e:
            logger.error(f"Error en L2 processing: {e}")

    async def on_news(self, msg):
        try:
            news = json.loads(msg.data.decode())
            text = news.get("text")
            
            # Analizar sentimiento vía Ray (FinBERT)
            sentiment_score = await self.sentiment_node.analyze.remote(text)
            
            logger.info(f"News Sentiment: {sentiment_score:.2f} for '{text[:50]}...'")
            
            # Publicar a NATS para que las estrategias lo consuman
            payload = {
                "source": news.get("source"),
                "sentiment": sentiment_score,
                "ts": datetime.now(timezone.utc).isoformat()
            }
            await self.nc.publish("intelligence.sentiment", json.dumps(payload).encode())
            
            # PERSISTENCIA en base de datos (Symbol genérico ya que las noticias suelen aplicar a varios)
            # Para scalping puro podemos asociarla al ticker principal si se detecta, 
            # de momento usamos 'GLOBAL' o el que venga en la metadata de news si existiera.
            await self.save_signal("GLOBAL", sentiment=sentiment_score)
            
        except Exception as e:
            logger.error(f"Error en News processing: {e}")

if __name__ == "__main__":
    if not ray.is_initialized():
        ray.init(address='auto', ignore_reinit_error=True)
    
    service = MicrostructureService()
    asyncio.run(service.start())

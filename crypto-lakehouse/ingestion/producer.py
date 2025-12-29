import asyncio
import json
import logging
from kafka import KafkaProducer
import websockets


# Config
KAFKA_TOPIC = "crypto_trades"
KAFKA_BOOTSTRAP = "localhost:9092"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

# Configure logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

async def stream_trades():
    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    logging.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")

    # Connect to Binance WebSocket
    async with websockets.connect(BINANCE_WS) as ws:
        logging.info(f"Connected to Binance WebSocket at {BINANCE_WS}")

        while True:
            try:
                # Read and parse the data
                msg = await ws.recv()
                raw = json.loads(msg)

                # Extract relevant fields
                trade = {
                    "symbol": raw["s"],
                    "price": float(raw["p"]),
                    "volume": float(raw["q"]),
                    "timestamp": raw["T"],
                    "trade_id": raw["a"],
                    "is_buyer_maker": raw["m"]
                }

                # Send to Kafka
                producer.send(KAFKA_TOPIC, value=trade)

                # Log every 50th trade for debugging
                if trade["trade_id"] % 50 == 0:
                    logging.info(f"Sent: {trade["price"]} | {trade["timestamp"]}")
            
            except Exception as e:
                logging.error(f"Error processing trade data: {e}")
                break

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(stream_trades())


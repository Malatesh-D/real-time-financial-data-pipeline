import yfinance as yf
from confluent_kafka import Producer
import json
import time

# 1. Connect using the industry-standard Confluent library
conf = {
    'bootstrap.servers': '127.0.0.1:29092',
    'client.id': 'stock-producer'
}

producer = Producer(conf)

symbols = ['RELIANCE.NS', 'TCS.NS', 'GOOGL', 'NVDA']

# 2. Callback function to confirm data actually arrived
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Success! Sent {msg.value().decode('utf-8')} to topic {msg.topic()}")

print("Starting Professional Producer... Fetching live prices.")

while True:
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            data = {
                "symbol": symbol,
                "price": ticker.fast_info['last_price'],
                "timestamp": time.time()
            }
            
            # 3. Serve delivery callback queue
            producer.poll(0)
            
            # 4. Produce the message to Kafka
            producer.produce(
                topic='stock_prices',
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            
    # 5. Flush ensures all messages are sent before sleeping
    producer.flush()
    
    print("--- Waiting 10 seconds to avoid API bans ---")
    time.sleep(10)
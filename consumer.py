from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import json
import time
from collections import deque

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'
DLQ_TOPIC = 'orders-dlq'
RETRY_TOPIC = 'orders-retry'

# Load Avro schema
with open('schemas/order.avsc', 'r') as f:
    schema_str = f.read()

# Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro deserializer
avro_deserializer = AvroDeserializer(
    schema_registry_client,
    schema_str,
    lambda order, ctx: order
)

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC, RETRY_TOPIC])

# DLQ Producer
dlq_producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
dlq_producer = Producer(dlq_producer_conf)

# Retry Producer
retry_producer = Producer(dlq_producer_conf)

# Price aggregation
price_sum = 0.0
order_count = 0
price_history = deque(maxlen=100)  # Keep last 100 prices

# Retry tracking
retry_counts = {}
MAX_RETRIES = 3

def calculate_running_average():
    """Calculate running average of prices"""
    if order_count == 0:
        return 0.0
    return price_sum / order_count

def process_order(order):
    """Process an order - simulates business logic with potential failures"""
    order_id = order['orderId']
    
    # Simulate random failures (10% chance)
    if random.random() < 0.1:
        raise Exception(f"Temporary processing failure for order {order_id}")
    
    # Update aggregation
    global price_sum, order_count
    price = order['price']
    price_sum += price
    order_count += 1
    price_history.append(price)
    
    running_avg = calculate_running_average()
    
    print(f"\n✓ Processed Order: {order_id} | Product: {order['product']} | Price: ${price:.2f}")
    print(f"  Running Average: ${running_avg:.2f} | Total Orders: {order_count}")
    
    return True

def send_to_dlq(order, error_message):
    """Send failed message to Dead Letter Queue"""
    dlq_message = {
        'original_order': order,
        'error': error_message,
        'timestamp': time.time()
    }
    
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        value=json.dumps(dlq_message).encode('utf-8')
    )
    dlq_producer.flush()
    print(f"✗ Sent to DLQ: Order {order['orderId']} - {error_message}")

def send_to_retry(order):
    """Send message to retry topic"""
    retry_producer.produce(
        topic=RETRY_TOPIC,
        value=json.dumps(order).encode('utf-8')
    )
    retry_producer.flush()
    print(f"↻ Sent to Retry: Order {order['orderId']}")

def consume_orders():
    """Main consumer loop with retry logic"""
    print("Starting consumer with real-time aggregation...")
    print("=" * 60)
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue
            
            try:
                # Deserialize message
                if msg.topic() == TOPIC:
                    order = avro_deserializer(
                        msg.value(),
                        SerializationContext(TOPIC, MessageField.VALUE)
                    )
                else:  # RETRY_TOPIC
                    order = json.loads(msg.value().decode('utf-8'))
                
                order_id = order['orderId']
                
                # Track retry attempts
                if order_id not in retry_counts:
                    retry_counts[order_id] = 0
                
                try:
                    # Process the order
                    process_order(order)
                    
                    # Success - commit offset
                    consumer.commit(msg)
                    
                    # Reset retry count on success
                    if order_id in retry_counts:
                        del retry_counts[order_id]
                    
                except Exception as e:
                    # Processing failed - implement retry logic
                    retry_counts[order_id] += 1
                    
                    if retry_counts[order_id] < MAX_RETRIES:
                        print(f"⚠ Processing failed (attempt {retry_counts[order_id]}/{MAX_RETRIES}): {e}")
                        send_to_retry(order)
                        consumer.commit(msg)
                        time.sleep(2)  # Backoff before retry
                    else:
                        # Max retries exceeded - send to DLQ
                        send_to_dlq(order, f"Max retries exceeded: {str(e)}")
                        consumer.commit(msg)
                        del retry_counts[order_id]
                
            except Exception as e:
                print(f"Error deserializing message: {e}")
                # Send unparseable messages directly to DLQ
                send_to_dlq({'raw_message': 'unparseable'}, str(e))
                consumer.commit(msg)
    
    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
        print(f"Final Statistics:")
        print(f"  Total Orders Processed: {order_count}")
        print(f"  Final Running Average: ${calculate_running_average():.2f}")
    
    finally:
        consumer.close()
        dlq_producer.flush()
        retry_producer.flush()

if __name__ == '__main__':
    import random
    consume_orders()
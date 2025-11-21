from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import json, time, traceback, random
from collections import deque

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'
DLQ_TOPIC = 'orders-dlq'
RETRY_TOPIC = 'orders-retry'

# Avro Schema Loading
with open('schemas/order.avsc', 'r') as f:
    schema_str = f.read()

schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

avro_deserializer = AvroDeserializer(
    schema_registry_client,
    schema_str,
    lambda order, ctx: order
)

# Consumer Configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC, RETRY_TOPIC])

# Force DLQ for these orders
ALWAYS_FAIL = {"12", "20", "32"}

# Producers
dlq_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
retry_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

# Aggregation Variables
price_sum = 0.0
order_count = 0
price_history = deque(maxlen=100)

retry_counts = {}
MAX_RETRIES = 3

# Helper Functions
def calculate_running_average():
    if order_count == 0:
        return 0.0
    return price_sum / order_count


def process_order(order):
    """Main business logic"""

    order_id = order['orderId']

    # Force DLQ for specific orders
    if order_id in ALWAYS_FAIL:
        raise Exception(f"Forced permanent failure for DLQ test (Order {order_id})")

    # Temporary random failures (10%)
    if random.random() < 0.1:
        raise Exception(f"Temporary processing failure for order {order_id}")

    global price_sum, order_count
    price = order['price']

    price_sum += price
    order_count += 1
    price_history.append(price)

    running_avg = calculate_running_average()

    print(f"\nProcessed Order: {order_id} | Product: {order['product']} | Price: ${price:.2f}")
    print(f"  Running Average: ${running_avg:.2f} | Total Orders: {order_count}")


def send_to_dlq(order, error_message, stack_trace=None):
    """Send failed message to DLQ as JSON"""

    dlq_message = {
        "failedOrder": order,
        "errorMessage": error_message,
        "stackTrace": stack_trace,
        "originalTopic": TOPIC,
        "timestamp": int(time.time())
    }

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        value=json.dumps(dlq_message).encode('utf-8')
    )

    dlq_producer.flush()

    print(f"Sent to DLQ: Order {order.get('orderId', 'UNKNOWN')} - {error_message}")


def send_to_retry(order):
    retry_producer.produce(
        topic=RETRY_TOPIC,
        value=json.dumps(order).encode('utf-8')
    )
    retry_producer.flush()
    print(f"Sent to Retry: Order {order['orderId']}")


# Main Consumer Loop
def consume_orders():
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
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                # Deserialization handling
                if msg.topic() == TOPIC:
                    order = avro_deserializer(
                        msg.value(),
                        SerializationContext(TOPIC, MessageField.VALUE)
                    )
                else:
                    order = json.loads(msg.value().decode('utf-8'))

                order_id = order['orderId']

                if order_id not in retry_counts:
                    retry_counts[order_id] = 0

                # Processing logic
                try:
                    process_order(order)
                    consumer.commit(msg)

                    if order_id in retry_counts:
                        del retry_counts[order_id]

                except Exception as e:
                    retry_counts[order_id] += 1

                    if retry_counts[order_id] < MAX_RETRIES:
                        print(f"Processing failed (attempt {retry_counts[order_id]}/{MAX_RETRIES}): {e}")
                        send_to_retry(order)
                        consumer.commit(msg)
                        time.sleep(1)

                    else:
                        send_to_dlq(
                            order,
                            f"Max retries exceeded: {str(e)}",
                            traceback.format_exc()
                        )
                        consumer.commit(msg)
                        del retry_counts[order_id]

            except Exception as e:
                print(f"Error deserializing message: {e}")

                send_to_dlq(
                    {"rawMessage": msg.value().decode('utf-8') if msg.value() else None},
                    "Deserialization Failed",
                    traceback.format_exc()
                )

                consumer.commit(msg)

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        print(f"Total Orders Processed: {order_count}")
        print(f"Final Running Average: ${calculate_running_average():.2f}")

    finally:
        consumer.close()
        dlq_producer.flush()
        retry_producer.flush()


if __name__ == '__main__':
    consume_orders()

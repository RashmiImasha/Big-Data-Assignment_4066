from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import random
import time
import json

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'

# Load Avro schema
with open('schemas/order.avsc', 'r') as f:
    schema_str = f.read()

# Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda order, ctx: order
)

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'order-producer'
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def generate_order(order_id):
    """Generate a random order"""
    products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Headphones']
    return {
        'orderId': str(order_id),
        'product': random.choice(products),
        'price': round(random.uniform(10.0, 1000.0), 2)
    }

def produce_orders(num_orders=100):
    """Produce orders to Kafka"""
    print(f"Starting to produce {num_orders} orders...")
    
    for i in range(1, num_orders + 1):
        order = generate_order(i)
        
        try:
            # Serialize and send
            serialized_value = avro_serializer(
                order,
                SerializationContext(TOPIC, MessageField.VALUE)
            )
            
            producer.produce(
                topic=TOPIC,
                value=serialized_value,
                callback=delivery_report
            )
            
            print(f"Produced: {order}")
            
            # Poll to handle callbacks
            producer.poll(0)
            
            # Small delay between messages
            time.sleep(0.5)
            
        except Exception as e:
            print(f"Error producing message: {e}")
    
    # Wait for all messages to be delivered
    producer.flush()
    print("All messages produced!")

if __name__ == '__main__':
    produce_orders(50)
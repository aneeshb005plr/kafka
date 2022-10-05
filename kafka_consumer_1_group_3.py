from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


# Global variables
API_KEY = 'RZU4KWLBV32BZHC2'
API_SECRET_KEY = 'Cqr/7x88Co25xGLwaSw43h+Piud5xtgfdjr7RzI44hzqiu4XUZusPPUmItSyXb7V'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = "SASL_SSL"
SSL_MECHANISM = 'PLAIN'
SCHEMA_ENDPOINT_URL = 'https://psrc-mw731.us-east-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = '5AKG3EGW3FL6IFJL'
SCHEMA_REGISTRY_API_SECRET = 'GHh7KrR966QOvG4IrLPfDRTi7diK77mLEOhpFUEmzUKce0EYaKCLFsNiRBpUl++K'
# Utility functions


def schema_config():
    return {
        'url': SCHEMA_ENDPOINT_URL,
        'basic.auth.user.info': f'{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}'
    }


def sasl_config():
    return {
        'sasl.mechanism': SSL_MECHANISM,
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'security.protocol': SECURITY_PROTOCOL,
        'sasl.username': API_KEY,
        'sasl.password': API_SECRET_KEY
    }


class Order:
    def __init__(self, order: dict):
        self.order = order

    @staticmethod
    def dict_to_order(data: dict, ctx):
        return Order(order=data)

    def __str__(self):
        return f'{self.order}'


def main(topic):
  # Create schema registry client
    schema_registry_client = SchemaRegistryClient(schema_config())

  # Get latest schema
    latest_schema = schema_registry_client.get_latest_version(
        'restaurant_order-value').schema.schema_str

    json_deserializer = JSONDeserializer(
        latest_schema, from_dict=Order.dict_to_order)

    consumer_config = sasl_config()
    consumer_config.update({
        'group.id': 'group3',
        'auto.offset.reset': 'earliest'
    })

    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    count = 0
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            order = json_deserializer(msg.value(), SerializationContext(
                msg.topic(), MessageField.VALUE))
            if order is not None:
                print(f'User record {msg.key()}: order: {order}\n')
                count += 1
                print(f'Message Count: {count}')

        except KeyboardInterrupt:
            break

    consumer.close()


main('restaurant_order')

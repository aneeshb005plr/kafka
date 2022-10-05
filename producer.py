import pandas as pd
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


# Global variables
FILE_PATH = "E:\\Big Data Engineering\\kafka\\Kafka_Assignment\\restaurant_orders.csv"
CSV_COLUMNS = ['order_num', "order_date", "item_name",
               "quantity", "product_price", "total_products"]
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

    def __str__(self):
        return f'{self.order}'


def get_order_instance(file_path):
    df = pd.read_csv(file_path)
    df = df.iloc[:]
    ls = []
    for data in df.values:
        order = Order(dict(zip(CSV_COLUMNS, data)))
        yield order


# convert object to dict before serialization
def order_to_dict(order: Order, ctx):
    return order.order


# Delivery report
def delivery_report(err, msg):
    if err is not None:
        print('Delivery failed for the user record {}: {}'.format(msg.key, err))
        return
    print('user record {} successfully produced and published to the topic \'{}\' in partition [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    # Create schema registry client
    schema_registry_client = SchemaRegistryClient(schema_config())

    # Get latest schema
    latest_schema = schema_registry_client.get_latest_version(
        'restaurant_order-value').schema.schema_str

    # creating JSON and String serializer
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(
        latest_schema, schema_registry_client, order_to_dict)

    # creating producer instance
    producer = Producer(sasl_config())

    producer.poll(0.0)
    try:
        for order in get_order_instance(FILE_PATH):
            print(order)
            producer.produce(topic=topic,
                             key=string_serializer(
                                 str(uuid4()), order_to_dict),
                             value=json_serializer(
                                 order, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()


main('restaurant_order')
# read_csv(FILE_PATH)

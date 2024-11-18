from confluent_kafka import Consumer
from zabbix_utils import ZabbixAPI, Sender
import json

# Настройки Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': '172.29.15.142:9092',
    'group.id': 'ops-mr',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,  # Включает автоматический коммит оффсетов
    'auto.commit.interval.ms': 5000  # Интервал автоматического коммита в миллисекундах
}

TOPIC = 'zabbix-test-items'

# Настройки Zabbix
ZABBIX_URL = 'http://172.29.39.131'
ZABBIX_USER = 'Admin'
ZABBIX_PASSWORD = 'zabbix'

# Настройки Sender
ZABBIX_SERVER = '172.29.39.131'
ZABBIX_PORT = 10051

try:
    # Подключение к серверу Zabbix API
    zabbix = ZabbixAPI(
        url=ZABBIX_URL,
        user=ZABBIX_USER,
        password=ZABBIX_PASSWORD
    )
    print("Успешное подключение к Zabbix API")
except Exception as e:
    print(f"Ошибка подключения к Zabbix API: {e}")
    exit(1)

# Инициализация Zabbix Sender
sender = Sender(server=ZABBIX_SERVER, port=ZABBIX_PORT)

# Инициализация Kafka Consumer
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([TOPIC])

def process_message(message):
    """Обработка сообщения из Kafka и отправка его в Zabbix"""
    try:
        # Декодируем JSON
        data = json.loads(message)
        host = data['host']['host']
        item_name = data['name']
        value = data['value']
        timestamp = data['clock']  # Используем clock из сообщения Kafka

        print(f"Обработка: Host={host}, Item={item_name}, Value={value}, Timestamp={timestamp}")

        # Проверяем, существует ли хост в Zabbix
        hosts = zabbix.host.get(filter={'host': host})
        if not hosts:
            print(f"Хост {host} не найден. Добавляем...")
            zabbix.host.create(
                host=host,
                interfaces=[{
                    "type": 1,
                    "main": 1,
                    "useip": 1,
                    "ip": "127.0.0.1",
                    "dns": "",
                    "port": "10050"
                }],
                groups=[{"groupid": "2"}]  # Группа по умолчанию
            )
            # Повторное получение хоста после добавления
            hosts = zabbix.host.get(filter={'host': host})

        # Получаем hostid
        host_id = hosts[0]['hostid']

        # Проверяем, существует ли элемент данных
        items = zabbix.item.get(filter={'hostid': host_id, 'name': item_name})
        if not items:
            print(f"Элемент данных {item_name} не найден. Добавляем...")
            zabbix.item.create(
                name=item_name,
                key_=f"{item_name.replace(' ', '_')}",
                hostid=host_id,
                type=2,  # Zabbix trapper
                value_type=3  # Numeric
            )

        # Отправляем значение в Zabbix через Sender
        response = sender.send_value(
            host=host,
            key=f"{item_name.replace(' ', '_')}",
            value=value,
            clock=timestamp  # Используем clock из Kafka
        )
        print(f"Ответ от Zabbix Sender: {response}")

    except Exception as e:
        print(f"Ошибка обработки сообщения: {e}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Ошибка Kafka: {msg.error()}")
            continue

        # Обрабатываем сообщение
        process_message(msg.value().decode('utf-8'))

except KeyboardInterrupt:
    print("Завершение работы")

finally:
    consumer.close()
    print("Kafka Consumer закрыт")

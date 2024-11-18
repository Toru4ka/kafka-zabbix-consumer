from confluent_kafka import Consumer
from zabbix_utils import ZabbixAPI, Sender, ItemValue
import json
import time

# Настройки Kafka
KAFKA_CONFIG = {
    'bootstrap.servers': '172.29.15.142:9092',
    'group.id': 'ops-mr',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Ручной коммит offset
}

TOPIC = 'zabbix-test-items'

# Настройки Zabbix
ZABBIX_URL = 'http://172.29.39.131'
ZABBIX_USER = 'Admin'
ZABBIX_PASSWORD = 'zabbix'

# Настройки Sender
ZABBIX_SERVER = '172.29.39.131'
ZABBIX_PORT = 10051

# Кэш для хостов и элементов данных
host_cache = {}
item_cache = {}
batch_data = []

# Инициализация
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


def get_host_id(host):
    """Получить ID хоста из Zabbix или добавить его, если он отсутствует."""
    if host in host_cache:
        return host_cache[host]

    hosts = zabbix.host.get(filter={'host': host})
    if hosts:
        host_id = hosts[0]['hostid']
        host_cache[host] = host_id
        return host_id

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
    hosts = zabbix.host.get(filter={'host': host})
    if hosts:
        host_id = hosts[0]['hostid']
        host_cache[host] = host_id
        return host_id
    return None


def get_item_key(host_id, item_name):
    """Получить ключ элемента данных или создать его, если он отсутствует."""
    key = f"{item_name.replace(' ', '_')}"
    if key in item_cache:
        return key

    items = zabbix.item.get(filter={'hostid': host_id, 'name': item_name})
    if items:
        item_cache[key] = key
        return key

    print(f"Элемент данных {item_name} не найден. Добавляем...")
    zabbix.item.create(
        name=item_name,
        key_=key,
        hostid=host_id,
        type=2,  # Zabbix trapper
        value_type=3  # Numeric
    )
    item_cache[key] = key
    return key


def process_message(msg):
    """Обработка одного сообщения из Kafka."""
    try:
        data = json.loads(msg)
        host = data['host']['host']
        item_name = data['name']
        value = data['value']
        timestamp = data['clock']  # Используем clock из сообщения Kafka

        print(f"Обработка: Host={host}, Item={item_name}, Value={value}, Timestamp={timestamp}")

        # Получаем host_id
        host_id = get_host_id(host)
        if not host_id:
            print(f"Ошибка: хост {host} не удалось создать или найти")
            return

        # Получаем ключ элемента данных
        key = get_item_key(host_id, item_name)

        # Добавляем данные в пакет как объект ItemValue
        batch_data.append(ItemValue(host=host, key=key, value=value, clock=timestamp))

        # Отправка данных пачкой, если накопили 10 записей
        if len(batch_data) >= 1:
            sender.send(batch_data)
            print(f"Отправлено {len(batch_data)} метрик в Zabbix")
            batch_data.clear()

    except Exception as e:
        print(f"Ошибка обработки сообщения: {e}")


def commit_offset(msg):
    """Коммит Offset после обработки сообщения."""
    try:
        consumer.commit(asynchronous=False)
    except Exception as e:
        print(f"Ошибка коммита Offset: {e}")


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

        # Коммитим Offset вручную
        commit_offset(msg)

except KeyboardInterrupt:
    print("Завершение работы")

finally:
    # Отправка оставшихся данных в Zabbix
    if batch_data:
        sender.send(batch_data)
        print(f"Отправлено {len(batch_data)} оставшихся метрик в Zabbix")
    consumer.close()
    print("Kafka Consumer закрыт")

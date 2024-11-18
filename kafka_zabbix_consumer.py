import yaml
from confluent_kafka import Consumer
from zabbix_utils import ZabbixAPI, Sender, ItemValue
import json
import time
import logging

# Загрузка конфигурации из YAML
def load_config(config_file):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

# Загружаем конфигурацию
config = load_config("config.yaml")

# Настройка логирования
log_level = config["logging"]["level"]
log_file = config["logging"]["file"]  # Путь к лог-файлу из конфигурации

logging.basicConfig(
    level=getattr(logging,log_level.upper()),  # Уровень логирования
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),  # Логирование в указанный файл
    ]
)
logger = logging.getLogger("kafka_zabbix_consumer")

# Настройки Kafka
# Извлечение настроек из конфигурации
KAFKA_CONFIG = {
    'bootstrap.servers': config['kafka']['bootstrap_servers'],
    'group.id': config['kafka']['group_id'],
    'auto.offset.reset': config['kafka']['auto_offset_reset'],
    'enable.auto.commit': config['kafka']['enable_auto_commit']
}

TOPIC = config['batch']['topic']
SEND_INTERVAL = config['batch']['send_interval'] # Интервал отправки в секундах

# Настройки Zabbix
ZABBIX_URL = config['zabbix']['url']
ZABBIX_USER = config['zabbix']['user']
ZABBIX_PASSWORD = config['zabbix']['password']
# Настройки Sender
ZABBIX_SERVER = config['zabbix']['server']
ZABBIX_PORT = config['zabbix']['port']

# Кэш для хостов и элементов данных
host_cache = {}
item_cache = {}
batch_data = []
# Таймер для отправки данных
last_send_time = time.time()  # Время последней отправки

# Инициализация
try:
    # Подключение к серверу Zabbix API
    zabbix = ZabbixAPI(
        url=ZABBIX_URL,
        user=ZABBIX_USER,
        password=ZABBIX_PASSWORD
    )
    logger.info("Успешное подключение к Zabbix API")
except Exception as e:
    logger.error(f"Ошибка подключения к Zabbix API: {e}")
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

    logger.warning(f"Хост {host} не найден. Добавляем...")
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

    logger.warning(f"Элемент данных {item_name} не найден. Добавляем...")
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

        logger.debug(f"Обработка: Host={host}, Item={item_name}, Value={value}, Timestamp={timestamp}")

        # Получаем host_id
        host_id = get_host_id(host)
        if not host_id:
            logger.error(f"Ошибка: хост {host} не удалось создать или найти")
            return

        # Получаем ключ элемента данных
        key = get_item_key(host_id, item_name)

        # Добавляем данные в пакет как объект ItemValue
        batch_data.append(ItemValue(host=host, key=key, value=value, clock=timestamp))

    except Exception as e:
        logger.error(f"Ошибка обработки сообщения: {e}")


def send_batch_if_needed():
    """Отправка метрик, если прошел интервал времени."""
    global last_send_time
    current_time = time.time()
    if current_time - last_send_time >= SEND_INTERVAL and batch_data:
        try:
            sender.send(batch_data)
            logger.info(f"Отправлено {len(batch_data)} метрик в Zabbix")
            batch_data.clear()
            last_send_time = current_time
        except Exception as e:
            logger.error(f"Ошибка отправки метрик в Zabbix: {e}")


def commit_offset(msg):
    """Коммит Offset после обработки сообщения."""
    try:
        consumer.commit(asynchronous=False)
    except Exception as e:
        logger.error(f"Ошибка коммита Offset: {e}")


try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            # Проверяем необходимость отправки данных
            send_batch_if_needed()
            continue
        if msg.error():
            logger.error(f"Ошибка Kafka: {msg.error()}")
            continue

        # Обрабатываем сообщение
        process_message(msg.value().decode('utf-8'))

        # Коммитим Offset вручную
        commit_offset(msg)

        # Проверяем необходимость отправки данных
        send_batch_if_needed()

except KeyboardInterrupt:
    logger.info("Завершение работы")

finally:
    # Отправка оставшихся данных в Zabbix
    if batch_data:
        sender.send(batch_data)
        logger.info(f"Отправлено {len(batch_data)} оставшихся метрик в Zabbix")
    consumer.close()
    logger.info("Kafka Consumer закрыт")

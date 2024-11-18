# Kafka Zabbix Consumer

Kafka Zabbix Consumer позволяет реплицировать метрики из одного Zabbix-сервера в другой с использованием Apache Kafka. Этот инструмент может быть полезен для синхронизации данных или репликации метрик между различными системами мониторинга.

## О проекте

Если вы хотите реплицировать метрики из одного Zabbix в другой, это руководство для вас. Ниже приведены шаги для настройки:

Настраиваем внутренний Zabbix для отправки данных в Kafka.
Устанавливаем Kafka и настраиваем Kafka connector for Zabbix server. Документация.
Клонируем и настраиваем этот проект.
Настраиваем сервис и конфигурации согласно инструкциям ниже.

### Установка и настройка

#### 1. Требования
Python 3.8 или выше

Установленные зависимости из requirements.txt:
```
confluent-kafka==2.6.0
PyYAML==6.0.2
zabbix-utils==2.0.1
```

#### 2. Установка
Установка необходимых пакетов для venv
```sh
sudo apt install python3.x-venv
```

Клонирование проекта
```sh
cd /opt
git clone https://github.com/Toru4ka/kafka-zabbix-consumer.git
cd kafka-zabbix-consumer
```

Создание виртуального окружения
```sh
python3 -m venv venv
source venv/bin/activate
```

Установка зависимостей
```sh
pip install -r requirements.txt
```

Редактирование конфигурации
```sh
vi config.yaml
```

#### 3. Конфигурационный файл (config.yaml)
Пример файла конфигурации:
```yaml
kafka:  
  bootstrap_servers: "server:port"  
  group_id: "group_id"  
  auto_offset_reset: "earliest/latest"  
  enable_auto_commit: false  

zabbix:  
  url: "http://127.0.0.1"  
  user: "Admin"  
  password: "password"  
  server: "127.0.0.1"  
  port: 10051  

batch:  
  send_interval: 60  
  topic: "zabbix-test-items"  

logging:  
  level: "INFO"  
  file: "/var/log/zabbix/kafka_zabbix_consumer.log"
```

#### 4. Настройка systemd
Создайте симлинк для systemd:
```sh
cd /etc/systemd/system/
sudo ln -s /opt/kafka-zabbix-consumer/kafka-zabbix-consumer.service kafka-zabbix-consumer.service
```

Пример systemd unit файла:
```ini
[Unit]
Description=Kafka to Zabbix Consumer Service
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/kafka-zabbix-consumer
ExecStart=/opt/kafka-zabbix-consumer/venv/bin/python /opt/kafka-zabbix-consumer/kafka_zabbix_consumer.py
Restart=always
RestartSec=5
User=zabbix
Group=zabbix
Environment=PYTHONUNBUFFERED=1
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
```

Запустите сервис:
```sh
sudo systemctl daemon-reload
sudo systemctl enable kafka-zabbix-consumer.service
sudo systemctl start kafka-zabbix-consumer.service
```
### Логирование
Логи хранятся в файле, указанном в config.yaml (например, /var/log/zabbix/kafka_zabbix_consumer.log).

Для настройки ротации используйте logrotate:

Создайте файл конфигурации:
```sh
sudo vi /etc/logrotate.d/kafka_zabbix_consumer
```

Пример конфигурации:
```sh
/var/log/zabbix/kafka_zabbix_consumer.log {
    daily
    missingok
    rotate 7
    compress
    delaycompress
    notifempty
    create 0640 zabbix zabbix
    postrotate
        systemctl restart kafka-zabbix-consumer.service
    endscript
}
```
Проверьте logrotate:
```sh
sudo logrotate -d /etc/logrotate.d/kafka_zabbix_consumer
sudo logrotate /etc/logrotate.d/kafka_zabbix_consumer
```
### Полезные команды

Проверка наполнения топика Kafka:
```sh
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server=ip:port --describe --all-groups
```
Просмотр оффсетов топика:
```sh
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list ip:port --topic zabbix-test-items
```

Поддержка

Если у вас возникли вопросы, создайте issue в этом репозитории или свяжитесь с автором.

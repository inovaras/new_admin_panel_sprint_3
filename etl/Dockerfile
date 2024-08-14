# Используем базовый образ Python
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /etl

# Копируем файлы проекта в рабочую директорию
COPY requirements.txt ./
COPY . /etl

# Создаем и активируем виртуальное окружение
RUN python -m venv venv
RUN /bin/bash -c "source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"

# Запускаем основной скрипт
CMD ["/bin/bash", "-c", "source venv/bin/activate && python etl.py"]

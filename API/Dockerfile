FROM python:3.8.10

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt
RUN mkdir -p /opt/kafka_toolkit

RUN chmod +x /app/wait-for-it.sh

CMD ["./wait-for-it.sh", "mainkafka:9092", "--", "python", "app.py"]
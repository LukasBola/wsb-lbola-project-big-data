import argparse
import json
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from confluent_kafka import Producer
from faker import Faker

PRODUCT_CATALOG = [
    {"item": "yogurt", "category": "dairy", "base_price": 3.20},
    {"item": "potatoes", "category": "vegetables", "base_price": 2.10},
    {"item": "apples", "category": "fruit", "base_price": 2.80},
    {"item": "bananas", "category": "fruit", "base_price": 3.10},
    {"item": "carrots", "category": "vegetables", "base_price": 2.40},
    {"item": "cheese", "category": "dairy", "base_price": 8.50},
    {"item": "bread", "category": "bakery", "base_price": 4.20},
    {"item": "rice", "category": "grains", "base_price": 5.90},
    {"item": "pasta", "category": "grains", "base_price": 6.20},
    {"item": "eggs", "category": "dairy", "base_price": 7.30},
]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
PAYMENT_METHODS = ["card", "blik", "cash", "mobile_wallet"]
SALES_CHANNELS = ["store", "online", "pickup"]

faker = Faker()


@dataclass
class Metrics:
    sent_ok: int = 0
    sent_error: int = 0
    ack_latency_total_ms: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuous Kafka producer for synthetic order events."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092,localhost:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=10.0,
        help="Target event rate. Use values > 0.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after this many events. 0 means run forever.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=0,
        help="Stop after this many seconds. 0 means no duration limit.",
    )
    parser.add_argument(
        "--report-every-seconds",
        type=int,
        default=5,
        help="How often to print metrics snapshot.",
    )
    return parser.parse_args()


def create_order() -> dict:
    now_dt = datetime.now()
    # Rozkladamy zakupy na ostatnie 4 tygodnie, z losowa pora dnia.
    purchase_dt = now_dt - timedelta(
        days=random.randint(0, 27),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    now_ms = int(now_dt.timestamp() * 1000)
    weekday_num = purchase_dt.weekday()
    product = random.choice(PRODUCT_CATALOG)
    quantity = faker.random_int(min=1, max=20)
    unit_price = round(
        random.uniform(product["base_price"] * 0.85, product["base_price"] * 1.20), 2
    )
    discount_pct = random.choice([0, 0, 0, 5, 10, 15])
    total_amount = round(quantity * unit_price * (1 - discount_pct / 100), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "user": faker.name(),
        "item": product["item"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_pct": discount_pct,
        "total_amount": total_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "sales_channel": random.choice(SALES_CHANNELS),
        "store_city": faker.city(),
        "purchase_datetime": purchase_dt.isoformat(timespec="seconds"),
        "purchase_date": purchase_dt.date().isoformat(),
        "purchase_time": purchase_dt.strftime("%H:%M:%S"),
        "weekday_name": WEEKDAYS[weekday_num],
        "weekday_num": weekday_num,
        "hour_of_day": purchase_dt.hour,
        "is_weekend": weekday_num >= 5,
        "event_time_ms": now_ms,
    }


def main() -> None:
    args = parse_args()
    if args.events_per_second <= 0:
        raise ValueError("--events-per-second must be greater than 0")

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    metrics = Metrics()
    should_stop = False

    def stop_handler(signum, frame):  # noqa: ARG001
        nonlocal should_stop
        should_stop = True

    def delivery_report(err, msg, started_at):
        if err is not None:
            metrics.sent_error += 1
            print(f"[producer][error] {err}")
            return

        metrics.sent_ok += 1
        metrics.ack_latency_total_ms += (time.time() - started_at) * 1000

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    start = time.time()
    last_report = start
    next_send_time = start
    produced = 0

    while not should_stop:
        now = time.time()
        if args.duration_seconds > 0 and now - start >= args.duration_seconds:
            break
        if args.max_events > 0 and produced >= args.max_events:
            break

        order = create_order()
        payload = json.dumps(order).encode("utf-8")
        produce_start = time.time()
        try:
            producer.produce(
                topic=args.topic,
                key=order["order_id"].encode("utf-8"),
                value=payload,
                callback=lambda err, msg, started=produce_start: delivery_report(
                    err, msg, started
                ),
            )
            produced += 1
        except BufferError:
            producer.poll(0.1)
            continue

        producer.poll(0)
        next_send_time += 1.0 / args.events_per_second
        sleep_for = next_send_time - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_send_time = time.time()

        if time.time() - last_report >= args.report_every_seconds:
            elapsed = max(time.time() - start, 0.001)
            throughput = metrics.sent_ok / elapsed
            avg_ack_ms = (
                metrics.ack_latency_total_ms / metrics.sent_ok if metrics.sent_ok else 0.0
            )
            print(
                "[producer][metrics] "
                f"sent_ok={metrics.sent_ok} sent_error={metrics.sent_error} "
                f"throughput_eps={throughput:.2f} avg_ack_ms={avg_ack_ms:.2f}"
            )
            last_report = time.time()

    producer.flush(10)
    elapsed = max(time.time() - start, 0.001)
    throughput = metrics.sent_ok / elapsed
    avg_ack_ms = metrics.ack_latency_total_ms / metrics.sent_ok if metrics.sent_ok else 0.0
    print(
        "[producer][summary] "
        f"sent_ok={metrics.sent_ok} sent_error={metrics.sent_error} "
        f"throughput_eps={throughput:.2f} avg_ack_ms={avg_ack_ms:.2f} "
        f"elapsed_s={elapsed:.2f}"
    )


if __name__ == "__main__":
    main()

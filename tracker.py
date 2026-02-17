import argparse
import json
import time
from dataclasses import dataclass

from confluent_kafka import Consumer


@dataclass
class Metrics:
    processed: int = 0
    errors: int = 0
    latency_total_ms: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka consumer for order stream with manual commits."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092,localhost:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--group-id", default="order-tracker")
    parser.add_argument("--commit-every", type=int, default=100)
    parser.add_argument("--report-every-seconds", type=int, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metrics = Metrics()
    started = time.time()
    last_report = started

    consumer_config = {
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([args.topic])
    print(f"[consumer] subscribed to topic={args.topic} group_id={args.group_id}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                metrics.errors += 1
                print(f"[consumer][error] {msg.error()}")
                continue

            try:
                payload = msg.value().decode("utf-8")
                order = json.loads(payload)
            except Exception as exc:  # pylint: disable=broad-except
                metrics.errors += 1
                print(f"[consumer][error] invalid message: {exc}")
                continue

            metrics.processed += 1
            event_time_ms = order.get("event_time_ms")
            if isinstance(event_time_ms, int):
                metrics.latency_total_ms += (time.time() * 1000) - event_time_ms

            print(
                "[consumer][event] "
                f"offset={msg.offset()} partition={msg.partition()} "
                f"order_id={order.get('order_id')} item={order.get('item')} "
                f"quantity={order.get('quantity')}"
            )

            if args.commit_every > 0 and metrics.processed % args.commit_every == 0:
                consumer.commit(asynchronous=False)

            if time.time() - last_report >= args.report_every_seconds:
                elapsed = max(time.time() - started, 0.001)
                throughput = metrics.processed / elapsed
                avg_latency_ms = (
                    metrics.latency_total_ms / metrics.processed
                    if metrics.processed
                    else 0.0
                )
                print(
                    "[consumer][metrics] "
                    f"processed={metrics.processed} errors={metrics.errors} "
                    f"throughput_eps={throughput:.2f} avg_end_to_end_latency_ms={avg_latency_ms:.2f}"
                )
                last_report = time.time()
    except KeyboardInterrupt:
        print("[consumer] stopping")
    finally:
        try:
            consumer.commit(asynchronous=False)
        except Exception:  # pylint: disable=broad-except
            pass
        consumer.close()

        elapsed = max(time.time() - started, 0.001)
        throughput = metrics.processed / elapsed
        avg_latency_ms = (
            metrics.latency_total_ms / metrics.processed if metrics.processed else 0.0
        )
        print(
            "[consumer][summary] "
            f"processed={metrics.processed} errors={metrics.errors} "
            f"throughput_eps={throughput:.2f} avg_end_to_end_latency_ms={avg_latency_ms:.2f} "
            f"elapsed_s={elapsed:.2f}"
        )


if __name__ == "__main__":
    main()

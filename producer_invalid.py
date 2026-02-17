import argparse
import json
import random
import signal
import time
from dataclasses import dataclass

from confluent_kafka import Producer

from producer import create_order

INVALID_MODES = [
    "missing_unit_price",
    "missing_quantity",
    "missing_both",
    "non_positive_unit_price",
    "non_positive_quantity",
]


@dataclass
class Metrics:
    sent_ok: int = 0
    sent_error: int = 0
    ack_latency_total_ms: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka producer that emits intentionally invalid order events."
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
    parser.add_argument(
        "--invalid-mode",
        default="random",
        choices=["random"] + INVALID_MODES,
        help="Invalid payload strategy. random picks one strategy for each event.",
    )
    return parser.parse_args()


def create_invalid_order(mode: str) -> dict:
    order = create_order()

    selected_mode = random.choice(INVALID_MODES) if mode == "random" else mode
    if selected_mode == "missing_unit_price":
        order.pop("unit_price", None)
    elif selected_mode == "missing_quantity":
        order.pop("quantity", None)
    elif selected_mode == "missing_both":
        order.pop("unit_price", None)
        order.pop("quantity", None)
    elif selected_mode == "non_positive_unit_price":
        order["unit_price"] = 0.0
    elif selected_mode == "non_positive_quantity":
        order["quantity"] = 0

    order["invalid_mode"] = selected_mode
    return order


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
            print(f"[producer_invalid][error] {err}")
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

        order = create_invalid_order(args.invalid_mode)
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
                "[producer_invalid][metrics] "
                f"sent_ok={metrics.sent_ok} sent_error={metrics.sent_error} "
                f"throughput_eps={throughput:.2f} avg_ack_ms={avg_ack_ms:.2f}"
            )
            last_report = time.time()

    producer.flush(10)
    elapsed = max(time.time() - start, 0.001)
    throughput = metrics.sent_ok / elapsed
    avg_ack_ms = metrics.ack_latency_total_ms / metrics.sent_ok if metrics.sent_ok else 0.0
    print(
        "[producer_invalid][summary] "
        f"sent_ok={metrics.sent_ok} sent_error={metrics.sent_error} "
        f"throughput_eps={throughput:.2f} avg_ack_ms={avg_ack_ms:.2f} "
        f"elapsed_s={elapsed:.2f}"
    )


if __name__ == "__main__":
    main()

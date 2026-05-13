from prometheus_client import Counter, start_http_server

processed_total = Counter("python_chain_processed_total", "Successfully processed events")
failed_total = Counter("python_chain_failed_total", "Failed events")
dlq_sent_total = Counter("python_chain_dlq_sent_total", "Events sent to DLQ")
produced_total = Counter("python_chain_produced_total", "Produced source events")


def start_metrics_server(port: int) -> None:
    start_http_server(port)

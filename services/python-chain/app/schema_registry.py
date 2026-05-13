import json
import logging
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings

LOGGER = logging.getLogger(__name__)


def _auth() -> HTTPBasicAuth:
    username, password = settings.schema_registry_user_info.split(":", 1)
    return HTTPBasicAuth(username, password)


def load_schema(name: str) -> dict:
    path = Path(__file__).resolve().parents[1] / "schemas" / name
    return json.loads(path.read_text(encoding="utf-8"))


@retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=1, min=1, max=10))
def register_json_schema(subject: str, schema: dict) -> int:
    response = requests.post(
        f"{settings.schema_registry_url}/subjects/{subject}/versions",
        auth=_auth(),
        json={
            "schemaType": "JSON",
            "schema": json.dumps(schema, separators=(",", ":")),
        },
        timeout=10,
        verify=False,
    )
    response.raise_for_status()
    schema_id = int(response.json()["id"])
    LOGGER.info(
        "schema_registered",
        extra={"_subject": subject, "_schema_id": schema_id},
    )
    return schema_id


@retry(stop=stop_after_attempt(8), wait=wait_exponential(multiplier=1, min=1, max=10))
def assert_json_schema_compatible(subject: str, schema: dict) -> None:
    response = requests.post(
        f"{settings.schema_registry_url}/compatibility/subjects/{subject}/versions/latest",
        auth=_auth(),
        json={
            "schemaType": "JSON",
            "schema": json.dumps(schema, separators=(",", ":")),
        },
        timeout=10,
        verify=False,
    )
    response.raise_for_status()
    is_compatible = bool(response.json().get("is_compatible"))
    if not is_compatible:
        raise ValueError(f"schema is not compatible with latest subject version: {subject}")
    LOGGER.info("schema_compatible", extra={"_subject": subject})


def register_contracts() -> None:
    input_subject = f"{settings.input_topic}-value"
    register_json_schema(input_subject, load_schema("order_event_v1.json"))
    order_event_v2 = load_schema("order_event_v2.json")
    assert_json_schema_compatible(input_subject, order_event_v2)
    register_json_schema(input_subject, order_event_v2)
    register_json_schema(f"{settings.output_topic}-value", load_schema("processed_order_event.json"))

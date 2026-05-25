from fastapi import FastAPI, HTTPException
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest
from starlette.responses import Response

from app.config import settings
from app.kafka import PreferencesEventPublisher, ensure_topics
from app.models import CategoriesPatch, ModePatch, Preference, PreferenceUpsert
from app.repository import PreferencesRepository

app = FastAPI(title=settings.service_name)

preference_updates_total = Counter(
    "preference_updates_total",
    "Total preference create/update operations",
)

repository = PreferencesRepository()
publisher = PreferencesEventPublisher()


@app.on_event("startup")
def startup() -> None:
    repository.init_schema()
    ensure_topics()


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict[str, str]:
    return {"status": "ready"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/preferences/{chat_id}", response_model=Preference)
def get_preferences(chat_id: str) -> Preference:
    preference = repository.get(chat_id)
    if preference is None:
        raise HTTPException(status_code=404, detail="preferences not found")
    return preference


@app.put("/v1/preferences/{chat_id}", response_model=Preference)
def put_preferences(chat_id: str, payload: PreferenceUpsert) -> Preference:
    preference = repository.upsert(chat_id, payload)
    publisher.publish_changed(preference)
    preference_updates_total.inc()
    return preference


@app.patch("/v1/preferences/{chat_id}/mode", response_model=Preference)
def patch_mode(chat_id: str, payload: ModePatch) -> Preference:
    preference = repository.update_mode(chat_id, payload.mode.value)
    if preference is None:
        raise HTTPException(status_code=404, detail="preferences not found")
    publisher.publish_changed(preference)
    preference_updates_total.inc()
    return preference


@app.patch("/v1/preferences/{chat_id}/categories", response_model=Preference)
def patch_categories(chat_id: str, payload: CategoriesPatch) -> Preference:
    preference = repository.update_categories(chat_id, [category.value for category in payload.categories])
    if preference is None:
        raise HTTPException(status_code=404, detail="preferences not found")
    publisher.publish_changed(preference)
    preference_updates_total.inc()
    return preference

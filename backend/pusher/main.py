import json
import logging
import os

logging.basicConfig(level=logging.INFO)

import firebase_admin
from fastapi import FastAPI

from routers import pusher, metrics
from utils.http_client import close_all_clients

# Self-host: when AUTH_PROVIDER=oidc, Firebase Admin isn't used; skip init so
# the container boots without Firebase credentials. Matches backend/main.py.
if os.environ.get('AUTH_PROVIDER', '').lower() == 'oidc':
    logging.info("AUTH_PROVIDER=oidc — skipping firebase_admin init (Firebase Auth not used)")
elif os.environ.get('SERVICE_ACCOUNT_JSON'):
    service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
    credentials = firebase_admin.credentials.Certificate(service_account_info)
    firebase_admin.initialize_app(credentials)
else:
    firebase_admin.initialize_app()

app = FastAPI()
app.include_router(pusher.router)
app.include_router(metrics.router)

paths = ['_temp', '_samples', '_segments', '_speech_profiles']
for path in paths:
    if not os.path.exists(path):
        os.makedirs(path)


@app.on_event("shutdown")
async def shutdown_event():
    await close_all_clients()


@app.get('/health')
async def health_check():
    return {"status": "healthy"}

import hashlib
import json
import os
import sys
import types
import uuid


# Self-host: when DB_BACKEND=mongo, replace `google.cloud.firestore` and
# `google.cloud.firestore_v1.base_query` in sys.modules with our MongoDB-backed
# shim BEFORE the real packages get imported anywhere. After this runs, every
# `from google.cloud import firestore` across database/* and utils/* picks up
# FirestoreCompatClient transparently — zero call-site changes.
if os.environ.get('DB_BACKEND', '').lower() == 'mongo':
    from database import firestore_compat as _fc

    _shim = types.ModuleType('google.cloud.firestore')
    _shim.Client = _fc.FirestoreCompatClient
    _shim.Query = _fc.Query
    _shim.SERVER_TIMESTAMP = _fc.SERVER_TIMESTAMP
    _shim.DELETE_FIELD = _fc.DELETE_FIELD
    _shim.Increment = _fc.Increment
    _shim.ArrayUnion = _fc.ArrayUnion
    _shim.ArrayRemove = _fc.ArrayRemove
    _shim.transactional = _fc.transactional
    _shim.DocumentReference = _fc.DocumentReference
    _shim.DocumentSnapshot = _fc.DocumentSnapshot
    _shim.CollectionReference = _fc.CollectionReference

    _base_query = types.ModuleType('google.cloud.firestore_v1.base_query')
    _base_query.FieldFilter = _fc.FieldFilter
    _base_query.BaseCompositeFilter = _fc.BaseCompositeFilter

    # The real google.cloud.firestore_v1 re-exports FieldFilter / transactional /
    # etc. at its top level. Call sites hit both import paths, so mirror the
    # re-exports here rather than forcing a codebase-wide sweep.
    _v1 = types.ModuleType('google.cloud.firestore_v1')
    _v1.FieldFilter = _fc.FieldFilter
    _v1.BaseCompositeFilter = _fc.BaseCompositeFilter
    _v1.transactional = _fc.transactional
    _v1.Increment = _fc.Increment
    _v1.ArrayUnion = _fc.ArrayUnion
    _v1.ArrayRemove = _fc.ArrayRemove
    _v1.SERVER_TIMESTAMP = _fc.SERVER_TIMESTAMP
    _v1.DELETE_FIELD = _fc.DELETE_FIELD
    _v1.Query = _fc.Query
    _v1.Client = _fc.FirestoreCompatClient
    _v1.DocumentReference = _fc.DocumentReference
    _v1.DocumentSnapshot = _fc.DocumentSnapshot
    _v1.CollectionReference = _fc.CollectionReference
    _v1.base_query = _base_query

    sys.modules['google.cloud.firestore'] = _shim
    sys.modules['google.cloud.firestore_v1'] = _v1
    sys.modules['google.cloud.firestore_v1.base_query'] = _base_query

from google.cloud import firestore  # noqa: E402

if os.environ.get('DB_BACKEND', '').lower() != 'mongo' and os.environ.get('SERVICE_ACCOUNT_JSON'):
    service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
    # create google-credentials.json
    with open('google-credentials.json', 'w') as f:
        json.dump(service_account_info, f)

db = firestore.Client()


def get_users_uid():
    users_ref = db.collection('users')
    return [str(doc.id) for doc in users_ref.stream()]


def document_id_from_seed(seed: str) -> uuid.UUID:
    """Avoid repeating the same data"""
    seed_hash = hashlib.sha256(seed.encode('utf-8')).digest()
    generated_uuid = uuid.UUID(bytes=seed_hash[:16], version=4)
    return str(generated_uuid)

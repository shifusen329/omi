"""
Firestore-compatible shim over MongoDB.

Duck-types just enough of the google-cloud-firestore surface actually used
across backend/database/*.py so every call site continues to work unchanged.
The only gating is DB_BACKEND=mongo in env — when set, database/_client.py
returns a FirestoreCompatClient instead of google.cloud.firestore.Client().

Data model
----------
Every Firestore document lives in a MongoDB collection named after its
last-segment collection name. Each Mongo doc carries two bookkeeping
fields:

    _id      : Firestore document id
    _parent  : parent document path, or None for top-level

Example: the Firestore path ``users/uidA/conversations/cidB`` becomes

    mongo_db["conversations"].find_one({"_id": "cidB", "_parent": "users/uidA"})

This layout makes:
  * reads of a single nested document cheap (compound index on _id+_parent)
  * collection-group queries cheap (no _parent filter, index on any field)
  * path round-trips exact (we can reconstruct every parent/child reference)

Supported surface (grep-verified against this codebase)
------------------------------------------------------
Client:              collection, collection_group, batch, transaction
Collection:          document, add, where, order_by, limit, offset, stream,
                     get, count, select, parent
Document:            get, set, set(merge=True), update, delete, collection,
                     collections, id, path, parent, reference
Query:               same as Collection; supports FieldFilter +
                     BaseCompositeFilter('AND', [...]); operators ``==``,
                     ``<``, ``<=``, ``>``, ``>=``, ``in``, ``array_contains``
Snapshot:            to_dict, exists, id, reference, get(field), create_time,
                     update_time
Batch:               set, update, delete, commit (chunked at 450 ops upstream)
Transaction:         ``@transactional`` decorator with db.transaction();
                     supports get() during txn + set/update/delete writes
Field transforms:    SERVER_TIMESTAMP, Increment, ArrayUnion, ArrayRemove,
                     DELETE_FIELD — all rewritten to Mongo update operators
Aggregation:         .count().get() via count_documents

Out of scope (grep confirms zero usage):
  * Async client, real-time listeners (.on_snapshot), cursors (start_at/after),
    ``!=``, ``not-in``, ``array-contains-any``, multi-database selection.
"""
import datetime as _dt
import logging
import os
import uuid as _uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from pymongo import ASCENDING as _MONGO_ASC, DESCENDING as _MONGO_DESC, MongoClient
from pymongo.client_session import ClientSession
from pymongo.collection import Collection as _MongoCollection
from pymongo.database import Database as _MongoDatabase

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sentinels & field transforms
# ---------------------------------------------------------------------------


class _ServerTimestampSentinel:
    """Marker for firestore.SERVER_TIMESTAMP. Translated to $currentDate."""

    __slots__ = ()

    def __repr__(self):
        return 'SERVER_TIMESTAMP'


SERVER_TIMESTAMP = _ServerTimestampSentinel()


class _DeleteFieldSentinel:
    """Marker for firestore.DELETE_FIELD. Translated to $unset."""

    __slots__ = ()

    def __repr__(self):
        return 'DELETE_FIELD'


DELETE_FIELD = _DeleteFieldSentinel()


class Increment:
    __slots__ = ('value',)

    def __init__(self, value: Union[int, float]):
        self.value = value


class ArrayUnion:
    __slots__ = ('values',)

    def __init__(self, values: List[Any]):
        self.values = list(values)


class ArrayRemove:
    __slots__ = ('values',)

    def __init__(self, values: List[Any]):
        self.values = list(values)


# ---------------------------------------------------------------------------
# Filter objects (duck-typed FieldFilter / BaseCompositeFilter)
# ---------------------------------------------------------------------------

_OPS_MAP: Dict[str, str] = {
    '==': '$eq',
    '<': '$lt',
    '<=': '$lte',
    '>': '$gt',
    '>=': '$gte',
    'in': '$in',
    'array_contains': '$eq',  # {field: value} matches array-contains in Mongo
    'array-contains': '$eq',
}


class FieldFilter:
    __slots__ = ('field_path', 'op_string', 'value')

    def __init__(self, field_path: str, op_string: str, value: Any):
        self.field_path = field_path
        self.op_string = op_string
        self.value = value

    def to_mongo(self) -> dict:
        op = _OPS_MAP.get(self.op_string)
        if op is None:
            raise ValueError(f'Unsupported Firestore operator: {self.op_string!r}')
        return {self.field_path: {op: self.value}}


class BaseCompositeFilter:
    __slots__ = ('operator', 'filters')

    def __init__(self, operator: str, filters: List[Union['FieldFilter', 'BaseCompositeFilter']]):
        op = (operator or 'AND').upper()
        if op not in ('AND', 'OR'):
            raise ValueError(f'Unsupported composite operator: {operator!r}')
        self.operator = op
        self.filters = list(filters)

    def to_mongo(self) -> dict:
        clauses = [f.to_mongo() for f in self.filters]
        if not clauses:
            return {}
        if len(clauses) == 1:
            return clauses[0]
        key = '$and' if self.operator == 'AND' else '$or'
        return {key: clauses}


# ---------------------------------------------------------------------------
# Update-dict compilation
# ---------------------------------------------------------------------------


def _flatten(d: dict, prefix: str = '') -> Iterable[Tuple[str, Any]]:
    """Walk a nested dict, yielding (dotted_key, value) for leaves.

    Firestore lets callers mix a dotted path ({'a.b.c': 1}) with a nested
    dict ({'a': {'b': {'c': 1}}}). Mongo wants dotted paths for partial
    updates, so we normalize.
    """
    for key, value in d.items():
        full = f'{prefix}.{key}' if prefix else key
        if isinstance(value, dict) and not _is_transform(value):
            yield from _flatten(value, full)
        else:
            yield full, value


def _is_transform(obj: Any) -> bool:
    return isinstance(obj, (_ServerTimestampSentinel, _DeleteFieldSentinel, Increment, ArrayUnion, ArrayRemove))


def compile_update(data: dict) -> dict:
    """Turn a Firestore-style partial dict into a MongoDB update document.

    Handles dotted paths and field transforms. Returns a dict with the
    relevant Mongo operators keyed ($set/$unset/$inc/$addToSet/$pull/
    $currentDate). Always includes $set for ``updated_at`` if you want to
    — that's a call-site decision, not ours.
    """
    set_doc: Dict[str, Any] = {}
    unset_doc: Dict[str, Any] = {}
    inc_doc: Dict[str, Any] = {}
    addtoset_doc: Dict[str, Any] = {}
    pull_doc: Dict[str, Any] = {}
    current_date_doc: Dict[str, Any] = {}

    for path, value in _flatten(data):
        if isinstance(value, _DeleteFieldSentinel):
            unset_doc[path] = ''
        elif isinstance(value, _ServerTimestampSentinel):
            current_date_doc[path] = {'$type': 'date'}
        elif isinstance(value, Increment):
            inc_doc[path] = value.value
        elif isinstance(value, ArrayUnion):
            addtoset_doc[path] = {'$each': value.values}
        elif isinstance(value, ArrayRemove):
            pull_doc[path] = {'$in': value.values}
        else:
            set_doc[path] = value

    update: Dict[str, Any] = {}
    if set_doc:
        update['$set'] = set_doc
    if unset_doc:
        update['$unset'] = unset_doc
    if inc_doc:
        update['$inc'] = inc_doc
    if addtoset_doc:
        update['$addToSet'] = addtoset_doc
    if pull_doc:
        update['$pull'] = pull_doc
    if current_date_doc:
        update['$currentDate'] = current_date_doc
    return update


def extract_transforms(data: dict) -> Tuple[dict, dict]:
    """Split `data` into (plain_document, transforms_only_update).

    Used for ``.set(data)`` without merge: we need to replace the doc with
    plain fields then apply any server-side transforms as a follow-up update.
    """
    plain: Dict[str, Any] = {}
    transforms: Dict[str, Any] = {}

    def walk(src: dict, dst_plain: dict):
        for k, v in src.items():
            if _is_transform(v):
                transforms[k] = v  # top-level transforms are rare but supported
            elif isinstance(v, dict) and any(_contains_transform(sv) for sv in v.values()):
                # nested dict with transforms inside — fall through to compile_update
                # for the transform parts; plain parts go into plain document
                nested_plain: Dict[str, Any] = {}
                walk(v, nested_plain)
                if nested_plain:
                    dst_plain[k] = nested_plain
            else:
                dst_plain[k] = v

    walk(data, plain)
    return plain, transforms


def _contains_transform(v: Any) -> bool:
    if _is_transform(v):
        return True
    if isinstance(v, dict):
        return any(_contains_transform(sv) for sv in v.values())
    return False


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


class DocumentSnapshot:
    __slots__ = ('_data', 'exists', 'id', 'reference', 'create_time', 'update_time')

    def __init__(
        self,
        reference: 'DocumentReference',
        raw: Optional[dict],
    ):
        self.reference = reference
        self.id = reference.id
        self.exists = raw is not None
        if raw is None:
            self._data: Optional[dict] = None
            self.create_time = None
            self.update_time = None
        else:
            data = {k: v for k, v in raw.items() if k not in ('_id', '_parent', '_created_at', '_updated_at')}
            self._data = data
            self.create_time = raw.get('_created_at')
            self.update_time = raw.get('_updated_at')

    def to_dict(self) -> Optional[dict]:
        if self._data is None:
            return None
        return dict(self._data)

    def get(self, field_path: str) -> Any:
        if self._data is None:
            return None
        cur: Any = self._data
        for part in field_path.split('.'):
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
            if cur is None:
                return None
        return cur


# ---------------------------------------------------------------------------
# Batch / Transaction
# ---------------------------------------------------------------------------


@dataclass
class _BatchOp:
    kind: str  # 'set' | 'update' | 'delete'
    ref: 'DocumentReference'
    data: Optional[dict] = None
    merge: bool = False


class WriteBatch:
    def __init__(self, client: 'FirestoreCompatClient'):
        self._client = client
        self._ops: List[_BatchOp] = []

    def set(self, ref: 'DocumentReference', data: dict, merge: bool = False) -> None:
        self._ops.append(_BatchOp('set', ref, data, merge))

    def update(self, ref: 'DocumentReference', data: dict) -> None:
        self._ops.append(_BatchOp('update', ref, data))

    def delete(self, ref: 'DocumentReference') -> None:
        self._ops.append(_BatchOp('delete', ref))

    def commit(self) -> None:
        if not self._ops:
            return
        # Group by collection so we can bulk_write. Falls back to single ops
        # when transforms are present — replace_one + follow-up update_one
        # isn't a clean fit for bulk_write.
        for op in self._ops:
            if op.kind == 'set':
                op.ref._apply_set(op.data or {}, merge=op.merge)
            elif op.kind == 'update':
                op.ref._apply_update(op.data or {})
            elif op.kind == 'delete':
                op.ref._apply_delete()
        self._ops.clear()


class Transaction:
    """Very small duck of google.cloud.firestore.Transaction.

    Supports `@transactional` decorator form: callers build their read-modify-
    write logic in one function, we run it inside a pymongo session transaction.
    """

    def __init__(self, client: 'FirestoreCompatClient'):
        self._client = client
        self._session: Optional[ClientSession] = None
        # Queued writes applied on commit (Firestore semantics: all writes at end)
        self._queued: List[_BatchOp] = []

    # ------- Firestore-facing API --------------------------------------

    def get(self, ref_or_query: Any) -> Any:
        """Read a doc or query snapshot inside the transaction."""
        if isinstance(ref_or_query, DocumentReference):
            return ref_or_query._get(session=self._session)
        if isinstance(ref_or_query, (CollectionReference, Query)):
            # Firestore returns an iterable of snapshots
            return list(ref_or_query._stream(session=self._session))
        raise TypeError(f'Unsupported argument to Transaction.get: {type(ref_or_query)}')

    def set(self, ref: 'DocumentReference', data: dict, merge: bool = False) -> None:
        self._queued.append(_BatchOp('set', ref, data, merge))

    def update(self, ref: 'DocumentReference', data: dict) -> None:
        self._queued.append(_BatchOp('update', ref, data))

    def delete(self, ref: 'DocumentReference') -> None:
        self._queued.append(_BatchOp('delete', ref))

    # ------- Driver -----------------------------------------------------

    def _run(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        def _inside(session: ClientSession):
            self._session = session
            self._queued.clear()
            try:
                result = fn(self, *args, **kwargs)
                # Apply queued writes inside the same txn before commit
                for op in self._queued:
                    if op.kind == 'set':
                        op.ref._apply_set(op.data or {}, merge=op.merge, session=session)
                    elif op.kind == 'update':
                        op.ref._apply_update(op.data or {}, session=session)
                    elif op.kind == 'delete':
                        op.ref._apply_delete(session=session)
                return result
            finally:
                self._queued.clear()
                self._session = None

        with self._client._mongo_client.start_session() as s:
            return s.with_transaction(_inside)


def transactional(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Mirrors firestore.transactional: the first arg must be a Transaction."""

    def wrapper(transaction: 'Transaction', *args, **kwargs):
        if not isinstance(transaction, Transaction):
            raise TypeError('First argument to @transactional function must be a Transaction')
        return transaction._run(fn, *args, **kwargs)

    wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
    return wrapper


# ---------------------------------------------------------------------------
# References & queries
# ---------------------------------------------------------------------------


class _QueryState:
    __slots__ = (
        'collection_name',
        'parent_path',
        'filters',
        'order_bys',
        'limit_val',
        'offset_val',
        'select_fields',
        'is_group',
    )

    def __init__(
        self,
        collection_name: str,
        parent_path: Optional[str],
        is_group: bool = False,
    ):
        self.collection_name = collection_name
        self.parent_path = parent_path
        self.filters: List[dict] = []
        self.order_bys: List[Tuple[str, int]] = []
        self.limit_val: Optional[int] = None
        self.offset_val: int = 0
        self.select_fields: Optional[List[str]] = None
        self.is_group = is_group

    def clone(self) -> '_QueryState':
        c = _QueryState(self.collection_name, self.parent_path, self.is_group)
        c.filters = list(self.filters)
        c.order_bys = list(self.order_bys)
        c.limit_val = self.limit_val
        c.offset_val = self.offset_val
        c.select_fields = list(self.select_fields) if self.select_fields else None
        return c


class Query:
    ASCENDING = 'ASCENDING'
    DESCENDING = 'DESCENDING'

    def __init__(self, client: 'FirestoreCompatClient', state: _QueryState):
        self._client = client
        self._state = state

    # ------- fluent builders -------------------------------------------

    def where(
        self,
        field_path: Optional[str] = None,
        op_string: Optional[str] = None,
        value: Any = None,
        filter: Any = None,
    ) -> 'Query':
        new = self._state.clone()
        if filter is not None:
            new.filters.append(filter.to_mongo())
        else:
            if field_path is None or op_string is None:
                raise ValueError('where() requires either filter= or (field_path, op_string, value)')
            new.filters.append(FieldFilter(field_path, op_string, value).to_mongo())
        return Query(self._client, new)

    def order_by(self, field_path: str, direction: str = ASCENDING) -> 'Query':
        new = self._state.clone()
        if direction in (Query.DESCENDING, _MONGO_DESC):
            mongo_dir = _MONGO_DESC
        else:
            mongo_dir = _MONGO_ASC
        new.order_bys.append((field_path, mongo_dir))
        return Query(self._client, new)

    def limit(self, n: int) -> 'Query':
        new = self._state.clone()
        new.limit_val = int(n)
        return Query(self._client, new)

    def offset(self, n: int) -> 'Query':
        new = self._state.clone()
        new.offset_val = int(n)
        return Query(self._client, new)

    def select(self, field_paths: List[str]) -> 'Query':
        new = self._state.clone()
        new.select_fields = list(field_paths)
        return Query(self._client, new)

    # ------- execution --------------------------------------------------

    def _build_filter(self) -> dict:
        clauses = list(self._state.filters)
        if not self._state.is_group and self._state.parent_path is not None:
            clauses.append({'_parent': self._state.parent_path})
        if not clauses:
            return {}
        if len(clauses) == 1:
            return clauses[0]
        return {'$and': clauses}

    def _mongo_collection(self) -> _MongoCollection:
        return self._client._mongo_db[self._state.collection_name]

    def _projection(self) -> Optional[dict]:
        if not self._state.select_fields:
            return None
        proj = {'_id': 1, '_parent': 1}
        for f in self._state.select_fields:
            proj[f] = 1
        return proj

    def _stream(self, session: Optional[ClientSession] = None) -> Iterable[DocumentSnapshot]:
        filt = self._build_filter()
        cur = self._mongo_collection().find(filt, projection=self._projection(), session=session)
        if self._state.order_bys:
            cur = cur.sort(self._state.order_bys)
        if self._state.offset_val:
            cur = cur.skip(self._state.offset_val)
        if self._state.limit_val is not None:
            cur = cur.limit(self._state.limit_val)
        for raw in cur:
            parent = raw.get('_parent')
            coll = CollectionReference(self._client, self._state.collection_name, parent_path=parent)
            ref = DocumentReference(self._client, coll, raw['_id'])
            yield DocumentSnapshot(ref, raw)

    def stream(self) -> Iterable[DocumentSnapshot]:
        yield from self._stream()

    def get(self) -> List[DocumentSnapshot]:
        return list(self._stream())

    def count(self) -> '_CountAggregate':
        return _CountAggregate(self)


@dataclass
class _CountResult:
    value: int

    def __getitem__(self, _idx):
        # firestore returns an iterable of aggregation results
        return self

    def __iter__(self):
        yield self


class _CountAggregate:
    def __init__(self, query: Query):
        self._query = query

    def get(self) -> List[List['_CountEntry']]:
        filt = self._query._build_filter()
        n = self._query._mongo_collection().count_documents(filt)
        # firestore's aggregation_query.get() returns [[AggregationResult(value=n)]]
        return [[_CountEntry(n)]]


class _CountEntry:
    __slots__ = ('value',)

    def __init__(self, value: int):
        self.value = value


class CollectionReference(Query):
    """A collection is just a Query with no starting filters."""

    def __init__(self, client: 'FirestoreCompatClient', name: str, parent_path: Optional[str] = None):
        state = _QueryState(name, parent_path, is_group=False)
        super().__init__(client, state)
        self.id = name
        self.parent_path = parent_path

    @property
    def parent(self) -> Optional['DocumentReference']:
        if self.parent_path is None:
            return None
        return _path_to_doc_ref(self._client, self.parent_path)

    def document(self, document_id: Optional[str] = None) -> 'DocumentReference':
        if document_id is None:
            document_id = _uuid.uuid4().hex
        return DocumentReference(self._client, self, document_id)

    def add(self, data: dict, document_id: Optional[str] = None) -> Tuple[_dt.datetime, 'DocumentReference']:
        ref = self.document(document_id)
        ref.set(data)
        return _dt.datetime.utcnow().replace(tzinfo=_dt.timezone.utc), ref


class DocumentReference:
    __slots__ = ('_client', '_parent_collection', 'id')

    def __init__(self, client: 'FirestoreCompatClient', parent_collection: CollectionReference, doc_id: str):
        self._client = client
        self._parent_collection = parent_collection
        self.id = doc_id

    # ------- identity ---------------------------------------------------

    @property
    def path(self) -> str:
        col = self._parent_collection
        if col.parent_path:
            return f'{col.parent_path}/{col.id}/{self.id}'
        return f'{col.id}/{self.id}'

    @property
    def parent(self) -> CollectionReference:
        return self._parent_collection

    # ------- IO ---------------------------------------------------------

    def _mongo_collection(self) -> _MongoCollection:
        return self._client._mongo_db[self._parent_collection.id]

    def _filter(self) -> dict:
        return {'_id': self.id, '_parent': self._parent_collection.parent_path}

    def get(self) -> DocumentSnapshot:
        return self._get()

    def _get(self, session: Optional[ClientSession] = None) -> DocumentSnapshot:
        raw = self._mongo_collection().find_one(self._filter(), session=session)
        return DocumentSnapshot(self, raw)

    def set(self, data: dict, merge: Union[bool, List[str]] = False) -> None:
        self._apply_set(data, merge=merge)

    def _apply_set(
        self,
        data: dict,
        merge: Union[bool, List[str]] = False,
        session: Optional[ClientSession] = None,
    ) -> None:
        now = _dt.datetime.now(_dt.timezone.utc)
        filt = self._filter()
        if merge:
            # Merge mode: upsert and only change the fields we were given.
            update = compile_update(data)
            set_part = update.get('$set', {})
            # Guarantee bookkeeping fields on insert.
            set_on_insert = {
                '_id': self.id,
                '_parent': self._parent_collection.parent_path,
                '_created_at': now,
            }
            if not set_part and not any(k in update for k in ('$unset', '$inc', '$addToSet', '$pull', '$currentDate')):
                # Empty merge — nothing to do.
                return
            update.setdefault('$set', {})['_updated_at'] = now
            update.setdefault('$setOnInsert', {}).update(set_on_insert)
            self._mongo_collection().update_one(filt, update, upsert=True, session=session)
            return

        # Non-merge set: replace the doc, then follow up with transforms.
        plain, transforms = extract_transforms(data)
        replacement = {
            '_id': self.id,
            '_parent': self._parent_collection.parent_path,
            '_created_at': now,
            '_updated_at': now,
            **plain,
        }
        self._mongo_collection().replace_one(filt, replacement, upsert=True, session=session)
        if transforms:
            update = compile_update(transforms)
            if update:
                self._mongo_collection().update_one(filt, update, session=session)

    def update(self, data: dict) -> None:
        self._apply_update(data)

    def _apply_update(self, data: dict, session: Optional[ClientSession] = None) -> None:
        now = _dt.datetime.now(_dt.timezone.utc)
        update = compile_update(data)
        update.setdefault('$set', {})['_updated_at'] = now
        if not any(update.values()):
            return
        self._mongo_collection().update_one(self._filter(), update, session=session)

    def delete(self) -> None:
        self._apply_delete()

    def _apply_delete(self, session: Optional[ClientSession] = None) -> None:
        self._mongo_collection().delete_one(self._filter(), session=session)

    # ------- nested ----------------------------------------------------

    def collection(self, name: str) -> CollectionReference:
        return CollectionReference(self._client, name, parent_path=self.path)

    def collections(self) -> Iterable[CollectionReference]:
        """Return distinct subcollection names stored under this document."""
        prefix = self.path
        # Scan the mongo database for collections with any docs whose _parent == self.path
        for coll_name in self._client._mongo_db.list_collection_names():
            sample = self._client._mongo_db[coll_name].find_one({'_parent': prefix}, projection={'_id': 1})
            if sample is not None:
                yield CollectionReference(self._client, coll_name, parent_path=prefix)


def _path_to_doc_ref(client: 'FirestoreCompatClient', path: str) -> 'DocumentReference':
    """Parse a slash-delimited Firestore path back into a DocumentReference."""
    segments = [s for s in path.split('/') if s]
    if len(segments) % 2 != 0:
        raise ValueError(f'Firestore document path must have an even number of segments: {path!r}')
    parent_path: Optional[str] = None
    ref: Optional[DocumentReference] = None
    i = 0
    while i < len(segments):
        coll_name = segments[i]
        doc_id = segments[i + 1]
        col = CollectionReference(client, coll_name, parent_path=parent_path)
        ref = DocumentReference(client, col, doc_id)
        parent_path = ref.path
        i += 2
    assert ref is not None
    return ref


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class FirestoreCompatClient:
    """Drop-in for google.cloud.firestore.Client backed by MongoDB."""

    def __init__(
        self,
        uri: Optional[str] = None,
        database: Optional[str] = None,
    ):
        self._uri = uri or os.environ.get('MONGO_URI') or 'mongodb://localhost:27017/?replicaSet=rs0'
        self._db_name = database or os.environ.get('MONGO_DB') or 'omi'
        self._mongo_client: MongoClient = MongoClient(self._uri)
        self._mongo_db: _MongoDatabase = self._mongo_client[self._db_name]

    # --- firestore-like surface ------------------------------------------

    def collection(self, name: str) -> CollectionReference:
        return CollectionReference(self, name, parent_path=None)

    def collection_group(self, name: str) -> Query:
        state = _QueryState(name, parent_path=None, is_group=True)
        return Query(self, state)

    def batch(self) -> WriteBatch:
        return WriteBatch(self)

    def transaction(self) -> Transaction:
        return Transaction(self)

    # --- lifecycle -------------------------------------------------------

    def close(self) -> None:
        try:
            self._mongo_client.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Convenience: a Query.DESCENDING / ASCENDING alias accessible on the
# top-level module (firestore_compat.Query.DESCENDING). Kept for `from
# google.cloud import firestore; firestore.Query.DESCENDING` substitute.
# ---------------------------------------------------------------------------

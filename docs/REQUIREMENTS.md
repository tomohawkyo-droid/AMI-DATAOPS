# AMI-DATAOPS Requirements

Gap analysis and forward-looking requirements for a polyglot persistence framework. Benchmarked against Spring Data, SQLAlchemy, Neomodel, pgvector, HashiCorp Vault, OpenTelemetry, and industry patterns for multi-backend data access.

## Current State

AMI-DATAOPS targets 7 storage backends behind a unified DAO abstraction:

| Backend | Implementation | Status |
|---------|---------------|--------|
| PostgreSQL (SQL) | `implementations/sql/` | Exists, has critical bugs (see audit) |
| Dgraph (Graph) | `implementations/graph/` | Exists, DQL injection vulnerabilities |
| Redis (In-Memory) | `implementations/mem/` | Exists, KEYS * vulnerability |
| pgvector (Vector) | `implementations/vec/` | Exists, minimal |
| REST (External APIs) | `implementations/rest/` | Exists, envelope guessing |
| OpenBao (Vault) | `implementations/vault/` | Exists, imports nonexistent package |
| Prometheus (Timeseries) | `implementations/timeseries/` | Exists, update/delete on append-only DB |

**Core abstractions:** BaseDAO, StorageModel, UnifiedCRUD, DAOFactory, StorageConfig

**Known issues:** 70 documented (see ARCHITECTURE-REVISION.md and ARCHITECTURE-REVISION-AUDIT-FINDINGS.md). DAO registry never populated, god class model, injection flaws, placebo features, hardcoded crypto keys.

Everything below defines what the system **must** do once remediation is complete.

---

## 1. DAO Contract and Backend Interface

The current BaseDAO mandates 23 abstract methods, most of which are no-ops for most backends. The interface needs a tiered capability model.

### 1.1 Tiered Capability Interface

Split the monolithic BaseDAO into composable capability protocols:

- **ReadableDAO** -- `find_by_id()`, `find()`, `count()`
- **WritableDAO** -- `create()`, `update()`, `delete()`
- **BulkDAO** -- `bulk_create()`, `bulk_update()`, `bulk_delete()` (batched, not sequential loops)
- **QueryableDAO** -- `raw_query()`, `raw_write_query()` with parameterized inputs
- **StreamableDAO** -- `find_stream()` returning async iterators for large result sets
- **TransactionalDAO** -- `begin()`, `commit()`, `rollback()`, context manager support

Each backend declares which capabilities it supports. Callers check capabilities at runtime or via type narrowing. No more raising `NotImplementedError` for unsupported operations.

### 1.2 Consistent Return Types

All DAO methods must have consistent, documented return types across backends:

- `create()` returns the created entity with generated ID
- `update()` returns the updated entity
- `delete()` returns `bool` (success/failure)
- `bulk_delete()` returns `int` (count deleted) -- not `dict` on some backends and `int` on others
- `find()` returns `list[T]`, never `None`
- `find_by_id()` returns `T | None`

### 1.3 Connection Lifecycle

Every DAO must implement:

- `connect()` -- establish connection, validate credentials
- `disconnect()` -- clean shutdown, release resources
- `test_connection()` -- lightweight health probe (ping, SELECT 1, etc.)
- `is_connected` property -- non-blocking status check

Async context manager support (`async with dao:`) for scoped connections.

### 1.4 Error Hierarchy

Unified exception hierarchy that all backends map their native errors to:

- `StorageError` (base)
- `ConnectionError` -- cannot reach backend
- `NotFoundError` -- entity does not exist
- `DuplicateError` -- unique constraint violation
- `ValidationError` -- schema/type mismatch
- `QueryError` -- malformed or rejected query
- `TransactionError` -- commit/rollback failure
- `TimeoutError` -- operation exceeded deadline
- `PermissionError` -- access denied

Native exceptions must never leak to callers.

---

## 2. Connection Management

No shared connection pool exists. Each DAO creates its own connections independently.

### 2.1 Connection Pool

Shared, configurable connection pool per backend type:

- Min/max pool size per backend
- Idle connection timeout and eviction
- Connection validation on checkout (test before use)
- Pool exhaustion strategy: queue with timeout, not fail-fast
- Metrics: active, idle, waiting, total created, total destroyed

Pool must be async-native (no `threading.Lock` in async code).

### 2.2 Connection Registry

Singleton registry that manages all active connections:

- Register/deregister backends by name
- Lookup by name or StorageType enum
- Lazy initialization (connect on first use)
- Graceful shutdown (disconnect all on process exit)
- Health check endpoint that probes all registered backends

### 2.3 Configuration

StorageConfig must support:

- Connection string parsing and building (with proper escaping of special characters in passwords)
- Environment variable override (`DATAOPS_{BACKEND}_HOST`, etc.)
- SSL/TLS configuration per backend
- Read replicas (primary for writes, replica pool for reads)
- Connection timeout, query timeout, idle timeout as separate values

---

## 3. Multi-Backend Orchestration

UnifiedCRUD orchestrates reads/writes across backends but has critical bugs (event-loop identity caching, sync/async mismatch).

### 3.1 Storage Routing

Models declare which backends they use via StorageConfig. The orchestrator must:

- Route writes to the designated primary backend
- Route reads to the designated read backend (may differ from write)
- Support multi-write (write to primary + replicate to secondary) with configurable sync strategy
- Never silently fall back to a different backend on failure

### 3.2 Sync Strategies

When a model uses multiple backends:

- **SEQUENTIAL** -- write to primary, then secondary, fail if either fails
- **PARALLEL** -- write to all concurrently, fail if any fails
- **PRIMARY_FIRST** -- write to primary, async replicate to secondary (eventual consistency)
- **BEST_EFFORT** -- write to primary, log errors on secondary failures

### 3.3 UID Registry

Global UID-to-backend mapping so entities can be looked up by UID without knowing which backend stores them. Must use a persistent store (not in-memory dict that loses state on restart).

### 3.4 Field Mapping

Support field-level mapping between backend schemas:

- Rename fields (snake_case in Python, camelCase in REST, etc.)
- Type coercion (datetime to ISO string, UUID to string, etc.)
- Computed fields (derived from other fields during hydration)
- Excluded fields (present in model but not persisted to specific backend)

---

## 4. SQL Backend (PostgreSQL)

### 4.1 Parameterized Queries

All queries must use parameterized inputs. No string interpolation of values into SQL. The current implementation has parameter ordering bugs and empty SET clause generation that must be fixed.

### 4.2 Schema Management

- Alembic integration for migration generation and execution
- Auto-detection of model changes vs current schema
- Rollback support for failed migrations
- Multi-tenant schema isolation (schema-per-tenant or row-level)

### 4.3 Query Builder

Type-safe query builder for common operations:

- Filter by field, operator, value
- Sorting, pagination (offset/limit and cursor-based)
- Joins across related models
- Aggregations (count, sum, avg, min, max, group by)
- Subqueries and CTEs for complex reports

### 4.4 Transaction Support

Full ACID transaction support:

- Explicit transaction blocks (`async with session.begin():`)
- Nested savepoints
- Read-only transactions for query optimization
- Transaction timeout enforcement

---

## 5. Graph Backend (Dgraph)

### 5.1 Injection-Safe Query Building

All DQL queries must use parameterized variables. The current GraphQueryBuilder interpolates unescaped values into query strings -- this is a critical injection vulnerability. Replace with Dgraph's native `$variable` binding.

### 5.2 Graph ORM

Model-level graph relationship declarations:

- `GraphRelation` annotations for edges (already partially exists)
- Eager and lazy loading of related nodes
- Relationship traversal with configurable depth limits
- Bidirectional relationship support
- Edge properties (weighted relationships, timestamps)

### 5.3 Query Capabilities

- k-hop traversal (currently broken -- ignores depth parameter)
- Filtered traversal (currently broken -- ignores filter parameter)
- Shortest path queries
- Aggregation along paths
- Full-text search integration (Dgraph's built-in)

### 5.4 Schema Management

- Dgraph schema declaration from Pydantic models
- Schema migration (add/modify predicates, indices)
- Type system integration (Dgraph types mapped to Python models)

---

## 6. Vector Backend (pgvector)

### 6.1 Embedding Storage

- Store embeddings alongside metadata in PostgreSQL via pgvector extension
- Support multiple embedding dimensions per collection
- Configurable vector column type (vector, halfvec, sparsevec)

### 6.2 Similarity Search

- Cosine similarity, L2 distance, inner product
- Top-K nearest neighbor queries with distance thresholds
- Filtered similarity search (combine vector search with metadata predicates)
- Hybrid search (vector + full-text keyword search combined)

### 6.3 Index Management

- HNSW index creation with configurable parameters (m, ef_construction)
- IVFFlat index as alternative for large datasets
- Automatic index recommendation based on dataset size
- Index rebuild/reindex operations
- ef_search tuning per query for recall vs speed tradeoff

### 6.4 Embedding Pipeline Integration

- Pluggable embedding providers (OpenAI, local models, etc.)
- Batch embedding generation
- Automatic re-embedding on content change
- Embedding versioning (track which model generated each embedding)

---

## 7. In-Memory Backend (Redis)

### 7.1 Safe Query Interface

The current implementation parses unsanitized strings and uses `KEYS *` (blocks entire Redis instance on large datasets). Replace with:

- `SCAN` for iteration (non-blocking, cursor-based)
- Parameterized key patterns (no raw string interpolation)
- Namespace isolation (key prefix per tenant/collection)

### 7.2 Data Structures

Support Redis-native data structures beyond simple key/value:

- Hashes (field-level get/set on entities)
- Sorted sets (leaderboards, time-ordered data)
- Streams (event log, CDC)
- JSON (via RedisJSON module) for document storage

### 7.3 Cache Layer

Redis as a caching tier in front of slower backends:

- Read-through cache (check Redis, fall back to primary, populate cache)
- Write-through cache (write to primary and Redis simultaneously)
- Write-behind cache (write to Redis, async flush to primary)
- TTL-based expiry with configurable per-model cache duration
- Cache invalidation on write operations

### 7.4 Pub/Sub Integration

- Publish events on data changes
- Subscribe to change notifications across services
- Channel-per-model or channel-per-tenant routing

---

## 8. REST Backend (External APIs)

### 8.1 Client Configuration

- Base URL, authentication (Bearer, API key, Basic, OAuth2 client credentials)
- Request/response timeout, retry policy (exponential backoff with jitter)
- Rate limiting (respect `Retry-After`, configurable burst/sustained limits)
- Circuit breaker (fail-open after N consecutive failures, probe to recover)

### 8.2 Response Mapping

- Configurable response envelope parsing (not guessing)
- JSON path extraction for nested response structures
- Pagination support (offset, cursor, link-header based)
- Error response mapping to StorageError hierarchy

### 8.3 Request Building

- URL template interpolation with proper encoding
- Query parameter serialization
- Request body serialization (JSON, form-encoded)
- Custom header injection per request

---

## 9. Vault Backend (Secrets and Encryption)

### 9.1 Vault Client

Working integration with OpenBao/HashiCorp Vault:

- Token authentication, AppRole authentication
- Secret read/write/delete/list operations
- KV v2 versioned secrets support
- Dynamic secrets (database credentials, cloud IAM)

### 9.2 Secrets Broker

The existing secrets broker architecture (adapter, client, pointer) needs:

- Production-ready HTTP backend (currently partially implemented)
- Async-native locking (replace `threading.Lock` with `asyncio.Lock`)
- Connection pooling for broker HTTP calls
- Retry with backoff on transient failures
- Health check endpoint

### 9.3 Field-Level Encryption

- Configurable encryption per field via `@sensitive_field()` decorator
- Envelope encryption (data key encrypted by master key)
- Key rotation without re-encrypting all data (key versioning)
- Searchable encryption for equality queries on encrypted fields
- Classification-based encryption strength (PUBLIC=none, CONFIDENTIAL=AES-256, etc.)

### 9.4 Key Management

- Master key derivation using proper KDF (Argon2id, not SHA-256 with zero iterations)
- Per-field key derivation with unique, random salts (not field name as salt)
- Key rotation schedule (configurable per classification level)
- Key escrow and recovery procedures
- No hardcoded keys or fallback to random generation

---

## 10. Timeseries Backend (Prometheus)

### 10.1 Append-Only Semantics

Respect Prometheus's append-only data model:

- `create()` maps to metric push (via Pushgateway or remote write)
- `find()` maps to PromQL range/instant queries
- `update()` and `delete()` must raise `NotSupportedError` (not silently attempt)
- `count()` maps to `count_over_time()` query

### 10.2 PromQL Safety

All PromQL queries must escape label values to prevent injection. Use parameterized label matchers, not string interpolation.

### 10.3 Metric Types

Support standard Prometheus metric types:

- Counter (monotonically increasing)
- Gauge (arbitrary value)
- Histogram (bucketed observations)
- Summary (quantile observations)

### 10.4 Query Capabilities

- Range queries with step interval
- Aggregation functions (sum, avg, rate, increase, histogram_quantile)
- Label filtering and grouping
- Recording rules (pre-computed queries)

---

## 11. Model Layer (StorageModel)

### 11.1 Decompose God Class

The current StorageModel combines persistence, security, audit, vault, DAO factory, and serialization in one class. Decompose into:

- **StorageModel** -- UID, timestamps, persistence hooks (to_storage_dict, from_storage_dict)
- **SecuredModelMixin** -- ACL, ownership, classification (opt-in via mixin)
- **AuditedModelMixin** -- created_by, modified_by, accessed_by tracking (opt-in)
- **EncryptedModelMixin** -- field-level encryption via `@sensitive_field` (opt-in)

Models compose only the mixins they need.

### 11.2 Model Metadata

ModelMetadata declaration for persistence configuration:

- Collection/table name
- Storage backend assignments (primary, replicas)
- Index definitions
- Field-level storage overrides (exclude, rename, transform)
- TTL (automatic expiry)

### 11.3 Serialization

- `to_storage_dict()` -- model to backend-specific dict
- `from_storage_dict()` -- backend dict to model instance
- Type coercion registry (extensible per backend)
- Nested model serialization (embedded documents, JSON columns)
- Datetime handling (timezone-aware, ISO 8601)

### 11.4 Validation

- Pydantic validation on model instantiation (already via BaseModel)
- Pre-persist validation hooks (custom business rules)
- Cross-field validation
- Backend-specific constraint validation (unique, foreign key) surfaced as ValidationError

---

## 12. Security and Access Control

### 12.1 ACL System

The SecuredModelMixin has ACL logic but minimal tests and complex deny-first evaluation. Needs:

- Well-tested DENY-first permission evaluation
- Role hierarchy (ADMIN inherits MEMBER permissions)
- Resource-level permissions (per-entity ACL entries)
- Tenant-scoped permission evaluation
- Permission caching (avoid re-evaluation on every access)

### 12.2 Row-Level Security

Filter query results based on caller's SecurityContext:

- Automatic WHERE clause injection for SQL backends
- Graph traversal filtering for Dgraph
- Key prefix filtering for Redis
- Applied transparently by the DAO, not by the caller

### 12.3 Audit Trail

Every data mutation must be auditable:

- Who made the change (user_id, service_id)
- What changed (entity type, entity ID, field-level diff)
- When (timestamp)
- From where (IP, request ID)

Store audit records in a separate, append-only backend (timeseries or dedicated audit table). The current `@record_event` decorator is a placebo -- it must actually persist events.

---

## 13. Observability

### 13.1 Metrics

Expose per-backend operational metrics:

- Query count, latency (p50, p95, p99), error rate
- Connection pool utilization (active, idle, waiting)
- Cache hit/miss ratio (for Redis cache layer)
- Bytes read/written

Export via OpenTelemetry or Prometheus client.

### 13.2 Distributed Tracing

OpenTelemetry trace spans for every DAO operation:

- Span per `create()`, `find()`, `update()`, `delete()` call
- Backend type, collection name, query parameters as span attributes
- Parent span propagation from caller context
- Trace ID in error logs for correlation

### 13.3 Health Checks

Aggregate health endpoint:

- Probe each registered backend (`test_connection()`)
- Report per-backend status (healthy, degraded, down)
- Include latency of health probe
- Expose via HTTP or structured JSON for orchestrator consumption

### 13.4 Structured Logging

- JSON-formatted log output (loguru already in use)
- Correlation ID propagation (request_id, trace_id)
- Sensitive field redaction in logs
- Log level per backend (debug SQL queries without flooding graph logs)

---

## 14. Schema and Migration Management

### 14.1 SQL Migrations

Alembic integration for PostgreSQL:

- Auto-generate migrations from model diffs
- Forward and rollback migration support
- Migration versioning and ordering
- Multi-tenant migration (apply per schema or globally)

### 14.2 Graph Schema

Dgraph schema management:

- Generate DQL schema from Pydantic model definitions
- Detect schema drift (compare declared vs actual)
- Apply schema updates (add predicates, indices, types)
- Schema versioning

### 14.3 Vector Index Management

pgvector index lifecycle:

- Create HNSW/IVFFlat indices for embedding columns
- Rebuild indices when parameters change
- Monitor index health (fragmentation, recall)

### 14.4 Configuration as Code

All backend schemas, indices, and configurations must be declarative and version-controlled. No manual DDL execution.

---

## 15. Event System and Data Pipeline

### 15.1 Change Events

Emit structured events on every data mutation:

- Event types: `entity.created`, `entity.updated`, `entity.deleted`
- Payload: entity type, entity ID, changed fields, old/new values, actor, timestamp
- Delivery: in-process event bus + optional external transport (Redis Pub/Sub, webhook)

### 15.2 Event Subscribers

Plugin-friendly subscription model:

- Register handlers per event type
- Async handler execution (non-blocking)
- Error isolation (failed handler doesn't block others)
- Dead-letter queue for failed deliveries

### 15.3 CDC Support

Change Data Capture for downstream consumers:

- PostgreSQL logical replication / WAL decoding
- Structured change feed (ordered, resumable)
- Consumer group support (multiple consumers, at-least-once delivery)

### 15.4 Caching Decorator

The current `@cached_result` decorator ignores TTL for non-memory backends. Replace with:

- TTL-honored caching with Redis or in-memory backend
- Cache key derivation from function signature + arguments
- Manual cache invalidation API
- Cache warming on startup (optional)

---

## 16. Testing Infrastructure

### 16.1 Test Backend Fixtures

Every backend must have a test fixture that:

- Spins up an isolated instance (Docker, in-memory, or mock)
- Seeds test data
- Tears down cleanly after tests
- Supports parallel test execution (no shared state)

### 16.2 Contract Tests

Shared test suite that runs against every DAO implementation:

- Verify all capability protocol methods work correctly
- Consistent behavior assertions across backends
- Error handling assertions (correct exception types)

### 16.3 Integration Tests

End-to-end tests covering:

- Multi-backend write + read consistency
- Secrets broker round-trip (store → retrieve → verify integrity)
- ACL enforcement (permitted and denied access)
- Connection pool behavior under load
- Transaction rollback correctness

### 16.4 Coverage Requirements

- Unit test coverage > 90% for core modules
- Integration test coverage > 50% for DAO implementations
- No production asserts (currently 36 sites that are disabled with `python -O`)
- No overly broad `except Exception` (currently 54+ sites)

---

## Priority Matrix

| Priority | Category | Rationale |
|----------|----------|-----------|
| P0 | DAO Contract Redesign (1) | Foundation for everything else; current interface is broken |
| P0 | Security Fixes (9.4, 5.1, 7.1, 10.2) | Injection and crypto vulnerabilities block any deployment |
| P0 | Connection Management (2) | No shared pool = resource leaks and connection storms |
| P0 | Error Hierarchy (1.4) | Callers cannot handle errors correctly today |
| P1 | SQL Backend Fixes (4) | Most-used backend, has parameter bugs and empty SET clauses |
| P1 | Model Decomposition (11.1) | God class blocks independent feature development |
| P1 | Secrets Broker Hardening (9.2, 9.3, 9.4) | Hardcoded keys, fake encryption, threading bugs |
| P1 | Audit Trail (12.3) | Compliance requirement, current implementation is placebo |
| P2 | Graph Backend (5) | Injection fixes are P0, ORM features are P2 |
| P2 | Vector Backend (6) | Functional but minimal, needs index management and hybrid search |
| P2 | Redis Cache Layer (7.3) | Performance optimization, not blocking |
| P2 | Observability (13) | Operational requirement, can instrument incrementally |
| P3 | REST Backend (8) | Works for basic cases, circuit breaker is nice-to-have |
| P3 | Timeseries Backend (10) | Niche use case, fix semantics then iterate |
| P3 | Event System / CDC (15) | Architectural enhancement, not blocking core persistence |
| P3 | Schema Management (14) | Alembic exists for SQL, other backends can be manual initially |

## References

- [Martin Fowler -- Polyglot Persistence](https://martinfowler.com/bliki/PolyglotPersistence.html)
- [Spring Data -- Multi-Backend Repository Pattern](https://spring.io/projects/spring-data)
- [Neomodel -- Neo4j OGM for Python](https://neomodel.readthedocs.io/)
- [pgvector -- Vector Similarity Search for PostgreSQL](https://github.com/pgvector/pgvector)
- [HashiCorp Vault -- Secrets Management](https://www.hashicorp.com/en/products/vault)
- [OpenTelemetry -- Observability Framework](https://opentelemetry.io/docs/concepts/observability-primer/)
- [RFC 7662 -- OAuth 2.0 Token Introspection](https://datatracker.ietf.org/doc/html/rfc7662)
- [Debezium -- CDC vs Event Sourcing](https://debezium.io/blog/2020/02/10/event-sourcing-vs-cdc/)
- [CQRS Pattern -- Azure Architecture](https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs)
- [Keycloak Server Administration Guide](https://www.keycloak.org/docs/latest/server_admin/index.html)
- [SCIM 2.0 Standard](https://scim.cloud/)
- [pgvector HNSW Guide -- Neon](https://neon.com/blog/understanding-vector-search-and-hnsw-index-with-pgvector)
- [ETL Frameworks 2026 -- Integrate.io](https://www.integrate.io/blog/etl-frameworks-in-2025-designing-robust-future-proof-data-pipelines/)
- [Vault Field-Level Encryption with KMIP](https://www.hashicorp.com/en/blog/mongodb-field-level-encryption-with-hashicorp-vault-kmip-secrets-engine)

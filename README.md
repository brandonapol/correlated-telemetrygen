# correlated-telemetrygen

Synthetic OTLP traces and logs in which **every log record belongs to a span of the same request**, so a
backend can jump from a log line to its trace and from a span to its logs. Built for the demo load in
[clickhouse-observability-stack](https://github.com/benhastings/clickhouse-observability-stack), where stock
[`telemetrygen`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen)
can't do this: its `logs` command takes one fixed `--trace-id`, and its `traces` command picks random IDs.

## What it sends

Each interaction is a shopper clicking "place order" in the `web` front end, which sends
`POST /api/v1/checkout` to the `checkout` service, which calls `payments`:

```text
web       INTERNAL  click                                    log: checkout submitted
web       CLIENT      POST  -> checkout
checkout  SERVER        POST /api/v1/checkout                log: checkout started
                                                             event: feature_flag.evaluation (new-checkout-flow)
checkout  CLIENT          GET cart                           (redis: the cart, cache.hit)
checkout  CLIENT          SELECT shop.inventory ...          (postgresql: 3–5 statements ending in INSERT shop.orders)
checkout  CLIENT          POST  -> payments
payments  SERVER            POST /api/v1/charge              log: authorizing payment
payments  CLIENT              POST  -> payment provider      log: payment gateway timed out (on error)
                                                             log: charge completed / charge failed   (wide event)
                                                             log: checkout completed / accepted with payment pending (wide event)
                                                             log: order confirmed / placed with payment pending (wide event)
```

- A fraction of charges (`--error-ratio`, default 25%) time out at the payment provider after ~900 ms. The payments
  server span and the calls into and out of it are marked as errors. Checkout accepts the order with payment
  pending (HTTP 202), so its server span succeeds.
- A fraction of orders (`--db-error-ratio`, default 2%) fail at the `INSERT` with Postgres serialization failure
  `40001`, recorded as `db.response.status_code` and `error.type`. Checkout logs the failure, returns 500 with
  `outcome=db_error` without calling payments, and its server span is marked as an error.
- Database and cache spans follow the
  [database span conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/): Postgres
  spans carry `server.address` (an RDS host in `--region`), `server.port`, `db.namespace`, `db.collection.name`,
  `db.operation.name`, a parameterized `db.query.text`, `db.query.summary` (also the span name) and, for SELECTs,
  `db.response.returned_rows`. Redis spans carry `server.address`, `server.port`, the database index as
  `db.namespace` and `db.query.text`.
- The `web` service stands in for the OpenTelemetry JS browser SDK. Its resource has `telemetry.sdk.language`
  `webjs`, `user_agent.original`, `browser.language` and, for Chromium browsers, `browser.brands`,
  `browser.platform` and `browser.mobile`. It has no cloud or k8s attributes. The trace context reaches
  checkout through the fetch, so a trace starts at the shopper's click.
- Each of 500 users belongs to one tenant (`user.team.id`) and has a browser session. One checkout in twenty
  starts a new session, and the browser then also records `session.previous_id`. The browser passes `session.id`
  to checkout and payments as if in W3C Baggage, so every wide event in a request carries it.
- Every log is emitted inside its span's context, so OTLP carries the span's `TraceId` and `SpanId`. They are
  also written as `trace_id` and `span_id` log attributes, for backends that link on attributes.
- Each service ends its request with one canonical **wide event** log (`main=true`). The browser's wide event
  records what the shopper saw: `session.id`, `user.id`, `user.team.id`, the status code, `outcome` and the
  user-perceived `duration_ms`. The checkout and payments wide events carry the same attributes as their server
  span: `http.request.method`, `http.route`, `url.path`, `url.scheme`, `server.address`, `server.port`,
  `client.address`, `network.peer.address`, `network.protocol.version`, `user_agent.original`,
  `http.request.body.size`, `http.response.body.size`, `http.response.status_code`, `duration_ms`, `user.id`,
  `user.team.id`, `session.id`, `db.query_count`, `db.duration_ms`, `cache.hit`, `outcome` and, on failure,
  `error.type`.
- HTTP spans follow the [HTTP span conventions](https://opentelemetry.io/docs/specs/semconv/http/http-spans/).
  Checkout's `client.address` is the shopper's IP (from `X-Forwarded-For`), and its `network.peer.address` is the
  load balancer node. On the call from checkout to payments, `network.peer.address` is the IP of the payments pod
  that served it, and `service.peer.name` names the service. Payments charges cards through one of three payment
  providers, which appears as the gateway span's `server.address` and `url.full`. Server span status is left unset
  unless the response is 5xx.
- Each service runs as several replicas (checkout × 3, payments × 2) spread across the availability zones of
  one simulated EKS cluster. Every request is served by one replica of each service, so you can ask whether a
  problem is one pod, one zone or one CPU architecture. The second replica of each service runs on an `arm64` node.
- Every resource has `service.name`, `service.namespace`, `service.version`, `service.instance.id`,
  `service.criticality`, `service.team`, `service.build.git_hash` and `deployment.environment.name`. It also
  has the attributes a resource detector and the collector's `k8sattributes` processor would add in EKS:
  `cloud.provider`, `cloud.platform`, `cloud.account.id`, `cloud.region`, `cloud.availability_zone`,
  `k8s.cluster.name`, `k8s.namespace.name`, `k8s.deployment.name`, `k8s.replicaset.name`, `k8s.pod.name`,
  `k8s.pod.uid`, `k8s.node.name`, `container.name`, `container.image.name`, `container.image.tags`,
  `host.arch`, `process.runtime.name` and `process.runtime.version`. The generator sets these itself: it runs
  as one process, so a collector would give every simulated service the same pod and node.
- `OTEL_RESOURCE_ATTRIBUTES` is read by the SDK and overrides or adds to the simulated attributes. To move the
  cluster to another region, use `--region`, which also sets the zones and the cluster name.

## Conventions

Telemetry follows the [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/) v1.44.0
(the schema URL on every resource and scope). On top of those, it uses company and domain conventions:

**Company conventions**, shared by every service:

| Attribute | Where | |
|---|---|---|
| `service.team`, `service.build.git_hash` | resource | owning team and build |
| `user.team.id`, `user.team.plan` | server spans, wide events | the customer team and its plan (`enterprise`, `smb`, `free`); `user.team.*` extends OTel's `user.*` namespace |
| `main` | logs | `true` on the one wide event per service per request |
| `outcome` | wide events | `success`, `payment_pending`, `upstream_error` or `db_error` |
| `duration_ms`, `cpu_ms` | wide events | wall-clock time and simulated CPU time of the request |
| `db.query_count`, `db.duration_ms`, `cache.hit` | backend wide events | database and cache work done for the request |
| `trace_id`, `span_id` | logs | the record's trace context, for backends that link on attributes |

**Domain conventions** (`shop.*`):

| Attribute | Where |
|---|---|
| `shop.cart.item_count` | checkout and web logs and wide events |
| `shop.order.id`, `shop.order.amount`, `shop.order.currency` | checkout and web wide events (`shop.order.id` also on payments), unless the order failed |
| `shop.payment.method` | `card`, `apple_pay` or `paypal` |
| `shop.payment.provider`, `shop.payment.amount`, `shop.payment.currency` | payments logs and wide events |

Checkout evaluates the `new-checkout-flow` feature flag, rolled out by user to 30% of users. It emits a
`feature_flag.evaluation` event, and its wide event carries `feature_flag.key`, `feature_flag.result.variant`,
`feature_flag.provider.name` and `feature_flag.result.reason`. The `treatment` variant checks stock per warehouse,
which costs one more database query.

Timestamps are simulated from the start of each interaction, so the process never sleeps inside a request.
Span ends can be up to about a second in the future.

## Usage

```bash
docker run --rm -e OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4317 \
  ghcr.io/brandonapol/correlated-telemetrygen:v0.1.0 --rate 2 --error-ratio 0.25
```

| Flag | Default | |
|---|---|---|
| `--rate` | `2` | checkout requests per second |
| `--error-ratio` | `0.25` | fraction of payments that time out |
| `--db-error-ratio` | `0.02` | fraction of orders that fail with a database serialization failure |
| `--environment` | `local` | `deployment.environment.name` |
| `--namespace` | `shop` | `service.namespace` and `k8s.namespace.name` |
| `--region` | `us-east-1` | `cloud.region`; zones are `<region>a`, `b` and `c` |
| `--duration` | `0` | how long to run; `0` runs until SIGTERM |
| `--seed` | `0` | random seed; `0` uses the clock |

The exporters read the standard `OTEL_EXPORTER_OTLP_*` variables. They send OTLP gRPC to `localhost:4317`
by default, and an `http://` endpoint means no TLS.

## Checking the telemetry schema

The conventions above are written down as an [OTel Weaver](https://github.com/open-telemetry/weaver) registry in
[`model/`](model), which depends on the OpenTelemetry semantic conventions v1.44.0:

- [`model/company.yaml`](model/company.yaml): company attributes, and the attribute groups that say what every
  resource, log and wide event must carry.
- [`model/shop.yaml`](model/shop.yaml): the `shop.*` domain attributes.
- [`model/signals.yaml`](model/signals.yaml): the spans the services emit, and the imported
  `feature_flag.evaluation` event.

[`.weaver.toml`](.weaver.toml) holds the live-check **matchers**. A span name or a log without an event name doesn't
say which definition it belongs to, so each matcher pairs samples with a span definition or with strict attribute
groups. For example, a log with `main=true` from checkout must satisfy `shop.wide_event`, `shop.wide_event.server` and
`shop.wide_event.checkout`.

[`policies/company.rego`](policies/company.rego) adds rules a requirement level can't express, each reported as a
violation:

- a wide event's `outcome` must agree with its status code
- a wide event that did not succeed must be logged at WARN or above, and a successful one below WARN
- `cpu_ms` can't exceed `duration_ms`
- the `trace_id` and `span_id` attributes must match the record's trace context
- `user.email`, `user.full_name` and `user.name` must not be emitted

Weaver applies its default OpenTelemetry policy only when no policy directory is given, so
[`policies/otel.rego`](policies/otel.rego) is a copy of it.

To check the registry and then the generator's telemetry, with Docker and Go installed:

```bash
docker run --rm -v "$PWD:/work" -w /work otel/weaver:v0.27.0 registry check -r model
./scripts/live-check.sh
```

The script starts Weaver as an OTLP receiver on port 4317, runs the generator against it for 10 seconds with the
failure modes turned up, and prints a summary of the findings. It exits non-zero if any finding is a violation, and
writes the full report to `live-check-report/live_check.json`. CI runs both steps.

The run reports some `improvement` and `information` findings. They are expected, and are left visible rather than
filtered out:

- `not_stable`: most of the attributes used, including all company and domain attributes, have development stability.
- `missing_namespace`: the company fields `main`, `outcome`, `duration_ms`, `cpu_ms`, `trace_id` and `span_id` have no
  namespace. Dashboards already query these names, so they are kept.
- `extends_namespace`: reported only for OpenTelemetry's own attributes, such as `db.system.name`, `service.name` and
  `user.id`, not for company attributes. With a v2 registry, the default policy's list of known attributes appears
  to leave out the dependency's attributes.
- `recommended_attribute_not_present`: the flag evaluation has no `feature_flag.error.message`, because no evaluation
  fails.
- `conditionally_required_attribute_not_present`: for example, `error.type` on requests that succeeded.
- `undefined_enum_variant`: `deployment.environment.name` is `local`, and `error.type` takes values such as
  `GatewayTimeout` and `40001`, which semconv does not list.

## Development

```bash
go test ./...      # asserts every log sits inside a span of the same trace and service
docker build .
```

Pushing a `v*` tag publishes a multi-arch (amd64, arm64) image to `ghcr.io/brandonapol/correlated-telemetrygen`.

package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// instance is one replica of a simulated service, or for the web front end one browser
// profile: the tracer and logger share a resource, so its spans and logs carry the same
// service.name, service.instance.id, k8s.pod.name and so on.
type instance struct {
	tracer trace.Tracer
	logger otellog.Logger
	podIP  string // empty for a browser
}

// simulator emits one checkout interaction at a time. Time is simulated: spans and logs get
// explicit timestamps laid out from the start of the interaction, so nothing sleeps.
type simulator struct {
	simConfig
	web      []instance // one per browser profile
	checkout []instance
	payments []instance
	rng      *rand.Rand
	sessions map[string]*session
}

type simConfig struct {
	namespace    string  // the Kubernetes namespace the services run in
	region       string  // the cloud region, which appears in the database's host name
	errorRatio   float64 // fraction of charges that time out at the payment gateway
	dbErrorRatio float64 // fraction of orders whose INSERT fails with a serialization failure
}

func newSimulator(web, checkout, payments []instance, cfg simConfig, rng *rand.Rand) *simulator {
	return &simulator{simConfig: cfg, web: web, checkout: checkout, payments: payments, rng: rng, sessions: map[string]*session{}}
}

// tenant is a customer team and its plan (user.team.id and user.team.plan).
type tenant struct{ id, plan string }

var tenants = []tenant{
	{"enterprise_corp_a", "enterprise"},
	{"enterprise_corp_b", "enterprise"},
	{"smb_north", "smb"},
	{"smb_south", "smb"},
	{"free_tier", "free"},
}

// paymentMethods are how shoppers pay, weighted by how often they are chosen.
var paymentMethods = []string{"card", "card", "card", "card", "card", "card", "card", "apple_pay", "apple_pay", "paypal"}

// newCheckoutFlow is the feature flag checkout evaluates for each order. The treatment checks
// stock per warehouse, which costs an extra query.
const newCheckoutFlow = "new-checkout-flow"

const users = 500

// session is a shopper's browser session. The browser sends session.id to checkout, and
// checkout to payments, in W3C Baggage, so every wide event in a request carries it.
type session struct {
	id, previousID string
	browser        int    // index into browserProfiles and simulator.web
	ip             string // the shopper's public address
}

// request is one checkout by one shopper.
type request struct {
	userID        string
	tenant        tenant
	session       *session
	items         int
	amount        float64 // order total in USD
	paymentMethod string
	variant       string // of newCheckoutFlow: control or treatment
}

// newRequest picks a shopper. A user belongs to one tenant and keeps their browser across
// sessions; one checkout in twenty starts a new session.
func (s *simulator) newRequest() request {
	n := s.rng.IntN(users)
	userID := fmt.Sprintf("usr_%05d", n)
	sess, ok := s.sessions[userID]
	switch {
	case !ok:
		sess = &session{id: uuidV4(s.rng), browser: s.rng.IntN(len(s.web)), ip: publicIP(s.rng)}
		s.sessions[userID] = sess
	case s.rng.Float64() < 0.05:
		sess.previousID, sess.id = sess.id, uuidV4(s.rng)
	}
	items := 1 + s.rng.IntN(5)
	// The flag is rolled out to 30% of users, by user.
	variant := "control"
	if n*7%10 < 3 {
		variant = "treatment"
	}
	return request{
		userID:        userID,
		tenant:        tenants[n%len(tenants)],
		session:       sess,
		items:         items,
		amount:        float64(items) * (5 + float64(s.rng.IntN(9500))/100),
		paymentMethod: paymentMethods[s.rng.IntN(len(paymentMethods))],
		variant:       variant,
	}
}

// userAttrs identify the shopper and their team.
func (r request) userAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("user.id", r.userID),
		attribute.String("user.team.id", r.tenant.id),
		attribute.String("user.team.plan", r.tenant.plan),
	}
}

// orderAttrs describe the order being placed.
func (r request) orderAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("shop.cart.item_count", r.items),
		attribute.Float64("shop.order.amount", r.amount),
		attribute.String("shop.order.currency", "USD"),
		attribute.String("shop.payment.method", r.paymentMethod),
	}
}

// publicIP returns an address from the IPv4 ranges reserved for documentation (RFC 5737).
func publicIP(rng *rand.Rand) string {
	nets := []string{"192.0.2", "198.51.100", "203.0.113"}
	return fmt.Sprintf("%s.%d", nets[rng.IntN(len(nets))], 1+rng.IntN(254))
}

// loadBalancerIPs are the addresses of the load balancer nodes in front of checkout, one per zone.
var loadBalancerIPs = []string{"10.0.4.17", "10.0.37.201", "10.0.70.88"}

// sessionAttrs are the session attributes the browser records.
func (r request) sessionAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("session.id", r.session.id)}
	if r.session.previousID != "" {
		attrs = append(attrs, attribute.String("session.previous_id", r.session.previousID))
	}
	return attrs
}

// clock hands out monotonically increasing timestamps within one interaction.
type clock struct{ now time.Time }

func (c *clock) advance(d time.Duration) time.Time {
	c.now = c.now.Add(d)
	return c.now
}

// pick chooses the replica that serves a request, as a load balancer would.
func (s *simulator) pick(fleet []instance) instance {
	return fleet[s.rng.IntN(len(fleet))]
}

func (s *simulator) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(s.rng.Int64N(int64(hi-lo)+1))
}

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

// checkoutResult is what checkout tells the browser.
type checkoutResult struct {
	status             int
	outcome, errorType string
	orderID            string // empty if the order was not placed
}

// run simulates one checkout as the shopper sees it: clicking "place order" in the browser
// sends POST /api/v1/checkout to the checkout service, and the page shows the result.
func (s *simulator) run(ctx context.Context, start time.Time) {
	c := &clock{now: start}
	req := s.newRequest()
	web := s.web[req.session.browser]

	ctx, click := web.tracer.Start(ctx, "click", trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(start), trace.WithAttributes(req.sessionAttrs()...))
	s.log(ctx, web, c.advance(time.Millisecond), otellog.SeverityInfo, "checkout submitted",
		append(req.sessionAttrs(), attribute.String("user.id", req.userID), attribute.Int("shop.cart.item_count", req.items))...)

	fctx, fetch := web.tracer.Start(ctx, "POST /api/v1/checkout", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(append(req.sessionAttrs(),
			attribute.String("http.request.method", "POST"),
			attribute.String("url.full", "https://shop.example.com/api/v1/checkout"),
			attribute.String("url.template", "/api/v1/checkout"),
			attribute.String("server.address", "shop.example.com"),
			attribute.Int("server.port", 443),
		)...))
	latency := s.between(10*time.Millisecond, 60*time.Millisecond)
	c.advance(latency)
	res := s.handleCheckout(fctx, c, req)
	fetch.SetAttributes(attribute.Int("http.response.status_code", res.status))
	if res.status >= 400 {
		fetch.SetAttributes(attribute.String("error.type", fmt.Sprint(res.status)))
		fetch.SetStatus(codes.Error, "")
	}
	fetch.End(trace.WithTimestamp(c.advance(latency)))

	severity, body := otellog.SeverityInfo, "order confirmed"
	switch res.outcome {
	case "payment_pending":
		severity, body = otellog.SeverityWarn, "order placed with payment pending"
	case "db_error":
		severity, body = otellog.SeverityError, "order failed"
	}
	end := c.advance(s.between(5*time.Millisecond, 20*time.Millisecond))
	// Main-thread time to handle the click and render the result; phones are slower.
	cpu := s.between(4*time.Millisecond, 12*time.Millisecond)
	if browserProfiles[req.session.browser].mobile || strings.Contains(browserProfiles[req.session.browser].userAgent, "iPhone") {
		cpu += s.between(5*time.Millisecond, 15*time.Millisecond)
	}
	wide := append(req.sessionAttrs(), req.userAttrs()...)
	wide = append(wide, req.orderAttrs()...)
	wide = append(wide,
		attribute.String("http.request.method", "POST"),
		attribute.String("url.full", "https://shop.example.com/api/v1/checkout"),
		attribute.Int("http.response.status_code", res.status),
		attribute.Float64("duration_ms", millis(end.Sub(start))),
		attribute.Float64("cpu_ms", millis(cpu)),
		attribute.String("outcome", res.outcome),
	)
	if res.orderID != "" {
		wide = append(wide, attribute.String("shop.order.id", res.orderID))
	}
	if res.errorType != "" {
		wide = append(wide, attribute.String("error.type", res.errorType))
	}
	s.log(ctx, web, end, severity, body, append(wide, attribute.Bool("main", true))...)
	click.End(trace.WithTimestamp(end))
}

// handleCheckout simulates checkout serving POST /api/v1/checkout: it reads the cart from
// cache, runs a few database queries and calls payments. A payment gateway timeout fails the
// payments request, and checkout accepts the order with payment pending, so its own request
// still succeeds. A database serialization failure fails the order with a 500 before
// payments is called.
func (s *simulator) handleCheckout(ctx context.Context, c *clock, req request) checkoutResult {
	checkout := s.pick(s.checkout)
	start := c.now
	// The load balancer terminates TLS and forwards over HTTP/1.1. The shopper's address and the
	// original scheme come from X-Forwarded-For and X-Forwarded-Proto.
	requestAttrs := []attribute.KeyValue{
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/api/v1/checkout"),
		attribute.String("url.path", "/api/v1/checkout"),
		attribute.String("url.scheme", "https"),
		attribute.String("server.address", "shop.example.com"),
		attribute.Int("server.port", 443),
		attribute.String("client.address", req.session.ip),
		attribute.String("network.peer.address", loadBalancerIPs[s.rng.IntN(len(loadBalancerIPs))]),
		attribute.String("network.protocol.version", "1.1"),
		attribute.String("user_agent.original", browserProfiles[req.session.browser].userAgent),
		attribute.Int("http.request.body.size", 120+90*req.items),
		attribute.String("session.id", req.session.id),
	}
	requestAttrs = append(requestAttrs, req.userAttrs()...)
	requestAttrs = append(requestAttrs, req.orderAttrs()...)
	ctx, root := checkout.tracer.Start(ctx, "POST /api/v1/checkout",
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(start), trace.WithAttributes(requestAttrs...))
	s.log(ctx, checkout, c.advance(time.Millisecond), otellog.SeverityInfo, "checkout started",
		attribute.String("user.id", req.userID), attribute.Int("shop.cart.item_count", req.items))

	// The flag is evaluated in process by the flagd provider, as a percentage rollout.
	flag := []attribute.KeyValue{
		attribute.String("feature_flag.key", newCheckoutFlow),
		attribute.String("feature_flag.result.variant", req.variant),
		attribute.String("feature_flag.provider.name", "flagd"),
		attribute.String("feature_flag.result.reason", "split"),
	}
	s.event(ctx, checkout, c.advance(100*time.Microsecond), "feature_flag.evaluation",
		append(flag,
			attribute.String("feature_flag.context.id", req.userID),
			attribute.String("feature_flag.set.id", "checkout"),
			attribute.String("feature_flag.version", "42"),
		)...)

	cacheHit := s.rng.Float64() < 0.8
	_, cache := checkout.tracer.Start(ctx, "GET cart", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("db.system.name", "redis"),
			attribute.String("db.operation.name", "GET"),
			attribute.String("db.namespace", "0"),
			attribute.String("db.query.text", "GET cart:"+req.userID),
			attribute.String("db.query.summary", "GET cart"),
			attribute.String("server.address", "redis."+s.namespace+".svc.cluster.local"),
			attribute.Int("server.port", 6379),
			attribute.Bool("cache.hit", cacheHit),
		))
	cache.End(trace.WithTimestamp(c.advance(s.between(time.Millisecond, 3*time.Millisecond))))

	// A serialization failure aborts the order's transaction at the INSERT, the last statement.
	statements := s.checkoutStatements(req, cacheHit)
	dbFailed := s.rng.Float64() < s.dbErrorRatio
	var dbTime time.Duration
	for i, st := range statements {
		d := s.between(2*time.Millisecond, 12*time.Millisecond)
		dbTime += d
		s.query(ctx, checkout, c, st, d, dbFailed && i == len(statements)-1)
	}

	var res checkoutResult
	if dbFailed {
		s.log(ctx, checkout, c.advance(time.Millisecond), otellog.SeverityError, "placing order failed",
			attribute.String("error.type", serializationFailure), attribute.String("db.response.status_code", serializationFailure))
		res = checkoutResult{status: 500, outcome: "db_error", errorType: serializationFailure}
	} else {
		res = checkoutResult{status: 200, outcome: "success", orderID: "ord_" + k8sSuffix(s.rng, 12)}
		if paid, errorType := s.charge(ctx, checkout, c, req, res.orderID); !paid {
			res.status, res.outcome, res.errorType = 202, "payment_pending", errorType
		}
	}

	severity, body := otellog.SeverityInfo, "checkout completed"
	switch res.outcome {
	case "payment_pending":
		severity, body = otellog.SeverityWarn, "checkout accepted with payment pending"
	case "db_error":
		severity, body = otellog.SeverityError, "checkout failed"
	}
	end := c.advance(s.between(time.Millisecond, 4*time.Millisecond))
	wide := append(requestAttrs,
		attribute.Int("http.response.status_code", res.status),
		attribute.Int("http.response.body.size", 280+s.rng.IntN(40)),
		attribute.Float64("duration_ms", millis(end.Sub(start))),
		attribute.Float64("cpu_ms", millis(s.cpu(time.Millisecond, len(statements), req.items))),
		attribute.Int("db.query_count", len(statements)),
		attribute.Float64("db.duration_ms", millis(dbTime)),
		attribute.Bool("cache.hit", cacheHit),
		attribute.String("outcome", res.outcome),
	)
	wide = append(wide, flag...)
	if res.orderID != "" {
		wide = append(wide, attribute.String("shop.order.id", res.orderID))
	}
	if res.errorType != "" {
		wide = append(wide, attribute.String("error.type", res.errorType))
	}
	// Server span status stays unset below 500: a 202 is a success for checkout.
	if res.status >= 500 {
		root.SetStatus(codes.Error, "")
	}
	root.SetAttributes(wide...)
	s.log(ctx, checkout, end, severity, body, append(wide, attribute.Bool("main", true))...)
	root.End(trace.WithTimestamp(end))
	return res
}

// serializationFailure is the SQLSTATE Postgres returns when a transaction conflicts with a
// concurrent one.
const serializationFailure = "40001"

// statement is one SQL statement checkout sends to Postgres.
type statement struct {
	op, table, text string
	rows            int // rows returned by a SELECT
}

// summary is the low-cardinality db.query.summary, which is also the span name.
func (st statement) summary() string {
	return st.op + " " + st.table
}

// checkoutStatements are the statements of one checkout: on a cache miss the cart is loaded
// from the database, stock is locked (and, in the new checkout flow, checked per warehouse),
// prices and any promotion are read, and the order is inserted.
func (s *simulator) checkoutStatements(req request, cacheHit bool) []statement {
	var sts []statement
	if !cacheHit {
		sts = append(sts, statement{"SELECT", "shop.carts", "SELECT sku, qty FROM shop.carts WHERE user_id = $1", req.items})
	}
	sts = append(sts,
		statement{"SELECT", "shop.inventory", "SELECT sku, qty FROM shop.inventory WHERE sku = ANY($1) FOR UPDATE", req.items},
		statement{"SELECT", "shop.prices", "SELECT sku, price FROM shop.prices WHERE sku = ANY($1)", req.items},
	)
	if req.variant == "treatment" {
		sts = append(sts, statement{"SELECT", "shop.inventory_locations", "SELECT sku, warehouse_id, qty FROM shop.inventory_locations WHERE sku = ANY($1)", 3 * req.items})
	}
	if s.rng.IntN(2) == 0 {
		sts = append(sts, statement{"SELECT", "shop.promotions", "SELECT discount FROM shop.promotions WHERE code = $1 AND expires_at > now()", s.rng.IntN(2)})
	}
	return append(sts, statement{"INSERT", "shop.orders", "INSERT INTO shop.orders (id, user_id, team_id, total) VALUES ($1, $2, $3, $4)", 0})
}

// query records one statement as a Postgres client span lasting d.
func (s *simulator) query(ctx context.Context, inst instance, c *clock, st statement, d time.Duration, fail bool) {
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "postgresql"),
		attribute.String("db.namespace", "shop"),
		attribute.String("db.operation.name", st.op),
		attribute.String("db.collection.name", st.table),
		attribute.String("db.query.text", st.text),
		attribute.String("db.query.summary", st.summary()),
		attribute.String("server.address", s.namespace+"-db.cluster-c8x2k4mbq7n1."+s.region+".rds.amazonaws.com"),
		attribute.Int("server.port", 5432),
	}
	if st.op == "SELECT" && !fail {
		attrs = append(attrs, attribute.Int("db.response.returned_rows", st.rows))
	}
	_, span := inst.tracer.Start(ctx, st.summary(), trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(attrs...))
	if fail {
		span.SetAttributes(
			attribute.String("db.response.status_code", serializationFailure),
			attribute.String("error.type", serializationFailure),
		)
		span.SetStatus(codes.Error, "could not serialize access due to concurrent update")
	}
	span.End(trace.WithTimestamp(c.advance(d)))
}

// paymentProvider is an external payment API that payments charges cards through.
type paymentProvider struct {
	name, host, path string
}

var paymentProviders = []paymentProvider{
	{name: "stripe", host: "api.stripe.com", path: "/v1/charges"},
	{name: "adyen", host: "checkout-live.adyen.com", path: "/v71/payments"},
	{name: "braintree", host: "payments.braintree-api.com", path: "/graphql"},
}

// charge is checkout's call to payments: a client span in checkout and, under it, the
// payments server span with its own gateway call and logs. It reports whether the charge
// succeeded and, if not, the error type.
func (s *simulator) charge(ctx context.Context, checkout instance, c *clock, req request, orderID string) (bool, string) {
	fail := s.rng.Float64() < s.errorRatio
	payments := s.pick(s.payments)
	ctx, call := checkout.tracer.Start(ctx, "POST /api/v1/charge", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("server.address", "payments"),
			attribute.Int("server.port", 8080),
			attribute.String("url.full", "http://payments:8080/api/v1/charge"),
			attribute.String("url.template", "/api/v1/charge"),
			attribute.String("network.peer.address", payments.podIP),
			attribute.Int("network.peer.port", 8080),
			attribute.String("network.protocol.version", "1.1"),
			attribute.String("service.peer.name", "payments"),
			attribute.String("service.peer.namespace", s.namespace),
		))
	start := c.advance(time.Millisecond)

	requestAttrs := []attribute.KeyValue{
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/api/v1/charge"),
		attribute.String("url.path", "/api/v1/charge"),
		attribute.String("url.scheme", "http"),
		attribute.String("server.address", "payments"),
		attribute.Int("server.port", 8080),
		attribute.String("client.address", checkout.podIP),
		attribute.String("network.peer.address", checkout.podIP),
		attribute.String("network.protocol.version", "1.1"),
		attribute.String("user_agent.original", "Go-http-client/1.1"),
		attribute.Int("http.request.body.size", 150+s.rng.IntN(30)),
		attribute.String("session.id", req.session.id),
	}
	requestAttrs = append(requestAttrs, req.userAttrs()...)
	pctx, srv := payments.tracer.Start(ctx, "POST /api/v1/charge",
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(start), trace.WithAttributes(requestAttrs...))
	provider := paymentProviders[s.rng.IntN(len(paymentProviders))]
	payment := []attribute.KeyValue{
		attribute.String("shop.order.id", orderID),
		attribute.String("shop.payment.provider", provider.name),
		attribute.String("shop.payment.method", req.paymentMethod),
		attribute.Float64("shop.payment.amount", req.amount),
		attribute.String("shop.payment.currency", "USD"),
	}
	s.log(pctx, payments, c.advance(time.Millisecond), otellog.SeverityInfo, "authorizing payment", payment...)

	_, gw := payments.tracer.Start(pctx, "POST "+provider.path, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("server.address", provider.host),
			attribute.Int("server.port", 443),
			attribute.String("url.full", "https://"+provider.host+provider.path),
			attribute.String("url.template", provider.path),
			attribute.String("network.protocol.version", "2"),
		))
	status, outcome, errorType := 200, "success", ""
	if fail {
		gwEnd := c.advance(s.between(800*time.Millisecond, 950*time.Millisecond))
		gw.SetAttributes(attribute.String("error.type", "GatewayTimeout"))
		gw.SetStatus(codes.Error, "gateway timeout")
		gw.End(trace.WithTimestamp(gwEnd))
		status, outcome, errorType = 504, "upstream_error", "GatewayTimeout"
		s.log(pctx, payments, c.advance(time.Millisecond), otellog.SeverityError, "payment gateway timed out",
			append(payment, attribute.String("error.type", errorType))...)
	} else {
		gw.SetAttributes(attribute.Int("http.response.status_code", 200))
		gw.End(trace.WithTimestamp(c.advance(s.between(30*time.Millisecond, 110*time.Millisecond))))
	}

	end := c.advance(s.between(time.Millisecond, 3*time.Millisecond))
	wide := append(requestAttrs,
		attribute.Int("http.response.status_code", status),
		attribute.Int("http.response.body.size", 90+s.rng.IntN(20)),
		attribute.Float64("duration_ms", millis(end.Sub(start))),
		attribute.Float64("cpu_ms", millis(s.cpu(500*time.Microsecond, 0, 1))),
		attribute.Int("db.query_count", 0),
		attribute.Bool("cache.hit", false),
		attribute.String("outcome", outcome),
	)
	wide = append(wide, payment...)
	severity, body := otellog.SeverityInfo, "charge completed"
	if fail {
		wide = append(wide, attribute.String("error.type", errorType))
		srv.SetStatus(codes.Error, "gateway timeout")
		severity, body = otellog.SeverityError, "charge failed"
	}
	srv.SetAttributes(wide...)
	s.log(pctx, payments, end, severity, body, append(wide, attribute.Bool("main", true))...)
	srv.End(trace.WithTimestamp(end))

	callEnd := c.advance(time.Millisecond)
	call.SetAttributes(attribute.Int("http.response.status_code", status))
	if fail {
		call.SetAttributes(attribute.String("error.type", errorType))
		call.SetStatus(codes.Error, "payments returned 504")
	}
	call.End(trace.WithTimestamp(callEnd))
	return !fail, errorType
}

// cpu is the simulated CPU time a backend request costs: a base, plus time to build and
// decode each database statement and to price each item, plus jitter.
func (s *simulator) cpu(base time.Duration, statements, items int) time.Duration {
	return base + time.Duration(statements)*350*time.Microsecond + time.Duration(items)*150*time.Microsecond +
		s.between(0, time.Millisecond)
}

// event emits a named event, such as feature_flag.evaluation, inside the span in ctx.
func (s *simulator) event(ctx context.Context, svc instance, ts time.Time, name string, attrs ...attribute.KeyValue) {
	var r otellog.Record
	r.SetEventName(name)
	r.SetTimestamp(ts)
	r.SetObservedTimestamp(ts)
	r.SetSeverity(otellog.SeverityInfo)
	r.SetSeverityText(severityText(otellog.SeverityInfo))
	r.AddAttributes(attrs...)
	s.emit(ctx, svc, r)
}

// log emits a record inside the span in ctx.
func (s *simulator) log(ctx context.Context, svc instance, ts time.Time, sev otellog.Severity, body string, attrs ...attribute.KeyValue) {
	var r otellog.Record
	r.SetTimestamp(ts)
	r.SetObservedTimestamp(ts)
	r.SetSeverity(sev)
	r.SetSeverityText(severityText(sev))
	r.SetBody(attribute.StringValue(body))
	r.AddAttributes(attrs...)
	s.emit(ctx, svc, r)
}

// emit sends a record from the span in ctx. The SDK copies the span's trace and span IDs onto
// the record; they are repeated as trace_id and span_id attributes so a log backend can link
// to the trace from the attributes alone.
func (s *simulator) emit(ctx context.Context, svc instance, r otellog.Record) {
	sc := trace.SpanContextFromContext(ctx)
	r.AddAttributes(
		attribute.String("trace_id", sc.TraceID().String()),
		attribute.String("span_id", sc.SpanID().String()),
	)
	svc.logger.Emit(ctx, r)
}

func severityText(sev otellog.Severity) string {
	switch {
	case sev >= otellog.SeverityError:
		return "ERROR"
	case sev >= otellog.SeverityWarn:
		return "WARN"
	default:
		return "INFO"
	}
}

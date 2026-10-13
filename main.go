// Command correlated-telemetrygen sends synthetic OTLP traces and logs in which every log
// record belongs to a span of the same request, so a backend can navigate from a log line to
// its trace and from a span to its logs.
//
// It simulates shoppers whose browsers call a checkout service, which calls a payments
// service; the services run as several replicas in a Kubernetes cluster. The OTLP endpoint comes from the standard
// OTEL_EXPORTER_OTLP_* environment variables, and OTEL_RESOURCE_ATTRIBUTES overrides or adds
// to the simulated resource attributes.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const scope = "github.com/brandonapol/correlated-telemetrygen"

// schemaURL is the semantic conventions version the generated telemetry follows.
const schemaURL = "https://opentelemetry.io/schemas/1.44.0"

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	rate := flag.Float64("rate", 2, "checkout requests per second")
	errorRatio := flag.Float64("error-ratio", 0.25, "fraction of payments that time out at the gateway")
	dbErrorRatio := flag.Float64("db-error-ratio", 0.02, "fraction of orders that fail with a database serialization failure")
	environment := flag.String("environment", "local", "deployment.environment.name on every resource")
	namespace := flag.String("namespace", "shop", "service.namespace and k8s.namespace.name on every resource")
	region := flag.String("region", "us-east-1", "cloud.region of the simulated cluster")
	duration := flag.Duration("duration", 0, "how long to run; 0 runs until interrupted")
	seed := flag.Uint64("seed", 0, "random seed; 0 picks one from the clock")
	inc := incident{}
	flag.Func("incident-kind", "errors, latency or drop (default errors)", func(v string) error {
		inc.kind = incidentKind(v)
		return nil
	})
	flag.DurationVar(&inc.after, "incident-after", 0, "start an incident this long after the run starts; 0 for none")
	flag.DurationVar(&inc.duration, "incident-duration", 10*time.Minute, "how long an incident lasts")
	flag.DurationVar(&inc.every, "incident-every", 0, "repeat the incident this often; 0 runs it once")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *rate <= 0 || *errorRatio < 0 || *errorRatio > 1 || *dbErrorRatio < 0 || *dbErrorRatio > 1 {
		log.Fatal("--rate must be positive, and --error-ratio and --db-error-ratio between 0 and 1")
	}
	if inc.kind == "" {
		inc.kind = incidentErrors
	}
	if err := inc.validate(); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if *seed == 0 {
		*seed = uint64(time.Now().UnixNano())
	}
	rng := rand.New(rand.NewPCG(*seed, *seed>>1))
	d := deployment{environment: *environment, namespace: *namespace, region: *region}

	var shutdowns []func(context.Context) error
	newFleet := func(replicas []replica) []instance {
		var fleet []instance
		for _, r := range replicas {
			inst, shutdown, err := newOTLPInstance(ctx, r)
			if err != nil {
				log.Fatalf("setting up telemetry: %v", err)
			}
			shutdowns = append(shutdowns, shutdown)
			fleet = append(fleet, inst)
		}
		return fleet
	}
	sim := newSimulator(
		newFleet(browserResources(d)),
		newFleet(replicaResources(checkoutIdentity, d, rng)),
		newFleet(replicaResources(paymentsIdentity, d, rng)),
		simConfig{namespace: *namespace, region: *region, errorRatio: *errorRatio, dbErrorRatio: *dbErrorRatio}, rng,
	)

	log.Printf("correlated-telemetrygen %s: %.2g checkout requests/s, %.0f%% payment errors, %.0f%% database errors",
		version, *rate, *errorRatio*100, *dbErrorRatio*100)
	if inc.after > 0 {
		log.Printf("%s incident after %s, lasting %s, every %s", inc.kind, inc.after, inc.duration, inc.every)
	}
	ticker := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	defer ticker.Stop()
	started := time.Now()
	sent := 0
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case now := <-ticker.C:
			elapsed := now.Sub(started)
			if kind := inc.at(elapsed); kind != sim.incident {
				if kind == "" {
					log.Printf("%s incident over", sim.incident)
				} else {
					log.Printf("%s incident started", kind)
				}
				sim.incident = kind
			}
			if inc.drops(elapsed, rng.Float64()) {
				continue
			}
			sim.run(context.Background(), now)
			sent++
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var errs []error
	for _, shutdown := range shutdowns {
		errs = append(errs, shutdown(shutdownCtx))
	}
	if err := errors.Join(errs...); err != nil {
		log.Fatalf("flushing telemetry: %v", err)
	}
	log.Printf("sent %d interactions", sent)
}

// instanceResource is the resource of one simulated instance. Its attributes include
// telemetry.sdk.*, because the web front end reports the browser SDK rather than this one.
// OTEL_RESOURCE_ATTRIBUTES is applied after the simulated attributes, so it overrides them,
// except for service.name: the env detector also reads OTEL_SERVICE_NAME, which would give
// every simulated service one name.
func instanceResource(ctx context.Context, attrs []attribute.KeyValue) (*resource.Resource, error) {
	var name []attribute.KeyValue
	for _, kv := range attrs {
		if kv.Key == "service.name" {
			name = append(name, kv)
		}
	}
	return resource.New(ctx,
		resource.WithSchemaURL(schemaURL),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
		resource.WithAttributes(name...),
	)
}

// newOTLPInstance builds a tracer and a logger that share one resource and export over OTLP gRPC.
func newOTLPInstance(ctx context.Context, r replica) (instance, func(context.Context) error, error) {
	res, err := instanceResource(ctx, r.resource)
	if err != nil {
		return instance{}, nil, err
	}

	spanExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return instance{}, nil, err
	}
	logExporter, err := otlploggrpc.New(ctx)
	if err != nil {
		return instance{}, nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(spanExporter))
	lp := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)))

	shutdown := func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), lp.Shutdown(ctx))
	}
	return instance{
		tracer: tp.Tracer(scope, trace.WithSchemaURL(schemaURL)),
		logger: lp.Logger(scope, otellog.WithSchemaURL(schemaURL)),
		podIP:  r.podIP,
	}, shutdown, nil
}

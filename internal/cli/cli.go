package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/enotpoloskun/mrfpipeline/internal/artifact"
	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/discovery"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/mrfparse"
	"github.com/enotpoloskun/mrfpipeline/internal/reconcile"
	"github.com/enotpoloskun/mrfpipeline/internal/release"
	"github.com/enotpoloskun/mrfpipeline/internal/work"
)

// version defaults to dev and may be replaced with a linker flag:
// -X github.com/enotpoloskun/mrfpipeline/internal/cli.version=<value>
var version = "dev"

const (
	cmdMigrate    = "migrate"
	cmdWork       = "work"
	cmdDiscover   = "discover"
	cmdReconcile  = "reconcile"
	cmdRetry      = "retry"
	cmdMonth      = "month"
	monthStatus   = "status"
	monthActivate = "activate"
)

// Main is the process entry: signals, os.Args, and standard streams.
func Main() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return Run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
}

// Run is the testable command entry point.
func Run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	text, err := execute(ctx, args, getenv)
	if err == nil {
		if _, werr := io.WriteString(stdout, text); werr != nil {
			return 1
		}
		return 0
	}
	return report(err, stderr)
}

func execute(ctx context.Context, args []string, getenv func(string) string) (string, error) {
	parsed, err := parse(args)
	if err != nil {
		return "", err
	}
	if parsed.help {
		return parsed.helpText, nil
	}
	if parsed.version {
		return "mrfpipeline " + version + "\n", nil
	}

	var text string
	var opErr error
	switch parsed.command {
	case cmdMigrate:
		text, opErr = runMigrate(ctx, getenv)
	case cmdWork:
		opErr = runWork(ctx, getenv)
	case cmdDiscover:
		text, opErr = runDiscover(ctx, getenv, parsed.payer, parsed.month, parsed.limit)
	case cmdReconcile:
		text, opErr = runReconcile(ctx, getenv)
	case cmdRetry:
		text, opErr = runRetry(ctx, getenv, parsed.stage, parsed.id)
	case cmdMonth:
		switch parsed.monthAction {
		case monthStatus:
			text, opErr = runMonthStatus(ctx, getenv, parsed.payer, parsed.month)
		case monthActivate:
			text, opErr = runMonthActivate(ctx, getenv, parsed.payer, parsed.month)
		default:
			return "", &usageError{command: cmdMonth, reason: "missing subcommand"}
		}
	default:
		return "", &usageError{reason: "unknown command"}
	}
	if opErr != nil {
		return "", &cmdError{command: parsed.command, err: opErr}
	}
	return text, nil
}

func runMigrate(ctx context.Context, getenv func(string) string) (string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	databaseURL := getenv(config.EnvDatabaseURL)
	if err := config.ValidateDatabaseURL(databaseURL); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	result, err := database.Migrate(ctx, databaseURL)
	if err != nil {
		return "", err
	}
	return database.FormatResult(result)
}

func runWork(ctx context.Context, getenv func(string) string) error {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := config.ValidateDatabaseURL(getenv(config.EnvDatabaseURL)); err != nil {
		return err
	}
	art, err := config.NormalizeLocalPath(config.EnvArtifactRoot, getenv(config.EnvArtifactRoot))
	if err != nil {
		return err
	}
	warehouse, err := config.NormalizeLocalPath(config.EnvWarehousePath, getenv(config.EnvWarehousePath))
	if err != nil {
		return err
	}
	catalog, err := config.NormalizeLocalPath(config.EnvProviderCatalogPath, getenv(config.EnvProviderCatalogPath))
	if err != nil {
		return err
	}
	services, err := config.NormalizeLocalPath(config.EnvServicesPath, getenv(config.EnvServicesPath))
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pool, err := database.Open(ctx, getenv(config.EnvDatabaseURL), database.WorkMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return err
	}
	if err := artifact.CheckOverlap(art, warehouse, catalog, services); err != nil {
		return err
	}
	ws, err := artifact.Init(ctx, art)
	if err != nil {
		return err
	}
	if err := os.Setenv("TMPDIR", ws.StagingDir()); err != nil {
		return err
	}
	return work.Runtime{
		Pool:                pool,
		Workspace:           ws,
		Logger:              jobs.NewLogger(os.Stderr),
		ServicesPath:        services,
		WarehousePath:       warehouse,
		ProviderCatalogPath: catalog,
	}.Run(ctx)
}

func runDiscover(ctx context.Context, getenv func(string) string, payer, month, limit string) (string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := config.ValidateDatabaseURL(getenv(config.EnvDatabaseURL)); err != nil {
		return "", err
	}
	if err := config.ValidatePayer(payer); err != nil {
		return "", err
	}
	if err := config.ValidateCollectionMonth(month); err != nil {
		return "", err
	}
	n, err := config.ValidateLimit(limit)
	if err != nil {
		return "", err
	}
	if n > math.MaxInt32 {
		return "", fmt.Errorf("%w: %s: must fit in a PostgreSQL integer", config.ErrInvalidConfig, config.FieldLimit)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	pool, err := database.Open(ctx, getenv(config.EnvDatabaseURL), database.DiscoverMaxConns)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	return discovery.Enqueue(ctx, pool, payer, month, n)
}

func runReconcile(ctx context.Context, getenv func(string) string) (string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := config.ValidateDatabaseURL(getenv(config.EnvDatabaseURL)); err != nil {
		return "", err
	}
	art, err := config.NormalizeLocalPath(config.EnvArtifactRoot, getenv(config.EnvArtifactRoot))
	if err != nil {
		return "", err
	}
	warehouse, err := config.NormalizeLocalPath(config.EnvWarehousePath, getenv(config.EnvWarehousePath))
	if err != nil {
		return "", err
	}
	catalog, err := config.NormalizeLocalPath(config.EnvProviderCatalogPath, getenv(config.EnvProviderCatalogPath))
	if err != nil {
		return "", err
	}
	services, err := config.NormalizeLocalPath(config.EnvServicesPath, getenv(config.EnvServicesPath))
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	pool, err := database.Open(ctx, getenv(config.EnvDatabaseURL), database.WorkMaxConns)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return "", err
	}
	if err := artifact.CheckOverlap(art, warehouse, catalog, services); err != nil {
		return "", err
	}
	ws, err := artifact.Init(ctx, art)
	if err != nil {
		return "", err
	}
	if err := os.Setenv("TMPDIR", ws.StagingDir()); err != nil {
		return "", err
	}
	report, err := reconcile.Command(ctx, reconcile.Params{
		Pool: pool, Workspace: ws, WarehousePath: warehouse,
		ProviderCatalogPath: catalog, ServicesPath: services,
		Logger: jobs.NewLogger(os.Stderr),
	})
	if err != nil {
		return "", err
	}
	return reconcile.FormatReport(report)
}

func runRetry(ctx context.Context, getenv func(string) string, stage, id string) (string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := config.ValidateDatabaseURL(getenv(config.EnvDatabaseURL)); err != nil {
		return "", err
	}
	if err := config.ValidateStage(stage, jobs.ProductionKinds()); err != nil {
		return "", err
	}
	domainID, err := config.ValidatePositiveID(config.FieldID, id)
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	pool, err := database.Open(ctx, getenv(config.EnvDatabaseURL), database.WorkMaxConns)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	result, err := reconcile.Retry(ctx, pool, stage, domainID)
	if err != nil {
		return "", err
	}
	return reconcile.FormatRetry(result)
}

func runMonthStatus(ctx context.Context, getenv func(string) string, payer, month string) (string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := config.ValidateDatabaseURL(getenv(config.EnvDatabaseURL)); err != nil {
		return "", err
	}
	if (payer == "") != (month == "") {
		return "", &usageError{command: "month status", reason: "--payer and --collection-month must be supplied together"}
	}
	var monthDate time.Time
	if payer != "" {
		if err := config.ValidatePayerIdentifier(payer); err != nil {
			return "", err
		}
		var err error
		monthDate, err = parseMonthDate(month)
		if err != nil {
			return "", err
		}
	}
	pool, err := database.Open(ctx, getenv(config.EnvDatabaseURL), database.DiscoverMaxConns)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return "", err
	}
	if payer == "" {
		outputs, err := release.ListActiveOutputs(ctx, pool)
		if err != nil {
			return "", err
		}
		out := struct {
			ActiveOutputs []struct {
				PayerID         string `json:"payer_id"`
				CollectionMonth string `json:"collection_month"`
				OutputID        string `json:"output_id"`
			} `json:"active_outputs"`
		}{ActiveOutputs: make([]struct {
			PayerID         string `json:"payer_id"`
			CollectionMonth string `json:"collection_month"`
			OutputID        string `json:"output_id"`
		}, 0, len(outputs))}
		for _, item := range outputs {
			out.ActiveOutputs = append(out.ActiveOutputs, struct {
				PayerID         string `json:"payer_id"`
				CollectionMonth string `json:"collection_month"`
				OutputID        string `json:"output_id"`
			}{item.PayerID, item.CollectionMonth, item.OutputID})
		}
		return encodeJSON(out)
	}
	readiness, err := release.Readiness(ctx, pool, payer, monthDate)
	if err != nil {
		return "", err
	}
	return encodeJSON(readiness)
}

func runMonthActivate(ctx context.Context, getenv func(string) string, payer, month string) (string, error) {
	if ctx == nil {
		panic("nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := config.ValidateDatabaseURL(getenv(config.EnvDatabaseURL)); err != nil {
		return "", err
	}
	if err := config.ValidatePayerIdentifier(payer); err != nil {
		return "", err
	}
	monthDate, err := parseMonthDate(month)
	if err != nil {
		return "", err
	}
	art, err := config.NormalizeLocalPath(config.EnvArtifactRoot, getenv(config.EnvArtifactRoot))
	if err != nil {
		return "", err
	}
	warehouse, err := config.NormalizeLocalPath(config.EnvWarehousePath, getenv(config.EnvWarehousePath))
	if err != nil {
		return "", err
	}
	catalogPath, err := config.NormalizeLocalPath(config.EnvProviderCatalogPath, getenv(config.EnvProviderCatalogPath))
	if err != nil {
		return "", err
	}
	servicesPath, err := config.NormalizeLocalPath(config.EnvServicesPath, getenv(config.EnvServicesPath))
	if err != nil {
		return "", err
	}
	services, err := mrfparse.InspectServices(servicesPath)
	if err != nil {
		return "", err
	}
	catalog, err := consumeringest.InspectCatalog(catalogPath)
	if err != nil {
		return "", err
	}
	warehouseState, err := consumeringest.InspectWarehouse(warehouse)
	if err != nil {
		return "", err
	}
	if err := artifact.CheckOverlap(art, warehouse, catalogPath, servicesPath); err != nil {
		return "", err
	}
	if err := consumeringest.CheckWarehouseCatalog(warehouseState, catalog, art, services.Path); err != nil {
		return "", err
	}
	pool, err := database.Open(ctx, getenv(config.EnvDatabaseURL), database.WorkMaxConns)
	if err != nil {
		return "", err
	}
	defer pool.Close()
	if err := database.ValidateCurrent(ctx, pool); err != nil {
		return "", err
	}
	lease, err := database.AcquireWorkerLease(ctx, pool)
	if err != nil {
		if database.IsLeaseUnavailable(err) {
			return "", jobs.Failure(jobs.FailureWorkerLeaseUnavailable)
		}
		return "", err
	}
	defer func() { _ = lease.Release(context.Background()) }()
	result, err := release.Activate(ctx, pool, payer, monthDate, func(targets []release.Target) error {
		for _, target := range targets {
			if err := consumeringest.InspectCompletedSnapshot(warehouse, target.PayerID, target.CollectionMonth, target.OutputID); err != nil {
				return jobs.Failure(jobs.FailureReleaseNotReady)
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return encodeJSON(result)
}

func parseMonthDate(raw string) (time.Time, error) {
	if err := config.ValidateCollectionMonth(raw); err != nil {
		return time.Time{}, err
	}
	month, err := time.Parse("2006-01-02", raw+"-01")
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s", config.ErrInvalidConfig, config.FieldCollectionMonth)
	}
	return month, nil
}

func encodeJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(raw) + "\n", nil
}

type parsed struct {
	command     string
	help        bool
	helpText    string
	version     bool
	payer       string
	month       string
	limit       string
	stage       string
	id          string
	monthAction string
}

func parse(args []string) (parsed, error) {
	if len(args) == 0 {
		return parsed{}, &usageError{reason: "missing command"}
	}
	if args[0] == "--help" {
		if len(args) != 1 {
			return parsed{}, &usageError{reason: "invalid help invocation"}
		}
		return parsed{help: true, helpText: rootHelp}, nil
	}
	if args[0] == "--version" {
		if len(args) != 1 {
			return parsed{}, &usageError{reason: "invalid version invocation"}
		}
		return parsed{version: true}, nil
	}
	switch args[0] {
	case cmdMigrate, cmdWork, cmdDiscover, cmdReconcile, cmdRetry, cmdMonth:
		return parseCommand(args[0], args[1:])
	default:
		return parsed{}, &usageError{reason: "unknown command"}
	}
}

func parseCommand(command string, rest []string) (parsed, error) {
	if len(rest) == 1 && rest[0] == "--help" {
		return parsed{command: command, help: true, helpText: helpFor(command)}, nil
	}
	switch command {
	case cmdMigrate, cmdWork, cmdReconcile:
		if err := rejectExtra(command, rest); err != nil {
			return parsed{}, err
		}
		return parsed{command: command}, nil
	case cmdDiscover:
		payer, month, limit, err := parseDiscoverFlags(rest)
		if err != nil {
			return parsed{}, err
		}
		return parsed{command: command, payer: payer, month: month, limit: limit}, nil
	case cmdRetry:
		stage, id, err := parseRetryFlags(rest)
		if err != nil {
			return parsed{}, err
		}
		return parsed{command: command, stage: stage, id: id}, nil
	case cmdMonth:
		return parseMonthCommand(rest)
	default:
		return parsed{}, &usageError{reason: "unknown command"}
	}
}

func parseMonthCommand(rest []string) (parsed, error) {
	if len(rest) == 1 && rest[0] == "--help" {
		return parsed{command: cmdMonth, help: true, helpText: monthHelp}, nil
	}
	if len(rest) == 0 {
		return parsed{}, &usageError{command: cmdMonth, reason: "missing subcommand"}
	}
	action := rest[0]
	if len(rest) == 2 && rest[1] == "--help" {
		switch action {
		case monthStatus:
			return parsed{command: cmdMonth, monthAction: action, help: true, helpText: monthStatusHelp}, nil
		case monthActivate:
			return parsed{command: cmdMonth, monthAction: action, help: true, helpText: monthActivateHelp}, nil
		}
	}
	switch action {
	case monthStatus:
		payer, month, err := parseMonthFlags("month status", rest[1:], false)
		if err != nil {
			return parsed{}, err
		}
		return parsed{command: cmdMonth, monthAction: action, payer: payer, month: month}, nil
	case monthActivate:
		payer, month, err := parseMonthFlags("month activate", rest[1:], true)
		if err != nil {
			return parsed{}, err
		}
		return parsed{command: cmdMonth, monthAction: action, payer: payer, month: month}, nil
	default:
		return parsed{}, &usageError{command: cmdMonth, reason: "unknown subcommand"}
	}
}

func parseMonthFlags(command string, rest []string, required bool) (payer, month string, err error) {
	var havePayer, haveMonth bool
	for i := 0; i < len(rest); {
		if rest[i] == "--" || isSingleDash(rest[i]) || !strings.HasPrefix(rest[i], "--") {
			return "", "", &usageError{command: command, reason: "invalid argument"}
		}
		name, value, next, ferr := takeFlag(command, rest, i)
		if ferr != nil {
			return "", "", ferr
		}
		switch name {
		case "payer":
			payer, havePayer = value, true
		case "collection-month":
			month, haveMonth = value, true
		default:
			return "", "", &usageError{command: command, reason: "unsupported flag"}
		}
		i = next
	}
	if required && (!havePayer || !haveMonth) {
		if !havePayer {
			return "", "", &usageError{command: command, reason: "missing required flag --payer"}
		}
		return "", "", &usageError{command: command, reason: "missing required flag --collection-month"}
	}
	if !required && havePayer != haveMonth {
		return "", "", &usageError{command: command, reason: "--payer and --collection-month must be supplied together"}
	}
	return payer, month, nil
}

func rejectExtra(command string, rest []string) error {
	for _, arg := range rest {
		if arg == "--" || isSingleDash(arg) || strings.HasPrefix(arg, "--") {
			return &usageError{command: command, reason: "unsupported flag"}
		}
		return &usageError{command: command, reason: "unexpected argument"}
	}
	return nil
}

func parseDiscoverFlags(rest []string) (payer, month, limit string, err error) {
	var havePayer, haveMonth, haveLimit bool
	for i := 0; i < len(rest); {
		arg := rest[i]
		if arg == "--" {
			return "", "", "", &usageError{command: cmdDiscover, reason: "unsupported flag"}
		}
		if isSingleDash(arg) {
			return "", "", "", &usageError{command: cmdDiscover, reason: "unsupported flag"}
		}
		if !strings.HasPrefix(arg, "--") {
			return "", "", "", &usageError{command: cmdDiscover, reason: "unexpected argument"}
		}
		name, value, next, ferr := takeFlag(cmdDiscover, rest, i)
		if ferr != nil {
			return "", "", "", ferr
		}
		switch name {
		case "payer":
			payer = value
			havePayer = true
		case "collection-month":
			month = value
			haveMonth = true
		case "limit":
			limit = value
			haveLimit = true
		default:
			return "", "", "", &usageError{command: cmdDiscover, reason: "unsupported flag"}
		}
		i = next
	}
	switch {
	case !havePayer:
		return "", "", "", &usageError{command: cmdDiscover, reason: "missing required flag --payer"}
	case !haveMonth:
		return "", "", "", &usageError{command: cmdDiscover, reason: "missing required flag --collection-month"}
	case !haveLimit:
		return "", "", "", &usageError{command: cmdDiscover, reason: "missing required flag --limit"}
	}
	return payer, month, limit, nil
}

func parseRetryFlags(rest []string) (stage, id string, err error) {
	var haveStage, haveID bool
	for i := 0; i < len(rest); {
		arg := rest[i]
		if arg == "--" || isSingleDash(arg) {
			return "", "", &usageError{command: cmdRetry, reason: "unsupported flag"}
		}
		if !strings.HasPrefix(arg, "--") {
			return "", "", &usageError{command: cmdRetry, reason: "unexpected argument"}
		}
		name, value, next, ferr := takeFlag(cmdRetry, rest, i)
		if ferr != nil {
			return "", "", ferr
		}
		switch name {
		case "stage":
			stage = value
			haveStage = true
		case "id":
			id = value
			haveID = true
		default:
			return "", "", &usageError{command: cmdRetry, reason: "unsupported flag"}
		}
		i = next
	}
	switch {
	case !haveStage:
		return "", "", &usageError{command: cmdRetry, reason: "missing required flag --stage"}
	case !haveID:
		return "", "", &usageError{command: cmdRetry, reason: "missing required flag --id"}
	}
	return stage, id, nil
}

func takeFlag(command string, args []string, i int) (name, value string, next int, err error) {
	body := strings.TrimPrefix(args[i], "--")
	if body == "" || body[0] == '-' {
		return "", "", 0, &usageError{command: command, reason: "unsupported flag"}
	}
	if name, value, ok := strings.Cut(body, "="); ok {
		if name == "" {
			return "", "", 0, &usageError{command: command, reason: "unsupported flag"}
		}
		return name, value, i + 1, nil
	}
	if i+1 >= len(args) || strings.HasPrefix(args[i+1], "--") {
		return "", "", 0, &usageError{command: command, reason: "missing flag argument"}
	}
	return body, args[i+1], i + 2, nil
}

func isSingleDash(arg string) bool {
	return strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--")
}

type usageError struct {
	command string
	reason  string
}

func (e *usageError) Error() string {
	if e.reason == "" {
		return "invalid usage"
	}
	return e.reason
}

type cmdError struct {
	command string
	err     error
}

func (e *cmdError) Error() string { return e.err.Error() }
func (e *cmdError) Unwrap() error { return e.err }

func report(err error, stderr io.Writer) int {
	command := commandOf(err)
	if isUsage(err) || errors.Is(err, config.ErrInvalidConfig) {
		if _, werr := fmt.Fprintln(stderr, err.Error()); werr != nil {
			return 1
		}
		if _, werr := fmt.Fprintln(stderr, hint(command)); werr != nil {
			return 1
		}
		return 2
	}
	if _, werr := fmt.Fprintln(stderr, err.Error()); werr != nil {
		return 1
	}
	if errors.Is(err, database.ErrDatabase) {
		return 3
	}
	if errors.Is(err, jobs.ErrJob) {
		return 4
	}
	return 1
}

func isUsage(err error) bool {
	var u *usageError
	return errors.As(err, &u)
}

func commandOf(err error) string {
	var c *cmdError
	if errors.As(err, &c) {
		return c.command
	}
	var u *usageError
	if errors.As(err, &u) {
		return u.command
	}
	return ""
}

func hint(command string) string {
	switch command {
	case cmdMigrate:
		return "Try 'mrfpipeline migrate --help'."
	case cmdWork:
		return "Try 'mrfpipeline work --help'."
	case cmdDiscover:
		return "Try 'mrfpipeline discover --help'."
	case cmdReconcile:
		return "Try 'mrfpipeline reconcile --help'."
	case cmdRetry:
		return "Try 'mrfpipeline retry --help'."
	case cmdMonth:
		return "Try 'mrfpipeline month --help'."
	case "month status":
		return "Try 'mrfpipeline month status --help'."
	case "month activate":
		return "Try 'mrfpipeline month activate --help'."
	default:
		return "Try 'mrfpipeline --help'."
	}
}

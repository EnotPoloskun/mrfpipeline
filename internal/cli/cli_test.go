package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/database"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

func fatalEnv(t *testing.T) func(string) string {
	t.Helper()
	return func(string) string {
		t.Fatal("informational command must not read the environment")
		return ""
	}
}

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func validDB() map[string]string {
	return map[string]string{config.EnvDatabaseURL: "postgres://user:supersecret@127.0.0.1:1/db"}
}

func validWorkEnv(t *testing.T) map[string]string {
	t.Helper()
	base := filepath.Join(t.TempDir(), "missing")
	return map[string]string{
		config.EnvDatabaseURL:         "postgres://user:supersecret@example.invalid/db",
		config.EnvArtifactRoot:        filepath.Join(base, "artifacts"),
		config.EnvWarehousePath:       filepath.Join(base, "warehouse"),
		config.EnvProviderCatalogPath: filepath.Join(base, "catalog"),
		config.EnvServicesPath:        filepath.Join(base, "services.csv"),
		config.EnvMRFResidentCapacity: "4",
	}
}

func discoverArgs(limit string) []string {
	return []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", limit, "--mrf-source-limit", "all"}
}

func runCLI(ctx context.Context, args []string, getenv func(string) string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := Run(ctx, args, getenv, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestInformationalInvocations(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		{"--help"},
		{"migrate", "--help"},
		{"work", "--help"},
		{"discover", "--help"},
		{"reconcile", "--help"},
		{"retry", "--help"},
		{"month", "--help"},
		{"--version"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Parallel()
			code, stdout, stderr := runCLI(context.Background(), args, fatalEnv(t))
			if code != 0 {
				t.Fatalf("exit %d stderr=%q", code, stderr)
			}
			if stdout == "" {
				t.Fatal("expected informational stdout")
			}
			if strings.Contains(stdout, "later story") {
				t.Fatalf("help mentioned later story: %q", stdout)
			}
			if args[0] == "--help" {
				if !strings.Contains(stdout, "reconcile") || !strings.Contains(stdout, "retry") {
					t.Fatalf("root help must list five commands: %q", stdout)
				}
			}
		})
	}
}

func TestCurrentOperatorCoordinationHelp(t *testing.T) {
	t.Parallel()
	for command, want := range map[string][]string{
		"retry":     {"may run while worker roles are active", "execution locks"},
		"reconcile": {"singleton control lease", "Stop the control worker", "MRF and consumer roles may remain running"},
	} {
		text := helpFor(command)
		for _, fragment := range want {
			if !strings.Contains(text, fragment) {
				t.Fatalf("%s help missing %q: %s", command, fragment, text)
			}
		}
	}
	if strings.Contains(helpFor("retry"), "worker must be stopped") {
		t.Fatal("retry help retained obsolete stop-worker instruction")
	}
}

func TestVersionOutput(t *testing.T) {
	t.Parallel()
	code, stdout, stderr := runCLI(context.Background(), []string{"--version"}, fatalEnv(t))
	if code != 0 || stderr != "" {
		t.Fatalf("exit %d stderr=%q", code, stderr)
	}
	if stdout != "mrfpipeline dev\n" {
		t.Fatalf("got %q", stdout)
	}
}

func TestUsageErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		args []string
		hint string
	}{
		{"no command", nil, "Try 'mrfpipeline --help'."},
		{"unknown command", []string{"frobnicate"}, "Try 'mrfpipeline --help'."},
		{"root extra after help", []string{"--help", "migrate"}, "Try 'mrfpipeline --help'."},
		{"root extra after version", []string{"--version", "x"}, "Try 'mrfpipeline --help'."},
		{"root unknown flag", []string{"--bogus"}, "Try 'mrfpipeline --help'."},
		{"root single dash", []string{"-help"}, "Try 'mrfpipeline --help'."},
		{"migrate extra", []string{"migrate", "now"}, "Try 'mrfpipeline migrate --help'."},
		{"migrate unknown flag", []string{"migrate", "--bogus"}, "Try 'mrfpipeline migrate --help'."},
		{"migrate single dash", []string{"migrate", "-x"}, "Try 'mrfpipeline migrate --help'."},
		{"migrate bare dashdash", []string{"migrate", "--"}, "Try 'mrfpipeline migrate --help'."},
		{"migrate version combo", []string{"migrate", "--version"}, "Try 'mrfpipeline migrate --help'."},
		{"migrate help combo", []string{"migrate", "--help", "--bogus"}, "Try 'mrfpipeline migrate --help'."},
		{"work extra", []string{"work", "x"}, "Try 'mrfpipeline work --help'."},
		{"work flag", []string{"work", "--queue", "x"}, "Try 'mrfpipeline work --help'."},
		{"work single dash", []string{"work", "-limit", "1"}, "Try 'mrfpipeline work --help'."},
		{"discover positional", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "1", "extra"}, "Try 'mrfpipeline discover --help'."},
		{"discover unknown flag", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "1", "--foo"}, "Try 'mrfpipeline discover --help'."},
		{"discover single dash", []string{"discover", "-payer", "uhc", "--collection-month", "2026-08", "--limit", "1"}, "Try 'mrfpipeline discover --help'."},
		{"discover bare dashdash", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "1", "--"}, "Try 'mrfpipeline discover --help'."},
		{"discover missing limit", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08"}, "Try 'mrfpipeline discover --help'."},
		{"discover missing payer", []string{"discover", "--collection-month", "2026-08", "--limit", "1"}, "Try 'mrfpipeline discover --help'."},
		{"discover missing month", []string{"discover", "--payer", "uhc", "--limit", "1"}, "Try 'mrfpipeline discover --help'."},
		{"discover limit no arg", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit"}, "Try 'mrfpipeline discover --help'."},
		{"discover limit then flag", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "--payer", "uhc"}, "Try 'mrfpipeline discover --help'."},
		{"discover help combo", []string{"discover", "--payer", "uhc", "--help"}, "Try 'mrfpipeline discover --help'."},
		{"reconcile extra", []string{"reconcile", "now"}, "Try 'mrfpipeline reconcile --help'."},
		{"reconcile flag", []string{"reconcile", "--all"}, "Try 'mrfpipeline reconcile --help'."},
		{"retry missing id", []string{"retry", "--stage", "toc.parse"}, "Try 'mrfpipeline retry --help'."},
		{"retry missing stage", []string{"retry", "--id", "1"}, "Try 'mrfpipeline retry --help'."},
		{"retry unknown flag", []string{"retry", "--stage", "toc.parse", "--id", "1", "--foo"}, "Try 'mrfpipeline retry --help'."},
		{"retry positional", []string{"retry", "toc.parse", "1"}, "Try 'mrfpipeline retry --help'."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, stdout, stderr := runCLI(context.Background(), tc.args, fatalEnv(t))
			if code != 2 {
				t.Fatalf("exit %d want 2 stderr=%q", code, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout %q", stdout)
			}
			if !strings.Contains(stderr, tc.hint+"\n") && !strings.Contains(stderr, tc.hint) {
				t.Fatalf("stderr %q missing hint %q", stderr, tc.hint)
			}
			_, err := execute(context.Background(), tc.args, fatalEnv(t))
			if errors.Is(err, config.ErrInvalidConfig) {
				t.Fatal("usage error wrapped ErrInvalidConfig")
			}
			if !isUsage(err) {
				t.Fatalf("want usage error, got %v", err)
			}
		})
	}
}

func TestRepeatedFlagsUseFinalOccurrence(t *testing.T) {
	t.Parallel()
	env := envMap(validDB())
	ok := []string{"discover", "--payer", "UHC", "--payer", "uhc", "--collection-month", "1999-01", "--collection-month", "2026-08", "--limit", "0", "--limit", "5", "--mrf-source-limit", "all"}
	code, stdout, _ := runCLI(context.Background(), ok, env)
	if code != 3 || stdout != "" {
		t.Fatalf("final valid values: exit %d stdout=%q", code, stdout)
	}
	_, err := execute(context.Background(), ok, env)
	if !errors.Is(err, database.ErrDatabase) {
		t.Fatalf("got %v", err)
	}

	bad := []string{"discover", "--payer", "uhc", "--payer", "UHC", "--collection-month", "2026-08", "--limit", "5"}
	code, stdout, stderr := runCLI(context.Background(), bad, env)
	if code != 2 || stdout != "" {
		t.Fatalf("final invalid payer: exit %d", code)
	}
	if !strings.Contains(stderr, config.FieldPayer) {
		t.Fatalf("stderr %q", stderr)
	}
}

func TestEqualsAndSpaceFlagForms(t *testing.T) {
	t.Parallel()
	env := envMap(validDB())
	args := []string{"discover", "--payer=uhc", "--collection-month=2026-08", "--limit=2", "--mrf-source-limit=all"}
	_, err := execute(context.Background(), args, env)
	if !errors.Is(err, database.ErrDatabase) {
		t.Fatalf("got %v", err)
	}
}

func TestCommandEnvironmentRequirements(t *testing.T) {
	t.Parallel()
	invalidWorker := map[string]string{
		config.EnvDatabaseURL:         "postgres://user:supersecret@127.0.0.1:1/db",
		config.EnvArtifactRoot:        "s3://bucket/artifacts",
		config.EnvWarehousePath:       "https://example.invalid/wh",
		config.EnvProviderCatalogPath: "file:///catalog",
		config.EnvServicesPath:        "http://example.invalid/services.csv",
	}

	t.Run("migrate requires only database", func(t *testing.T) {
		t.Parallel()
		code, stdout, stderr := runCLI(context.Background(), []string{"migrate"}, envMap(invalidWorker))
		if code != 3 || stdout != "" {
			t.Fatalf("exit %d stdout=%q stderr=%q", code, stdout, stderr)
		}
		if strings.Contains(stderr, "supersecret") || strings.Contains(stderr, "Try '") {
			t.Fatalf("stderr %q", stderr)
		}
		_, err := execute(context.Background(), []string{"migrate"}, envMap(invalidWorker))
		if !errors.Is(err, database.ErrDatabase) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("discover ignores worker paths", func(t *testing.T) {
		t.Parallel()
		_, err := execute(context.Background(), discoverArgs("1"), envMap(invalidWorker))
		if !errors.Is(err, database.ErrDatabase) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("migrate missing database", func(t *testing.T) {
		t.Parallel()
		code, stdout, stderr := runCLI(context.Background(), []string{"migrate"}, envMap(nil))
		if code != 2 || stdout != "" {
			t.Fatalf("exit %d", code)
		}
		if !strings.Contains(stderr, config.EnvDatabaseURL) {
			t.Fatalf("stderr %q", stderr)
		}
		if !strings.Contains(stderr, "Try 'mrfpipeline migrate --help'.") {
			t.Fatalf("stderr %q", stderr)
		}
	})

	t.Run("reconcile requires every worker path", func(t *testing.T) {
		t.Parallel()
		env := validWorkEnv(t)
		delete(env, config.EnvServicesPath)
		code, stdout, stderr := runCLI(context.Background(), []string{"reconcile"}, envMap(env))
		if code != 2 || stdout != "" {
			t.Fatalf("exit %d", code)
		}
		if !strings.Contains(stderr, config.EnvServicesPath) {
			t.Fatalf("stderr %q", stderr)
		}
	})

	t.Run("retry requires only database", func(t *testing.T) {
		t.Parallel()
		_, err := execute(context.Background(), []string{"retry", "--stage", "toc.parse", "--id", "1"}, envMap(invalidWorker))
		if !errors.Is(err, database.ErrDatabase) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("work requires every worker path", func(t *testing.T) {
		t.Parallel()
		env := validWorkEnv(t)
		delete(env, config.EnvServicesPath)
		code, stdout, stderr := runCLI(context.Background(), []string{"work", "--role", "control"}, envMap(env))
		if code != 2 || stdout != "" {
			t.Fatalf("exit %d", code)
		}
		if !strings.Contains(stderr, config.EnvServicesPath) {
			t.Fatalf("stderr %q", stderr)
		}
		if strings.Contains(stderr, env[config.EnvArtifactRoot]) {
			t.Fatalf("stderr exposed a path: %q", stderr)
		}
	})

	t.Run("work first path failure wins", func(t *testing.T) {
		t.Parallel()
		env := validWorkEnv(t)
		env[config.EnvArtifactRoot] = "s3://nope"
		env[config.EnvWarehousePath] = "s3://also"
		_, err := execute(context.Background(), []string{"work", "--role", "control"}, envMap(env))
		if !errors.Is(err, config.ErrInvalidConfig) {
			t.Fatalf("got %v", err)
		}
		if !strings.Contains(err.Error(), config.EnvArtifactRoot) {
			t.Fatalf("got %v", err)
		}
		if strings.Contains(err.Error(), config.EnvWarehousePath) {
			t.Fatalf("should stop at first path: %v", err)
		}
	})
}

func TestDatabaseConfigRedactionOnCLI(t *testing.T) {
	t.Parallel()
	secret := "postgres://user:supersecret@example.invalid/db\n"
	code, stdout, stderr := runCLI(context.Background(), []string{"migrate"}, envMap(map[string]string{
		config.EnvDatabaseURL: secret,
	}))
	if code != 2 || stdout != "" {
		t.Fatalf("exit %d", code)
	}
	if strings.Contains(stderr, "supersecret") || strings.Contains(stderr, secret) {
		t.Fatalf("stderr leaked database value: %q", stderr)
	}
	if !strings.Contains(stderr, config.EnvDatabaseURL) {
		t.Fatalf("stderr %q", stderr)
	}
}

func TestDiscoverSemanticValidation(t *testing.T) {
	t.Parallel()
	env := envMap(validDB())
	cases := []struct {
		name  string
		args  []string
		field string
	}{
		{"payer", []string{"discover", "--payer", "UHC", "--collection-month", "2026-08", "--limit", "1"}, config.FieldPayer},
		{"month", []string{"discover", "--payer", "uhc", "--collection-month", "0000-01", "--limit", "1"}, config.FieldCollectionMonth},
		{"limit zero", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "0"}, config.FieldLimit},
		{"limit plus", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "+5"}, config.FieldLimit},
		{"limit hex", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "0x10"}, config.FieldLimit},
		{"limit empty", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit="}, config.FieldLimit},
		{"limit overflow", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "9223372036854775808"}, config.FieldLimit},
		{"limit exceeds integer", []string{"discover", "--payer", "uhc", "--collection-month", "2026-08", "--limit", "2147483648"}, config.FieldLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, stdout, stderr := runCLI(context.Background(), tc.args, env)
			if code != 2 || stdout != "" {
				t.Fatalf("exit %d stderr=%q", code, stderr)
			}
			if !strings.Contains(stderr, tc.field) {
				t.Fatalf("stderr %q missing %s", stderr, tc.field)
			}
			_, err := execute(context.Background(), tc.args, env)
			if !errors.Is(err, config.ErrInvalidConfig) {
				t.Fatalf("got %v", err)
			}
			if isUsage(err) {
				t.Fatal("semantic error classified as usage")
			}
		})
	}

	for _, limit := range []string{"1", "2", "5", "10", "02"} {
		_, err := execute(context.Background(), discoverArgs(limit), env)
		if !errors.Is(err, database.ErrDatabase) {
			t.Fatalf("limit %s: %v", limit, err)
		}
	}
}

func TestRetrySemanticValidation(t *testing.T) {
	t.Parallel()
	env := envMap(validDB())
	cases := []struct {
		name  string
		args  []string
		field string
	}{
		{"stage", []string{"retry", "--stage", "not.a.kind", "--id", "1"}, config.FieldStage},
		{"id zero", []string{"retry", "--stage", "toc.parse", "--id", "0"}, config.FieldID},
		{"id plus", []string{"retry", "--stage", "toc.parse", "--id", "+5"}, config.FieldID},
		{"id hex", []string{"retry", "--stage", "toc.parse", "--id", "0x10"}, config.FieldID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, stdout, stderr := runCLI(context.Background(), tc.args, env)
			if code != 2 || stdout != "" {
				t.Fatalf("exit %d stderr=%q", code, stderr)
			}
			if !strings.Contains(stderr, tc.field) {
				t.Fatalf("stderr %q missing %s", stderr, tc.field)
			}
		})
	}
	_, err := execute(context.Background(), []string{"retry", "--stage", "consumer.attach_plans", "--id", "0230"}, env)
	if !errors.Is(err, database.ErrDatabase) {
		t.Fatalf("leading zeroes: %v", err)
	}
}

func TestNilContextPanicsBeforeValidation(t *testing.T) {
	t.Parallel()
	getenv := func(string) string {
		t.Fatal("must not read environment before nil-context panic")
		return ""
	}
	for _, fn := range []func(){
		func() { _, _ = runMigrate(nil, getenv) },
		func() { _ = runWork(nil, getenv) },
		func() { _, _ = runDiscover(nil, getenv, "UHC", "bad", "0") },
		func() { _, _ = runReconcile(nil, getenv) },
		func() { _, _ = runRetry(nil, getenv, "toc.parse", "1") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		}()
	}
}

type sequencedCtx struct {
	context.Context
	hits int
}

func (c *sequencedCtx) Err() error {
	c.hits++
	if c.hits >= 2 {
		return context.Canceled
	}
	return nil
}

func TestCancellationPrecedence(t *testing.T) {
	t.Parallel()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := runMigrate(canceled, envMap(nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("entry cancel: %v", err)
	}
	if errors.Is(err, config.ErrInvalidConfig) {
		t.Fatal("entry cancel must precede invalid config")
	}

	later := &sequencedCtx{Context: context.Background()}
	_, err = runMigrate(later, envMap(validDB()))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("post-validation cancel: %v", err)
	}
	if errors.Is(err, database.ErrDatabase) {
		t.Fatal("post-validation cancel must precede database work")
	}

	race := &sequencedCtx{Context: context.Background()}
	_, err = runMigrate(race, envMap(nil))
	if !errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("config should win when entry is not canceled: %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("must not re-check cancellation during validation")
	}
}

func TestErrorClassification(t *testing.T) {
	t.Parallel()
	env := envMap(validWorkEnv(t))
	_, err := execute(context.Background(), []string{"work", "--role", "control"}, env)
	if !errors.Is(err, database.ErrDatabase) || errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("work database failure: %v", err)
	}

	_, err = execute(context.Background(), []string{"migrate"}, envMap(validDB()))
	if !errors.Is(err, database.ErrDatabase) || errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("migrate database failure: %v", err)
	}
	code, stdout, stderr := runCLI(context.Background(), []string{"migrate"}, envMap(validDB()))
	if code != 3 || stdout != "" {
		t.Fatalf("migrate database exit %d stdout=%q", code, stdout)
	}
	if strings.Contains(stderr, "supersecret") || strings.Contains(stderr, "Try '") {
		t.Fatalf("stderr %q", stderr)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = execute(canceled, []string{"migrate"}, envMap(validDB()))
	if !errors.Is(err, context.Canceled) || errors.Is(err, config.ErrInvalidConfig) {
		t.Fatalf("cancel: %v", err)
	}

	code, stdout, _ = runCLI(canceled, []string{"migrate"}, envMap(validDB()))
	if code != 1 || stdout != "" {
		t.Fatalf("cancel exit %d stdout=%q", code, stdout)
	}

	code, stdout, _ = runCLI(context.Background(), []string{"work", "--role", "control"}, env)
	if code != 3 || stdout != "" {
		t.Fatalf("work database exit %d", code)
	}

	var jobStderr bytes.Buffer
	code = report(fmt.Errorf("%w: insert", jobs.ErrJob), &jobStderr)
	if code != 4 {
		t.Fatalf("job exit %d", code)
	}
	if strings.Contains(jobStderr.String(), "Try '") {
		t.Fatalf("job error printed a hint: %q", jobStderr.String())
	}

	var bothStderr bytes.Buffer
	code = report(fmt.Errorf("%w: %w: insert", jobs.ErrJob, database.ErrDatabase), &bothStderr)
	if code != 3 {
		t.Fatalf("job+database exit %d", code)
	}
}

func TestWorkUnreachableDatabaseLeavesPathsUntouched(t *testing.T) {
	t.Parallel()
	env := validWorkEnv(t)
	code, stdout, _ := runCLI(context.Background(), []string{"work", "--role", "control"}, envMap(env))
	if code != 3 || stdout != "" {
		t.Fatalf("exit %d", code)
	}
	for _, key := range []string{
		config.EnvArtifactRoot,
		config.EnvWarehousePath,
		config.EnvProviderCatalogPath,
		config.EnvServicesPath,
	} {
		if _, err := os.Lstat(env[key]); !os.IsNotExist(err) {
			t.Fatalf("%s: got %v, path should not exist", key, err)
		}
	}
}

func TestHelpVersionWriteFailure(t *testing.T) {
	t.Parallel()
	code := Run(context.Background(), []string{"--version"}, fatalEnv(t), failWriter{}, io.Discard)
	if code != 1 {
		t.Fatalf("exit %d", code)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

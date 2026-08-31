package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repositoryFile(t *testing.T, name string) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository")
	}
	return filepath.Join(filepath.Dir(source), "..", "..", name)
}

func readRepositoryFile(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(repositoryFile(t, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(raw)
}

func TestLocalPackagingContract(t *testing.T) {
	dockerfile := readRepositoryFile(t, "Dockerfile")
	for _, fragment := range []string{
		"--mount=type=ssh",
		"CGO_ENABLED=0 go build",
		"/usr/local/bin/mrfpipeline",
		"FROM debian:bookworm-slim AS duckdb",
		"ARG TARGETARCH",
		"duckdb_cli-linux-amd64.zip",
		"duckdb_cli-linux-arm64.zip",
		"08c0ca117111fcede14239d0093792352befdc174218c344d232c13279643d05",
		"02163197027a42149147364d31fa67cac82108517a4be43304a1cc226eaef07a",
		"sha256sum --check --status",
		"duckdb_version='v1.5.5'",
		"duckdb_reported_version",
		"install -d /out",
		"/out/duckdb",
		"COPY --from=duckdb /out/duckdb /usr/local/bin/duckdb",
		"apt-get install --no-install-recommends --yes ca-certificates",
		"apt-get clean",
		"rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*",
	} {
		if !strings.Contains(dockerfile, fragment) {
			t.Fatalf("Dockerfile missing %q", fragment)
		}
	}
	runtimeStart := strings.LastIndex(dockerfile, "\nFROM debian:bookworm-slim\n")
	if runtimeStart < 0 {
		t.Fatal("Dockerfile missing final slim runtime stage")
	}
	runtime := dockerfile[runtimeStart:]
	for _, forbidden := range []string{"curl", "unzip", "sha256sum", "github.com/duckdb/duckdb/releases"} {
		if strings.Contains(runtime, forbidden) {
			t.Fatalf("runtime stage retains build-only content %q", forbidden)
		}
	}

	compose := readRepositoryFile(t, "docker-compose.story21.yml")
	for _, fragment := range []string{
		"image: mrfpipeline:local",
		"ssh:",
		"- default",
		"command: [\"migrate\"]",
		"command: [\"work\", \"--role\", \"control\"]",
		"command: [\"work\", \"--role\", \"mrf\"]",
		"command: [\"work\", \"--role\", \"consumer\"]",
		"profiles: [operator]",
		"profiles: [workers]",
		"${MRFPIPELINE_PROVIDER_CATALOG_DIR:-./local/provider-catalog}",
		"${MRFPIPELINE_SERVICES_FILE:-./local/services.csv}",
		"restart: \"no\"",
		"restart: \"on-failure:5\"",
	} {
		if !strings.Contains(compose, fragment) {
			t.Fatalf("Compose file missing %q", fragment)
		}
	}
	controlStart := strings.Index(compose, "  control:\n")
	mrfStart := strings.Index(compose, "  mrf:\n")
	if controlStart < 0 || mrfStart < controlStart || strings.Contains(compose[controlStart:mrfStart], "healthcheck:") {
		t.Fatal("control must not use a stale-file health check")
	}
	if !strings.Contains(compose[controlStart:mrfStart], "migrate:\n        condition: service_completed_successfully") {
		t.Fatal("control must wait for the repeatable migration service")
	}

	readme := readRepositoryFile(t, "README.md")
	for _, fragment := range []string{
		"DOCKER_BUILDKIT=1 docker compose -f docker-compose.story21.yml build",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli retry",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli reconcile",
		"docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer",
		"docker compose -f docker-compose.story21.yml stop control",
		"worker_started",
		"CGO_ENABLED=0 go build ./cmd/mrfpipeline",
		"go test -p 1 ./...",
		"docker compose -f docker-compose.story21.yml exec -T postgres psql -U mrfpipeline -d mrfpipeline -v ON_ERROR_STOP=1 < scripts/stalled-work.sql",
		"MRFPIPELINE_PRIVATE_MODULES_SSH_KEY",
		"[`DOCKER.md`](DOCKER.md)",
	} {
		if !strings.Contains(readme, fragment) {
			t.Fatalf("README missing %q", fragment)
		}
	}

	dockerGuide := readRepositoryFile(t, "DOCKER.md")
	for _, fragment := range []string{
		"DOCKER_BUILDKIT=1 docker compose -f docker-compose.story21.yml build",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli migrate",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli discover",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli month status",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli month sources set-total",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli retry",
		"docker compose -f docker-compose.story21.yml --profile operator run --rm cli reconcile",
		"docker compose -f docker-compose.story21.yml --profile workers up -d --scale mrf=2 mrf consumer",
		"docker compose -f docker-compose.story21.yml stop control",
		"docker compose -f docker-compose.story21.yml exec -T postgres psql -U mrfpipeline -d mrfpipeline -v ON_ERROR_STOP=1 < scripts/stalled-work.sql",
		"RIVER_SCHEMA=mrfpipeline_river",
		"--scale mrf=3",
	} {
		if !strings.Contains(dockerGuide, fragment) {
			t.Fatalf("DOCKER.md missing %q", fragment)
		}
	}
	if strings.Contains(dockerGuide, "postgres://") {
		t.Fatal("DOCKER.md must not embed a postgres URL")
	}

	workflow := readRepositoryFile(t, ".github/workflows/ci.yml")
	for _, fragment := range []string{
		"MRFPIPELINE_PRIVATE_MODULES_SSH_KEY",
		"go test ./...",
		"go vet ./...",
		"go test -p 1 ./...",
		"go test -race ./internal/jobs ./internal/mrfparse ./internal/work ./internal/reconcile",
		"docker compose -f docker-compose.story21.yml config --quiet",
		"docker compose -f docker-compose.story21.yml build",
		"COMPOSE_PROFILES: operator,workers",
		"workflow_dispatch:",
		"branches: [master]",
		"mrfpipeline_test_ci",
	} {
		if !strings.Contains(workflow, fragment) {
			t.Fatalf("CI workflow missing %q", fragment)
		}
	}
	if strings.Contains(workflow, "TestRealUHCAcceptance") {
		t.Fatal("CI must not run live UHC acceptance")
	}

	selector := readRepositoryFile(t, "config/services.csv.example")
	if !strings.HasPrefix(selector, "billing_code_type,billing_code\n") {
		t.Fatal("selector example must use the current CSV header")
	}
	envExample := readRepositoryFile(t, ".env.example")
	if strings.Contains(envExample, "postgres://") || strings.Contains(envExample, "/Users/") {
		t.Fatal(".env.example contains a credential or machine-specific path")
	}
	for _, name := range []string{".gitignore", ".dockerignore"} {
		ignore := readRepositoryFile(t, name)
		if !strings.Contains(ignore, "\n/mrfpipeline\n") {
			t.Fatalf("%s must ignore the host-built root binary", name)
		}
	}
}

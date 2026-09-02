package filtercatalog

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/enotpoloskun/mrfpipeline/internal/config"
	"github.com/enotpoloskun/mrfpipeline/internal/consumeringest"
	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
)

const (
	duckDBVersion          = "v1.5.5"
	warehouseSchemaVersion = "2.0.0"
	warehouseRootToken     = "__WAREHOUSE_ROOT__"
	selectedOutputsToken   = "__SELECTED_OUTPUT_VALUES__"
	valueInvalidMarker     = "filter_catalog_value_invalid"
	warehouseInvalidMarker = "filter_catalog_warehouse_invalid"
	maxProtocolLine        = 16 << 20
	maxCapturedStderr      = 64 << 10
)

//go:embed sql/extract.sql.tmpl
var extractionSQLTemplate []byte

var extractionGate = make(chan struct{}, 1)

// ResolveDuckDB locates the host-installed DuckDB CLI and verifies its exact
// supported version before any extraction state can be created.
func ResolveDuckDB(ctx context.Context) (DuckDB, error) {
	if ctx == nil {
		panic("filtercatalog: nil context")
	}
	if err := ctx.Err(); err != nil {
		return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	path, err := exec.LookPath("duckdb")
	if err != nil {
		return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogDuckDBUnavailable)
	}
	command := exec.CommandContext(ctx, path, "--version")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = 5 * time.Second
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	var stdout, stderr boundedCapture
	stdout.limit = maxCapturedStderr
	stderr.limit = maxCapturedStderr
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogCancelled)
		}
		return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogDuckDBVersionInvalid)
	}
	if ctx.Err() != nil {
		return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	fields := strings.Fields(stdout.String())
	versionValid := len(fields) > 0 && fields[0] == duckDBVersion
	if ctx.Err() != nil {
		return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	if !versionValid {
		return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogDuckDBVersionInvalid)
	}
	if ctx.Err() != nil {
		return DuckDB{}, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	return DuckDB{path: path}, nil
}

// Extract performs one read-only in-memory DuckDB extraction for the exact
// caller-supplied output relation.
func Extract(ctx context.Context, duck DuckDB, params Params) (Result, error) {
	if ctx == nil {
		panic("filtercatalog: nil context")
	}
	var zero Result
	if err := ctx.Err(); err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	select {
	case extractionGate <- struct{}{}:
	case <-ctx.Done():
		return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	defer func() { <-extractionGate }()
	if err := ctx.Err(); err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	outputs, fingerprint, err := validateParams(params)
	if err != nil {
		return zero, err
	}
	cleanWarehouse, err := config.NormalizeLocalPath(config.EnvWarehousePath, params.WarehousePath)
	if err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
	}
	if duck.path == "" {
		return zero, jobs.Failure(jobs.FailureFilterCatalogDuckDBUnavailable)
	}
	warehouse, err := consumeringest.InspectWarehouse(cleanWarehouse)
	if err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	schemaVersion, catalogMonthText, ok := warehouse.RecognizedCatalog()
	if !ok {
		return zero, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	catalog, err := consumeringest.InspectCatalog(filepath.Join(warehouse.Path, "provider_catalog"))
	if err != nil || catalog.Path != filepath.Join(warehouse.Path, "provider_catalog") {
		return zero, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	manifestSchemaVersion, manifestReleaseMonth, err := consumeringest.InspectCatalogIdentity(catalog.Path)
	if err != nil || manifestSchemaVersion != schemaVersion || manifestReleaseMonth != catalogMonthText {
		return zero, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	catalogMonth, err := time.Parse("2006-01", catalogMonthText)
	if err != nil || catalogMonth.Day() != 1 {
		return zero, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}
	for _, output := range outputs {
		if err := ctx.Err(); err != nil {
			return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
		}
		if err := consumeringest.InspectCompletedSnapshot(warehouse.Path, output.PayerID, output.CollectionMonth, output.OutputID); err != nil {
			return zero, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
		}
	}
	if err := ctx.Err(); err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	sqlText, err := renderExtractionSQL(warehouse.Path, outputs)
	if err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
	}
	rows, err := runExtraction(ctx, duck, sqlText, params.processObserver)
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	result, err := validateRows(rows, outputs, fingerprint)
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	result.WarehouseSchemaVersion = warehouseSchemaVersion
	result.ProviderCatalogSchemaVersion = schemaVersion
	result.ProviderCatalogReleaseMonth = catalogMonth
	result.OutputFingerprint = fingerprint
	sortResult(&result)
	if err := ctx.Err(); err != nil {
		return zero, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	return result, nil
}

func validateParams(params Params) ([]Output, string, error) {
	if len(params.Outputs) == 0 || !validFingerprint(params.OutputFingerprint) {
		return nil, "", jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
	}
	copyOutputs := append([]Output(nil), params.Outputs...)
	payer := copyOutputs[0].PayerID
	month := copyOutputs[0].CollectionMonth
	if config.ValidatePayerIdentifier(payer) != nil || config.ValidateCollectionMonth(month) != nil {
		return nil, "", jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
	}
	seenOutput := make(map[string]struct{}, len(copyOutputs))
	seenSnapshot := make(map[int64]struct{}, len(copyOutputs))
	for _, output := range copyOutputs {
		if output.PayerID != payer || output.CollectionMonth != month || output.SnapshotID <= 0 ||
			config.ValidatePayerIdentifier(output.OutputID) != nil ||
			output.OutputID != consumeringest.FormatSnapshotOutputID(output.SnapshotID) {
			return nil, "", jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
		}
		if _, ok := seenOutput[output.OutputID]; ok {
			return nil, "", jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
		}
		if _, ok := seenSnapshot[output.SnapshotID]; ok {
			return nil, "", jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
		}
		seenOutput[output.OutputID] = struct{}{}
		seenSnapshot[output.SnapshotID] = struct{}{}
	}
	sort.Slice(copyOutputs, func(i, j int) bool { return strings.Compare(copyOutputs[i].OutputID, copyOutputs[j].OutputID) < 0 })
	fingerprint := outputFingerprint(copyOutputs)
	if fingerprint != params.OutputFingerprint {
		return nil, "", jobs.Failure(jobs.FailureFilterCatalogConfigInvalid)
	}
	return copyOutputs, fingerprint, nil
}

func validFingerprint(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for i := range value {
		if !((value[i] >= '0' && value[i] <= '9') || (value[i] >= 'a' && value[i] <= 'f')) {
			return false
		}
	}
	return true
}

func outputFingerprint(outputs []Output) string {
	hash := sha256.New()
	for _, output := range outputs {
		_, _ = hash.Write([]byte(output.PayerID))
		_, _ = hash.Write([]byte{0x1f})
		_, _ = hash.Write([]byte(output.CollectionMonth))
		_, _ = hash.Write([]byte{0x1f})
		_, _ = hash.Write([]byte(output.OutputID))
		_, _ = hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func renderExtractionSQL(warehouse string, outputs []Output) (string, error) {
	if strings.Count(string(extractionSQLTemplate), warehouseRootToken) != 5 ||
		strings.Count(string(extractionSQLTemplate), selectedOutputsToken) != 1 {
		return "", errors.New("invalid extraction template")
	}
	absolute, err := filepath.Abs(filepath.Clean(warehouse))
	if err != nil {
		return "", err
	}
	var encoded strings.Builder
	for _, character := range filepath.ToSlash(absolute) {
		switch character {
		case '*':
			encoded.WriteString("[*]")
		case '?':
			encoded.WriteString("[?]")
		case '[':
			encoded.WriteString("[[]")
		case ']':
			encoded.WriteString("[]]")
		default:
			encoded.WriteRune(character)
		}
	}
	pathLiteral := strings.ReplaceAll(encoded.String(), "'", "''")
	values := make([]string, len(outputs))
	for i, output := range outputs {
		values[i] = "(" + sqlString(output.PayerID) + ", " + sqlString(output.CollectionMonth) + ", " + sqlString(output.OutputID) + ", " + strconv.FormatInt(output.SnapshotID, 10) + ")"
	}
	sqlText := strings.ReplaceAll(string(extractionSQLTemplate), selectedOutputsToken, strings.Join(values, ",\n"))
	sqlText = strings.ReplaceAll(sqlText, warehouseRootToken, pathLiteral)
	return sqlText, nil
}

func sqlString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

type boundedCapture struct {
	bytes.Buffer
	limit int
}

func (capture *boundedCapture) Write(p []byte) (int, error) {
	if capture.limit <= capture.Len() {
		return len(p), nil
	}
	remaining := capture.limit - capture.Len()
	if remaining > len(p) {
		remaining = len(p)
	}
	_, _ = capture.Buffer.Write(p[:remaining])
	return len(p), nil
}

func (capture *boundedCapture) String() string { return capture.Buffer.String() }

type protocolResult struct {
	rows  rowSet
	err   error
	empty bool
}

type rowSet struct {
	summary              *summaryRow
	billingCodes         []BillingCode
	codeFilterValues     []CodeFilterValue
	outputCodeNetworks   []OutputCodeNetwork
	providerFilterValues []ProviderFilterValue
	stage                int
	lastKey              []string
}

type summaryRow struct {
	standardFactCount int64
}

func runExtraction(ctx context.Context, duck DuckDB, sqlText string, observer func(BuildPhase)) (rowSet, error) {
	commandCtx, commandCancel := context.WithCancel(ctx)
	defer commandCancel()
	command := exec.CommandContext(commandCtx, duck.path, ":memory:")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = 5 * time.Second
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogQueryFailed)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogQueryFailed)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		_ = stdout.Close()
		_ = stdin.Close()
		return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogQueryFailed)
	}
	if err := command.Start(); err != nil {
		_ = stderr.Close()
		_ = stdout.Close()
		_ = stdin.Close()
		if ctx.Err() != nil {
			return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogCancelled)
		}
		return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogDuckDBUnavailable)
	}
	if observer != nil {
		observer(BuildPhaseDuckDBStart)
	}
	protocolDone := make(chan protocolResult, 1)
	go func() { protocolDone <- readProtocol(ctx, stdout) }()
	stderrDone := make(chan boundedCapture, 1)
	go func() {
		var capture boundedCapture
		capture.limit = maxCapturedStderr
		_, _ = io.Copy(&capture, stderr)
		stderrDone <- capture
	}()
	type writeResult struct {
		writeErr error
		closeErr error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		_, writeErr := io.WriteString(stdin, sqlText)
		closeErr := stdin.Close()
		writeDone <- writeResult{writeErr: writeErr, closeErr: closeErr}
	}()

	var protocol protocolResult
	var write writeResult
	protocolReady := false
	writeReady := false
	protocolFailure := false
	writerFailure := false
supervise:
	for !protocolReady || !writeReady {
		select {
		case protocol = <-protocolDone:
			protocolReady = true
			if protocol.err != nil {
				protocolFailure = true
				commandCancel()
				break supervise
			}
		case write = <-writeDone:
			writeReady = true
			if write.writeErr != nil || write.closeErr != nil {
				writerFailure = true
				commandCancel()
				break supervise
			}
		case <-ctx.Done():
			commandCancel()
			break supervise
		}
	}
	if ctx.Err() != nil {
		commandCancel()
	}
	waitErr := command.Wait()
	if observer != nil && waitErr == nil && ctx.Err() == nil {
		observer(BuildPhaseDuckDBDone)
	}
	if !protocolReady {
		protocol = <-protocolDone
	}
	if !writeReady {
		write = <-writeDone
	}
	stderrCapture := <-stderrDone
	if ctx.Err() != nil {
		return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogCancelled)
	}
	if protocolFailure {
		return rowSet{}, protocol.err
	}
	if writerFailure || write.writeErr != nil || write.closeErr != nil {
		return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogQueryFailed)
	}
	if waitErr != nil {
		stderrText := stderrCapture.String()
		switch {
		case hasGuardDiagnostic(stderrText, warehouseInvalidMarker):
			return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
		case hasGuardDiagnostic(stderrText, valueInvalidMarker):
			return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogValueInvalid)
		default:
			return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogQueryFailed)
		}
	}
	if protocol.empty {
		return rowSet{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
	}
	return protocol.rows, nil
}

func hasGuardDiagnostic(stderr, marker string) bool {
	want := "Invalid Input Error: " + marker
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == want || line == "Error: "+want {
			return true
		}
	}
	return false
}

func readProtocol(ctx context.Context, stdout io.Reader) protocolResult {
	reader := bufio.NewReaderSize(stdout, 64*1024)
	var rows rowSet
	rows.stage = -1
	lineCount := 0
	for {
		line, readErr := readProtocolLine(reader)
		hasLine := len(line) > 0 || readErr == nil
		if hasLine {
			lineCount++
			row, err := decodeProtocolRow(line)
			if err != nil {
				return protocolResult{err: jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)}
			}
			if err := rows.add(row); err != nil {
				return protocolResult{err: err}
			}
		}
		if lineCount%64 == 0 && ctx.Err() != nil {
			return protocolResult{err: jobs.Failure(jobs.FailureFilterCatalogCancelled)}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			if lineCount == 0 {
				return protocolResult{empty: true}
			}
			return protocolResult{err: jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)}
		}
	}
	if rows.summary == nil {
		if lineCount == 0 {
			return protocolResult{empty: true}
		}
		return protocolResult{err: jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)}
	}
	return protocolResult{rows: rows}
}

func readProtocolLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	tooLong := false
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(fragment) > 0 {
			if len(line)+len(fragment) <= maxProtocolLine {
				line = append(line, fragment...)
			} else {
				tooLong = true
			}
		}
		if err == nil {
			if tooLong {
				return nil, errors.New("protocol line too long")
			}
			return bytes.TrimSuffix(line, []byte{'\n'}), nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if tooLong {
				return nil, errors.New("protocol line too long")
			}
			return line, io.EOF
		}
		return line, err
	}
}

func decodeProtocolRow(line []byte) (protocolRow, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || !utf8.Valid(line) || rejectUnpairedSurrogates(line) != nil {
		return protocolRow{}, errors.New("blank or invalid protocol line")
	}
	fields, err := strictJSONObject(line)
	if err != nil {
		return protocolRow{}, err
	}
	dataset, err := protocolString(fields, "dataset")
	if err != nil {
		return protocolRow{}, err
	}
	row := protocolRow{dataset: dataset}
	switch dataset {
	case "summary":
		if err := exactFields(fields, "dataset", "standard_fact_count"); err != nil {
			return protocolRow{}, err
		}
		count, err := protocolInt(fields, "standard_fact_count")
		if err != nil {
			return protocolRow{}, err
		}
		row.summary = &summaryRow{standardFactCount: count}
	case "billing_codes":
		if err := exactFields(fields, "dataset", "billing_code_type", "billing_code", "billing_code_type_version", "warehouse_service_name", "warehouse_service_description", "observation_count", "unmodified_observation_count"); err != nil {
			return protocolRow{}, err
		}
		billingCodeType, err := protocolString(fields, "billing_code_type")
		if err != nil {
			return protocolRow{}, err
		}
		billingCode, err := protocolString(fields, "billing_code")
		if err != nil {
			return protocolRow{}, err
		}
		version, err := protocolNullableString(fields, "billing_code_type_version")
		if err != nil {
			return protocolRow{}, err
		}
		name, err := protocolNullableString(fields, "warehouse_service_name")
		if err != nil {
			return protocolRow{}, err
		}
		description, err := protocolNullableString(fields, "warehouse_service_description")
		if err != nil {
			return protocolRow{}, err
		}
		observationCount, err := protocolInt(fields, "observation_count")
		if err != nil {
			return protocolRow{}, err
		}
		unmodifiedCount, err := protocolInt(fields, "unmodified_observation_count")
		if err != nil {
			return protocolRow{}, err
		}
		row.billing = &BillingCode{
			BillingCodeType:             billingCodeType,
			BillingCode:                 billingCode,
			BillingCodeTypeVersion:      version,
			WarehouseServiceName:        name,
			WarehouseServiceDescription: description,
			ObservationCount:            observationCount,
			UnmodifiedObservationCount:  unmodifiedCount,
		}
	case "code_filter_values":
		if err := exactFields(fields, "dataset", "billing_code_type", "billing_code", "filter_kind", "filter_value", "display_label", "observation_count"); err != nil {
			return protocolRow{}, err
		}
		codeType, err := protocolString(fields, "billing_code_type")
		if err != nil {
			return protocolRow{}, err
		}
		code, err := protocolString(fields, "billing_code")
		if err != nil {
			return protocolRow{}, err
		}
		kind, err := protocolString(fields, "filter_kind")
		if err != nil {
			return protocolRow{}, err
		}
		value, err := protocolString(fields, "filter_value")
		if err != nil {
			return protocolRow{}, err
		}
		label, err := protocolString(fields, "display_label")
		if err != nil {
			return protocolRow{}, err
		}
		count, err := protocolInt(fields, "observation_count")
		if err != nil {
			return protocolRow{}, err
		}
		row.codeFilter = &CodeFilterValue{
			BillingCodeType:  codeType,
			BillingCode:      code,
			FilterKind:       kind,
			FilterValue:      value,
			DisplayLabel:     label,
			ObservationCount: count,
		}
	case "output_code_networks":
		if err := exactFields(fields, "dataset", "output_id", "billing_code_type", "billing_code", "network_name", "observation_count"); err != nil {
			return protocolRow{}, err
		}
		outputID, err := protocolString(fields, "output_id")
		if err != nil {
			return protocolRow{}, err
		}
		codeType, err := protocolString(fields, "billing_code_type")
		if err != nil {
			return protocolRow{}, err
		}
		code, err := protocolString(fields, "billing_code")
		if err != nil {
			return protocolRow{}, err
		}
		network, err := protocolString(fields, "network_name")
		if err != nil {
			return protocolRow{}, err
		}
		count, err := protocolInt(fields, "observation_count")
		if err != nil {
			return protocolRow{}, err
		}
		row.network = &OutputCodeNetwork{
			OutputID:         outputID,
			BillingCodeType:  codeType,
			BillingCode:      code,
			NetworkName:      network,
			ObservationCount: count,
		}
	case "provider_filter_values":
		if err := exactFields(fields, "dataset", "filter_kind", "parent_value", "filter_value", "provider_count"); err != nil {
			return protocolRow{}, err
		}
		kind, err := protocolString(fields, "filter_kind")
		if err != nil {
			return protocolRow{}, err
		}
		parent, err := protocolString(fields, "parent_value")
		if err != nil {
			return protocolRow{}, err
		}
		value, err := protocolString(fields, "filter_value")
		if err != nil {
			return protocolRow{}, err
		}
		count, err := protocolInt(fields, "provider_count")
		if err != nil {
			return protocolRow{}, err
		}
		row.provider = &ProviderFilterValue{
			FilterKind:    kind,
			ParentValue:   parent,
			FilterValue:   value,
			ProviderCount: count,
		}
	default:
		return protocolRow{}, errors.New("unknown dataset")
	}
	return row, nil
}

type protocolRow struct {
	dataset    string
	summary    *summaryRow
	billing    *BillingCode
	codeFilter *CodeFilterValue
	network    *OutputCodeNetwork
	provider   *ProviderFilterValue
}

func strictJSONObject(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("object expected")
	}

	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok || name == "" {
			return nil, errors.New("invalid object key")
		}
		if _, exists := fields[name]; exists {
			return nil, errors.New("duplicate object key")
		}

		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, err
		}
		fields[name] = append(json.RawMessage(nil), raw...)
	}

	end, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != '}' {
		return nil, errors.New("object not closed")
	}

	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing protocol content")
	}
	return fields, nil
}

func exactFields(fields map[string]json.RawMessage, names ...string) error {
	if len(fields) != len(names) {
		return errors.New("unexpected protocol fields")
	}
	for _, name := range names {
		if _, ok := fields[name]; !ok {
			return errors.New("missing protocol field")
		}
	}
	return nil
}

func protocolString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", errors.New("missing or null protocol string")
	}

	var value string
	if err := json.Unmarshal(raw, &value); err != nil || !utf8.ValidString(value) {
		return "", errors.New("invalid protocol string")
	}
	return value, nil
}

func protocolNullableString(fields map[string]json.RawMessage, name string) (*string, error) {
	raw, ok := fields[name]
	if !ok {
		return nil, errors.New("missing protocol nullable string")
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}

	value, err := protocolString(fields, name)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func protocolInt(fields map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, errors.New("missing protocol integer")
	}
	value := string(bytes.TrimSpace(raw))
	if value == "" || value[0] == '+' {
		return 0, errors.New("invalid protocol integer")
	}

	start := 0
	if value[0] == '-' {
		start = 1
	}
	if start == len(value) || (value[start] == '0' && len(value)-start > 1) {
		return 0, errors.New("invalid protocol integer")
	}
	for i := start; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return 0, errors.New("invalid protocol integer")
		}
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, errors.New("invalid protocol integer")
	}
	return parsed, nil
}

func rejectUnpairedSurrogates(data []byte) error {
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		i++
		for i < len(data) {
			switch data[i] {
			case '"':
				goto nextString
			case '\\':
				if i+1 >= len(data) {
					return errors.New("unfinished escape")
				}
				if data[i+1] != 'u' {
					i += 2
					continue
				}
				if i+5 >= len(data) {
					return errors.New("unfinished unicode escape")
				}
				value, ok := unicodeEscapeValue(data[i+2 : i+6])
				if !ok {
					return errors.New("invalid unicode escape")
				}
				if value >= 0xDC00 && value <= 0xDFFF {
					return errors.New("unpaired low surrogate")
				}
				if value >= 0xD800 && value <= 0xDBFF {
					if i+11 >= len(data) || data[i+6] != '\\' || data[i+7] != 'u' {
						return errors.New("unpaired high surrogate")
					}
					low, ok := unicodeEscapeValue(data[i+8 : i+12])
					if !ok || low < 0xDC00 || low > 0xDFFF {
						return errors.New("unpaired high surrogate")
					}
					i += 11
					continue
				}
				i += 5
			default:
				i++
			}
		}
		return errors.New("unterminated JSON string")
	nextString:
	}
	return nil
}

func unicodeEscapeValue(value []byte) (int, bool) {
	if len(value) != 4 {
		return 0, false
	}
	result := 0
	for _, digit := range value {
		result <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			result += int(digit - '0')
		case digit >= 'a' && digit <= 'f':
			result += int(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			result += int(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return result, true
}

func (rows *rowSet) add(row protocolRow) error {
	index := datasetIndex(row.dataset)
	if index < 0 || index < rows.stage {
		return jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
	}
	if index > rows.stage {
		rows.stage = index
		rows.lastKey = nil
	}
	if index == 0 {
		if rows.summary != nil {
			return jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		rows.summary = row.summary
		return nil
	}
	key := protocolRowKey(row)
	if len(rows.lastKey) > 0 && compareStrings(key, rows.lastKey) <= 0 {
		return jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
	}
	rows.lastKey = key
	switch row.dataset {
	case "billing_codes":
		rows.billingCodes = append(rows.billingCodes, *row.billing)
	case "code_filter_values":
		rows.codeFilterValues = append(rows.codeFilterValues, *row.codeFilter)
	case "output_code_networks":
		rows.outputCodeNetworks = append(rows.outputCodeNetworks, *row.network)
	case "provider_filter_values":
		rows.providerFilterValues = append(rows.providerFilterValues, *row.provider)
	default:
		return jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
	}
	return nil
}

func datasetIndex(dataset string) int {
	switch dataset {
	case "summary":
		return 0
	case "billing_codes":
		return 1
	case "code_filter_values":
		return 2
	case "output_code_networks":
		return 3
	case "provider_filter_values":
		return 4
	default:
		return -1
	}
}

func protocolRowKey(row protocolRow) []string {
	switch row.dataset {
	case "billing_codes":
		return []string{row.billing.BillingCodeType, row.billing.BillingCode}
	case "code_filter_values":
		return []string{row.codeFilter.BillingCodeType, row.codeFilter.BillingCode, row.codeFilter.FilterKind, row.codeFilter.FilterValue}
	case "output_code_networks":
		return []string{row.network.OutputID, row.network.BillingCodeType, row.network.BillingCode, row.network.NetworkName}
	case "provider_filter_values":
		return []string{row.provider.FilterKind, row.provider.ParentValue, row.provider.FilterValue}
	default:
		return nil
	}
}

func compareStrings(left, right []string) int {
	for index := range left {
		if cmp := strings.Compare(left[index], right[index]); cmp != 0 {
			return cmp
		}
	}
	return len(left) - len(right)
}

func validateRows(rows rowSet, outputs []Output, fingerprint string) (Result, error) {
	if rows.summary == nil {
		return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
	}
	if rows.summary.standardFactCount < 0 {
		return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
	}
	if rows.summary.standardFactCount == 0 || len(rows.billingCodes) == 0 {
		return Result{}, jobs.Failure(jobs.FailureFilterCatalogWarehouseInvalid)
	}

	codes := make(map[codeKey]int64, len(rows.billingCodes))
	var observationTotal int64
	for _, row := range rows.billingCodes {
		if row.ObservationCount <= 0 ||
			row.UnmodifiedObservationCount < 0 ||
			row.UnmodifiedObservationCount > row.ObservationCount {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		if row.ObservationCount > math.MaxInt64-observationTotal {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		observationTotal += row.ObservationCount
		if err := validateValue(row.BillingCodeType, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.BillingCode, false); err != nil {
			return Result{}, err
		}
		if err := validateNullableValue(row.BillingCodeTypeVersion); err != nil {
			return Result{}, err
		}
		if err := validateNullableValue(row.WarehouseServiceName); err != nil {
			return Result{}, err
		}
		if err := validateNullableValue(row.WarehouseServiceDescription); err != nil {
			return Result{}, err
		}

		key := codeKey{
			billingCodeType: row.BillingCodeType,
			billingCode:     row.BillingCode,
		}
		if _, exists := codes[key]; exists {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		codes[key] = row.ObservationCount
	}
	if observationTotal != rows.summary.standardFactCount {
		return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
	}

	selectedOutputs := make(map[string]struct{}, len(outputs))
	for _, output := range outputs {
		selectedOutputs[output.OutputID] = struct{}{}
	}
	for _, row := range rows.codeFilterValues {
		key := codeKey{
			billingCodeType: row.BillingCodeType,
			billingCode:     row.BillingCode,
		}
		codeObservationCount, exists := codes[key]
		if row.ObservationCount <= 0 ||
			!exists ||
			row.ObservationCount > codeObservationCount {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		switch row.FilterKind {
		case "modifier", "place_of_service", "billing_class", "setting", "negotiation_arrangement":
		default:
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		if err := validateValue(row.BillingCodeType, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.BillingCode, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.FilterValue, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.DisplayLabel, false); err != nil {
			return Result{}, err
		}
		if row.FilterKind == "place_of_service" && row.FilterValue == "CSTM-00" {
			if row.DisplayLabel != "Broad or unspecified place of service" {
				return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
			}
		} else if row.DisplayLabel != row.FilterValue {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
	}

	for _, row := range rows.outputCodeNetworks {
		if row.ObservationCount <= 0 {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		if _, exists := selectedOutputs[row.OutputID]; !exists {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		key := codeKey{
			billingCodeType: row.BillingCodeType,
			billingCode:     row.BillingCode,
		}
		codeObservationCount, exists := codes[key]
		if !exists || row.ObservationCount > codeObservationCount {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		if err := validateValue(row.OutputID, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.BillingCodeType, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.BillingCode, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.NetworkName, false); err != nil {
			return Result{}, err
		}
	}

	for _, row := range rows.providerFilterValues {
		if row.ProviderCount <= 0 {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		switch row.FilterKind {
		case "taxonomy", "state", "city":
		default:
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		if row.FilterKind != "city" && row.ParentValue != "" {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		if row.FilterKind == "city" && row.ParentValue == "" {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogProtocolInvalid)
		}
		if err := validateValue(row.FilterValue, false); err != nil {
			return Result{}, err
		}
		if err := validateValue(row.ParentValue, row.FilterKind != "city"); err != nil {
			return Result{}, err
		}
		if row.FilterKind == "city" && row.FilterValue != strings.ToLower(row.FilterValue) {
			return Result{}, jobs.Failure(jobs.FailureFilterCatalogValueInvalid)
		}
	}

	return Result{
		StandardFactCount:    rows.summary.standardFactCount,
		BillingCodes:         rows.billingCodes,
		CodeFilterValues:     rows.codeFilterValues,
		OutputCodeNetworks:   rows.outputCodeNetworks,
		ProviderFilterValues: rows.providerFilterValues,
		OutputFingerprint:    fingerprint,
	}, nil
}

type codeKey struct {
	billingCodeType string
	billingCode     string
}

func validateValue(value string, allowEmpty bool) error {
	if !utf8.ValidString(value) ||
		(!allowEmpty && value == "") ||
		strings.ContainsAny(value, "\r\n") {
		return jobs.Failure(jobs.FailureFilterCatalogValueInvalid)
	}
	return nil
}

func validateNullableValue(value *string) error {
	if value == nil {
		return nil
	}
	return validateValue(*value, false)
}

func sortResult(result *Result) {
	sort.Slice(result.BillingCodes, func(i, j int) bool {
		left, right := result.BillingCodes[i], result.BillingCodes[j]
		if cmp := strings.Compare(left.BillingCodeType, right.BillingCodeType); cmp != 0 {
			return cmp < 0
		}
		return strings.Compare(left.BillingCode, right.BillingCode) < 0
	})
	sort.Slice(result.CodeFilterValues, func(i, j int) bool {
		left, right := result.CodeFilterValues[i], result.CodeFilterValues[j]
		if cmp := strings.Compare(left.BillingCodeType, right.BillingCodeType); cmp != 0 {
			return cmp < 0
		}
		if cmp := strings.Compare(left.BillingCode, right.BillingCode); cmp != 0 {
			return cmp < 0
		}
		if cmp := strings.Compare(left.FilterKind, right.FilterKind); cmp != 0 {
			return cmp < 0
		}
		return strings.Compare(left.FilterValue, right.FilterValue) < 0
	})
	sort.Slice(result.OutputCodeNetworks, func(i, j int) bool {
		left, right := result.OutputCodeNetworks[i], result.OutputCodeNetworks[j]
		if cmp := strings.Compare(left.OutputID, right.OutputID); cmp != 0 {
			return cmp < 0
		}
		if cmp := strings.Compare(left.BillingCodeType, right.BillingCodeType); cmp != 0 {
			return cmp < 0
		}
		if cmp := strings.Compare(left.BillingCode, right.BillingCode); cmp != 0 {
			return cmp < 0
		}
		return strings.Compare(left.NetworkName, right.NetworkName) < 0
	})
	sort.Slice(result.ProviderFilterValues, func(i, j int) bool {
		left, right := result.ProviderFilterValues[i], result.ProviderFilterValues[j]
		if cmp := strings.Compare(left.FilterKind, right.FilterKind); cmp != 0 {
			return cmp < 0
		}
		if cmp := strings.Compare(left.ParentValue, right.ParentValue); cmp != 0 {
			return cmp < 0
		}
		return strings.Compare(left.FilterValue, right.FilterValue) < 0
	})
}

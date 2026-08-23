package artifact

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// ErrArtifact is wrapped by path, workspace, filesystem, and manifest failures.
var ErrArtifact = errors.New("artifact operation failed")

// ErrDownload is wrapped by URL-policy, DNS, dial, TLS, redirect, timeout,
// HTTP-status, copy, content-length, and body-close failures.
var ErrDownload = errors.New("download failed")

// ErrPlanBatchInput is a preserved, mismatched, or unsafe plan-batch leaf.
var ErrPlanBatchInput = errors.New("plan batch input")

// ErrPlanBatchOutput is a failed plan-batch publication.
var ErrPlanBatchOutput = errors.New("plan batch output")

const (
	KindTOC       = "toc"
	KindMRF       = "mrf"
	KindPlanBatch = "plan-batch"

	Application   = "mrfpipeline"
	SchemaVersion = "1.0.0"

	markerName    = "workspace.json"
	markerTmpName = "workspace.json.tmp"
	stagingName   = ".staging"
	dirTOC        = "toc"
	dirMRF        = "mrf"
	dirPlanBatch  = "plan-batches"
	dirDownload   = "download"
	dirParsed     = "parsed"
	fileData      = "data"
	fileManifest  = "manifest.json"
	filePlans     = "plans.json"

	tocPrefix = "toc-"
	mrfPrefix = "mrf-source-"
	batPrefix = "plan-batch-"

	stagingTOCPrefix   = "toc-download-"
	stagingMRFPrefix   = "mrf-download-"
	stagingBatchPrefix = "plan-batch-"

	dirMode  os.FileMode = 0700
	fileMode os.FileMode = 0600

	CopyBufferSize = 256 * 1024

	dialTimeout         = 30 * time.Second
	keepAlive           = 30 * time.Second
	tlsHandshakeTimeout = 10 * time.Second
	headerTimeout       = 2 * time.Minute
	expectContinue      = time.Second
	idleConnTimeout     = 90 * time.Second
	maxResponseHeader   = 1 << 20
	maxIdleConns        = 8
	maxIdleConnsPerHost = 4
	maxConnsPerHost     = 4
	maxRedirects        = 5

	userAgent      = "mrfpipeline"
	acceptEncoding = "identity"
)

func artErr(op string) error {
	return fmt.Errorf("%w: %s", ErrArtifact, op)
}

func dlErr(op string) error {
	return fmt.Errorf("%w: %s", ErrDownload, op)
}

func classifyArt(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return artErr(op)
}

func classifyDL(ctx context.Context, op string, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return dlErr("timeout")
	}
	return dlErr(op)
}

func joinErr(err, extra error) error {
	if extra == nil {
		return err
	}
	if err == nil {
		return extra
	}
	return fmt.Errorf("%w: %w", err, extra)
}

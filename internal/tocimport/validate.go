package tocimport

import (
	"context"
	"errors"
	"io"
	"math"
	"path/filepath"

	"github.com/enotpoloskun/mrfpipeline/internal/jobs"
	"github.com/enotpoloskun/mrfpipeline/internal/tocparse"
)

func mapValidateErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, errManifest):
		return jobs.Failure(jobs.FailureTOCImportManifestInvalid)
	case errors.Is(err, errSchema):
		return jobs.Failure(jobs.FailureTOCImportSchemaInvalid)
	case errors.Is(err, errRow):
		return jobs.Failure(jobs.FailureTOCImportRowInvalid)
	case errors.Is(err, errOrder):
		return jobs.Failure(jobs.FailureTOCImportOrderInvalid)
	default:
		return jobs.Failure(jobs.FailureTOCImportManifestInvalid)
	}
}

func validateOutput(ctx context.Context, dir, tocOutputID, payer, month, sourceURI string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m, err := tocparse.ValidateCompletedOutput(dir, tocOutputID, payer, month, sourceURI)
	if err != nil {
		return 0, jobs.Failure(jobs.FailureTOCImportManifestInvalid)
	}
	if err := validateSchemas(dir); err != nil {
		return 0, mapValidateErr(err)
	}
	if err := validateTOCFiles(ctx, filepath.Join(dir, datasetTOCFiles), tocOutputID, payer, month, sourceURI); err != nil {
		return 0, mapValidateErr(err)
	}
	n, err := validateAssociations(ctx, filepath.Join(dir, datasetAssociations), tocOutputID, payer, month)
	if err != nil {
		return 0, mapValidateErr(err)
	}
	if n != m.Counts.MRFPlanAssociations {
		return 0, jobs.Failure(jobs.FailureTOCImportRowInvalid)
	}
	return n, nil
}

func validateSchemas(dir string) error {
	tocParts, err := listParts(filepath.Join(dir, datasetTOCFiles))
	if err != nil {
		return err
	}
	for _, p := range tocParts {
		if err := validatePartSchema(p, tocFileSchema); err != nil {
			return err
		}
	}
	assocParts, err := listParts(filepath.Join(dir, datasetAssociations))
	if err != nil {
		return err
	}
	for _, p := range assocParts {
		if err := validatePartSchema(p, assocSchema); err != nil {
			return err
		}
	}
	return nil
}

func validateTOCFiles(ctx context.Context, dir, tocOutputID, payer, month, sourceURI string) error {
	parts, err := listParts(dir)
	if err != nil {
		return err
	}
	var count int64
	for _, path := range parts {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := forEachTOCRow(ctx, path, func(row tocFileRow) error {
			return validateTOCFileRow(row, tocOutputID, payer, month, sourceURI)
		})
		if err != nil {
			return err
		}
		if count > math.MaxInt64-n {
			return errRow
		}
		count += n
	}
	if count != 1 {
		return errRow
	}
	return nil
}

func validateAssociations(ctx context.Context, dir, tocOutputID, payer, month string) (int64, error) {
	parts, err := listParts(dir)
	if err != nil {
		return 0, err
	}
	var (
		count int64
		prev  assocRow
		have  bool
	)
	for _, path := range parts {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := forEachAssocRow(ctx, path, func(row assocRow) error {
			if err := validateAssocRow(row, tocOutputID, payer, month); err != nil {
				return err
			}
			if have {
				c := compareAssoc(prev, row)
				if c == 0 || c > 0 {
					return errOrder
				}
			}
			prev = row
			have = true
			return nil
		})
		if err != nil {
			return 0, err
		}
		if count > math.MaxInt64-n {
			return 0, errRow
		}
		count += n
	}
	return count, nil
}

func forEachTOCRow(ctx context.Context, path string, fn func(tocFileRow) error) (int64, error) {
	return forEachTyped(ctx, path, fn)
}

func forEachAssocRow(ctx context.Context, path string, fn func(assocRow) error) (int64, error) {
	return forEachTyped(ctx, path, fn)
}

func forEachTyped[T any](ctx context.Context, path string, fn func(T) error) (int64, error) {
	f, pf, r, err := openTyped[T](path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	defer r.Close()
	var count int64
	buf := make([]T, 1)
	groups := pf.RowGroups()
	var groupIdx int
	var remaining int64
	if len(groups) > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		remaining = groups[0].NumRows()
		groupIdx = 1
	}
	for {
		if remaining == 0 && groupIdx < len(groups) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			remaining = groups[groupIdx].NumRows()
			groupIdx++
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		n, err := readTyped(r, buf)
		if n == 1 {
			if remaining > 0 {
				remaining--
			}
			if err := fn(buf[0]); err != nil {
				return count, err
			}
			if count == math.MaxInt64 {
				return count, errRow
			}
			count++
		}
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
	}
}

package stats

// Redacted aggregate queries. They return payer identifiers, collection months,
// and nonnegative counts only.
const (
	sqlTOCDownload = `
SELECT payer_id, collection_month,
       count(*) FILTER (WHERE download_status = 'blocked') AS blocked,
       count(*) FILTER (WHERE download_status = 'pending') AS pending,
       count(*) FILTER (WHERE download_status = 'running') AS running,
       count(*) FILTER (WHERE download_status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE download_status = 'failed') AS failed,
       count(*) AS total
FROM mrfpipeline.toc_files
WHERE ($1::text IS NULL OR payer_id = $1)
  AND ($2::date IS NULL OR collection_month = $2)
GROUP BY payer_id, collection_month
HAVING count(*) > 0`

	sqlTOCParse = `
SELECT payer_id, collection_month,
       count(*) FILTER (WHERE parse_status = 'blocked') AS blocked,
       count(*) FILTER (WHERE parse_status = 'pending') AS pending,
       count(*) FILTER (WHERE parse_status = 'running') AS running,
       count(*) FILTER (WHERE parse_status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE parse_status = 'failed') AS failed,
       count(*) AS total
FROM mrfpipeline.toc_files
WHERE ($1::text IS NULL OR payer_id = $1)
  AND ($2::date IS NULL OR collection_month = $2)
GROUP BY payer_id, collection_month
HAVING count(*) > 0`

	sqlTOCImport = `
SELECT payer_id, collection_month,
       count(*) FILTER (WHERE import_status = 'blocked') AS blocked,
       count(*) FILTER (WHERE import_status = 'pending') AS pending,
       count(*) FILTER (WHERE import_status = 'running') AS running,
       count(*) FILTER (WHERE import_status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE import_status = 'failed') AS failed,
       count(*) AS total
FROM mrfpipeline.toc_files
WHERE ($1::text IS NULL OR payer_id = $1)
  AND ($2::date IS NULL OR collection_month = $2)
GROUP BY payer_id, collection_month
HAVING count(*) > 0`

	sqlMRFDownload = `
SELECT s.payer_id, s.collection_month,
       count(*) FILTER (WHERE m.download_status = 'blocked') AS blocked,
       count(*) FILTER (WHERE m.download_status = 'pending') AS pending,
       count(*) FILTER (WHERE m.download_status = 'running') AS running,
       count(*) FILTER (WHERE m.download_status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE m.download_status = 'failed') AS failed,
       count(*) AS total
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.mrf_sources m
  ON m.id = s.mrf_source_id AND m.collection_month = s.collection_month
WHERE ($1::text IS NULL OR s.payer_id = $1)
  AND ($2::date IS NULL OR s.collection_month = $2)
GROUP BY s.payer_id, s.collection_month
HAVING count(*) > 0`

	sqlMRFParse = `
SELECT s.payer_id, s.collection_month,
       count(*) FILTER (WHERE m.parse_status = 'blocked') AS blocked,
       count(*) FILTER (WHERE m.parse_status = 'pending') AS pending,
       count(*) FILTER (WHERE m.parse_status = 'running') AS running,
       count(*) FILTER (WHERE m.parse_status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE m.parse_status = 'failed') AS failed,
       count(*) AS total
FROM mrfpipeline.mrf_snapshots s
JOIN mrfpipeline.mrf_sources m
  ON m.id = s.mrf_source_id AND m.collection_month = s.collection_month
WHERE ($1::text IS NULL OR s.payer_id = $1)
  AND ($2::date IS NULL OR s.collection_month = $2)
GROUP BY s.payer_id, s.collection_month
HAVING count(*) > 0`

	sqlConsumerIngest = `
SELECT payer_id, collection_month,
       count(*) FILTER (WHERE consume_status = 'blocked') AS blocked,
       count(*) FILTER (WHERE consume_status = 'pending') AS pending,
       count(*) FILTER (WHERE consume_status = 'running') AS running,
       count(*) FILTER (WHERE consume_status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE consume_status = 'failed') AS failed,
       count(*) AS total
FROM mrfpipeline.mrf_snapshots
WHERE ($1::text IS NULL OR payer_id = $1)
  AND ($2::date IS NULL OR collection_month = $2)
GROUP BY payer_id, collection_month
HAVING count(*) > 0`

	sqlAttachPlans = `
SELECT s.payer_id, s.collection_month,
       0::bigint AS blocked,
       count(*) FILTER (WHERE b.status = 'pending') AS pending,
       count(*) FILTER (WHERE b.status = 'running') AS running,
       count(*) FILTER (WHERE b.status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE b.status = 'failed') AS failed,
       count(*) AS total
FROM mrfpipeline.plan_attachment_batches b
JOIN mrfpipeline.mrf_snapshots s ON s.id = b.mrf_snapshot_id
WHERE ($1::text IS NULL OR s.payer_id = $1)
  AND ($2::date IS NULL OR s.collection_month = $2)
GROUP BY s.payer_id, s.collection_month
HAVING count(*) > 0`
)

// QueryTexts returns every stats SQL constant for redaction tests.
func QueryTexts() []string {
	return []string{
		sqlTOCDownload,
		sqlTOCParse,
		sqlTOCImport,
		sqlMRFDownload,
		sqlMRFParse,
		sqlConsumerIngest,
		sqlAttachPlans,
	}
}

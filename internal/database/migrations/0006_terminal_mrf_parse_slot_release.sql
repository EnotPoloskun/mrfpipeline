ALTER TABLE mrfpipeline.mrf_sources
    DROP CONSTRAINT mrf_sources_parse_blocked_check,
    ADD CONSTRAINT mrf_sources_parse_blocked_check
        CHECK (
            download_status = 'succeeded'
            OR parse_status = 'blocked'
            OR (download_status = 'blocked' AND parse_status = 'failed')
        );

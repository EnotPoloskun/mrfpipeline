ALTER TABLE mrfpipeline.toc_files
    DROP CONSTRAINT toc_files_payer_source_url_key,
    ADD CONSTRAINT toc_files_payer_collection_month_source_url_key
        UNIQUE (payer_id, collection_month, source_url);

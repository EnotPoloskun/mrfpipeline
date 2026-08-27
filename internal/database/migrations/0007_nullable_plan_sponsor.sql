ALTER TABLE mrfpipeline.toc_mrf_plan_associations
    DROP CONSTRAINT toc_mrf_plan_associations_sponsor_check,
    ADD CONSTRAINT toc_mrf_plan_associations_sponsor_check
        CHECK (plan_sponsor_name IS NULL OR plan_sponsor_name <> '');

ALTER TABLE mrfpipeline.mrf_plans
    DROP CONSTRAINT mrf_plans_sponsor_check,
    ADD CONSTRAINT mrf_plans_sponsor_check
        CHECK (
            (plan_id_type = 'hios' AND plan_sponsor_name IS NULL)
            OR (plan_id_type = 'ein' AND (plan_sponsor_name IS NULL OR plan_sponsor_name <> ''))
        );

CREATE TABLE IF NOT EXISTS log.actor_log
(
    log_id integer NOT NULL,
    message text COLLATE pg_catalog."default" NOT NULL,
    actor_path text COLLATE pg_catalog."default" NOT NULL,
    severity character(1) COLLATE pg_catalog."default" NOT NULL,
    insert_datetime timestamp without time zone NOT NULL,
    db_insert_datetime timestamp without time zone NOT NULL DEFAULT now(),
    job_run_id int,
    CONSTRAINT actor_log_severity_check CHECK (severity = ANY (ARRAY['D'::bpchar, 'W'::bpchar, 'I'::bpchar, 'E'::bpchar]))
);

CREATE INDEX IF NOT EXISTS "IX_actor_log_log_id"
    ON log.actor_log USING btree
    (log_id)
    TABLESPACE pg_default;

	
CREATE INDEX IF NOT EXISTS "IX_actor_log_job_run_id"
    ON log.actor_log USING btree
    (job_run_id)
    TABLESPACE pg_default;
	

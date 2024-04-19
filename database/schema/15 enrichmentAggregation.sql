CREATE TABLE IF NOT EXISTS meta.enrichment_aggregation (
     enrichment_aggregation_id int NOT NULL,
     enrichment_id int NOT NULL,
     expression text,
     function text,
     relation_ids int[],
     create_datetime timestamp without time zone NOT NULL DEFAULT now(),
     CONSTRAINT pk_enrichment_aggregation PRIMARY KEY (enrichment_id, enrichment_aggregation_id),
     CONSTRAINT enrichment_aggregation_enrichment_id_fkey FOREIGN KEY (enrichment_id) 
     REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE
 );


CREATE INDEX IF NOT EXISTS ix_enrichment_aggregation_enrichment_id
    ON meta.enrichment_aggregation (enrichment_id);  


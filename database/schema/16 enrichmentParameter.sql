  CREATE TABLE IF NOT EXISTS meta.enrichment_parameter (
     enrichment_parameter_id int NOT NULL,
     aggregation_id int NULL,
     parent_enrichment_id int, -- container for the parameter
     type text NOT NULL, -- raw, enrichment, system
     enrichment_id int NULL, -- enrichment_id
     raw_attribute_id int NULL, --  raw_metadata_id
     system_attribute_id int NULL, -- system_attribute_id
     source_id int , -- redundant, can be inferred from enrichment / raw/ system tables
     source_relation_ids int[],
     self_relation_container text,
     create_datetime timestamp without time zone NOT NULL DEFAULT now(),
     CONSTRAINT pk_enrichment_parameter PRIMARY KEY (parent_enrichment_id, enrichment_parameter_id),
     CONSTRAINT enrichment_parameter_parent_enrichment_id FOREIGN KEY (parent_enrichment_id) REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE,
     CONSTRAINT enrichment_parameter_enrichment_id FOREIGN KEY (enrichment_id) REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE,
     CONSTRAINT enrichment_parameter_raw_attribute_id FOREIGN KEY (raw_attribute_id) REFERENCES meta.raw_attribute(raw_attribute_id),
     CONSTRAINT enrichment_parameter_system_attribute_id FOREIGN KEY (system_attribute_id) REFERENCES  meta.system_attribute(system_attribute_id),
     CONSTRAINT enrichment_parameter_source_id FOREIGN KEY (source_id) REFERENCES meta.source(source_id),
     CONSTRAINT enrichment_parameter_not_blank CHECK(enrichment_id IS NOT NULL OR raw_attribute_id IS NOT NULL OR system_attribute_id IS NOT NULL),
     CONSTRAINT enrichment_parameter_type_check CHECK(type IN ('raw','enrichment','system'))
     );




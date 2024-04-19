  CREATE TABLE IF NOT EXISTS meta.source_relation_parameter (
     source_relation_parameter_id int NOT NULL,
     source_relation_id int, -- container for the parameter
     type text NOT NULL CHECK(type IN ('raw','enrichment','system')), -- raw, enriched, system
     enrichment_id int NULL, -- enrichment_id
     raw_attribute_id int NULL, --  raw_metadata_id
     system_attribute_id int NULL, -- system_attribute_id
     source_id int , -- redundant, but very handy. can be inferred from enrichment / raw/ system tables
     self_relation_container text,
     CONSTRAINT pk_source_relation_parameter_id PRIMARY KEY (source_relation_id, source_relation_parameter_id),
     CONSTRAINT FK_source_relation_parameter_source_relation FOREIGN KEY (source_relation_id) REFERENCES meta.source_relation(source_relation_id)  ON DELETE CASCADE,
     CONSTRAINT FK_source_relation_parameter_enrichment_id FOREIGN KEY (enrichment_id) REFERENCES meta.enrichment(enrichment_id) ON DELETE CASCADE,
     CONSTRAINT FK_source_relation_parameter_raw_attribute_id FOREIGN KEY (raw_attribute_id) REFERENCES meta.raw_attribute(raw_attribute_id),
     CONSTRAINT FK_source_relation_parameter_attribute_id FOREIGN KEY (system_attribute_id) REFERENCES  meta.system_attribute(system_attribute_id),
     CONSTRAINT FK_source_relation_parameter__source_id FOREIGN KEY (source_id) REFERENCES meta.source(source_id),
     CONSTRAINT relation_parameter_not_blank CHECK(enrichment_id IS NOT NULL OR raw_attribute_id IS NOT NULL OR system_attribute_id IS NOT NULL)
);


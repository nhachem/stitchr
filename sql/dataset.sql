CREATE SEQUENCE dataset_id_seq;

CREATE TABLE
    dataset
    (
        id SERIAL NOT NULL,
        format CHARACTER VARYING(64),
        storage_type CHARACTER VARYING(64),
        MODE CHARACTER VARYING(64),
        container CHARACTER VARYING(64),
        object_type CHARACTER VARYING(64),
        object_name CHARACTER VARYING(64),
-- to deprecate query to dataset_transform
--        query CHARACTER VARYING(64000),
        partition_key CHARACTER VARYING(64),
        number_partitions INTEGER,
        schema_id INTEGER DEFAULT -1 NOT NULL, -- points to schema_column.schema_id (not a real FK as the schema is denormalized)
        data_persistence_id INTEGER, -- FK to data_persistence
        log_timestamp TIMESTAMP(6) WITH TIME ZONE DEFAULT now() NOT NULL -- log on inserts only for now 
	, last_updated timestamp
    , object_key CHARACTER VARYING(30) default null -- used to override object_ref
);
    
ALTER TABLE dataset ALTER COLUMN id SET DEFAULT nextval('dataset_id_seq');
alter table dataset add constraint dataset_pk
    primary key (id);
drop index uk_name_datasource_idx;
create unique index concurrently uk_name_datasource_idx
    on dataset(object_name, container,  data_persistence_id);
alter table dataset add constraint uk_name_datasource_idx
    unique using index uk_name_datasource_idx;
-- add FKs here
alter table dataset add constraint fk_source_persistence
    FOREIGN KEY (data_persistence_id)
        REFERENCES data_persistence(id);
alter table dataset add constraint fk_schema
    FOREIGN KEY (schema_id)
        REFERENCES dataset_schema(id);


-- end of add FKs
-- important adjust the sequence if you prepopulate
-- ALTER SEQUENCE [ IF EXISTS ] dataset_id_seq restart
ALTER SEQUENCE dataset_id_seq restart with 70;

-- comments
comment on table dataset is 'holds all the dataset metadata that are managed by the system. core data catalog table';
comment on column dataset.data_persistence_id is ' references the data persistence of this dataset... that is where it is stored';

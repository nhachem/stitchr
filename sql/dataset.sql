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
        query CHARACTER VARYING(64000),
        partition_key CHARACTER VARYING(64),
        number_partitions INTEGER,
	    -- priority_level INTEGER,
       	--  dataset_state_id INTEGER,
        schema_id INTEGER, -- points tp schema_column.schema_id (not a real FK as the schema is denormalized)
        data_persistence_src_id INTEGER, -- FK to data_persistence
        data_persistence_dest_id INTEGER DEFAULT '-1'::INTEGER NOT NULL, -- FK to data_persistence need to add -1 as a dummy persistence
        log_timestamp TIMESTAMP(6) WITH TIME ZONE DEFAULT now() NOT NULL -- log on inserts only for now 
    );
    
-- ALTER TABLE dataset ALTER COLUMN id SET DEFAULT nextval('dataset_id_seq');
alter table dataset add constraint dataset_pk primary key (id);
create unique index concurrently uk_name_datasource_idx  on dataset(object_name, data_persistence_src_id);
alter table dataset add constraint name_ds_uk unique using index uk_name_datasource_idx;
-- TODO add FKs here

-- end of add FKs
-- important adjust the sequence if you prepopulate
-- ALTER SEQUENCE [ IF EXISTS ] dataset_id_seq restart
ALTER SEQUENCE dataset_id_seq restart with 70

-- comments
comment on table dataset is 'holds all the dataset metadata that are managed by the system. core data catalog table'
comment on column data_persistence_src_id is ' references the data persistence sourcing this dataset'
comment on column data_persistence_dest_id is ' references the data persistence that the dataset would move to. we have a 1to1 in this version but will expand to a M2M support'

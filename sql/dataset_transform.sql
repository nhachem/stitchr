-- this should be extended to capture an array of dataset inputs and an array of dataset output.
-- we cover only a linear transform here and complex transforms with sql queries...
DROP TABLE IF EXISTS dataset_transform;
CREATE TABLE
    dataset_transform 
    (
        transform_type_id INTEGER,
        dataset_id integer,
        target_format CHARACTER VARYING(15) default 'parquet' not null, -- only if persistence is for files... which we can default and use this to override
	    add_run_time_ref BOOLEAN default false not null,
        write_mode CHARACTER VARYING(15) default 'overwrite' not null,
	    query CHARACTER VARYING(64000), -- needed for query transforms
        primary key(transform_type_id, dataset_id)
    )
    ;
alter table dataset_transform add constraint dataset_transform_dataset_fk foreign key (dataset_id) references dataset(id);
alter table dataset_transform add CONSTRAINT transform_dataset_transform_id_fk FOREIGN KEY (transform_type_id) REFERENCES transform_type (id);

comment on column dataset_transform.dataset_id is 'this is the key as the output of the transform query';
comment on column dataset_transform.transform_type_id is 'we cover only query for now... so this column is really irrelevant for now';
comment on table dataset_transform is 'used to specify transforms that help in tracking data transformations. this is simplistic for now';

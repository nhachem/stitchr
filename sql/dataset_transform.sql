-- this should be extended to capture an array of dataset inputs and an array of dataset output.
-- we cover only a linear transform here and complex transforms with sql queries...
CREATE TABLE
    dataset_transform 
    (
        transform_id INTEGER,
        source_dataset_id integer,
        target_persistence_id integer, -- this should be a dataset id but to
	-- target_dataset_id integer, -- this should be the norm
        target_format CHARACTER VARYING(15) default 'parquet' not null, -- only if persistence is for files... which we can default and use this to override
	add_run_time_ref BOOLEAN default false not null,
        write_mode CHARACTER VARYING(15) default 'overwrite' not null,
	query CHARACTER VARYING(64000), -- needed for query transforms
        primary key(transform_id, source_dataset_id, target_persistence_id)
    )
    ;
alter table dataset_transform add constraint dataset_transform_dataset_fk foreign key (source_dataset_id) references dataset(id);
alter table dataset_transform add constraint dataset_transform_dataset_persistence_fk foreign key (target_persistence_id) references data_persistence(id);
alter table dataset_transform add CONSTRAINT transform_dataset_transform_id_fk FOREIGN KEY (transform_id) REFERENCES transform (id);

comment on table dataset_transform is 'used to specify transforms that help in tracking data transformations. this is simplistic and I should extend to cover sophisticated tarnsforms';
comment on column dataset_transform.target_persistence_id is 'used to specify the target of the transform. Should be actually a target_dataset_id as we would transform sets of input datasets to a set of output datasets in general. but this is needed as a transition while we are refactoring the code';

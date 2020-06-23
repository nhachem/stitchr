-- views to support the stitchr data catalog read api

create or replace view data_persistence_v
	as select * from data_persistence;


-- adjusting the dataset view (in prep to drop the target data persistence field
CREATE or replace VIEW
    dataset_v
            (
             id,
             format,
             storage_type,
             MODE,
             container,
             object_type,
             object_name,
             query,
             partition_key,
             number_partitions,
             schema_id,
             data_persistence_src_id,
             data_persistence_dest_id,
             log_timestamp,
             add_run_time_ref,
             write_mode
                ) AS
SELECT
    d.id,
    d.format,
    d.storage_type,
    d.mode,
    d.container,
    d.object_type,
    d.object_name,
    dt.query,
    d.partition_key,
    d.number_partitions,
    d.schema_id,
    d.data_persistence_src_id,
    dt.target_persistence_id AS data_persistence_dest_id,
    d.log_timestamp,
    dt.add_run_time_ref, -- may be an individual transform step
    dt.write_mode -- need to  be added as a parameter of the transform not the dataset
FROM dataset d JOIN dataset_transform dt
    ON dt.source_dataset_id = d.id AND dt.transform_id = 1;

create or replace view schema_column_v as
    select schema_id as id
         , column_name
         , column_position
         , column_type
         , column_precision
         , string_length
         , is_nullable
    from schema_column;

create or replace view 
	batch_group_v as 
	select id
		 , name
	 from dataset_collection;

create or replace view 
	batch_group_members_v as 
	select collection_id as group_id
		 , dataset_id 
	from dataset_collection_member;

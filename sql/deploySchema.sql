-- deploySchema

--
@data_persistence.sql
@transform.sql -- includes inserts
@dataset_schema.sql
@schema_column.sql -- rename to schema_def.sql?
@dataset.sql
@dataset_transform.sql
@collectionDDL.sql -- populates the dataset_collection seeds

--current stitchr read support needs
@stitchr_views.sql

--populate
@insert_data_persistence.sql
@insert_dataset_schema.sql
@insert_schema_column.sql
@insert_dataset.sql
@insert_dataset_transform.sql
@insert_dataset_collection.sql
@populate_dataset_collection_member.sql

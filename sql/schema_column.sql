-- can be used to get new ids
-- NH: 4/7/2020 to deprecate and associate with the dataset_schema table
CREATE SEQUENCE schema_column_id_seq start with 1;

CREATE TABLE
    schema_column
    (
        schema_id INTEGER,
        column_name CHARACTER VARYING(64),
        column_position INTEGER,
        column_type CHARACTER VARYING(64),
        column_precision INTEGER,
        string_length INTEGER,
        is_nullable BOOLEAN
    );

alter table schema_column add constraint schema_column_pk  primary key (schema_id, column_position);
create unique index concurrently uk_name_id_idx  on schema_column(column_name, schema_id);
alter table schema_column add constraint name_id_uk unique using index uk_name_id_idx;

alter table schema_column add constraint schema_column_schema_fk foreign key (schema_id) references dataset_schema(id);

comment on table schema_column is 'holds the description of the schema and is referenced from dataset.schema_id';
comment on column schema_column.column_precision is 'holds the precision of the numeric type';
comment on column schema_column.string_length is 'length of varchar in general';

-- this is the dummy entry as we do not want to use null entries for the FK
-- not needed insert into schema_column (schema_id, column_position) values (-1,-1);

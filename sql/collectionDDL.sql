CREATE TABLE
    dataset_collection
    (
        id SERIAL NOT NULL,
        name CHARACTER VARYING(64)
    );
alter table dataset_collection add constraint collection_pk primary key(id);
create unique index concurrently uc_name_collection_idx on dataset_collection(name);


CREATE TABLE
    dataset_collection_member
    (
        collection_id INTEGER NOT NULL, -- FK to batch_group
        dataset_id INTEGER NOT NULL -- FK to dataset_id
    );

-- pk is both columns
alter table dataset_collection_member add constraint collection_members_pk primary key(collection_id, dataset_id);
alter table dataset_collection_member add constraint collection_member_collection_fk foreign key (collection_id) references dataset_collection(id);
alter table dataset_collection_member add constraint collection_member_dataset_fk foreign key (dataset_id) references dataset(id);

-- populate collection

-- INSERT INTO dataset_collection (id, name) VALUES (1, 'tpcds_postgres');
-- INSERT INTO dataset_collection (id, name) VALUES (2, 'tpcds_file');
-- INSERT INTO dataset_collection (id, name) VALUES (3, 'mixed');
-- INSERT INTO dataset_collection (id, name) VALUES (4, 'tpcds_base_files');
-- INSERT INTO dataset_collection (id, name) VALUES (5, 'mixed_base');

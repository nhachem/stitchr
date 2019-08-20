CREATE TABLE
    batch_group
    (
        id SERIAL NOT NULL,
        name CHARACTER VARYING(64)
    );
alter table batch_group add constraint batch_group_pk primary key(id);
create unique index concurrently uc_name_batch_group_idx on batch_group(name);


CREATE TABLE
    batch_group_members
    (
        group_id INTEGER NOT NULL, -- FK to batch_group
        dataset_id INTEGER NOT NULL -- FK to dataset_id
    );

-- pk is both columns
alter table batch_group_members add constraint batch_group_members_pk primary key(group_id, dataset_id);

-- populate batch


INSERT INTO batch_group (id, name) VALUES (1, 'tpcds_postgres');
INSERT INTO batch_group (id, name) VALUES (2, 'tpcds_file');
INSERT INTO batch_group (id, name) VALUES (3, 'mixed');
INSERT INTO batch_group (id, name) VALUES (4, 'tpcds_base_files');
INSERT INTO batch_group (id, name) VALUES (5, 'mixed_base');



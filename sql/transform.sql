-- we can add an array of options to be used in read and writee api calls
/*
this is to replace data_persistence_dest_id
*/

CREATE TABLE IF NOT EXISTS
    transform
(
    id SERIAL NOT NULL,
    name CHARACTER VARYING NOT NULL,
    description TEXT,
    input_description TEXT NULL,
    output_description TEXT NULL,
    script TEXT,
    PRIMARY KEY (id),
    UNIQUE (name)
);

insert into transform (id, name, description) values(1, 'copy', 'basic copy from src to dest persistence');
insert into transform (id, name, description) values(0, 'base', 'assumed available from another process... like base tables or views or files');
insert into transform (id, name, description) values(2, 'query', 'derived as a query');

CREATE TABLE IF NOT EXISTS
    transform_type
(
    id SERIAL NOT NULL,
    name CHARACTER VARYING NOT NULL,
    description TEXT,
    PRIMARY KEY (id),
    UNIQUE (name)
);

insert into transform_type (id, name, description) values(1, 'copy', 'basic copy from src to dest persistence');
insert into transform_type (id, name, description) values(0, 'base', 'assumed available from another process... like base tables or views or files');
insert into transform_type (id, name, description) values(2, 'query', 'derived as a query');

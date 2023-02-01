-- Define schema for query-audit-tables
--
-- This script is used by automated integration-tests to set up a temporary (containerized) database.
--
-- For reference, here is a typical report query used by auditors:
--   select s.username, o.at, o.command
--   from operation o
--   join session s
--   on s.id = o.session
--   order by o.at asc

create table session (
    id serial primary key,
    username varchar(64) not null,
    database varchar(64) not null,
    at timestamp not null,
    expiresAt timestamp not null,
    approver varchar(64),
    reason varchar(128)
);

create table login (
    session integer references session,
    at timestamp,
    primary key (session, at)
);

create table operation (
    session integer references session,
    at timestamp,
    command varchar(4096) not null,
    primary key (session, at)
);
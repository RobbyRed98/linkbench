CREATE TABLE linkdb0.linktable
(
    id1        bigint  NOT NULL DEFAULT '0' REFERENCES linkdb0.nodetable,
    id2        bigint  NOT NULL DEFAULT '0' REFERENCES linkdb0.nodetable,
    link_type  bigint  NOT NULL DEFAULT '0',
    visibility smallint     NOT NULL DEFAULT '0',
    data       varbinary(255) NOT NULL,
    time       bigint  NOT NULL DEFAULT '0',
    version    bigint       NOT NULL DEFAULT '0',
    PRIMARY KEY (link_type, id1, id2)
);

CREATE TABLE linkdb0.counttable
(
    id        bigint NOT NULL DEFAULT '0',
    link_type bigint NOT NULL DEFAULT '0',
    count     int         NOT NULL DEFAULT '0',
    time      bigint NOT NULL DEFAULT '0',
    version   bigint NOT NULL DEFAULT '0',
    PRIMARY KEY (id, link_type)
);

CREATE TABLE linkdb0.nodetable
(
    id      bigint NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
    type    int       NOT NULL,
    version numeric   NOT NULL,
    time    int       NOT NULL,
    data    blob      NOT NULL
);
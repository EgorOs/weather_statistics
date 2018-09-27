DROP TABLE IF EXISTS db_scheme_version;

CREATE TABLE db_scheme_version (
    db_version NUMERIC NOT NULL,
    upgraded_on TIMESTAMPTZ NOT NULL
);

-- db_scheme_version should contain single value
CREATE UNIQUE INDEX db_scheme_version_one_row
    ON db_scheme_version((db_version IS NOT NULL));

INSERT INTO db_scheme_version(db_version, upgraded_on)  VALUES('1.0', now());

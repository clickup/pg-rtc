BEGIN;

CREATE SCHEMA test_pg_rtc;
SET search_path TO test_pg_rtc;
SET client_min_messages TO WARNING;
\set ON_ERROR_STOP on

-- Load the library.

\ir ./pg-rtc-up.sql

-- Initial fixture.

CREATE TABLE parent(id bigint PRIMARY KEY);

INSERT INTO parent(id)
  SELECT id FROM generate_series(0, 10) id;

CREATE FUNCTION parent_trigger_delete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  arr bigint[];
BEGIN
  UPDATE child
    SET parent_ids = array_remove(child.parent_ids, old_rows.id)
    FROM old_rows
    WHERE
      child.parent_ids && ARRAY[old_rows.id]
      AND old_rows.id = 1;
  RETURN NULL;
END;
$$;

CREATE TRIGGER parent_trigger_delete AFTER DELETE ON parent
  REFERENCING OLD TABLE AS old_rows
  FOR EACH STATEMENT EXECUTE PROCEDURE parent_trigger_delete();

CREATE TABLE child(id bigint PRIMARY KEY, parent_id bigint, parent_ids bigint[]);

INSERT INTO child(id, parent_id, parent_ids)
  SELECT id, id % 10, ARRAY[id % 10, null, id % 4]
    FROM generate_series(0, 999) id;

-- Add counters.

SELECT rtc_create('child', 'parent_id', 'parent', true) \gset
SELECT rtc_create('child', 'parent_ids', 'parent', true) \gset

-- Force differential triggers to fire and modify the counters.

INSERT INTO child(id, parent_id, parent_ids)
  SELECT id, id % 10, ARRAY[id % 10, null, id % 4]
    FROM generate_series(1000, 1999) id;

UPDATE child
  SET
    parent_id = (id + 3) % 10,
    parent_ids = ARRAY[(id + 3) % 10, null, (id + 3) % 4]
  WHERE id IN(SELECT id FROM generate_series(500, 999) id);

UPDATE child
  SET
    parent_id = 1,
    parent_ids = ARRAY[1, 2]
  WHERE id = 1999;

DELETE FROM parent WHERE id = 1;

DELETE FROM child
  WHERE id IN(SELECT id FROM generate_series(1500, 1999) id);

-- Test for single-value FKs.

\echo ''

\echo 'Counters for some of parent rows referred by single-value FK (parent_id):'
WITH data AS (
  SELECT
    parent.id,
    cnt AS rtc_cnt,
    (SELECT count(1) FROM child WHERE parent_id = parent.id) AS real_cnt,
    cnt = (SELECT count(1) FROM child WHERE parent_id = parent.id) AS correct
  FROM parent
  LEFT JOIN child_rtc_parent_id ON child_rtc_parent_id.id = parent.id
)
  SELECT * FROM data
  ORDER BY correct, id
  LIMIT 5;

DO $$
  BEGIN
    PERFORM parent.id
      FROM parent
      LEFT JOIN child_rtc_parent_id ON child_rtc_parent_id.id = parent.id
      WHERE cnt <> (SELECT count(1) FROM child WHERE parent_id = parent.id);
    IF FOUND THEN
      RAISE 'Mismatches found, see above.';
    END IF;
  END;
$$ LANGUAGE plpgsql;

-- Test for array-value FK.

\echo 'Counters for some of parent rows referred by ARRAY-value FK (parent_ids):'
WITH data AS (
  SELECT
    parent.id,
    cnt AS rtc_cnt,
    (SELECT count(1) FROM child WHERE parent_ids && ARRAY[parent.id]) AS real_cnt,
    cnt = (SELECT count(1) FROM child WHERE parent_ids && ARRAY[parent.id]) AS correct
  FROM parent
  LEFT JOIN child_rtc_parent_ids ON child_rtc_parent_ids.id = parent.id
)
  SELECT * FROM data
  ORDER BY correct, id
  LIMIT 5;

DO $$
  BEGIN
    PERFORM parent.id
      FROM parent
      LEFT JOIN child_rtc_parent_ids ON child_rtc_parent_ids.id = parent.id
      WHERE cnt <> (SELECT count(1) FROM child WHERE parent_ids && ARRAY[parent.id]);
    IF FOUND THEN
      RAISE 'Mismatches found, see above.';
    END IF;
  END;
$$ LANGUAGE plpgsql;

-- Test that cleanup works.

SELECT rtc_drop('child', 'parent_id') \gset
SELECT rtc_drop('child', 'parent_ids') \gset

\ir ./pg-rtc-down.sql

ROLLBACK;

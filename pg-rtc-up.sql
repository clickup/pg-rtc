--
-- 1. Mnemonics: child_table.child_fk_field -> parent_table.id
-- 2. Notice that parent_table is used to create FOREIGN KEY for the counters
--    table pointing to that parent_table.
-- 3. The assumption is that all table has ther PK fields named "id".
-- 4. Array-FK support: child_fk_field may be an array field too.
--
CREATE OR REPLACE FUNCTION rtc_create(
  child_table text,
  child_fk_field text,
  parent_table text,
  prefill boolean
) RETURNS text LANGUAGE plpgsql AS $body$
DECLARE
  template text;
  cnt_table text := child_table || '_rtc_' || child_fk_field;
  as_rows text;
BEGIN
  PERFORM attrelid
    FROM pg_catalog.pg_attribute
    WHERE
      attrelid = child_table::regclass::oid
      AND attname = child_fk_field
      AND attndims > 0 -- is array
      AND NOT attisdropped;
  IF NOT FOUND THEN
    -- Example of the future generated SQL:
    -- SELECT rows.child_fk_field FROM new_rows AS rows
    as_rows := 'AS rows';
  ELSE
    -- Example of the future generated SQL:
    -- SELECT rows.child_fk_field
    -- FROM new_rows AS table_rows,
    -- LATERAL unnest(table_rows.child_fk_field) AS rows(child_fk_field)
    as_rows := replace(
      'AS table_rows, ' ||
      'LATERAL (SELECT DISTINCT unnest(table_rows.{child_fk_field}) t) AS rows({child_fk_field})',
      '{child_fk_field}',
      child_fk_field
    );
  END IF;

  template := $$
    LOCK TABLE {child_table} IN EXCLUSIVE MODE;

    CREATE TABLE IF NOT EXISTS {cnt_table} (
      id bigint not null PRIMARY KEY,
      cnt integer NOT NULL
    );

    DO $C$
      BEGIN
        ALTER TABLE {cnt_table} ADD CONSTRAINT {cnt_table}_id_fk
          FOREIGN KEY (id) REFERENCES {parent_table}(id) ON DELETE CASCADE;
      EXCEPTION
        WHEN duplicate_object THEN -- nothing
      END;
    $C$;

    CREATE OR REPLACE FUNCTION {cnt_table}_trigger() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    AS $f$
    DECLARE
      arr {cnt_table}[];
    BEGIN
      IF TG_OP = 'INSERT' THEN
        INSERT INTO {cnt_table} (id, cnt)
          SELECT rows.{child_fk_field} AS id, count(1) AS cnt
            FROM new_rows {as_rows}
            WHERE rows.{child_fk_field} IS NOT NULL
            GROUP BY 1
            ORDER BY 1
          ON CONFLICT (id) DO UPDATE SET cnt = {cnt_table}.cnt + EXCLUDED.cnt;
      ELSIF TG_OP = 'UPDATE' THEN
        -- 1. "HAVING sum(delta) <> 0" filters out most of updates since
        --    {child_fk_field} is changed rarely.
        -- 2. "HAVING ... AND EXISTS ..." rechecks that the record still exists
        --    in the parent table (if so, we can insert it to {cnt_table}). We
        --    recheck, because otherwise there could be an attempt to insert a
        --    counter for a row which was already removed by some other ON
        --    DELETE CASCADE foreign key.
        -- 3. Ideally, (2) could've been solved with BEFORE EACH STATEMENT
        --    trigger, but unfortunately BEFORE EACH STATEMENT doesn't allow to
        --    specify the transition table name ("old_rows").
        INSERT INTO {cnt_table} (id, cnt)
          SELECT id, sum(delta) AS cnt
            FROM (
              SELECT rows.{child_fk_field} AS id, -count(1) AS delta
                FROM old_rows {as_rows}
                WHERE rows.{child_fk_field} IS NOT NULL
                GROUP BY 1
              UNION ALL
              SELECT rows.{child_fk_field} AS id, +count(1) AS delta
                FROM new_rows {as_rows}
                WHERE rows.{child_fk_field} IS NOT NULL
                GROUP BY 1
            ) agg
            GROUP BY id
            HAVING
              sum(delta) <> 0
              AND EXISTS(SELECT 1 FROM {parent_table} WHERE {parent_table}.id = agg.id)
            ORDER BY id
          ON CONFLICT (id) DO UPDATE SET cnt = {cnt_table}.cnt + EXCLUDED.cnt;
      ELSIF TG_OP = 'DELETE' THEN
        -- PG 10.6 Aurora (at least) has a bug which makes old_rows absent if it's
        -- directly referred in UPDATE statement (via subquery or WITH). So we do
        -- a hack and store increments in an intermediate array (arr) which then unnest.
        SELECT array_agg(ROW({child_fk_field}, cnt)) INTO arr
          FROM (
            SELECT rows.{child_fk_field}, count(1) AS cnt
              FROM old_rows {as_rows}
              WHERE rows.{child_fk_field} IS NOT NULL
              GROUP BY 1
              ORDER BY 1
          ) a;
        UPDATE {cnt_table}
          SET cnt = {cnt_table}.cnt - agg.cnt
          FROM unnest(arr) AS agg
          WHERE {cnt_table}.id = agg.id;
      END IF;
      RETURN NULL;
    END;
    $f$;

    DROP TRIGGER IF EXISTS {cnt_table}_trigger_insert ON {child_table};
    CREATE TRIGGER {cnt_table}_trigger_insert AFTER INSERT ON {child_table}
      REFERENCING NEW TABLE AS new_rows
      FOR EACH STATEMENT EXECUTE PROCEDURE {cnt_table}_trigger();

    DROP TRIGGER IF EXISTS {cnt_table}_trigger_update ON {child_table};
    CREATE TRIGGER {cnt_table}_trigger_update AFTER UPDATE ON {child_table}
      REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
      FOR EACH STATEMENT EXECUTE PROCEDURE {cnt_table}_trigger();

    DROP TRIGGER IF EXISTS {cnt_table}_trigger_delete ON {child_table};
    CREATE TRIGGER {cnt_table}_trigger_delete AFTER DELETE ON {child_table}
      REFERENCING OLD TABLE AS old_rows
      FOR EACH STATEMENT EXECUTE PROCEDURE {cnt_table}_trigger();
  $$ ||
  CASE
    WHEN prefill THEN $$
      INSERT INTO {cnt_table} (id, cnt)
        SELECT rows.{child_fk_field}, count(1)
          FROM {child_table} {as_rows}
          WHERE rows.{child_fk_field} IS NOT NULL
          GROUP BY 1;
    $$ ELSE ''
  END;

  template := replace(template, '{cnt_table}', cnt_table);
  template := replace(template, '{child_table}', child_table);
  template := replace(template, '{child_fk_field}', child_fk_field);
  template := replace(template, '{parent_table}', parent_table);
  template := replace(template, '{as_rows}', as_rows);
  EXECUTE template;

  RETURN cnt_table;
END;
$body$;


CREATE OR REPLACE FUNCTION rtc_drop(
  child_table text,
  child_fk_field text
) RETURNS text LANGUAGE plpgsql AS $body$
DECLARE
  template text;
  cnt_table text := child_table || '_rtc_' || child_fk_field;
BEGIN
  template := $$
    DROP TABLE {cnt_table};
    DROP FUNCTION {cnt_table}_trigger CASCADE;
  $$;
  template := replace(template, '{cnt_table}', cnt_table);
  EXECUTE template;
  RETURN cnt_table;
END;
$body$;

# pg-rtc: allows to add cheap real-time counters to a FK column

The library allows to create a table with counters which is automatically
updated once a row in some other table ("child table") appears or disappears.

In addition to regular (single-value) foreign keys, array-like FKs are also
supported. If you refer some parent table with a bigint[] child.parent_ids
field, the library will track the counters automatically too.

For efficiency, "FOR EACH STATEMENT" triggers are used. Our experiments show
that a naive "FOR EACH ROW" approach would be O(n^2) in Postgres, especially
when running a huge transaction (e.g. cascaded mass-deletion of a parent_id
based tree).

# Example

```
CREATE TABLE parent(id bigint PRIMARY KEY);
CREATE TABLE child(id bigint PRIMARY KEY, parent_id bigint, parent_ids bigint[]);

-- Create child_rtc_parent_id(id, cnt) table which holds real-time counters
-- for each parent.id value. Automatically updated when rows in child table
-- are inserted, updated or deleted.
SELECT rtc_create('child', 'parent_id', 'parent', true);

-- Same, but for array-value parent_ids field.
SELECT rtc_create('child', 'parent_ids', 'parent', true);

-- Verification for child_rtc_parent_id.
SELECT
  parent.id,
  cnt AS rtc_cnt,
  (SELECT count(1) FROM child WHERE parent_id = parent.id) AS real_cnt,
  cnt = (SELECT count(1) FROM child WHERE parent_id = parent.id) AS correct
FROM parent
LEFT JOIN child_rtc_parent_id ON child_rtc_parent_id.id = parent.id;

-- Verification for child_rtc_parent_ids (array-value FK).
SELECT
  parent.id,
  cnt AS rtc_cnt,
  (SELECT count(1) FROM child WHERE parent_ids && ARRAY[parent.id]) AS real_cnt,
  cnt = (SELECT count(1) FROM child WHERE parent_ids && ARRAY[parent.id]) AS correct
FROM parent
LEFT JOIN child_rtc_parent_id ON child_rtc_parent_id.id = parent.id;
```

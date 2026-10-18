# Btrieve key compliance

How wbtrv32's SQLite key handling compares with Actian's documented Btrieve key
semantics, and the fixes still outstanding. Written after composite keys moved
to one SQLite column per segment (PR #29, database version 4).

## Sources

- [MicroKernel Engine Fundamentals: Key Attributes](https://docs.actian.com/psql/PSQLv13/prog_gde/fundmnts.htm)
  (segmentation, duplicates, modifiability, sort order, case, ACS, null values)
- [Btrieve API: Create (14) key specification block](https://docs.actian.com/psql/PSQLv13/btrieveapi/btrapi.htm#ww178107)
  (key flag values and extended data type codes)
- [SQL Engine Reference: Btrieve Key Data Types](https://docs.actian.com/psql/PSQLv13/sqlref/sqldtype.htm)
  (storage layout and comparison of each key type)
- [Records](https://docs.actian.com/psql/PSQLv13/prog_gde/Records.htm) has
  little on keys beyond "null keys aren't indexed".

### Key flags (Create key specification)

| Flag | Value | Meaning |
|---|---|---|
| DUP | 0x001 | Duplicates allowed (linked by default) |
| MOD | 0x002 | Modifiable |
| BIN | 0x004 | Old BINARY type (non-extended) |
| NUL | 0x008 | Null key, all segments |
| SEG | 0x010 | Segmented, the next spec is the next segment |
| ALT | 0x020 | Uses the alternate collating sequence |
| NUMBERED_ACS | 0x420 | Uses an ACS stored in the file |
| NAMED_ACS | 0xC20 | Uses a named ACS |
| DESC_KEY | 0x040 | Descending sort order |
| REPEAT_DUPS_KEY | 0x080 | Repeating duplicates (with DUP) |
| EXTTYPE_KEY | 0x100 | Extended data type |
| MANUAL_KEY | 0x200 | Null key, any segment |
| NOCASE_KEY | 0x400 | Case insensitive |

NOCASE_KEY and NUMBERED_ACS share bit 0x400, so "has a numbered ACS" must
test both bits: `(attributes & MultipleACS) == MultipleACS`.

## Correctly supported

| Documented rule | Implementation |
|---|---|
| Each segment can have its own type | One typed column per segment (`key_N_S`), converted per segment |
| A unique segmented key is unique as a whole, single segments may repeat | `UNIQUE` index over all of the key's columns |
| Segments may overlap | Each segment is extracted by its own offset and length |
| Each segment uses the key's ACS or none; only segments that specify it collate with it | `Key::applyACS` checks each segment's flag |
| NUL (0x08): null only if every segment is null. MANUAL_KEY (0x200): null if any segment is | `Key::isNullCompositeKeyData` |
| If one segment is modifiable, all must be | Primary segment's flag; the trigger covers every column |
| Partial key search: set known leading segments, low trailing ones, Get Greater Or Equal | Row value `>=`; short key buffers are zero padded |
| Duplicates come back in insertion order | Every key query orders by `id` last |
| An AUTOINCREMENT key can be an integer segment of a higher numbered key | Its single segment key assigns the value before the composite columns are computed |
| LSTRING ignores bytes past its length, ZSTRING bytes after the first zero | Both convert to TEXT of the real length |
| Partial key search on a nullable key: low trailing segments may match the null value | Search keys are never converted to NULL (`Key::searchKeyDataToSqliteObjects`), only stored keys are |
| A non-modifiable key can't change, including to or from null | The trigger compares columns with `IS NOT`, since `!=` is never true against NULL (the version 3 to 4 upgrade recreates it) |
| A record whose key is null isn't put into the key's index | Its key columns are NULL, and First, Last and Equal skip them (`sqliteKeyIndexedWhere`), as do range comparisons since comparing with NULL is false. Get Direct, Insert or Update onto such a record leaves no logical position, so Get Next/Previous return 8. Stat doesn't count them as unique values. Only keys with a null flag store NULL; a single string key without one stores its empty string as a value |
| Case insensitive key without an ACS (0x400 alone) loads | Fixed: the loader used to treat it as NUMBERED_ACS and threw "Can't load ACS" (`BtrieveDatabase_test.cc` `LoadsCaseInsensitiveKeyWithoutACS`) |

## Outstanding fixes

These apply to single segment keys too, not only composite ones. None appear
in the 11 local `.DAT` files (test assets, MBBSEmu, one module), which use
only ZSTRING, INTEGER, FLOAT, UNSIGNED BINARY and AUTOINCREMENT, plus ACS.

### 1. Case insensitive keys don't fold case

The loader now accepts NOCASE_KEY (0x400 without 0x20), but nothing applies
it, so such keys still compare case sensitively. Fold string segments
(STRING, LSTRING, ZSTRING) to upper case during conversion when 0x400 is set
and the segment has no ACS, the same way an upper casing ACS would. The docs
say case sensitivity has no effect on keys that use an ACS.

`AttributeMask.h` has no NOCASE constant yet, so add one (`1 << 10`) rather
than testing against `MultipleACS`.

### 2. Descending segments are ignored

DESC_KEY (0x40) is per segment: "The key type and sort order can be different
for each segment in the key." On a descending key, Get Greater returns the
next lower value, and Get Next/Previous run in reverse.

- Indexes: `CREATE INDEX key_N_index ON data_t(key_N_0 ASC, key_N_1 DESC)`.
- `ORDER BY` uses each segment's direction for forward traversal and the
  opposite for reverse.
- Comparisons: a row value like `(a, b) > (?, ?)` assumes every column goes
  the same way. When a key is all descending, flip the operator. When a key
  mixes directions, generate the expanded form, e.g.
  `a > ?1 OR (a = ?1 AND b < ?2)`.
- `SqliteQuery::changeDirection` needs the same treatment.

### 3. Types stored as raw bytes sort incorrectly

`segmentDataToSqliteObject` in `Key.cc` falls through to a BLOB of the raw
bytes for these, which `memcmp` misorders:

| Type (code) | Layout | Fix |
|---|---|---|
| DATE (3) | day, month, 2 byte year (little endian) | INTEGER `year * 10000 + month * 100 + day` |
| TIME (4) | hundredths, seconds, minutes, hours | INTEGER `((hours * 60 + minutes) * 60 + seconds) * 100 + hundredths` |
| DECIMAL (5), MONEY (6) | Packed BCD, sign nibble 0xF/0xC positive, 0xD negative | Decode to a sortable value, e.g. a signed INTEGER when it fits, else a sign-adjusted BLOB |
| NUMERIC (8) | ASCII digits, sign overpunched in the last byte | Decode the sign, same as DECIMAL |
| BFLOAT (9) | Microsoft Binary Format float | Convert to a double, REAL |
| UNSIGNED BINARY (14), 8 bytes | Unsigned little endian | Values above `INT64_MAX` become negative INTEGERs; use a big endian BLOB for 8 byte unsigned |
| CURRENCY (19) | 8 byte signed little endian | INTEGER (not in `KeyDataType` yet) |
| TIMESTAMP (20) | 8 byte unsigned little endian | Big endian BLOB, or INTEGER if values stay below 2^63 (not in `KeyDataType` yet) |

Changing how an existing type is stored needs a database version bump and an
upgrade that recomputes the key columns. `upgradeDatabaseFrom3To4` already
rebuilds `data_t` from the stored records and can be generalised.

### 4. True nulls (null indicator segments) aren't supported

Type 255 is a one byte null indicator segment before a nullable field. Nulls
group together for sorting but aren't equal for uniqueness, so a unique key
can hold several. Only files created through the SQL engine use these, so
it's low priority for MBBS data.

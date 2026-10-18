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
| NUL (0x08): null only if every segment is null. MANUAL_KEY (0x200): null if any segment is. A segment is null when every byte is the null value, strings included | `Key::isNullKeyData`, for single and composite keys alike |
| If one segment is modifiable, all must be | Primary segment's flag; the trigger covers every column |
| Partial key search: set known leading segments, low trailing ones, Get Greater Or Equal | Row value `>=`; a key buffer shorter than the key (only possible through the driver, since the DLL returns status 21) is zero padded, except that a signed integer segment cut short extends its last byte's sign |
| Duplicates come back in insertion order | Every key query orders by `id` last |
| An AUTOINCREMENT key can be an integer segment of a higher numbered key | Its single segment key assigns the value before the composite columns are computed |
| LSTRING ignores bytes past its length, ZSTRING bytes after the first zero | Both convert to TEXT of the real length |
| Partial key search on a nullable key: low trailing segments may match the null value | Search keys are never converted to NULL (`Key::searchKeyDataToSqliteObjects`), only stored keys are |
| A non-modifiable key can't change, including to or from null | The trigger compares columns with `IS NOT`, since `!=` is never true against NULL (the version 3 to 4 upgrade recreates it) |
| A record whose key is null isn't put into the key's index | Its key columns are NULL, and First, Last and Equal skip them (`sqliteKeyIndexedWhere`), as do range comparisons since comparing with NULL is false. Get Direct, Insert or Update onto such a record leaves no logical position, so Get Next/Previous return 8. Stat doesn't count them as unique values. Only keys with a null flag store NULL; a single string key without one stores its empty string as a value |
| DESC_KEY (0x40) is per segment, and Get Greater/Less and Next/Previous follow the key's order, so on a descending segment Greater means a lower value | Descending columns are indexed and ordered `DESC` (`sqliteKeyOrder`, `sqliteKeyIndexColumns`). `sqliteKeyComparison` flips the operator when every segment is descending, and compares a mixed key segment by segment, with a bound on the leading column so SQLite can still seek the index |
| Each key type compares by value | `Key::segmentDataToSqliteObject` converts each type to a value SQLite orders correctly. DATE (day, month, year) and TIME (hundredths, seconds, minutes, hours) are little endian unsigned INTEGERs, whose high bytes are the most significant fields. DECIMAL and MONEY (packed BCD, sign nibble 0xD negative) and NUMERIC (EBCDIC overpunched sign), NUMERICSA (ASCII overpunch) and NUMERICSTS (trailing `+`/`-`) are signed INTEGERs of their digits up to 18 digits, else a BLOB of a sign byte then a byte per digit, inverted when negative; non-digits read as 0. BFLOAT is an INTEGER of its exponent then mantissa, negated when negative, which is exact where a double would round an 8 byte mantissa. CURRENCY is a signed INTEGER. 8 byte unsigned values (UNSIGNED BINARY, TIMESTAMP) flip their top bit to sort as unsigned in SQLite's signed INTEGER. LOGICAL stays raw bytes. The version 3 to 4 upgrade recomputes every key column, so no new version is needed |
| A case insensitive key (0x400 without 0x20) sorts without regard to case; it has no effect with an ACS | String segments (STRING, LSTRING, ZSTRING, legacy ASCII) fold a to z to upper case during conversion, as an upper casing ACS would (`KeyDefinition::isCaseInsensitive`, `Key::applyCollation`). The docs don't say which case Btrieve folds to; it only matters for ``[ \ ] ^ _ ` `` between `Z` and `a`. The loader used to treat the flag as NUMBERED_ACS and threw "Can't load ACS" (`BtrieveDatabase_test.cc` `LoadsCaseInsensitiveKeyWithoutACS`) |

## Outstanding fixes

These apply to single segment keys too, not only composite ones. None appear
in the 11 local `.DAT` files (test assets, MBBSEmu, one module), which use
only ZSTRING, INTEGER, FLOAT, UNSIGNED BINARY and AUTOINCREMENT, plus ACS.

### 1. True nulls (null indicator segments) aren't supported

Type 255 is a one byte null indicator segment before a nullable field. Nulls
group together for sorting but aren't equal for uniqueness, so a unique key
can hold several. Only files created through the SQL engine use these, so
it's low priority for MBBS data.

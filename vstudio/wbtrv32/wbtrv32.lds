/* The Linux equivalent of wbtrv32.def: export BTRCALL and nothing else, so
   the libraries linked in (Abseil, SQLite) stay private to wbtrv32.so rather
   than binding to, or clashing with, the host program's copies. */
{
  global: BTRCALL;
  local: *;
};

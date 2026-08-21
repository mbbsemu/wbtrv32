#ifndef __BTRIEVE_BYTE_STRING_VIEW_TRAITS_H_
#define __BTRIEVE_BYTE_STRING_VIEW_TRAITS_H_

#include <cstring>
#include <string_view>

// This codebase uses std::basic_string_view<uint8_t> (== unsigned char) as
// its canonical byte-buffer view. libstdc++ provides std::char_traits for
// unsigned char as a nonstandard extension, which makes that type usable out
// of the box; libc++ (used by the windows-gnu zig-cc toolchain) does not, so
// any use of std::basic_string_view<uint8_t> fails to even construct there.
// Mirror libstdc++'s extension so the type behaves identically under both
// standard libraries. Must be included before the first use of
// std::basic_string_view<uint8_t> in any translation unit that is compiled
// with libc++.
#if defined(_LIBCPP_VERSION)
namespace std {
template <>
struct char_traits<unsigned char> {
  using char_type = unsigned char;
  using int_type = int;

  static void assign(char_type &c1, const char_type &c2) noexcept {
    c1 = c2;
  }
  static bool eq(char_type c1, char_type c2) noexcept { return c1 == c2; }
  static bool lt(char_type c1, char_type c2) noexcept { return c1 < c2; }

  static int compare(const char_type *s1, const char_type *s2, size_t n) {
    return memcmp(s1, s2, n);
  }
  static size_t length(const char_type *s) {
    size_t i = 0;
    while (s[i] != 0) ++i;
    return i;
  }
  static const char_type *find(const char_type *s, size_t n,
                                const char_type &a) {
    return static_cast<const char_type *>(memchr(s, a, n));
  }
  static char_type *move(char_type *s1, const char_type *s2, size_t n) {
    return static_cast<char_type *>(memmove(s1, s2, n));
  }
  static char_type *copy(char_type *s1, const char_type *s2, size_t n) {
    return static_cast<char_type *>(memcpy(s1, s2, n));
  }
  static char_type *assign(char_type *s, size_t n, char_type a) {
    return static_cast<char_type *>(memset(s, a, n));
  }

  static int_type not_eof(int_type c) noexcept {
    return c == eof() ? 0 : c;
  }
  static char_type to_char_type(int_type c) noexcept {
    return static_cast<char_type>(c);
  }
  static int_type to_int_type(char_type c) noexcept {
    return static_cast<int_type>(c);
  }
  static bool eq_int_type(int_type c1, int_type c2) noexcept {
    return c1 == c2;
  }
  static int_type eof() noexcept { return static_cast<int_type>(-1); }
};
}  // namespace std
#endif  // defined(_LIBCPP_VERSION)

#endif

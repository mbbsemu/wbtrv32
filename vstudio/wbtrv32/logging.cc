#include "logging.h"

#include <cstdio>
#include <string>

#include "absl/base/no_destructor.h"
#include "absl/log/globals.h"
#include "absl/log/log_entry.h"
#include "absl/time/time.h"
#include "framework.h"
#include "wbtrv32_version.h"

#ifdef WIN32
#include <share.h>
#endif

namespace wbtrv32 {
namespace {

// How much goes to wbtrv32.log, set with LogLevel= or WBTRV32_LOGLEVEL (see
// readSetting()). Each level maps onto Abseil's: problems are LOG(WARNING)
// and above, routine statuses LOG(INFO), and opens VLOG(1).
enum LogLevel : int {
  LogOff = 0,
  // failures and conversion problems, but not the statuses applications get
  // as routine answers (key not found, end of file, partial read)
  LogProblems = 1,
  LogAllStatuses = 2,  // every non-zero status
  LogEverything = 3,   // also every open
};

// Writes log messages to wbtrv32.log and, on Windows, to an attached
// debugger.
class LogFileSink final : public absl::LogSink {
 public:
  void open(FILE *file) { file_ = file; }

  void close() {
    if (file_ != nullptr) {
      fclose(file_);
      file_ = nullptr;
    }
  }

  void Send(const absl::LogEntry &entry) override {
    // Abseil's own prefix needs absl::InitializeLog() for a readable time
    // (see openLog()), so this one uses the C library's local time
    const time_t seconds = absl::ToTimeT(entry.timestamp());
    struct tm local = {};
#ifdef WIN32
    localtime_s(&local, &seconds);
#else
    localtime_r(&seconds, &local);
#endif
    const absl::string_view file = entry.source_basename();
    char prefix[128];
    snprintf(prefix, sizeof(prefix),
             "%c %04d-%02d-%02d %02d:%02d:%02d.%06d %d %.*s:%d] ",
             absl::LogSeverityName(entry.log_severity())[0],
             local.tm_year + 1900, local.tm_mon + 1, local.tm_mday,
             local.tm_hour, local.tm_min, local.tm_sec,
             static_cast<int>(absl::ToUnixMicros(entry.timestamp()) % 1000000),
             static_cast<int>(entry.tid()), static_cast<int>(file.size()),
             file.data(), entry.source_line());
    const absl::string_view message = entry.text_message();

    if (file_ != nullptr) {
      // one call, so lines from different threads don't interleave
      fprintf(file_, "%s%.*s\r\n", prefix, static_cast<int>(message.size()),
              message.data());
      fflush(file_);
    }
#ifdef WIN32
    if (IsDebuggerPresent()) {
      OutputDebugStringA(
          (std::string(prefix) + std::string(message) + "\r\n").c_str());
    }
#endif
  }

 private:
  FILE *file_ = nullptr;
};

// Never destroyed, so the Linux .so destructor can still close the log after
// static objects are gone; and it lives in the library, not the heap, so
// unloading the library frees it.
LogFileSink &logFileSink() {
  static absl::NoDestructor<LogFileSink> sink;
  return *sink;
}

int readLogLevel() {
  return readSetting("LogLevel", LogProblems, LogOff, LogEverything);
}

// Every log starts by saying which build wrote it, and for which program.
void logVersionStamp(int logLevel) {
  char program[MAX_PATH] = "?";
  char library[MAX_PATH] = "?";
#ifdef WIN32
  GetModuleFileNameA(nullptr, program, ARRAYSIZE(program));
  HMODULE self = nullptr;
  if (GetModuleHandleExA(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                             GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                         reinterpret_cast<LPCSTR>(&readLogLevel), &self)) {
    GetModuleFileNameA(self, library, ARRAYSIZE(library));
  }
#endif
  WBTRV32_LOG(absl::LogSeverity::kInfo)
      << "wbtrv32 " << WBTRV32_VERSION_STR << " (built " << __DATE__ << " "
      << __TIME__ << ") " << library << " loaded by " << program
      << ", log level " << logLevel;
}

}  // namespace

absl::LogSink &logSink() { return logFileSink(); }

void openLog() {
  const int logLevel = readLogLevel();

  // We're a guest in someone else's process, so every message goes to
  // logSink() alone (WBTRV32_LOG), never to Abseil's global sinks, which
  // include the host's stderr. That also lets us skip absl::InitializeLog()
  // and absl::AddLogSink(): both keep allocations for the life of the
  // process, which would leak each time this library is unloaded.
  if (logLevel != LogOff) {
#ifdef WIN32
    FILE *file = _wfsopen(L"wbtrv32.log", L"ab", _SH_DENYWR);
#else
    FILE *file = fopen("/tmp/wbtrv32.log", "ab");
#endif  // WIN32
    logFileSink().open(file);
    // before the level below is set, so it's never filtered out
    absl::SetMinLogLevel(absl::LogSeverityAtLeast::kInfo);
    logVersionStamp(logLevel);
  }

  absl::SetMinLogLevel(logLevel >= LogAllStatuses
                           ? absl::LogSeverityAtLeast::kInfo
                       : logLevel >= LogProblems
                           ? absl::LogSeverityAtLeast::kWarning
                           : absl::LogSeverityAtLeast::kInfinity);
  absl::SetGlobalVLogLevel(logLevel >= LogEverything ? 1 : 0);
}

void closeLog() { logFileSink().close(); }

}  // namespace wbtrv32

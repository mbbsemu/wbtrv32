#ifndef __LOGGING_H_
#define __LOGGING_H_

#include "absl/base/log_severity.h"
#include "absl/log/log.h"
#include "absl/log/log_sink.h"

namespace wbtrv32 {
// Where every message goes: wbtrv32.log and, on Windows, an attached debugger.
absl::LogSink &logSink();

// Opens wbtrv32.log and sets Abseil's levels from the LogLevel setting.
void openLog();

void closeLog();
}  // namespace wbtrv32

// Abseil's LOG and VLOG, sent to logSink() alone: see openLog().
// WBTRV32_LOG takes an absl::LogSeverity, not LOG's INFO or ERROR: a macro
// argument is expanded before LOG sees it, and windows.h defines ERROR.
#define WBTRV32_LOG(severity) \
  LOG(LEVEL(severity)).ToSinkOnly(&::wbtrv32::logSink())
#define WBTRV32_VLOG(level) VLOG(level).ToSinkOnly(&::wbtrv32::logSink())

#endif

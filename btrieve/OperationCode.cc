#include "OperationCode.h"

#include <cstdio>

namespace btrieve {

#define CASE_WITH_RECORD_LOCK(a)   \
  case a:                          \
  case a##_SingleWaitRecordLock:   \
  case a##_SingleNoWaitRecordLock: \
  case a##_MultipleWaitRecordLock: \
  case a##_MultipleNoWaitRecordLock

// clang-format off
bool requiresKey(OperationCode operationCode) {
  switch (operationCode) {
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireGreater):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireGreaterOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLess):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLessOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryGreater):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryGreaterOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryLess):
    CASE_WITH_RECORD_LOCK(OperationCode::QueryLessOrEqual):
      return true;
    default:
      return false;
  }
}

bool acquiresData(OperationCode operationCode) {
  switch (operationCode) {
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireNext):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquirePrevious):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireGreater):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireGreaterOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLess):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLessOrEqual):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireFirst):
    CASE_WITH_RECORD_LOCK(OperationCode::AcquireLast):
    CASE_WITH_RECORD_LOCK(OperationCode::StepFirst):
    CASE_WITH_RECORD_LOCK(OperationCode::StepLast):
    CASE_WITH_RECORD_LOCK(OperationCode::StepNext):
    CASE_WITH_RECORD_LOCK(OperationCode::StepNextExtended):
    CASE_WITH_RECORD_LOCK(OperationCode::StepPrevious):
    CASE_WITH_RECORD_LOCK(OperationCode::StepPreviousExtended):
      return true;
    default:
      return false;
  }
}

bool usesPreviousQuery(OperationCode operationCode) {
  switch (operationCode) {
  CASE_WITH_RECORD_LOCK(OperationCode::AcquireNext):
  CASE_WITH_RECORD_LOCK(OperationCode::AcquirePrevious):
  CASE_WITH_RECORD_LOCK(OperationCode::StepNext):
  CASE_WITH_RECORD_LOCK(OperationCode::StepNextExtended):
  CASE_WITH_RECORD_LOCK(OperationCode::StepPrevious):
  CASE_WITH_RECORD_LOCK(OperationCode::StepPreviousExtended):
  CASE_WITH_RECORD_LOCK(OperationCode::QueryNext):
  CASE_WITH_RECORD_LOCK(OperationCode::QueryPrevious):
      return true;
    default:
      return false;
  }
}

#define HANDLE_OPERATION_CODE(a) \
  case a:                        \
    return #a

#define HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(a)  \
  case a:                                          \
    return #a;                                     \
  case a##_SingleWaitRecordLock:                   \
    return #a  "_SingleWaitRecordLock";             \
  case a##_SingleNoWaitRecordLock:                 \
    return #a  "_SingleNoWaitRecordLock";           \
  case a##_MultipleWaitRecordLock:                 \
    return #a  "_MultipleWaitRecordLock";           \
  case a##_MultipleNoWaitRecordLock:                \
    return #a  "_MultipleNoWaitRecordLock"

const char* toString(OperationCode operationCode) {
  switch (operationCode) {
    HANDLE_OPERATION_CODE(Open);
    HANDLE_OPERATION_CODE(Close);
    HANDLE_OPERATION_CODE(Insert);
    HANDLE_OPERATION_CODE(Update);
    HANDLE_OPERATION_CODE(Delete);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireEqual);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireNext);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquirePrevious);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireGreater);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireGreaterOrEqual);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireLess);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireLessOrEqual);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireFirst);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(AcquireLast);
    HANDLE_OPERATION_CODE(Create);
    HANDLE_OPERATION_CODE(Stat);
    HANDLE_OPERATION_CODE(Extend);
    HANDLE_OPERATION_CODE(GetPosition);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(GetDirectChunkOrRecord);
    HANDLE_OPERATION_CODE(SetOwner);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(StepFirst);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(StepLast);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(StepNext);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(StepNextExtended);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(StepPrevious);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(StepPreviousExtended);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryEqual);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryNext);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryPrevious);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryGreater);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryGreaterOrEqual);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryLess);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryLessOrEqual);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryFirst);
    HANDLE_OPERATION_CODE_WITH_RECORD_LOCK(QueryLast);
    HANDLE_OPERATION_CODE(Stop);
    HANDLE_OPERATION_CODE(None);
    default:
      static char buf[16];
      sprintf(buf, "0x%X", operationCode);
      return buf;
  }
}

}  // namespace btrieve

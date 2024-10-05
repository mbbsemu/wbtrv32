#include "OperationCode.h"

#include <cstdio>

namespace btrieve {
bool requiresKey(OperationCode operationCode) {
  switch (operationCode) {
    case OperationCode::AcquireEqual:
    case OperationCode::AcquireGreater:
    case OperationCode::AcquireGreaterOrEqual:
    case OperationCode::AcquireLess:
    case OperationCode::AcquireLessOrEqual:
    case OperationCode::QueryEqual:
    case OperationCode::QueryGreater:
    case OperationCode::QueryGreaterOrEqual:
    case OperationCode::QueryLess:
    case OperationCode::QueryLessOrEqual:
      return true;
    default:
      return false;
  }
}

bool acquiresData(OperationCode operationCode) {
  switch (operationCode) {
    case OperationCode::AcquireEqual:
    case OperationCode::AcquireNext:
    case OperationCode::AcquirePrevious:
    case OperationCode::AcquireGreater:
    case OperationCode::AcquireGreaterOrEqual:
    case OperationCode::AcquireLess:
    case OperationCode::AcquireLessOrEqual:
    case OperationCode::AcquireFirst:
    case OperationCode::AcquireLast:
    case OperationCode::StepFirst:
    case OperationCode::StepLast:
    case OperationCode::StepNext:
    case OperationCode::StepNextExtended:
    case OperationCode::StepPrevious:
    case OperationCode::StepPreviousExtended:
      return true;
    default:
      return false;
  }
}

bool usesPreviousQuery(OperationCode operationCode) {
  switch (operationCode) {
    case OperationCode::AcquireNext:
    case OperationCode::AcquirePrevious:
    case OperationCode::StepNext:
    case OperationCode::StepNextExtended:
    case OperationCode::StepPrevious:
    case OperationCode::StepPreviousExtended:
    case OperationCode::QueryNext:
    case OperationCode::QueryPrevious:
      return true;
    default:
      return false;
  }
}

#define HANDLE_OPERATION_CODE(a) \
  case a:                        \
    return #a

const char* toString(OperationCode operationCode) {
  switch (operationCode) {
    HANDLE_OPERATION_CODE(Open);
    HANDLE_OPERATION_CODE(Close);
    HANDLE_OPERATION_CODE(Insert);
    HANDLE_OPERATION_CODE(Update);
    HANDLE_OPERATION_CODE(Delete);
    HANDLE_OPERATION_CODE(AcquireEqual);
    HANDLE_OPERATION_CODE(AcquireNext);
    HANDLE_OPERATION_CODE(AcquirePrevious);
    HANDLE_OPERATION_CODE(AcquireGreater);
    HANDLE_OPERATION_CODE(AcquireGreaterOrEqual);
    HANDLE_OPERATION_CODE(AcquireLess);
    HANDLE_OPERATION_CODE(AcquireLessOrEqual);
    HANDLE_OPERATION_CODE(AcquireFirst);
    HANDLE_OPERATION_CODE(AcquireLast);
    HANDLE_OPERATION_CODE(Create);
    HANDLE_OPERATION_CODE(Stat);
    HANDLE_OPERATION_CODE(Extend);
    HANDLE_OPERATION_CODE(GetPosition);
    HANDLE_OPERATION_CODE(GetDirectChunkOrRecord);
    HANDLE_OPERATION_CODE(SetOwner);
    HANDLE_OPERATION_CODE(StepFirst);
    HANDLE_OPERATION_CODE(StepLast);
    HANDLE_OPERATION_CODE(StepNext);
    HANDLE_OPERATION_CODE(StepNextExtended);
    HANDLE_OPERATION_CODE(StepPrevious);
    HANDLE_OPERATION_CODE(StepPreviousExtended);
    HANDLE_OPERATION_CODE(QueryEqual);
    HANDLE_OPERATION_CODE(QueryNext);
    HANDLE_OPERATION_CODE(QueryPrevious);
    HANDLE_OPERATION_CODE(QueryGreater);
    HANDLE_OPERATION_CODE(QueryGreaterOrEqual);
    HANDLE_OPERATION_CODE(QueryLess);
    HANDLE_OPERATION_CODE(QueryLessOrEqual);
    HANDLE_OPERATION_CODE(QueryFirst);
    HANDLE_OPERATION_CODE(QueryLast);
    HANDLE_OPERATION_CODE(Stop);
    HANDLE_OPERATION_CODE(None);
    default:
      static char buf[16];
      sprintf(buf, "0x%X", operationCode);
      return buf;
  }
}

}  // namespace btrieve

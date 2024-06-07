#include "OperationCode.h"

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
  default:
    return false;
  }
}

} // namespace btrieve
#include "ErrorCode.h"

#include <cstdio>

#define HANDLE_ERROR_CODE(a) \
  case a:                    \
    return #a

namespace btrieve {
const char* errorToString(BtrieveError errorCode) {
  switch (errorCode) {
    HANDLE_ERROR_CODE(Success);
    HANDLE_ERROR_CODE(InvalidOperation);
    HANDLE_ERROR_CODE(IOError);
    HANDLE_ERROR_CODE(FileNotOpen);
    HANDLE_ERROR_CODE(KeyValueNotFound);
    HANDLE_ERROR_CODE(DuplicateKeyValue);
    HANDLE_ERROR_CODE(InvalidPositioning);
    HANDLE_ERROR_CODE(EndOfFile);
    HANDLE_ERROR_CODE(NonModifiableKeyValue);
    HANDLE_ERROR_CODE(InvalidFileName);
    HANDLE_ERROR_CODE(FileNotFound);
    HANDLE_ERROR_CODE(ExtendedFileError);
    HANDLE_ERROR_CODE(PreImageOpenError);
    HANDLE_ERROR_CODE(PreImageIOError);
    HANDLE_ERROR_CODE(ExpansionError);
    HANDLE_ERROR_CODE(CloseError);
    HANDLE_ERROR_CODE(DiskFull);
    HANDLE_ERROR_CODE(UnrecoverableError);
    HANDLE_ERROR_CODE(RecordManagerInactive);
    HANDLE_ERROR_CODE(KeyBufferTooShort);
    HANDLE_ERROR_CODE(DataBufferLengthOverrun);
    HANDLE_ERROR_CODE(PositionBlockLength);
    HANDLE_ERROR_CODE(PageSizeError);
    HANDLE_ERROR_CODE(CreateIOError);
    HANDLE_ERROR_CODE(InvalidNumberOfKeys);
    HANDLE_ERROR_CODE(InvalidKeyPosition);
    HANDLE_ERROR_CODE(BadRecordLength);
    HANDLE_ERROR_CODE(BadKeyLength);
    HANDLE_ERROR_CODE(NotBtrieveFile);
    HANDLE_ERROR_CODE(TransactionIsActive);
    HANDLE_ERROR_CODE(OperationNotAllowed);
    HANDLE_ERROR_CODE(InvalidRecordAddress);
    HANDLE_ERROR_CODE(InvalidKeyPath);
    HANDLE_ERROR_CODE(AccessDenied);
    HANDLE_ERROR_CODE(InvalidACS);
    HANDLE_ERROR_CODE(InvalidInterface);
    HANDLE_ERROR_CODE(FileAlreadyExists);

    default:
      static char buf[16];
      sprintf(buf, "%d", errorCode);
      return buf;
  }
}
}  // namespace btrieve

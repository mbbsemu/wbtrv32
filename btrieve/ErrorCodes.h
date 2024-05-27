#ifndef __ERROR_CODES_H_
#define __ERROR_CODES_H_

namespace btrieve {
enum BtrieveError {
  Success = 0,
  InvalidOperation = 1,
  IOError = 2,
  FileNotOpen = 3,
  KeyValueNotFound = 4,
  DuplicateKeyValue = 5,
  InvalidKeyNumber = 6,
  DifferentKeyNumber = 7,
  InvalidPositioning = 8,
  EOF = 9,
  NonModifiableKeyValue = 10,
  InvalidFileName = 11,
  FileNotFound = 12,
  ExtendedFileError = 13,
  PreImageOpenError = 14,
  PreImageIOError = 15,
  ExpansionError = 16,
  CloseError = 17,
  DiskFull = 18,
  UnrecoverableError = 19,
  RecordManagerInactive = 20,
  KeyBufferTooShort = 21,
  DataBufferLengthOverrun = 22,
  PositionBlockLength = 23,
  PageSizeError = 24,
  CreateIOError = 25,
  InvalidNumberOfKeys = 26,
  InvalidKeyPosition = 27,
  BadRecordLength = 28,
  BadKeyLength = 29,
  NotBtrieveFile = 30,
  TransactionIsActive = 37,
  /* Btrieve version 5.x returns this status code
if you attempt to perform a Step, Update, or Delete operation on a
key-only file or a Get operation on a data only file */
  OperationNotAllowed = 41,
  AccessDenied = 46,
  InvalidInterface = 53,
};
}

#endif
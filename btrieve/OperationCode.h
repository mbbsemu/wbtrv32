#ifndef __OPERATION_CODES_H_
#define __OPERATION_CODES_H_

namespace btrieve {

#define WITH_RECORD_LOCK(a, value)                   \
  a = value, a##_SingleWaitRecordLock = value + 100, \
  a##_SingleNoWaitRecordLock = value + 200,          \
  a##_MultipleWaitRecordLock = value + 300,          \
  a##_MultipleNoWaitRecordLock = value + 400

enum OperationCode {
  // Utility
  Open = 0x0,
  Close = 0x1,
  Insert = 0x2,
  Update = 0x3,
  Delete = 0x4,

  // Acquire Operations
  //[AcquiresData]
  //[RequiresKey]
  WITH_RECORD_LOCK(AcquireEqual, 0x5),

  //[AcquiresData]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(AcquireNext, 0x6),

  //[AcquiresData]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(AcquirePrevious, 0x7),

  //[AcquiresData]
  //[RequiresKey]
  WITH_RECORD_LOCK(AcquireGreater, 0x8),

  //[AcquiresData]
  //[RequiresKey]
  WITH_RECORD_LOCK(AcquireGreaterOrEqual, 0x9),

  //[AcquiresData]
  //[RequiresKey]
  WITH_RECORD_LOCK(AcquireLess, 0xA),

  //[AcquiresData]
  //[RequiresKey]
  WITH_RECORD_LOCK(AcquireLessOrEqual, 0xB),

  //[AcquiresData]
  WITH_RECORD_LOCK(AcquireFirst, 0xC),

  //[AcquiresData]
  WITH_RECORD_LOCK(AcquireLast, 0xD),

  Create = 0xE,
  // Information Operations
  Stat = 0xF,
  Extend = 0x10,
  SetDirectory = 0x11,
  GetDirectory = 0x12,
  GetPosition = 0x16,
  WITH_RECORD_LOCK(GetDirectChunkOrRecord, 0x17),
  Version = 0x1A,
  Reset = 0x1C,
  SetOwner = 0x1D,
  ClearOwner = 0x1E,

  // Step Operations, operates on physical offset not keys
  //[AcquiresData]
  WITH_RECORD_LOCK(StepFirst, 0x21),

  //[AcquiresData]
  WITH_RECORD_LOCK(StepLast, 0x22),

  //[AcquiresData]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(StepNext, 0x18),

  // Extended operations: filtered reads of many records in one call
  WITH_RECORD_LOCK(GetNextExtended, 0x24),
  WITH_RECORD_LOCK(GetPreviousExtended, 0x25),

  //[AcquiresData]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(StepNextExtended, 0x26),

  //[AcquiresData]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(StepPrevious, 0x23),

  //[AcquiresData]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(StepPreviousExtended, 0x27),
  InsertExtended = 0x28,

  // Puts files in backup mode (key number 0) and takes them out again (1 or
  // 2). A database in one SQLite file needs no backup mode.
  ContinuousOperation = 0x2A,

  // Information about an open file; only the listing of extension files
  // (subfunction 1) is answered
  StatExtended = 0x41,

  // Logs in to (key number 0) or out of (1) a database server
  LoginLogout = 0x4E,

  // Query Operations
  //[QueryOnly]
  //[RequiresKey]
  WITH_RECORD_LOCK(QueryEqual, 0x37),

  //[QueryOnly]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(QueryNext, 0x38),

  //[QueryOnly]
  //[UsesPreviousQuery]
  WITH_RECORD_LOCK(QueryPrevious, 0x39),

  //[QueryOnly]
  //[RequiresKey]
  WITH_RECORD_LOCK(QueryGreater, 0x3A),

  //[QueryOnly]
  //[RequiresKey]
  WITH_RECORD_LOCK(QueryGreaterOrEqual, 0x3B),

  //[QueryOnly]
  //[RequiresKey]
  WITH_RECORD_LOCK(QueryLess, 0x3C),

  //[QueryOnly]
  //[RequiresKey]
  WITH_RECORD_LOCK(QueryLessOrEqual, 0x3D),

  //[QueryOnly]
  WITH_RECORD_LOCK(QueryFirst, 0x3E),

  //[QueryOnly]
  WITH_RECORD_LOCK(QueryLast, 0x3F),

  // Transactions span every file the client writes until End or Abort. A
  // lock bias (+100..+400) on Begin is accepted and ignored. A concurrent
  // transaction (1019) is run as an exclusive one, and may also carry the
  // +500 no-wait bias (1519..1919).
  WITH_RECORD_LOCK(BeginTransaction, 0x13),
  WITH_RECORD_LOCK(BeginConcurrentTransaction, 1019),
  WITH_RECORD_LOCK(BeginConcurrentNoWaitTransaction, 1519),
  EndTransaction = 0x14,
  AbortTransaction = 0x15,

  Stop = 0x19,

  // Releases record locks. Key number -1 releases the single-record lock on
  // the position passed in the data buffer, -2 releases all multiple-record
  // locks, and any other key number releases the current single-record lock.
  Unlock = 0x1B,

  None = 0xFFFF,
};

bool requiresKey(OperationCode operationCode);

bool acquiresData(OperationCode operationCode);

bool usesPreviousQuery(OperationCode operationCode);

const char* toString(OperationCode operationCode);

}  // namespace btrieve

#endif

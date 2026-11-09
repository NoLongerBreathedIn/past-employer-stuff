{$T+}
unit ThreadPool;

interface

uses
  System.SysUtils;

// Currently does not include most of the API.

type
  TCallbackInstance = type NativeUInt;
  TReadCallback = TProc<TCallbackInstance, Cardinal, TBytes>;
  TWriteCallback = TProc<TCallbackInstance, Cardinal, NativeUInt>;
  TConnectCallback = TProc<TCallbackInstance, Cardinal>;

// On successful creation, a TPoolIO owns the handle you pass it.
// If creation raises, it owns nothing.
// It is an error to pass a file with FILE_SKIP_COMPLETION_PORT_ON_SUCCESS;
// also, the file must support async I/O.
// While a callback is running, the TPoolIO is still safe to access.
// It is an error to destroy a TPoolIO from within a callback handed to it.
// Possible statuses:
// ERROR_SUCCESS => inline: callback will be called; in callback: success.
// ERROR_OPERATION_ABORTED => TPoolIO is being destroyed. Do not use it further. If inline, callback will not be called.
// ERROR_ALREADY_CONNECTED => in callback only; informational. Treat as success.
// ERROR_IO_PENDING => converted to success; not used.
// ERROR_INVALID_PARAMETER => inline: possibly: you tried to read/write no data
// all other errors => as returned by OS.
type
  TPoolIO = class
  private
    FIO: NativeUInt;
    FHandle: THandle;
    ECallback: THandle;
    FLock: Pointer; // this is an SRW lock
    class procedure Callback(Instance: TCallbackInstance; Context: Pointer; Overlapped: POverlapped; Status: Cardinal; BytesTransferred, IO: NativeUInt); stdcall; static;
  public
    constructor Create(File: THandle);
    destructor Destroy; override;
    function Read(MaxLen: Cardinal; Callback: TReadCallback; out Status: Cardinal; Offset: UInt64 = 0): Boolean;
    // You are not permitted to edit the contents of Data after calling Write until the callback is called.
    function Write(Data: TBytes; Callback: TWriteCallback; out Status: Cardinal; DataOffset: Cardinal = 0; Len: Cardinal = High(Cardinal); Offset: UInt64 = 0): Boolean;
    // You may not call ConnectPipe again while a call to it on the same TPoolIO is outstanding.
    function ConnectPipe(Callback: TConnectCallback; out Status: Cardinal): Boolean;
  end;

  TWorkProc = procedure(Instance: TCallbackInstance; Context: Pointer; Work: NativeUInt); stdcall;

  TMiniPoolWork = record
  private
    FWork: NativeUInt;
  public
    procedure Initialize(Callback: TWorkProc; Context: Pointer); inline;
    procedure Destroy; inline;
    procedure Submit; inline;
  end;

  TProcCBI = TProc<TCallbackInstance>;

  TPoolWork = class
  private
    FWork: TMiniPoolWork;
    FAct: TProcCBI;
    class procedure WorkProc(Instance: TCallbackInstance; Context: Pointer; Work: NativeUInt); stdcall; static;
  public
    constructor Create(Callback: TProcCBI);
    destructor Destroy; override;
    procedure Submit;
  end;

function RunOnThreadpool(Callback: TProcCBI; out Status: Cardinal): Boolean;

procedure WhenCallbackReturnsSetEvent(Instance: TCallbackInstance; Evt: THandle); stdcall;

implementation

procedure WhenCallbackReturnsSetEvent(Instance: TCallbackInstance; Evt: THandle); stdcall;
external 'kernel32.dll' name 'SetEventWhenCallbackReturns';

type TCallbackEnv = type NativeUInt;

procedure CreateThreadpoolWork(Work: TWorkProc; Context: Pointer; Env: TCallbackEnv); stdcall;
external 'kernel32.dll' name 'CreateThreadpoolWork';
procedure CloseThreadpoolWork(Work: NativeUInt); stdcall;
external 'kernel32.dll' name 'CloseThreadpoolWork';
procedure SubmitThreadpoolWork(Work: NativeUInt); stdcall;
external 'kernel32.dll' name 'SubmitThreadpoolWork';

procedure TMiniPoolWork.Initialize(Callback: TWorkProc; Context: Pointer);
begin
  FWork := CreateThreadpoolWork(Callback, Context, TCallbackEnv(0));
  if FWork = 0 then RaiseLastOSError;
end;

procedure TMiniPoolWork.Destroy;
begin
  CloseThreadpoolWork(FWork);
end;

procedure TMiniPoolWork.Submit;
begin
  SubmitThreadpoolWork(FWork);
end;

class procedure TPoolWork.WorkProc(Instance: TCallbackInstance; Context: Pointer; Work: NativeUInt);
begin
  try
    TPoolWork(Context).FAct(Instance);
  except
  end;
end;

constructor TPoolWork.Create(Callback: TProcCBI);
begin
  inherited Create;
  FAct := Callback;
  FWork.Initialize(WorkProc, self);
end;

destructor TPoolWork.Destroy;
begin
  FWork.Destroy;
  inherited Destroy;
end;

procedure TPoolWork.Submit;
begin
  FWork.Submit;
end;

type TOverlapped = record
  Internal, InternalHigh: NativeUInt;
  Offset, OffsetHigh: Cardinal;
  Evt: THandle;
end;

type POverlapped = ^TOverlapped;

type IOCallback = procedure(Instance: TCallbackInstance; Context: Pointer; Overlapped: POverlapped; result: Cardinal; BytesTransferred, IO: NativeUInt); stdcall;

function CreateThreadpoolIO(Handle: THandle; Callback: IOCallback; Context: Pointer; Environment: TCallbackEnv): NativeUInt; stdcall;
external 'kernel32.dll' name 'CreateThreadpoolIO';
procedure StartThreadpoolIO(IO: NativeUInt); stdcall;
external 'kernel32.dll' name 'StartThreadpoolIO';
procedure CancelThreadpoolIO(IO: NativeUInt); stdcall;
external 'kernel32.dll' name 'CancelThreadpoolIO';
procedure CloseThreadpoolIO(IO: NativeUInt); stdcall;
external 'kernel32.dll' name 'CloseThreadpoolIO';
procedure WaitForThreadpoolIOCallbacks(IO: NativeUInt; Cancel: LongBool); stdcall;
external 'kernel32.dll' name 'WaitForThreadpoolIOCallbacks';

function CancelIOEx(File: THandle; Overlapped: POverlapped): LongBool; stdcall;
external 'kernel32.dll' name 'CancelIOEx';

procedure AcquireSRWLockExclusive(var SRWLock: Pointer); stdcall;
external 'kernel32.dll' name 'AcquireSRWLockExclusive';
procedure ReleaseSRWLockExclusive(var SRWLock: Pointer); stdcall;
external 'kernel32.dll' name 'ReleaseSRWLockExclusive';

function TryAcquireSRWLockShared(var SRWLock: Pointer): LongBool; stdcall;
external 'kernel32.dll' name 'TryAcquireSRWLockShared';
procedure ReleaseSRWLockShared(var SRWLock: PPointer); stdcall;
external 'kernel32.dll' name 'ReleaseSRWLockShared';

procedure InitializeSRWLock(out SRWLock: Pointer); stdcall;
external 'kernel32.dll' name 'InitializeSRWLock';

function CreateEvent(SecAttr: Pointer; ManualReset, InitialState: LongBool; Name: PChar): THandle; stdcall;
external 'kernel32.dll' name 'CreateEventW';
function SetEvent(Evt: THandle): LongBool; stdcall;
external 'kernel32.dll' name 'SetEvent';
function ResetEvent(Evt: THandle): LongBool; stdcall;
external 'kernel32.dll' name 'ResetEvent';

function WaitForSingleObject(Obj: THandle; Timeout: Cardinal): Cardinal;
external 'kernel32.dll' name 'WaitForSingleObject';

function CloseHandle(Obj: THandle): LongBool; stdcall;
external 'kernel32.dll' name 'CloseHandle';

function ReadFile(File: THandle; var Data; MaxLength: Cardinal; out BytesRead: Cardinal; var Overlapped: TOverlapped): LongBool; stdcall;
external 'kernel32.dll' name 'ReadFile';
function WriteFile(File: THandle; const Data; MaxLength: Cardinal; out BytesWritten: Cardinal; var Overlapped: TOverlapped): LongBool; stdcall;
external 'kernel32.dll' name 'WriteFile';
function ConnectNamedPipe(File: THandle; var Overlapped: TOverlapped): LongBool; stdcall;

constructor TPoolIO.Create(File: THandle);
begin
  inherited Create;
  InitializeSRWLock(FLock);
  FIO := CreateThreadpoolIO(File, Callback, self, TCallbackEnv(0));
  if FIO = 0 then RaiseLastOSError;
  FCallback := CreateEvent(nil, True, True, nil);
  if FCallback = THandle(0) then RaiseLastOSError;
  FHandle := File;
end;

destructor TPoolIO.Destroy;
begin
  if FCallback <> THandle(0) then begin
    AcquireSRWLockExclusive(FLock);
    try
      CancelIOEx(FHandle, nil);
      WaitForSingleObject(FCallback, High(Cardinal));
      CloseHandle(FHandle);
      CloseHandle(FCallback);
      WaitForThreadpoolIOCallbacks(FIO, False);
    finally
      ReleaseSRWLockExclusive(FLock);
    end;
  end;
  if FIO <> 0 then CloseThreadpoolIO(FIO);
  inherited Destroy;
end;


type TIOAction = TProc<TPoolIO, TCallbackInstance, NativeUInt, Cardinal>;

// this is a trampoline to ensure that heap-allocated records
// do not hold managed data.
type TProcHolder<T> = class
  private
    FAct: T;
  public
    constructor Create(Action: T);
    property Act: T read FAct;
  end;

constructor TProcHolder<T>.Create(Action: T);
begin
  inherited Create;
  FAct := Action;
end;

type TIOAct = TProcHolder<TIOAction>;

type IOCtx = record
  FOlap: TOverlapped;
  FAct: IOAct;
  FSop: Cardinal;
end;

type PCtx = ^IOCtx;

function  MakeCtx(Callback: TIOAction; Offset: UInt64): PCtx; forward;
procedure FreeCtx(Ctx: PCtx); forward;


class procedure TPoolIO.Callback(Instance: TCallbackInstance; Context: Pointer; Overlapped: POverlapped; Status: Cardinal; BytesTransferred, IO: NativeUInt);
var
  Ctx: PCtx;
begin
  Ctx := PCtx(PByte(Overlapped) + (PByte(nil) - PByte(@(PCtx(nil)^.FOlap))));
  Ctx^.FAct.Act(TPoolIO(Context), Instance, BytesTransferred, Status);
  FreeCtx(Ctx);
end;

type IOBackupCallback = reference to function(out Status: Cardinal): Boolean;
type IOStartCallback = reference to function(Ctx: PCtx; out Status: Cardinal; out Backup: IOBackupCallback): Boolean;

function InterpretLastError(Success: Boolean; var LastError: Cardinal): Boolean;
begin
  result := Success;
  if LastError = 997 {ERROR_IO_PENDING} then result := True;
  if result then LastError := 0 {ERROR_SUCCESS};
end;

function MakeCall(var Lock: Pointer; IO: NativeUInt; Starter: IOStartCallback; Callback: TIOAction; out Status: Cardinal; Offset: UInt64 = 0): Boolean;
var
  ctx: PCtx;
  backup: IOBackupCallback;
begin
  ctx := MakeCtx(Callback, Offset);
  if not TryAcquireSRWLockShared(Lock) then begin
    Status := 995 {ERROR_OPERATION_ABORTED};
    FreeCtx(ctx);
    exit(False);
  end;

  try
    try
      StartThreadpoolIO(IO);
      result := Starter(ctx, Status, backup);
    except
      CancelThreadpoolIO(IO);
      FreeCtx(ctx);
      raise;
    end;

    result := InterpretLastError(result, Status);
    if not result then begin
      CancelThreadpoolIO(IO);
      FreeCtx(ctx);
    end;

    if Assigned(backup) then begin
      result := backup(Status);
      result := InterpretLastError(result, Status);  
    end;
  finally
    ReleaseSRWLockShared(Lock);
  end;
end;

function StartRead(Handle: THandle; MaxLen: Cardinal; var Data): IOStartCallback; forward;
function StartWrite(Handle: THandle; Len: Cardinal; const Data): IOStartCallback; forward;
function StartConnect(Handle, Evt: THandle; Callback: TConnectCallback): IOStartCallback; forward;
function FinishRead(Data: TBytes; Callback: TReadCallback): TIOAction; forward;
function FinishWrite(Data: TBytes; Callback: TWriteCallback): TIOAction; forward;
function FinishConnect(Callback: TConnectCallback): TIOAction; forward;

function TPoolIO.Read(MaxLen: Cardinal; Callback: TReadCallback; out Status: Cardinal; Offset: UInt64): Boolean;
var
  data: TBytes;
begin
  if MaxLen = 0 then begin
    Status := 87 {ERROR_INVALID_PARAMETER};
    exit(False);
  end;
  SetLength(data, MaxLen);
  result := MakeCall(FLock, FIO, StartRead(FHandle, MaxLen, data[0]), FinishRead(data, Callback), Status, Offset);
end;

function TPoolIO.Write(Data: TBytes; Callback: TWriteCallback; out Status: Cardinal; DataOffset: Cardinal; Len: Cardinal; Offset: UInt64): Boolean;
begin
  if Length(Data) < DataOffset then begin
    Status := 87 {ERROR_INVALID_PARAMETER};
    exit(False);
  end;

  if Len > Length(Data) - DataOffset then Len := Length(Data) - DataOffset;

  if Len = 0 then begin
    Status := 87 {ERROR_INVALID_PARAMETER};
    exit(False);
  end;

  result := MakeCall(FLock, FIO, StartWrite(FHandle, Len, data[DataOffset]), FinishWrite(data, Callback), Status, Offset);
end;

function TPoolIO.ConnectPipe(Callback: TConnectCallback; out Status: Cardinal): Boolean;
begin
  Result := MakeCall(FLock, FIO, StartConnect(FHandle, FCallback, Callback), FinishConnect(Callback), Status);
end;

function StartRead(Handle: THandle; MaxLen: Cardinal; var Data): IOStartCallback;
var
  datap: Pointer;
begin
  datap := @Data;
  result := function(Ctx: PCtx; out Status: Cardinal; out Backup: IOBackupCallback): Boolean
    begin
      Backup := nil;
      result := ReadFile(Handle, datap^, MaxLen, Ctx^.FSop, Ctx^.FOlap);
      Status := GetLastError;
    end;
end;
function FinishRead(Data: TBytes; Callback: TReadCallback): TIOAction;
begin
  result := procedure(Pool: TPoolIO; Instance: TCallbackInstance; BytesRead: NativeUInt; Status: Cardinal)
    begin
      try
        SetLength(Data, BytesRead);
        Callback(Instance, Status, Data);
      except
      end;
    end;
end;

function StartWrite(Handle: THandle; Len: Cardinal; const Data): IOStartCallback;
var
  datap: Pointer;
begin
  datap := @Data;
  result := function(Ctx: PCtx; out Status: Cardinal; out Backup: IOBackupCallback): Boolean
    begin
      Backup := nil;
      result := WriteFile(Handle, datap^, Len, Ctx^.FSop, Ctx^.FOlap);
      Status := GetLastError;
    end;
end;
function FinishWrite(Data: TBytes; Callback: TWriteCallback): TIOAction;
begin
  result := procedure(Pool: TPoolIO; Instance: TCallbackInstance; BytesWritten: NativeUInt; Status: Cardinal)
    begin
      SetLength(Data, 0); // keep the data alive until the callback is called.
      try
        Callback(Instance, Status, BytesWritten);
      except
      end;
    end;
end;

function ConnectCBToCBI(Evt: THandle; Callback: TConnectCallback): TProcCBI; forward;
function CBIToBackup(Evt: THandle; Callback: TProcCBI): IOBackupCallback; forward;
function DoStartConnect(Handle: THandle; BackupProc: IOBackupCallback): IOStartCallback; forward;

function StartConnect(Handle, Evt: THandle; Callback: TConnectCallback): IOStartCallback;
begin
  result := DoStartConnect(Handle, CBIToBackup(Evt, ConnectCBToCBI(Evt, Callback)));
end;
function FinishConnect(Callback: TConnectCallback): TIOAction;
begin
  result := procedure(Pool: TPoolIO; Instance: TCallbackInstance; SomeData: NativeUInt; Status: Cardinal)
    begin
      try
	Callback(Instance, Status);
      except
      end;
    end;
end;

function ConnectCBToCBI(Evt: THandle; Callback: TConnectCallback): TProcCBI;
begin
  result := procedure(Instance: TCallbackInstance)
    begin
      try
	Callback(Instance, 535 {ERROR_PIPE_CONNECTED});
      except
      end;
      SetEvent(Evt);
    end;
end;

function CBIToBackup(Evt: THandle; Callback: TProcCBI): IOBackupCallback;
begin
  result := function(out Status: Cardinal): Boolean
    begin
      ResetEvent(Evt);
      try
	result := RunOnThreadpool(Callback, Status);
      except
	SetEvent(Evt);
	raise;
      end;
      if not result then SetEvent(Evt);
    end;
end;

function DoStartConnect(Handle: THandle; BackupProc: IOBackupCallback): IOStartCallback;
begin
  result := function(Ctx: PCtx; out Status: Cardinal; out Backup: IOBackupCallback): Boolean
    begin
      Backup := nil;
      result := WriteFile(Handle, datap^, Len, Ctx^.FSop, Ctx^.FOlap);
      Status := GetLastError;
      if not result and (Status = 535 {ERROR_PIPE_CONNECTED}) then
	Backup := BackupProc;
    end;
end;

function MakeCtx(Callback: TIOAction; Offset: UInt64): PCtx;
begin
  New(result);
  result^.FOlap.Internal := 0;
  result^.FOlap.InternalHigh := 0;
  result^.FOlap.Offset := Cardinal(Offset and High(Cardinal));
  result^.FOlap.OffsetHigh := Cardinal(Offset shr 32);
  result^.FOlap.Evt := CreateEvent(nil, True, False, nil);
  try
    if result^.FOlap.Evt = 0 then RaiseLastOSError;
    result^.FAct := TIOAct.Create(Callback);
  except
    if result^.FOlap.Evt <> 0 then CloseHandle(result^.Evt);
    Dispose(result);
    raise;
  end;
end;
procedure FreeCtx(Ctx: PCtx);
begin
  Ctx^.FAct.Free;
  CloseHandle(Ctx^.FOlap.Evt);
  Dispose(Ctx);
end;

type SimpleCallback = procedure(Instance: TCallbackInstance; Context: Pointer); stdcall;

function TrySubmitThreadpoolCallback(Callback: SimpleCallback; Context: Pointer; Environment: TCallbackEnv): LongBool; stdcall;
external 'kernel32.dll' name 'TrySubmitThreadpoolCallback';

procedure RunCBI(Instance: TCallbackInstance; Context: Pointer); stdcall;
begin
  try
    TProcHolder<TProcCBI>(Context).Act(Instance);
  except
  end;
  TProcHolder<TProcCBI>(Context).Free;
end;

function RunOnThreadpool(Callback: TProcCBI; out Status: Cardinal): Boolean;
var
  ph: TProcHolder<TProcCBI>;
begin
  ph := TProcHolder<TProcCBI>.Create(Callback);
  result := TrySubmitThreadpoolCallback(RunCBI, ph, TCallbackEnv(0));
  Status := GetLastError;
end;



end.

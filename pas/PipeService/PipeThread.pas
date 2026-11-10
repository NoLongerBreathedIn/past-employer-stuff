{$T+}
unit PipeThread;

interface

uses
  ThreadPool,
  System.SysUtils,
  System.Classes,
  Winapi.Windows;

type TPipeDirection = (pdRead, pdWrite, pdMisc);

// This gives out abstract references, whose ownership can cross threads.
// Increment creates a reference. Decrement destroys one.
// If your thread does not own a reference, it is an error to call Decrement.
type TRTLRefCounter = record
  strict private
    FLock: TRTLSRWLock;
    FCount: NativeUInt;
    EZero: THandle;
  public
    procedure Initialize;
    procedure Destroy;
    procedure Increment;
    procedure Decrement;
    procedure WaitZero; inline;
  end;

// Internal use only.
type TPipeWrite = record
  Data: TBytes;
  Offset, Len: Cardinal;
end;

// Internal use only.
type TWriteNode = class
  strict private
    FData: TPipeWrite;
    FProc: TProc;
  private
    Next: TWriteNode;
    constructor Create(ToWrite: TPipeWrite; ToDo: TProc = nil);
    property Data: TPipeWrite read FData;
    property Action: TProc read FProc;
  end;
type PWriteNode = ^TWriteNode;

type TAutoWriteQueue = class
  private
    ContinueSending_Proc: TWriteCallback;
    FError: TProc<HRESULT>;
    FPipe: TPoolIO;
    FToDo: TProc;
    FHead: TWriteNode;
    FTail: PWriteNode;
    FLock: TRTLCriticalSection;
    FCallbacks: TRTLRefCounter;
    FSending: Boolean;
    FFullyCreated: Boolean;
    procedure Clear;
    procedure SendMore;
    function  Dequeue(out PipeWrite: TPipeWrite): Boolean;
    procedure ContinueSending(Instance: TCallbackInstance; Status: Cardinal; BytesWritten: NativeUInt);
  public
    constructor Create(Pipe: TPoolIO; OnError: TProc<HRESULT>);
    destructor Destroy; override;
    procedure Send(Data: TBytes; Offset: Cardinal = 0; Len: Cardinal = High(Cardinal); WhenSent: TProc = nil);
  end;

function ArrangeMessage(Name: string; Params: TArray<string>; Data: TBytes): TBytes;

// Do not destroy until IO is destroyed.
// NOTE: When you get a message, you MUST free the stream.
// Even if you raise, you still must free it.
type TPipeReader = class
  private
    ContinueReading_Proc: TReadCallback;
    FPipe: TPoolIO;
    FError: TProc<HRESULT>; // NOT called on error-before-started.
    FMessaged: TProc<TStream>;
    FBroken: TProcCBI; // NOT called if broken before started.
    FMessage: TStream; // holds current message
    FStarted: Pointer;
    function  ReadMore(Instance: TCallbackInstance): Boolean;
    procedure ContinueReading(Instance: TCallbackInstance; Status: Cardinal; Data: TBytes);
  public
    constructor Create(Pipe: TPoolIO; OnError: TProc<HRESULT>; OnMessage: TProc<TStream>; OnBroken: TProcCBI);
    destructor Destroy;
    function Start: Boolean;
  end;

// Internal use only.
type TActQueue = class abstract
  strict private
    FEvt: THandle;
  private
    Next: TActQueue;
    constructor Create(Evt: THandle);
    procedure Act; virtual; abstract;
    property Event: THandle read FEvt;
  end;
type PActQueue = ^TActQueue;

// Internal use only.
type TActQueueProc = class(TActQueue)
  strict private
    FAct: TProc;
  private
    constructor Create(Proc: TProc; Evt: THandle = 0);
    procedure Act; override;
  end;

// Internal use only.
type TActQueueFunc = class(TActQueue)
  strict private
    FAct: TProc<Pointer>;
    FClean: TProc<Pointer>;
    FData: Pointer;
  private
    constructor Create(Proc, Clean: TProc<Pointer>; Evt: THandle = 0);
    destructor Destroy; override;
    procedure Act; override;
    property Data: Pointer read FData write FData;
  end;

// In C++ terms, this is a std::shared_ptr<std::unique_ptr<TActQueueFunc, nop> >,
// where nop is a deleter that does nothing.
// Setting and getting Data, if done simultaneously,
// may fail/return nil sporadically but will not corrupt memory.
// A race between SubmitPrealloc and Destroy is perfectly safe.
// A race between either of those and setting/getting Data
// MAY result in a memory leak, but will NOT corrupt memory.
type TPreallocAct = record
  private
    FAct: TArray<TActQueueFunc>;
    procedure SetData(Value: Pointer);
    function  GetData: Pointer;
  public
    class function Create(Proc: TProc<Pointer>; Clean: TProc<Pointer> = nil): TPreallocAct; static;
    class function CreateForSync(Proc: TProc<Pointer>; Clean: TProc<Pointer> = nil): TPreallocAct; static;
    procedure Destroy;
    property Data: Pointer read GetData write SetData;
  end;

type TSyncer = class
  private
    FLock: TRTLCriticalSection;
    FNeedsPoke: Pointer;
    FHead: TActQueue;
    FTail: PActQueue;
    EDestruct: THandle;
    FWnd: HWnd;
    FOwningThread: Cardinal;
    FDequeueing: Boolean;
    FReentrant: Boolean;
    FDestroying: Boolean;
    procedure DrainQueue;
    class function WndProc(Wnd: HWnd; Msg: Cardinal; WPar: WParam; LPar: LParam): LResult; stdcall; static;
    function SubmitQ(Act: TActQueue): Boolean;
    function Submit(Act: TProc; Evt: THandle = 0);
  public
    constructor Create(AllowReentry: Boolean = False);
    destructor Destroy; override;
    function SubmitWork(Act: TProc; InOrder: Boolean = True): Boolean;
    function Sync(Act: TProc): Boolean;
    function SubmitPrealloc(Act: TPreallocAct): Boolean;
    function OnThread: Boolean;
    function OnStack: Boolean; inline;
  end;

type TSRWLockHelper = record helper for TRTLSRWLock
  function  TryEnterShared: LongBool; inline;
  function  TryEnterExclusive: LongBool; inline;
  procedure EnterShared; inline;
  procedure EnterExclusive; inline;
  procedure LeaveShared; inline;
  procedure LeaveExclusive; inline;
  procedure Initialize; inline;
end;

implementation

uses
  System.SyncObjs;

function HResultFromWin32(Status: Cardinal): HRESULT; inline;
begin
  Result := $80070000 or (Status and $FFFF);
end;

procedure TRTLRefCounter.Initialize;
begin
  FLock.Initialize;
  FCount := 0;
  EZero := CreateEventW(nil, True, True, nil);
  if EZero = 0 then RaiseLastOSError;
end;

procedure TRTLRefCounter.Destroy;
begin
  FLock.EnterExclusive;
  FLock.LeaveExclusive;
  CloseHandle(EZero);
end;

procedure TRTLRefCounter.Increment;
begin
  if AtomicIncrement(FCount) = 1 then begin
    FLock.EnterExclusive;
    FLock.LeaveExclusive;
    ResetEvent(EZero);
  end;
end;
procedure TRTLRefCounter.Decrement;
begin
  FLock.EnterShared;
  if AtomicDecrement(FCount) = 0 then
    SetEvent(EZero);
  FLock.LeaveShared;
end;
procedure TRTLRefCounter.WaitZero;
begin
  WaitForSingleObject(EZero, INFINITE);
end;

type MetaBlock = record
  StartSig: Cardinal;
  Size: Cardinal;
  Code: Cardinal;
  EndSig: Cardinal;
end;

const
  MB_START = $73626F6F; // oobs
  MB_END = $45424F4F; // OOBE
  MB_MULTIBLOCK_START = $626D7473; // stmb
  MB_MULTIBLOCK_END = $424D4f45; // EOMB
  MAX_BLOCK_LEN = High(Word) - 16; // extra space for misc overhead from Windows.

function AllocMetaBlock(Code: Cardinal): TPipeWrite;
var
  block: MetaBlock;
begin
  block.StartSig := MB_START;
  block.EndSig := MB_END;
  block.Size := SizeOf(block);
  block.Code := Code;
  result.Offset := 0;
  result.Len := SizeOf(block);
  SetLength(result.Data, result.Len);
  System.Move(block, result.Data[0], SizeOf(block));
end;

// returns first portion
function SplitPipeWrite(SplitPos: Cardinal; var ToSplit: TPipeWrite): TPipeWrite;
begin
  result := ToSplit;
  Inc(result.Offset, SplitPos);
  Dec(result.Len, SplitPos);
  ToSplit.Len := SplitPos;
end;

constructor TWriteNode.Create(ToWrite: TPipeWrite; ToDo: TProc);
begin
  inherited Create;
  FData := ToWrite;
  FProc := ToDo;
  Next := nil;
end;

function FreeGetNext(Node: TWriteNode): TWriteNode;
begin
  result := Node.Next;
  Node.Free;
end;

function MakeMultiNode(PipeWrite: TPipeWrite; Act: TProc; out Head: TWriteNode): PWriteNode;
var
  splitPos: Cardinal;
begin
  splitPos := MAX_BLOCK_LEN;
  Head := TWriteNode.Create(AllocMetaBlock(MB_MULTIBLOCK_START));
  try
    result := @(Head.Next);
    while PipeWrite.Len > MAX_BLOCK_LEN do begin
      if PipeWrite.Len = MAX_BLOCK_LEN + SizeOf(MetaBlock) then
        splitPos := MAX_BLOCK_LEN - 1;
      // ensure last data block isn't the same size as a meta block
      result^ := TWriteNode.Create(SplitPipeWrite(splitPos, PipeWrite));
      result := @(result^.Next);
    end;
    result^ := TWriteNode.Create(PipeWrite);
    result := @(result^.Next);
    result^ := TWriteNode.Create(AllocMetaBlock(MB_MULTIBLOCK_END));
    result := @(result^.Next);
  except
    while Assigned(Head) do
      Head := FreeGetNext(Head);
    raise;
  end;
end;

function IsConfusable(PipeWrite: TPipeWrite): Boolean;
var
  block: MetaBlock;
begin
  if PipeWrite.Len <> SizeOf(block) then exit(False);
  System.Move(PipeWrite.Data[PipeWrite.Offset], block, PipeWrite.Len);
  result := (block.Size = SizeOf(block)) and
    (block.StartSig = MB_START) and (block.EndSig = MB_END);
end;



constructor TAutoWriteQueue.Create(Pipe: TPoolIO; OnError: TProc<HRESULT>);
begin
  inherited Create;
  FTail := @FHead;
  FPipe := Pipe;
  FError := OnError;
  FCallbacks.Initialize;
  ContinueSending_Proc := ContinueSending;
  InitializeCriticalSectionAndSpinCount(FLock, 6);
  FFullyCreated := True;
end;
destructor TAutoWriteQueue.Destroy; override;
begin
  if FFullyCreated then begin
    Clear;
    FCallbacks.WaitZero;
    FLock.Destroy;
    FCallbacks.Destroy;
  end;
  inherited Destroy;
end;

procedure TAutoWriteQueue.Send(Data: TBytes; Offset: Cardinal; Len: Cardinal; WhenSent: TProc);
var
  pipeWrite: TPipeWrite;
  first: TWriteNode;
  last: PWriteNode;
  wasSending: Boolean;
begin
  if Offset >= Length(Data) then exit;
  if Len > Length(Data) - Offset then
    Len := Length(Data) - Offset;
  pipeWrite.Data := Data;
  pipeWrite.Offset := Offset;
  pipeWrite.Len := Len;

  if (Len > MAX_BLOCK_LEN) or IsConfusable(pipeWrite) then
    last := MakeMultiNode(pipeWrite, WhenSent, first)
  else begin
    first := TWriteNode.Create(pipeWrite, WhenSent);
    last := @(first.Next);
  end;
  FLock.Enter;
  wasSending := FSending;
  FSending := True;
  FTail^ := first;
  FTail := last;
  FLock.Leave;
  if not wasSending then SendMore;
end;

procedure TAutoWriteQueue.Clear;
var
  Node: TWriteNode;
begin
  FLock.Enter;
  Node := FHead;
  FTail := @FHead;
  FHead := nil;
  FLock.Leave;
  while Assigned(Node) do
    Node := FreeGetNext(Node);
end;

function TAutoWriteQueue.Dequeue(out PipeWrite: TPipeWrite): Boolean;
var
  Node: TWriteNode;
begin
  PipeWrite := default(TPipeWrite);
  FLock.Enter;
  Node := FHead;
  FSending := Assigned(Node);
  result := Assigned(Node);
  if Assigned(FHead) then begin
    FHead := FHead.Next;
    if not Assigned(FHead) then FTail := @FHead;
  end;
  FLock.Leave;
  if not result then exit;
  PipeWrite := Node.Data;
  FToDo := Node.Action;
  Node.Free;
end;

procedure TAutoWriteQueue.SendMore;
var
  pipeWrite: TPipeWrite;
  status: Cardinal;
begin
  while True do begin
    if not Dequeue(pipeWrite) then exit;
    FCallbacks.Increment;
    try
      if FPipe.Write(pipeWrite.Data, ContinueSending_Proc, status, pipeWrite.Offset, pipeWrite.Len) then exit;
      FCallbacks.Decrement;
    except
      FCallbacks.Decrement;
      status := ERROR_NOT_ENOUGH_MEMORY;
    end;
    if status = ERROR_OPERATION_ABORTED then begin
      FToDo := nil;
      Clear;
    end else try
      FError(HResultFromWin32(status))
    except
    end;
  end;
end;

procedure TAutoWriteQueue.ContinueSending(Instance: TCallbackInstance; Status: Cardinal; BytesWritten: NativeUInt);
begin
  if (Status = ERROR_OPERATION_ABORTED) or (Status = ERROR_PIPE_BROKEN) then begin
    FToDo := nil;
    Clear;
    exit;
  end;
  if Status <> ERROR_SUCCESS then try
    FError(HResultFromWin32(Status));
  except
  end else if Assigned(FToDo) then try
    FToDo();
  except
  end;
  FToDo := nil;
  SendMore;
end;

function ArrangeMessage(Name: string; Params: TArray<string>; Data: TBytes): TBytes;
var
  bld: TStringBuilder;
  head: Integer;
begin
  bld := TStringBuilder.Create(Name);
  try
    for var param of Params do
      bld.Append(#31).Append(param);
    bld.Append(#10);
    result := TEncoding.UTF8.GetBytes(bld.ToString);
  finally
    bld.Free;
  end;
  if Length(Data) > 0 then begin
    head := Length(result);
    SetLength(result, head + Length(Data));
    System.Move(Data[0], result[head], Length(Data));
  end;
end;

function IsMetaBlock(Data: TBytes; out Code: Cardinal): Boolean;
var
  block: MetaBlock;
begin
  if Length(Data) <> SizeOf(block) then exit(False);
  System.Move(Data[0], block, SizeOf(block));
  Code := block.Code;
  result := (block.StartSig = MB_START) and (block.EndSig = MB_END) and (block.Size = SizeOf(block));
end;

constructor TPipeReader.Create(Pipe: TPoolIO; OnError: TProc<HRESULT>; OnMessage: TProc<TStream>; OnBroken: TProcCBI);
begin
  inherited Create;
  FPipe := Pipe;
  FError := OnError;
  FMessaged := OnMessage;
  FBroken := OnBroken;
  ContinueReading_Proc := ContinueReading;
end;

destructor TPipeReader.Destroy;
begin
  FMessage.Free;
  inherited Destroy;
end;

function TPipeReader.Start: Boolean;
begin
  result := Assigned(AtomicExchange(FStarted, Pointer(self)));
  if not result then result := ReadMore(TPipeInstance(0));
end;

function TPipeReader.ReadMore(Instance: TCallbackInstance): Boolean;
var
  status: Cardinal;
begin
  try
    result := FPipe.Read(MAX_BLOCK_LEN, ContinueReading_Proc, status);
  except
    result := False;
    status := ERROR_OUT_OF_MEMORY;
  end;
  if result then exit;
  AtomicExchange(FStarted, nil);
  if Instance = TCallbackInstance(0) then exit;
  try
    case status of
    ERROR_BROKEN_PIPE: FBroken(Instance);
    ERROR_OPERATION_ABORTED: exit;
    else
      FError(HResultFromWin32(status));
    end;
  except
  end;
end;

procedure TPipeReader.ContinueReading(Instance: TCallbackInstance; Status: Cardinal; Data: TBytes);
var
  code: Cardinal;
  Message: TStream;
begin
  Message := nil;
  if Status = ERROR_BROKEN_PIPE then begin
    try
      FBroken(Instance);
    except
    end;
    exit;
  end;
  if Status = ERROR_OPERATION_ABORTED then begin
    FreeAndNil(FMessage);
    exit;
  end;

  if Status = ERROR_SUCCESS then begin
    if Assigned(FMessage) then begin
      // multi-block message - first block is never treated as special.
      if (FMessage.Size <> 0) and IsMetaBlock(Data, code) and (code = MB_MULTIBLOCK_END) then begin
        Message := FMessage;
        FMessage := nil;
        Message.Position := 0;
      end else try
        FMessage.WriteBuffer(Data, 0, SizeOf(Data));
      except
        Status := ERROR_OUT_OF_MEMORY;
      end;
    end else begin
      if IsMetaBlock(Data, code) and (code = MB_MULTIBLOCK_START) then try
        FMessage := TMemoryStream.Create;
      except
        Status := ERROR_OUT_OF_MEMORY;
      end else begin
        try
          Message := TBytesStream.Create(Data);
        except
          Status := ERROR_OUT_OF_MEMORY;
        end;
      end;
    end;
  end;
  if Status <> ERROR_SUCCESS then try
    FError(HResultFromWin32(Status));
  except
  end;

  if Assigned(Message) then try
    FMessaged(Message);
  except
  end;

  ReadMore(Instance);
end;


constructor TActQueue.Create(Evt: THandle);
begin
  inherited Create;
  FEvt := Evt;
end;

constructor TActQueueProc.Create(Proc: TProc; Evt: THandle);
begin
  inherited Create(Evt);
  FAct := Proc;
end;

procedure TActQueueProc.Act;
begin
  FAct();
end;

constructor TActQueueFunc.Create(Proc, Clean: TProc<Pointer>; Evt: THandle);
begin
  inherited Create(Evt);
  FAct := Proc;
  FClean := Clean;
  FData := nil;
end;

destructor TActQueueFunc.Destroy;
begin
  if Assigned(FClean) then try
    FClean(FData);
  except
  end;
  inherited Destroy;
end;

procedure TActQueueFunc.Act;
begin
  FAct(FData);
end;

class function TPreallocAct.Create(Proc: TProc<Pointer>; Clean: TProc<Pointer>): TPreallocAct;
begin
  SetLength(result.FAct, 1);
  result.FAct[0] := TActQueueFunc.Create(Proc, Clean);
end;

class function TPreallocAct.CreateForSync(Proc: TProc<Pointer>; Clean: TProc<Pointer>): TPreallocAct;
var
  evt: THandle;
begin
  evt := CreateEventW(nil, True, False, nil);
  if evt = 0 then RaiseLastOSError;
  try
    SetLength(result.FAct, 1);
    result.FAct[0] := TActQueueFunc.Create(Proc, Clean, evt);
  except
    CloseHandle(evt);
    raise;
  end;
end;

procedure TPreallocAct.Destroy;
var
  act: TActQueue;
begin
  if Length(FAct) = 0 then exit;
  act := TActQueue(AtomicExchange(Pointer(FAct[0]), nil));
  if not Assigned(act) then exit;
  if act.Event <> 0 then CloseHandle(act.Event);
  act.Free;
end;

procedure TPreallocAct.SetData(Value: Pointer);
var
  act: TActQueueFunc;
begin
  if Length(FAct) = 0 then exit;
  act := TActQueueFunc(AtomicExchange(Pointer(FAct[0]), nil));
  if not Assigned(act) then exit;
  act.Data := Value;
  AtomicExchange(Pointer(FAct[0]), Pointer(act));
end;

function TPreallocAct.GetData: Pointer;
var
  act: TActQueueFunc;
begin
  if Length(FAct) = 0 then exit(nil);
  act := TActQueueFunc(AtomicExchange(Pointer(FAct[0]), nil));
  if not Assigned(act) then exit(nil);
  result := act.Data;
  AtomicExchange(Pointer(FAct[0]), Pointer(act));
end;



end.

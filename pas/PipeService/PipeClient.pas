unit PipeClient;

interface

uses
  System.Classes,
  PipeThread,
  Winapi.Windows,
  ThreadPool;

type TMessagePumper = PipeThread.TMessagePumper;
type TPipeDirection = PipeThread.TPipeDirection;
const pdRead = PipeThread.pdRead;
const pdWrite = PipeThread.pdWrite;
const pdMisc = PipeThread.pdMisc;

type TPipeClient = class;

type TPipeClientErrorHandler = TProc<TPipeClient, TPipeDirection, HRESULT, Boolean>;
type TPipeClientMessageHandler = reference to procedure(Client: TPipeClient; Header: TArray<string>; var Body: TStreamReader);
// Rule: if you still need the stream after the message handler returns,
// nil out the body.

// Connect* functions return:
// ERROR_SUCCESS => connection established
// ERROR_PIPE_CONNECTED => already connected, in compatible way
// ERROR_INVALID_STATE => already connected, incompatibly
// anything else => connection failed

// compatible means: either ConnectThreaded/ConnectThreaded, and same thread,
// or ConnectFree/ConnectFree.
// everything else is incompatible.

type TPipeClient = class{$IFNDEF PIPE_NOT_COMPONENT}(TComponent){$ENDIF}
  private
    Messaged_Proc: TProc<TStream>;
    Broken_Proc: TProcCBI;
    FReadError, FWriteError: TProc<HRESULT>;
    FBreakPrealloc: TPreallocAct;
    FDiscWork: TMiniPoolWork;
    FBreakWork: TMiniPoolWork;
    FPipeName: string;
    FServer: string;
    FPipe: TPoolIO;
    FReader: TPipeReader;
    FWriter: TAutoWriteQueue;
    FCallbacks: TRTLRefCounter;
    FDestroyOK: TRTLRefCounter;
    FSyncFail: TProc<TPipeClient>;
    FMessaged: TPipeClientMessageHandler;
    FError: TPipeClientErrorHandler;
    FBroken: TProc<TPipeClient>;
    FThread: TSyncer;
    FDiscLock: TRTLSRWLock;
    FStarted: Boolean;
    FBreaking: Boolean;
    FDisconnecting: Boolean;
    FRunning: Boolean;
    FCreationStatus: Byte;
    procedure SetMessaged(Value: TPipeClientMessageHandler);
    procedure SetError(Value: TPipeClientErrorHandler);
    procedure SetBroken(Value: TProc<TPipeClient>);
    procedure SetPipeName(Value: string);
    procedure SetServer(Value: string);
    procedure DoError(Dir: TPipeDirection; Err: HRESULT);
    function  DoDisconnect(Threaded: Boolean = False): TSyncer;
    procedure Messaged(Data: TStream);
    procedure Broken(Instance: TCallbackInstance);
    function  AdjustCallback(Callback: TProc; Marshal: Boolean): TProc;
    function  DoConnect: Cardinal;
  public
    constructor Create{$IFNDEF PIPE_NOT_COMPONENT}(AOwner: TComponent){$ENDIF};
    destructor Destroy; inherited;
    function  ConnectThreaded(SyncFail: TProc<TPipeClient> = nil): Cardinal;
    function  ConnectFree: Cardinal;
    procedure Disconnect(Pumper: TMessagePumper = nil);
    // note - during disconnection process,
    // the pumper is not used if not on the thread that called StartThreaded.
    // also - if disconnect occurs during destruction, there may be a stall.
    procedure DisconnectAsync;
    function  Send(Name: string; Params: TArray<string> = nil; Body: TBytes = nil; WhenSent: TProc = nil; Marshal: Boolean = True): Boolean;
    // returns whether the message got queued
    function  OnOwningThread: Boolean;
    function  InStack: Boolean;
    // NOTE: always returns false if StartFree,
    // but that's fine, because in that case you'd know -
    // threadpool threads do not pump messages.
    property Connected: Boolean read FRunning;
{$IFNDEF PIPE_NOT_COMPONENT}
  published
{$ENDIF}
    property OnMessage: TPipeClientMessageHandler read FMessaged write SetMessaged;
    // third argument is whether marshalling succeeded.
    // if it failed, then you are on a threadpool thread.
    property OnError: TPipeClientErrorHandler read FError write SetError;
    property OnBroken: TProc<TPipeClient> read FBroken write SetBroken;
    property PipeName: string read FPipeName write SetPipeName;
    property ServerName: string read FServer write SetServer;
  end;

implementation

uses
  System.Math;

procedure TPipeClient.SetMessaged(Value: TPipeClientMessageHandler);
begin
  FDiscLock.EnterExclusive;
  try
    if FStarted then raise EPipeError('cannot set OnMessage while connected');
    FMessaged := Value;
  finally
    FDiscLock.LeaveExclusive;
  end;
end;
procedure TPipeClient.SetError(Value: TPipeClientErrorHandler);
begin
  FDiscLock.EnterExclusive;
  try
    if FStarted then raise EPipeError('cannot set OnError while connected');
    FError := Value;
  finally
    FDiscLock.LeaveExclusive;
  end;
end;
procedure TPipeClient.SetBroken(Value: TProc<TPipeClient>);
begin
  FDiscLock.EnterExclusive;
  try
    if FStarted then raise EPipeError('cannot set OnBroken while connected');
    FBroken := Value;
  finally
    FDiscLock.LeaveExclusive;
  end;
end;

procedure TPipeClient.SetPipeName(Value: string);
begin
  FDiscLock.EnterExclusive;
  try
    if FStarted then raise EPipeError('cannot set PipeName while connected');
    FPipeName := Value;
  finally
    FDiscLock.LeaveExclusive;
  end;
end;

procedure TPipeClient.SetServer(Value: string);
begin
  FDiscLock.EnterExclusive;
  try
    if FStarted then raise EPipeError('cannot set ServerName while connected');
    FServer := Value;
  finally
    FDiscLock.LeaveExclusive;
  end;
end;

procedure PipeClientDisconnect(Instance: TPipeInstance; Context: Pointer; Work: NativeUInt); stdcall;
var
  thread: TSyncer;
begin
  thread := TPipeClient(Context).DoDisconnect;
  TPipeClient(Context).FDestroyOK.Decrement;
  thread.Free;
end;

procedure PipeClientBroken(Instance: TPipeInstance; Context: Pointer; Work: NativeUInt); stdcall; forward;

constructor TPipeClient.Create{$IFNDEF PIPE_NOT_COMPONENT}(AOwner: TComponent){$ENDIF};
begin
  inherited Create{$IFNDEF PIPE_NOT_COMPONENT}(AOwner){$ENDIF};

  // can be abandoned
  FDiscLock.Initialize;
  FReadError := procedure(Err: HRESULT)
    begin
      DoError(pdRead, Err);
    end;
  FWriteError := procedure(Err: HRESULT)
    begin
      DoError(pdWrite, Err);
    end;
  Messaged_Proc := Messaged;
  Broken_Proc := Broken;

  FCallbacks.Initialize;
  FCreationStatus := 1;
  FDestroyOK.Initialize;
  FCreationStatus := 2;
  FDiscWork.Initialize(PipeClientDisconnect, self);
  FCreationStatus := 3;
  FBreakWork.Initialize(PipeClientBroken, self);
  FCreationStatus := 4;
end;

destructor TPipeClient.Destroy;
begin
  if FCreationStatus = 4 then begin
    Disconnect;
    FDestroyOK.WaitZero;
  end;

  if FCreationStatus >= 1 then FCallbacks.Destroy;
  if FCreationStatus >= 2 then FDestroyOK.Destroy;
  if FCreationStatus >= 3 then FDiscWork.Destroy;
  if FCreationStatus = 4 then FBreakWork.Destroy;

  FBreakPrealloc.Destroy;

  inherited Destroy;
end;

function TPipeClient.OnOwningThread: Boolean;
begin
  FDiscLock.EnterShared;
  result := Assigned(FThread) and FThread.OnOwningThread;
  FDiscLock.LeaveShared;
end;

function TPipeClient.InStack: Boolean;
begin
  FDiscLock.EnterShared;
  result := Assigned(FThread) and FThread.OnStack;
  FDiscLock.LeaveShared;
end;

function TPipeClient.Send(Name: string; Params: TArray<string>; Body: TBytes; WhenSent: TProc; Marshal: Boolean): Boolean;
begin
  FDiscLock.EnterShared;
  try
    if FBreaking or not FRunning then exit(False);
    FWriter.Send(ArrangeMessage(Name, Params, Body), 0, 0, AdjustCallback(WhenSent, Marshal));
  finally
    FDiscLock.LeaveShared;
  end;
end;

procedure TPipeClient.DisconnectAsync;
begin
  FDiscLock.EnterExclusive;
  try
    if not FRunning then exit;
    FRunning := False;
  end
  FDestroyOK.Increment;
  FDiscWork.Submit;
end;

function TPipeClient.ConnectFree: Cardinal;
begin
  FDiscLock.EnterExclusive;
  try
    if FStarted then
      exit(IfThen(Assigned(FThread), ERROR_INVALID_STATE, ERROR_PIPE_CONNECTED));

    // OK, we do need to start.
    result := DoConnect;
    if result = 0 then try
      FRunning := FReader.Start;
    except
      FreeAndNil(FPipe);
      FreeAndNil(FReader);
      FreeAndNil(FWriter);
      FStarted := False;
      raise;
    end;
    if not FRunning then
      result := ERROR_BROKEN_PIPE;


  finally
    FDiscLock.LeaveExclusive;
  end;

  if result = ERROR_BROKEN_PIPE then DoDisconnect;
end;

var PipeBroken: TProc<Pointer>;

function TPipeClient.ConnectThreaded(SyncFail: TProc<TPipeClient>): Cardinal;
begin
  FDiscLock.EnterExclusive;
  try
    if FStarted then
      exit(IfThen(Assigned(FThread) and FThread.OnOwningThread, ERROR_PIPE_CONNECTED, ERROR_INVALID_STATE));

    if not FBreakPrealloc.IsUsable then begin
      FBreakPrealloc := TPreallocAct.Create(PipeBroken);
      FBreakPrealloc.Data := self;
    end;

    FThread := TSyncer.Create;

    result := DoConnect;

    FSyncFail := SyncFail;
    if result <> 0 then exit;
    try
      FRunning := FReader.Start;
    except
      FreeAndNil(FPipe);
      FreeAndNil(FReader);
      FreeAndNil(FWriter);
      FStarted := False;
      FreeAndNil(FThread);
      FSyncFail := nil;
      raise;
    end;

    if not FRunning then
      result := ERROR_BROKEN_PIPE;
  finally
    FDiscLock.LeaveExclusive;
  end;

  if result = ERROR_BROKEN_PIPE then DoDisconnect;
end;

var DefaultPumper: TMessagePumper;

procedure TPipeClient.Disconnect(Pumper: TMessagePumper);
var
  pump, needDisconnect: Boolean;
  quitReceived: Boolean;
  quitParam: NativeUInt;
begin
  FDiscLock.EnterExclusive;
  try
    if not FStarted then exit; // already fully disconnected.
    pump := Assigned(FThread) and FThread.OnOwningThread;
    needDisconnect := FRunning;
    FRunning := False;
  finally
    FDiscLock.LeaveExclusive;
  end;

  if pump and not Assigned(Pumper) then begin
    if needDisconnect then begin
      DoDisconnect(True);
      exit;
    end else
      // oh crap, there's been an async disconnect.
      // We can't return until the disconnect has finished;
      // we'll have to pump only syncer messages.
      Pumper := DefaultPumper;
  end;

  if needDisconnect then begin
    FDestroyOK.Increment;
    FDiscWork.Submit;
  end;

  if pump then begin
    quitReceived := False;
    while pump do begin
      if Pumper(quitParam) then quitReceived := True;
      FDiscLock.EnterShared;
      pump := FStarted;
      FDiscLock.LeaveShared;
    end;
    if quitReceived then PostQuitMessage(quitParam);
  end else
    FDestroyOK.WaitZero;
end;
function TPipeClient.DoDisconnect(Threaded: Boolean): TSyncer;
var
  thread: TSyncer;
begin
  FDiscLock.EnterExclusive;
  try
    if FDisconnecting or not FStarted then exit;
    FBreaking := False;
    FDisconnecting := True;
    FRunning := False;
  finally
    FDiscLock.LeaveExclusive;
  end;
  FreeAndNil(FPipe);
  FreeAndNil(FReader);
  FreeAndNil(FWriter);
  if Threaded then begin
    FreeAndNil(FThread);
    result := nil;
  end else begin
    result := FThread;
    FThread := nil;
    FCallbacks.WaitZero;
  end;
  FSyncFail := nil;

  FDiscLock.EnterExclusive;
  FDisconnecting := False;
  FRunning := False;
  FDiscLock.LeaveExclusive;
end;

type ClError = class
  private
    FCli: TPipeClient;
    FDir: TPipeDirection;
    FErr: HRESULT;
  public
    FMar: Boolean; // marshalled
    constructor Create(Cli: TPipeClient; Dir: TPipeDirection; Err: HRESULT);
    destructor Destroy; override;
    procedure Run;
  end;

function RunClError(Err: ClError): TProc;
begin
  result := procedure
  begin
    Err.Run;
    Err.Free;
  end;
end;

function RunClError_Pool(Err: Pointer): Cardinal; stdcall;
begin
  ClError(Err).Run;
  ClError(Err).Free;
  result := 0;
end;

procedure TPipeClient.DoError(Dir: TPipeDirection; Err: HRESULT);
var
  pack: ClError;
begin
  if not Assigned(FError) then exit;

  pack := ClError.Create(self, Dir, Err);

  if Assigned(FThread) then try
    if FThread.SubmitWork(RunClError(pack)) then exit;
  except
  end;
  pack.FMar := not Assigned(FThread);
  if QueueUserWorkItem(RunClError_Pool, Pointer(pack), 0) then exit;
  pack.Free; // NOT in finally - on successful exit, we raise.
end;

constructor ClError.Create(Cli: TPipeClient; Dir: TPipeDirection; Err: HRESULT);
begin
  inherited Create;
  FCli := Cli;
  FDir := Dir;
  FErr := Err;
  FMar := False;
  FCli.FCallbacks.Increment;
end;
destructor ClError.Destroy;
begin
  FCli.FCallbacks.Decrement;
  inherited Destroy;
end;

procedure ClError.Run;
begin
  try
    FCli.FError(FCli, FDir, FErr, FMar);
  except
  end;
end;

type ClMessaged = class
  private
    FClient: TPipeClient;
    FHeader: TArray<string>;
    FBody: TStreamReader;
  public
    constructor Create(Client: TPipeClient; Header: TArray<string>; Body: TStreamReader);
    destructor Destroy; override;
    procedure Run;
  end;

function RunClMessaged(Data: ClMessaged): TProc;
begin
  result := procedure
  begin
    Data.Run;
    Data.Free;
  end;
end;

function RunClMessaged_Pool(Data: Pointer): Cardinal; stdcall;
begin
  ClMessaged(Data).Run;
  ClMessaged(Data).Free;
  result := 0;
end;

procedure TPipeClient.Messaged(Data: TStream);
var
  reader: TStreamReader;
  pack: ClMessaged;
begin
  if not Assigned(FMessaged) then begin
    Data.Free;
    exit;
  end;

  reader := WrapStreamInReader(Data);
  try
    pack := ClMessaged.Create(self, ReadPipeMessageHeader(reader), reader);
  except
    reader.Free;
    exit;
  end;

  if Assigned(FThread) then begin
    try
      if FThread.SubmitWork(RunClMessaged(pack)) then exit;
    except
    end;
    if Assigned(FSyncFail) then try
      FSyncFail(self);
    except
    end;
    // this does not fall through because we can't call the message handler on some random thread.
  end else
    if QueueUserWorkItem(RunClMessaged_Pool, Pointer(pack), 0) then exit;

  // not in finally; ownership has transferred.
  pack.Free;
end;

constructor ClMessaged.Create(Client: TPipeClient; Header: TArray<string>; Body: TStreamReader);
begin
  inherited Create;
  FClient := Client;
  FHeader := Header;
  FBody := Body;
  FClient.FCallbacks.Increment;
end;

destructor ClMessaged.Destroy;
begin
  FClient.FCallbacks.Decrement;
  FBody.Free;
  inherited Destroy;
end;

procedure ClMessaged.Run;
begin
  try
    FClient.FMessaged(FClient, FHeader, FBody);
  except
  end;
end;

procedure TPipeClient.Broken(Instance: TCallbackInstance);
var
  wasRunning: Boolean;
begin
  FDestroyOK.Increment;
  // we don't have to worry against racing with ourselves, only with Disconnect.
  FDiscLock.EnterExclusive;
  wasRunning := FRunning;
  FBreaking := FRunning;
  FRunning := False;
  FDiscLock.LeaveExclusive;

  if not wasRunning then begin
    FDestroyOK.Decrement;
    exit;
  end;

  if Assigned(FThread) then begin
    if not FThread.SubmitPrealloc(FBreakPrealloc) then
      // skip notification, because we couldn't send it. Oh well.
      FDiscWork.Submit;
  end else FBreakWork.Submit;
end;

function ThreadSubmitter(Thread: TSyncer; Callback: TProc): TProc; forward;
function FreeSubmitter(var RefCtr: TRTLRefCounter; CBDec: TProcCBI): TProc; forward;
function AndDecRC(var RefCtr: TRTLRefCounter; Callback: TProc): TProcCBI; forward;

function TPipeClient.AdjustCallback(Callback: TProc; Marshal: Boolean): TProc;
begin
  if Assigned(FThread) and Marshal then
    result := ThreadSubmitter(FThread, Callback)
  else
    result := FreeSubmitter(FCallbacks, AndDecRC(FCallbacks, Callback))
end;
function TPipeClient.DoConnect: Cardinal;
begin
  //TODO: fill in
end;

procedure BreakCB(Client: TPipeClient);
begin
  if Assigned(Client.FBroken) then try
    Client.FBroken(Client);
  except
  end;
end;

procedure PipeClientBroken(Instance: TPipeInstance; Context: Pointer; Work: NativeUInt);
begin
  BreakCB(TPipeClient(Context));
  PipeClientDisconnect(Instance, Context, Work);
end;

procedure PipeBroken_Impl(Context: Pointer);
begin
  BreakCB(TPipeClient(Context));
  TPipeClient(Context).FDiscWork.Submit;
end;

type PRTLRefCounter = ^TRTLRefCounter;

function FreeSubmitter_Impl(RefCtr: PRTLRefCounter; CBDec: TProcCBI): TProc; forward;
function AndDecRC_Impl(RefCtr: PRTLRefCounter; Callback: TProc): TProcCBI; forward;

function ThreadSubmitter(Thread: TSyncer; Callback: TProc): TProc;
begin
  result := procedure
  begin
    Thread.Submit(Callback);
  end;
end;
function FreeSubmitter(var RefCtr: TRTLRefCounter; CBDec: TProcCBI): TProc;
begin
  result := FreeSubmitter_Impl(@RefCtr, CBDec);
end;
function AndDecRC(var RefCtr: TRTLRefCounter; Callback: TProc): TProcCBI;
begin
  result := AndDecRC_Impl(@RefCtr, Callback);
end;

function FreeSubmitter_Impl(RefCtr: PRTLRefCounter; CBDec: TProcCBI): TProc;
begin
  result := procedure
  var
    status: Cardinal;
  begin
    RefCtr^.Increment;
    try
      if not RunOnThreadpool(CBDec, status) then
	RefCtr^.Decrement;
    except
      RefCtr^.Decrement;
    end;
  end;
end;
function AndDecRC_Impl(RefCtr: PRTLRefCounter; Callback: TProc): TProcCBI;
begin
  result := procedure(Instance: TCallbackInstance)
  begin
    try
      Callback();
    finally
      RefCtr^.Decrement;
    end;
  end;
end;

initialization

PipeBroken := PipeBroken_Impl;
DefaultPumper := TSyncer.HandleSyncerMessage;

end.

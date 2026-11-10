{$T+}
unit PipeThread;

interface

uses
  ThreadPool,
  System.SysUtils,
  System.Classes,
  Winapi.Windows;

type TPipeDirection = (pdRead, pdWrite, pdMisc);

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
  Offset, Length: Cardinal;
end;

// Internal use only.
type TWriteNode = class
  strict private
    FData: TPipeWrite;
    FProc: TProc;
  private
    FNext: TWriteNode;
    constructor Create(ToWrite: TPipeWrite; ToDo: TProc);
    property Data: TPipeWrite read FData;
    property Action: TProc read FProc;
  end;
type PWriteNode = ^TWriteNode;

type TAutoWriteQueue = class
  private
    ContinueSending_Proc: TWriteCallback;
    FError: TProc<HRESULT>;
    FIO: TPoolIO;
    FToDo: TProc;
    FHead: TWriteNode;
    FTail: PWriteNode;
    FLock: TRTLCriticalSection;
    FCallbacks: TRTLRefCounter;
    FSending: Boolean;
    procedure Clear;
    procedure SendMore;
    procedure ContinueSending(Instance: TCallbackInstance; Status: Cardinal; BytesWritten: NativeUInt);
  public
    constructor Create(IO: TPoolIO; OnError: TProc<HRESULT>);
    destructor Destroy; override;
    procedure Send(Data: TBytes; Offset: Cardinal = 0; Length: Cardinal = High(Cardinal); WhenSent: TProc = nil);
  end;

// Do not destroy until IO is destroyed.
type TPipeReader = class
  private
    ContinueReading_Proc: TReadCallback;
    FIO: TPoolIO;
    FError: TProc<HRESULT>;
    FMessaged: TProc<TStream, TCallbackInstance>;
    FBroken: TProcCBI; // NOT called if broken before started.
    FMessage: TStream; // holds current message
    FStarted: Boolean;
    procedure ContinueReading(Instance: TCallbackInstance; Status: Cardinal; Data: TBytes);
  public
    constructor Create(IO: TPoolIO; OnError: TProc<HRESULT>; OnMessage: TProc<TStream, TCallbackInstance>; OnBroken: TProcCBI);
    destructor Destroy;
    function Start: Boolean;
  end;

// Internal use only.
type TActQueue = class abstract
  strict private
    FEvt: THandle;
  private
    FNext: TActQueue;
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
    FWnd: HWnd;
    FDequeueing: Boolean;
    FReentrant: Boolean;
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

end.

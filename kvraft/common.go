package kvraft

const (
	OK             	= "OK"
	ErrNoKey       	= "ErrNoKey"
	ErrWrongLeader 	= "ErrWrongLeader"
	ErrShutdown     = "ErrShutdown"
	ErrInitElection = "ErrInitElection"
)

const (
	GetOperation   		= "Get"
	PutOperation       	= "Put"
	AppendOperation 	= "Append"

)


type Err string

// Put or Append
type PutAppendArgs struct {
	Key			string
	Value 		string
	ClientID	int64
	OPID 		int
	OP    		string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}


type GetReply struct {
	Err   Err
	Value string
}


type GetArgs struct{
	Key 		string
	ClientID	int64
	OPID 		int
}


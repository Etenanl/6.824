package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "time"
import "log"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	me 			int64
	leader 		int
	opID 		int
}

const (
	InitTime = 100	
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.me = nrand()
	ck.opID = 1
	ck.leader = 0
	log.Printf("1")
	return ck
}


//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//log.Printf("client : %d  start to get  %T", ck.me,key)
	args := &GetArgs{
		Key: 		key,
		ClientID: 	ck.me,
		OPID:		ck.opID,
	}

	ck.opID++
	serverID := ck.leader
	for{
		reply := &GetReply{}
		ok := ck.servers[serverID].Call("KVServer.Get", args, reply)
		if	!ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown{
			serverID = (serverID + 1)%len(ck.servers)
			//log.Printf("client : %d  start to get  from %d", ck.me,serverID)
			continue
		}
		if reply.Err == ErrInitElection{
			time.Sleep(InitTime * time.Millisecond)
			continue
		}
		ck.leader = serverID
		if reply.Err == ErrNoKey{
			return ""
		} 
		if reply.Err == OK{
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//log.Printf("client : %d  start to putappend  %T", ck.me,key)
	 
	args := &PutAppendArgs{
		Key:		key,	
		Value:		value,
		ClientID:	ck.me,
		OPID:		ck.opID,
		OP:			op,
	}
	ck.opID++
	serverID := ck.leader
	for{
		reply := &PutAppendReply{}
		ok := ck.servers[serverID].Call("KVServer.PutAppend", args, reply)
		if	!ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown{
			serverID = (serverID + 1)%len(ck.servers)
			//log.Printf("client : %d  finish to put  from %d", ck.me,serverID)
				 
			continue
		}
		if reply.Err == ErrInitElection{
			time.Sleep(InitTime * time.Millisecond)
			continue
		}
		ck.leader = serverID
		if reply.Err == OK{
			return 
		}

	}
	//log.Printf("client : %d  finish to putappend  %T", ck.me,key)

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOperation)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOperation)
}

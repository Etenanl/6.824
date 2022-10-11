package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false
const (
	CheckTermInterval = 100
	SnapshotThreshold = 0.9
	SnapshoterCheckInterval = 100
	SnapshoterAppliedMsgInterval  = 50
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command 	string
	Key 		string
	Value 		string
	ClientID   	int64
	OPID 	int

}

type applyResult struct{
	Err 	Err
	Value 	string
	OPID	int
}

type commandEntry struct{
	op 				Op
	replyChannel 	chan applyResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate float64 // snapshot if log grows this big

	// Your definitions here.
	// 最后一个提交给applyCh的index
	lastApplied		int
	// commandndex to commandntry
	commandApply	map[int]commandEntry
	// Client to RPC结果
	ClientSequence	map[int64]applyResult
	// 维护的本地结果s
	DB 				map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//log.Printf("server:%d start to get %T", kv.me,args.Key)

	if kv.killed(){
		reply.Err = ErrShutdown
		return
	}

	op := Op{
		Command:	GetOperation,	
		Key:		args.Key,
		Value:		"",
		ClientID:	args.ClientID,
		OPID:		args.OPID,
	}

	kv.mu.Lock()
	index,term,isleader := kv.rf.Start(op)
	//log.Printf("server:%d receive get %s client %d  index %d raft_index %d",kv.me, op.Key,args.ClientID,args.OPID,index)
	if term == 0{
		kv.mu.Unlock()
		reply.Err = ErrInitElection
		return
	}
	if !isleader{
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	channel := make(chan applyResult)
	kv.commandApply[index] = commandEntry{
		op:				op,
		replyChannel:	channel,
	}
	kv.mu.Unlock()
//log.Printf("server:%d start to wait get %s  last %d,client %d  Index %d raft_index %d", kv.me,args.Key,index,args.ClientID,args.OPID,index)

CheckTerm:
	
	for !kv.killed() {
		select{
			case result,ok := <- channel:
				if !ok{
					reply.Err = ErrShutdown
					return
				}
				reply.Err = result.Err
				reply.Value = result.Value
				//log.Printf("server:%d finish Get %s", kv.me,args.Key)
				return
			case  <- time.After(CheckTermInterval * time.Millisecond):
				tempTerm,isLeader := kv.rf.GetState()
				if tempTerm != term || !isLeader{
					reply.Err = ErrWrongLeader
					break CheckTerm
				}
				//log.Printf("server:%d still wait get %s  last %d,client %d  Index %d", kv.me,args.Key,index,args.ClientID,args.OPID)
		}
	}

	go func(){<-channel}()

	if kv.killed(){
		reply.Err = ErrShutdown
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	log.Printf("server:%d start to PutAppend %T", kv.me,args.Key)
	 
	if kv.killed(){
		reply.Err = ErrShutdown
		return
	}

	op := Op{
		Command:	args.OP,
		Key:		args.Key,
		Value:		args.Value,
		ClientID:	args.ClientID,
		OPID:		args.OPID,
	}

	kv.mu.Lock()
	index,term,isleader := kv.rf.Start(op)
	log.Printf("server:%d receive  put %s  client %d  index %d raft_index %d",kv.me, op.Key,args.ClientID,args.OPID,index)	 
	log.Println(op)
	if term == 0{
		kv.mu.Unlock()
		reply.Err = ErrInitElection
		return
	}
	if !isleader{
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	channel := make(chan applyResult)
	kv.commandApply[index] = commandEntry{
		op:				op,
		replyChannel:	channel,
	}
	kv.mu.Unlock()
timeout:= 0
log.Printf("server:%d start to wait put %s,%s ,last %d,client %d  Index %d", kv.me,args.Key,args.Value,index,args.ClientID,args.OPID)

CheckTerm:
	
	for !kv.killed() {
		select{
			case result,ok := <- channel:
				if !ok{
					reply.Err = ErrShutdown
					return
				}
				reply.Err = result.Err
				log.Printf("server:%d finish put %s %s ", kv.me,args.Key,args.Value)

					 
				return
			case  <-time.After(CheckTermInterval * time.Millisecond):
				tempTerm,isLeader :=kv.rf.GetState()
				timeout++
				if tempTerm != term ||!isLeader{
					reply.Err = ErrWrongLeader
					break CheckTerm
				}
				log.Printf("server:%d still wait put %s,%s ,last %d,client %d  Index %d", kv.me,args.Key,args.Value,index,args.ClientID,args.OPID)

		}
	}

	go func(){<-channel}()

	if kv.killed(){
		reply.Err = ErrShutdown
		return
	}

}

func(kv *KVServer) apply(applyCh <-chan raft.ApplyMsg,  lastSnapshoterTriggeredCommandIndex int,snapshotTriger chan<- bool){

	var result string 

	for message := range applyCh{

		if message.SnapshotValid{
			kv.mu.Lock()
			kv.lastApplied = message.SnapshotIndex
			log.Printf("server:%d start send snapshot last apply %d", kv.me,kv.lastApplied)
			log.Println(kv.DB,kv.lastApplied,kv.me)
			kv.readSnapShot(message.Snapshot)
			// clear all pending reply channel, to avoid goroutine resource leak
			for _, ca := range kv.commandApply {
				ca.replyChannel <- applyResult{Err: ErrWrongLeader}
				log.Printf("server:%d forgive %s",kv.me, ca.op.Key)
			}
			kv.commandApply = make(map[int]commandEntry)
			log.Printf("server:%d finish send snapshot last apply %d", kv.me,kv.lastApplied)
			log.Println(kv.DB,kv.lastApplied,kv.me)
			kv.mu.Unlock()
			continue
		}

		if !message.CommandValid{
			continue
		}
		if message.CommandIndex - lastSnapshoterTriggeredCommandIndex > SnapshoterAppliedMsgInterval{
			select{
				case snapshotTriger <- true:
					lastSnapshoterTriggeredCommandIndex = message.CommandIndex
				default:
			}

		}


		op := message.Command.(Op)
		log.Printf("server:%d  apply %s  receive with client %d index %d", kv.me,op.Key,op.ClientID,op.OPID)
		kv.mu.Lock()
		kv.lastApplied = message.CommandIndex
		lastResult,ok := kv.ClientSequence[op.ClientID]

		if lastResult.OPID >= op.OPID {
			result = lastResult.Value
			log.Printf("server:%d use old value ", kv.me)
			log.Println(kv.ClientSequence[op.ClientID])
			log.Println(lastResult)
		}else{
			switch op.Command{
				case GetOperation:
					result = kv.DB[op.Key]
				case PutOperation:
					kv.DB[op.Key] = op.Value
					result = ""
				case AppendOperation:
					kv.DB[op.Key] = kv.DB[op.Key] + op.Value
					result = ""
			}
			kv.ClientSequence[op.ClientID] = applyResult{Value:result,OPID:op.OPID}
			log.Printf("server:%d add new value value %s  receive with client %d index %d", kv.me,op.Key,op.ClientID,op.OPID)
			log.Println(kv.ClientSequence[op.ClientID])
			log.Println(lastResult.Value,"  1   ",lastResult.OPID,op.Command,op.Value)
			log.Println(kv.DB[op.Key])
		}
		lastCommand,ok := kv.commandApply[message.CommandIndex]
		if ok{
			delete(kv.commandApply, message.CommandIndex)
			kv.mu.Unlock()
			if lastCommand.op != op{
				lastCommand.replyChannel <- applyResult{Err:ErrWrongLeader}
				log.Printf("server:%d finish apply but fail %s with client %d index %d", kv.me,op.Key,op.ClientID,op.OPID)
				 
			}else{
				lastCommand.replyChannel <- applyResult{Err:OK,Value:result,OPID:op.OPID}
				log.Printf("server:%d finish apply and apply %s with client %d index %d", kv.me,op.Key,op.ClientID,op.OPID)
				 		 
			}
		}else{
			log.Printf("server:%d hanv no client %d index %d", kv.me,op.ClientID,op.OPID)
			kv.mu.Unlock()
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _,ce := range kv.commandApply{
		close(ce.replyChannel)
	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//






func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = float64(maxraftstate)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	

	kv.lastApplied = kv.rf.LastIncludedIndex
	kv.commandApply = make(map[int]commandEntry)
	kv.ClientSequence = make(map[int64]applyResult)
	kv.DB = make(map[string]string)

	kv.readSnapShot(persister.ReadSnapshot())

	snapshotTriger := make(chan bool , 1)
	log.Printf("server:%d start ", kv.me)
	log.Println(kv.DB,kv.me,kv.lastApplied)
	go kv.apply(kv.applyCh, kv.lastApplied,snapshotTriger)
	go kv.snapshot(persister, snapshotTriger)

	return kv
}

func (kv *KVServer) snapshot(persister *raft.Persister,snapshotTriger <- chan bool){

	if kv.maxraftstate < 0{
		return
	}
	for !kv.killed(){
		ratio := float64(persister.RaftStateSize()) / kv.maxraftstate
		if ratio > SnapshotThreshold{
			kv.mu.Lock()
			if data := kv.kvServerSnapShot();data == nil{
				log.Printf("server :%d has some thing wrong in persist" ,kv.me)
			}else{
				log.Printf("server :%d snapshot" ,kv.me)
				log.Println(kv.DB,kv.me,kv.lastApplied)
				kv.rf.Snapshot(kv.lastApplied, data)
			}
			ratio = 0.0
			kv.mu.Unlock()
		}
		select{
			case <- snapshotTriger:
			case <-time.After(time.Duration((1-ratio)*SnapshoterCheckInterval) * time.Millisecond):
		}
	}

}

func (kv *KVServer)kvServerSnapShot()[]byte{
	w := new(bytes.Buffer)
	e :=labgob.NewEncoder(w)

	if e.Encode(kv.DB) != nil||
		e.Encode(kv.ClientSequence) != nil{
			return nil
		}

	return w.Bytes()
}

func (kv *KVServer)readSnapShot(data []byte){
	if len(data) == 0{
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var	cs map[int64]applyResult
	if d.Decode(&db) !=nil ||
		d.Decode(&cs) != nil {
			return 
		}
	kv.DB = db
	kv.ClientSequence = cs
		log.Printf("server :%d read snapshot" ,kv.me)
	log.Println(kv.DB,cs,kv.me)
}


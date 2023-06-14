package shardctrler


import (
	"6.824/raft"
	"sync/atomic"
	"6.824/labrpc"
	"sync"
	"6.824/labgob"
	"log"
	"sort"
	"time"


)



const(
	CheckTermInterval = 100
	SnapshoterAppliedMsgInterval = 100
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead	int32
	// Your data here.
	command 		map[int]commandEntry
	lastApplied 	int
	clientSequence  map[int64]applyResult
	configs 		[]Config // indexed by config num
}


type Op struct {
	// Your data here.
	Args interface{}

	ClientID int64
	OPID     int
}
type applyResult struct {
	Err    Err
	Value Config
	OPID   int
}

type commandEntry struct {
	OP      Op
	replyCh chan applyResult
}


type gidShardsPair struct{
	gid		int
	shards 	[]int
}

type gidShardsPairList []gidShardsPair

func(pl gidShardsPairList) Len()int{
	return len(pl)
}
func(pl gidShardsPairList) Less(i,j int)bool{
	l1,l2 := len(pl[i].shards),len(pl[j].shards)
	if l1<l2 || (l1 == l2 && pl[i].gid<pl[j].gid){
		return true
	}else{
		return false
	}
}
func(pl gidShardsPairList) Swap(i,j int){
	p := pl[i]
	pl[i] = pl[j]
	pl[j] = p
}
// groups  :  gid -> shard
// shards  :  
func(c *Config) GetBalance()(groups map[int][]int,shards gidShardsPairList){
	groups = make(map[int][]int)

	for gid := range c.Groups{
		groups[gid] = []int{}
	}
	for shard,gid := range c.Shards{
		groups[gid] = append(groups[gid],shard)
	}
	

	shards = make(gidShardsPairList,len(groups))
	i:=0
	for k,v := range groups{
		shards[i]=gidShardsPair{k,v} 
		i++
	}

	sort.Sort(shards)
	////log.Println("GetBalance",c,groups,shards)
	return
}


func(c *Config) ReBalance(config *Config,action Action,gids[]int){
	groups,shards := config.GetBalance()
	ogl := len(shards)
	copy(c.Shards[:],config.Shards[:])
	if len(c.Groups) == 0{
		for i := range c.Shards{
			c.Shards[i] = 0
		}
		return
	}else{
		switch action{
			case JOIN:
				ngl := len(c.Groups)
				minLoad := NShards / ngl
				highLoadGroups := NShards - minLoad * ngl
				joinGroup :=map[int]int{}

				for i, gid := range gids{
					if i >= highLoadGroups{
						joinGroup[gid] = minLoad
					}else{
						joinGroup[gid] = minLoad + 1
					}
				}
				i := 0
				for _,gid := range gids{
					for j := 0 ;j<joinGroup[gid];j++{
						index := (ogl-1-i)%ogl
						if index < 0{index = index + ogl}

						sourceGroup := &shards[index]
						//log.Println(c.Shards,shards)
						c.Shards[sourceGroup.shards[0]] = gid
						sourceGroup.shards = sourceGroup.shards[1:]
						i++
					}
				}
				//log.Println("    join balance",c.Shards,config,gids)
			case LEAVE:
				i := 0
				leave := make(map[int]bool)
				for _,gid := range gids{
					leave[gid] = true
				}

				for _,gid := range gids{
					for _, shard:= range groups[gid]{
						for leave[shards[i%ogl].gid]{
							i++
						}
						c.Shards[shard] = shards[i%ogl].gid
						i++
					}
				}
				//log.Println("    leave balance",c.Shards,config,gids)


		}
	}


}



func(sc *ShardCtrler) HandleCommand(op Op)(e Err,config Config){
	if sc.killed(){
		e = ErrShutdown
		return
	}

	sc.mu.Lock()
	index,term,isLeader := sc.rf.Start(op)
	if term == 0{
		sc.mu.Unlock()
		e = ErrInitElection
		return
	}
	if !isLeader{
		sc.mu.Unlock()
		e = ErrWrongLeader
		return 
	}


	channel := make(chan applyResult)
	sc.command[index] = commandEntry{
		OP:				op,
		replyCh:	channel,
	}
	sc.mu.Unlock()

CheckTerm:
	
	for !sc.killed() {
		select{
			case result,ok := <- channel:
				if !ok{
					e = ErrShutdown
					return
				}
				e = result.Err
				config = result.Value
				////log.Printf("server:%d finish Get %s", sc.me,args.Key)
				return
			case  <- time.After(CheckTermInterval * time.Millisecond):
				tempTerm,_ := sc.rf.GetState()
				if tempTerm != term {
					e = ErrWrongLeader
					break CheckTerm
				}
				////log.Printf("server:%d still wait get %s  last %d,client %d  Index %d", sc.me,args.Key,index,args.ClientID,args.OPID)
		}
	}

	go func(){<-channel}()

	if sc.killed(){
		e = ErrShutdown
		
	}
	return
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err,_ = sc.HandleCommand(Op{Args:*args,ClientID:args.ClientID,OPID:args.OPID})
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err,_ = sc.HandleCommand(Op{Args:*args,ClientID:args.ClientID,OPID:args.OPID})
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err,_ = sc.HandleCommand(Op{Args:*args,ClientID:args.ClientID,OPID:args.OPID})
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Err,reply.Config = sc.HandleCommand(Op{Args:*args,ClientID:args.ClientID,OPID:args.OPID})
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}
func (sc *ShardCtrler) killed() bool{
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.lastApplied = 1
	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	initConfig := Config{Num: 0}
	for i := range initConfig.Shards {
		initConfig.Shards[i] = 0
	}
	sc.configs = []Config{initConfig}

	sc.command = make(map[int]commandEntry)
	sc.clientSequence = make(map[int64]applyResult)
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	snapshotTriger := make(chan bool , 1)
	go sc.apply(sc.applyCh, sc.lastApplied, snapshotTriger)

	// Your code here.
	log.Printf("")
	return sc
}


func(sc *ShardCtrler) apply(applyCh <-chan raft.ApplyMsg,  lastSnapshoterTriggeredCommandIndex int,snapshotTriger chan<- bool){
	var result Config 
	for message := range applyCh{
		if message.SnapshotValid{

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
		//	//log.Printf("server:%d  apply %s  receive with client %d index %d", kv.me,op.Key,op.ClientID,op.OPID)
		sc.mu.Lock()
		sc.lastApplied = message.CommandIndex
		lastResult,ok := sc.clientSequence[op.ClientID]

		if lastResult.OPID >= op.OPID {
			result = lastResult.Value
			//log.Printf("server:%d use old value ", sc.me)
			//log.Println(op.Args)
			////log.Println(kv.ClientSequence[op.ClientID])
			////log.Println(lastResult)
		}else{
			l := len(sc.configs)
			lastConfig := sc.configs[l-1]
			newConfig := Config{
				Num:lastConfig.Num + 1,
				Groups : make(map[int][]string),
			}
			for k, v := range lastConfig.Groups {
				newConfig.Groups[k] = v
			}

			switch args := op.Args.(type){
				case QueryArgs:
					if args.Num == -1 || args.Num >= l{
						args.Num = l-1
					}
					result = sc.configs[args.Num]
					
					//log.Println(args)
				case JoinArgs:
					gids := []int{}
					for k,v := range args.Servers{
						gids = append(gids,k)
						newConfig.Groups[k] = v
					}
					sort.Ints(gids)
					//log.Println("joinargs",newConfig.Groups,lastConfig.Groups)
					newConfig.ReBalance(&lastConfig, JOIN, gids)
					sc.configs = append(sc.configs, newConfig)
					result = Config{Num:-1}


				case LeaveArgs:
					for _,gid := range args.GIDs{
						delete(newConfig.Groups,gid)
					}
					//log.Println("leaveargs",newConfig.Groups,lastConfig.Groups)
					newConfig.ReBalance(&lastConfig, LEAVE, args.GIDs)
					sc.configs = append(sc.configs, newConfig)
					result = Config{Num:-1}


				case MoveArgs:
					copy(newConfig.Shards[:], lastConfig.Shards[:])
					newConfig.Shards[args.Shard] = args.GID
					sc.configs = append(sc.configs, newConfig)
					result = Config{Num: -1}
					//log.Printf("server:%d move", sc.me)
					//log.Println(args)

				default:
					panic(args)

			}

			if result.Num < 0{
			}
			sc.clientSequence[op.ClientID] = applyResult{OPID:op.OPID,Value:result}
		}
		lastCommand,ok := sc.command[message.CommandIndex]
		if ok{
			delete(sc.command, message.CommandIndex)
			sc.mu.Unlock()
			if lastCommand.OP.ClientID != op.ClientID || lastCommand.OP.OPID != op.OPID{
				lastCommand.replyCh <- applyResult{Err:ErrWrongLeader}
				////log.Printf("server:%d finish apply but fail %s with client %d index %d", kv.me,op.Key,op.ClientID,op.OPID)
				 
			}else{
				lastCommand.replyCh <- applyResult{Err:OK,Value:result,OPID:op.OPID}
				////log.Printf("server:%d finish apply and apply %s with client %d index %d", kv.me,op.Key,op.ClientID,op.OPID)
				 		 
			}
		}else{
			//log.Printf("server:%d have no client %d index %d", sc.me,op.ClientID,op.OPID)
			sc.mu.Unlock()
		}
	}
	close(snapshotTriger)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _,ce := range sc.command{
		close(ce.replyCh)
	}
}
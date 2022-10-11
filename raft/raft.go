package raft


import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"log"
	"sort"
)


//
// 程序里所有到的一些常量
//

const (
	NILL = 10000
)
const (
	Leader = iota
	Candidate
	Follower
)
const (
	CTimeOut = 3
	FTimeOut = 10
)

//
// 一些工具方法
//

func Max(x int,y int) int{
	if x >=y{
		return x
	}else{
		return y
	}
}

func Min(x int,y int) int{
	if x <= y{
		return x
	}else{
		return y
	}
}

//
// 结构体
//
type Entry struct {
	Command	interface{}
	Term	int
	IsEmpty	bool
}


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
//
// RPC结构体 
// field names must start with capital letters!  
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 候选人的任期号
	Term			int
	// 请求选票的候选人的 Id
	CandidateId		int
	//候选人的最后日志条目的索引值
	LastLogIndex	int
	// 候选人最后日志条目的任期号
	LastLogTerm		int
}
//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {

	// Your data here (2A).isleader
	// 当前任期号，以便于候选人去更新自己的任期号
	Term 		int
	// 候选人赢得了此张选票时为真
	VoteGranted	bool

}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	// 领导人的任期号
	Term			int
	// 领导的Id
	LeaderId		int
	// 最后日志条目的索引值
	PrevLogIndex	int
	// 最后日志条目的任期号
	PrevLogTerm		int
	// 准备存储的日志条目
	Entries 		[]Entry
	// 领导人已经提交的日志的索引值
	LeaderCommit	int

	//
	IsEmpty			bool
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {

	// Your data here (2A).
	// 当前任期号，以便于领导人去更新自己的任期号
	Term 		int
	// Follower匹配了PrevLogIndex和PrevLogTerm时为真
	Success		bool

	// 自用
	// 更新请求节点的nextIndex【i】
	UpNextIndex	int
	IsRepet		bool


}
type InstallSnapShotArgs struct{
	// 领导人的任期号
	Term				int
	// 领导的Id
	LeaderId			int
	// 最后日志条目的索引值
	LastIncludedIndex	int
	// 最后日志条目的任期号
	LastIncludedTerm	int
	// 准备存储的日志条目
	Data 				[]byte

}


type InstallSnapShotReply struct{
	// 任期号
	Term				int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// 所有server都有的持久化状态
	// 遇到的最新的term号，初始化为0
	currentTerm int
	// 这个server投给的candidated编号
	votedFor	int
	// entry序列
	log			[]Entry
	// 提交给状态机的chan
	applyMessage	chan ApplyMsg
	// 所有server都有的不稳定状态
	// 已知提交的最高的log编号，初始化0
	commitIndex	int
	// 最后被应用到状态机的日志条目索引值（初始化为 0
	lastApplied int


	// leader的不稳定状态
	// 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	nextIndex	[]int
	// 对于每一个服务器，已经复制给他的日志的最高索引值
	matchIndex	[]int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


	// 自己定义
	// 标志server的状态，leader，candidate，follower
	state	int
	// 标志server超时的时间，单位100ms
	candidateOut	int
	followerOut		int
	// snapshot中最后一项的实际index
	LastIncludedIndex	int
	// snapshot中最后一项的term
	LastIncludedTerm	int

	// 目前不知道什么用

	IsSnapShot 	bool
	isCommit	bool
	HasSnapShot bool
}

//
// 工具方法
// 


// 全部长度
func (rf *Raft) LastLength()int{
	if rf.LastIncludedIndex == 0{
		return len(rf.log)
	}else{
		return len(rf.log) + rf.LastIncludedIndex + 1
	}
	
}
// 返回全部log的最后一个的index，包括snapshot
func(rf *Raft) LastIndex() int{
	if rf.LastIncludedIndex == 0{
		return len(rf.log) - 1
	}else if len(rf.log) == 0{
		return rf.LastIncludedIndex
	}else{
		return rf.LastIncludedIndex + len(rf.log)
	}
}
// 返回全部log的最后一个的Term，包括snapshot
func (rf *Raft)LastTerm() int{
	lastTerm := 0
	if  len(rf.log) > 0{
		lastTerm = rf.log[len(rf.log)-1].Term
	}else if len(rf.log) == 0{
		lastTerm = rf.LastIncludedTerm
	}else{
		panic("term wrong,len(log) smaller than 0")
	}
	return lastTerm
}
// 入大出小  Index
func (rf *Raft)ScalerIndexToReal(index int) int{
	if rf.LastIncludedIndex == 0{
		return index
	}else{
		var result = index - rf.LastIncludedIndex - 1
		if result >= 0{
			return result
		}else{
			log.Printf("index %d  lastIncludedIndex  %d",index,rf.LastIncludedIndex)
			panic("  ")
		}
	}
}
// 入大出小 Term
func (rf *Raft)ScalerTermToReal(index int) int{
	if index == rf.LastIncludedIndex{
		return rf.LastIncludedTerm
	}
	return rf.log[rf.ScalerIndexToReal(index)].Term
}

// 入小出大 indedx
func (rf *Raft)ScalerIndexToLast(index int) int{
	var result = index + rf.LastIncludedIndex
	if result >= 0{
		return result
	}else{
		panic("wrong index smaller than LastIncludeindex")
	}
}
// 入小出大 term
func (rf *Raft)ScalerTermToLast(index int) int{
	if index > 0 {
		if rf.RealLength() == 0{
			return rf.LastIncludedTerm
		}else{
			return rf.log[index].Term
		}
	}else if index == 0{
		return rf.LastIncludedTerm
	}else {
		panic("wrong index smaller than 0")
	}

}
// 返回目前log长度
func (rf *Raft)RealIndex() int{
	return len(rf.log) - 1
}
func (rf *Raft)RealLength() int{
	return len(rf.log)
}


//
// 系统调用 
// 


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state==Leader{
		isleader = true
	}else{
		isleader = false
	}
	///log.Printf("peer:%d,term:%d,isleader : %t",rf.me, term,isleader)
	return term, isleader
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
// 
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader{
		return -1,-1,false
	}

	logEntry := Entry{
		Command:command,
		Term:rf.currentTerm,
		IsEmpty:false,
	}


	rf.log = append(rf.log, logEntry)
	index = rf.LastIndex()
	term = rf.currentTerm
	rf.persist()

	log.Printf("Leader:%d append log",rf.me)
	//log.Println(rf)
	log.Println(command)
	rf.AppendNow(command)
	return index,term,isLeader

}




func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}



// 
// RPC方法
//


func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) raftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
			e.Encode(rf.votedFor) != nil ||
			e.Encode(rf.log) != nil ||
			e.Encode(rf.LastIncludedIndex) != nil ||
			e.Encode(rf.LastIncludedTerm) != nil {
			return nil
	}
	return w.Bytes()
}


func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	if data := rf.raftState();data == nil{
		log.Printf("peer :%d has some thing wrong in persist",rf.me)
	}else{
		rf.persister.SaveRaftState(data)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor ,indedx,term int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&indedx) != nil ||
		d.Decode(&term) != nil {
		log.Printf("peer :%d has some thing wrong in read  persist",rf.me)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logs
	rf.LastIncludedIndex = indedx
	rf.LastIncludedTerm = term
}





func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	log.Printf("peer :%d going to snapshot : index %d  ,  applied %d",rf.me,index,rf.lastApplied)
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index > rf.commitIndex || rf.LastIncludedIndex >= index{
		return
	}

	snapLogs := make([]Entry,0)
	//snapLogs = append(snapLogs,Entry{0,-1,true})
	for i := rf.ScalerIndexToReal(index + 1); i < rf.RealLength(); i++ {
		snapLogs = append(snapLogs, rf.log[i])
	}

	rf.LastIncludedTerm = rf.ScalerTermToReal(index)
	rf.LastIncludedIndex = index

	
	rf.log = snapLogs

	// if index > rf.commitIndex{
	// 	rf.commitIndex = index
	// }

	// if index >= rf.lastApplied{
	// 	rf.lastApplied = index + 1
	// 	log.Printf("502 line %d",rf.lastApplied)
	// }

	//rf.commitIndex = Max(index, rf.commitIndex)
	//rf.lastApplied = index + 1
	log.Printf("peer %d  856 line,%d",rf.me,rf.lastApplied)
	log.Println(rf.log)

	rf.persister.SaveStateAndSnapshot(rf.raftState(), snapshot)


}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 如果收到的term<=自己的term，说明对方开始竞选前的term小于自己
	if args.Term < rf.currentTerm{
		log.Printf("Peer %d(%d,%d) : hav higher term than %d,ID%d", rf.me,rf.currentTerm,rf.state,args.Term,args.CandidateId)
		rf.mu.Unlock()
		return 
	}else if args.Term >= rf.currentTerm{

		if args.Term > rf.currentTerm {
			rf.mu.Unlock()
			rf.BeFollower(args.Term, NILL)
			rf.persist()
			rf.mu.Lock()
		}


		if (rf.votedFor == NILL || args.CandidateId == rf.votedFor) && 
		(args.LastLogTerm>rf.LastTerm()|| 
		(args.LastLogTerm==rf.LastTerm()&& args.LastLogIndex>=rf.LastIndex()) ){
			// 如果候选者的log比receiver新
			log.Printf("Peer %d(%d,%d) : receive RequestVote,Be Follower vote for %d", rf.me,rf.currentTerm,rf.state,args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.followerOut = 0
			rf.mu.Unlock()
			rf.BeFollower(args.Term, args.CandidateId)

			rf.persist()
			return
		
		}else{
			// 如果votedfor不为NILL则说明已经进行了投票
			rf.mu.Unlock()
			return
		}
	}
}



func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.UpNextIndex = rf.lastApplied
	rf.mu.Lock()
	log.Printf("Peer %d(%d,%d) : receive appendentry  lastApplied%d", rf.me,rf.currentTerm,rf.state,rf.lastApplied)
	if  args.Term > rf.currentTerm{
		rf.followerOut = 0
		log.Println("beat1")
		rf.mu.Unlock()
		log.Printf("Peer %d(%d,%d) : change to follower", rf.me,rf.currentTerm,rf.state)
		rf.BeFollower(args.Term, args.LeaderId)

		return
	}else if args.Term == rf.currentTerm{

		rf.followerOut = 0
		rf.mu.Unlock()
		rf.BeFollower(args.Term, args.LeaderId)
		rf.mu.Lock()

		// 心跳，直接返回
		if args.IsEmpty == true{
		log.Printf("heart beat from %d   term : %d   args.Term  %d",args.LeaderId,rf.currentTerm,args.Term)
			if rf.LastTerm() == args.Term{
				if args.LeaderCommit > rf.commitIndex{
					//log.Printf("peer: %d change commitIndex %d", rf.me,rf.commitIndex)

					rf.commitIndex = Min(args.LeaderCommit, rf.LastIndex())
					log.Printf("peer: %d change commitIndex %d instead %d  and data is empty", rf.me,rf.commitIndex , rf.LastIndex()-1)
				}
			}
			
			rf.mu.Unlock()
			return
		}
		if args.PrevLogIndex < rf.LastIncludedIndex {	
			log.Printf("peer:%d  lastindex %d < prevlogIndex %d   lastapplied %d", rf.me,rf.LastIndex(),args.PrevLogIndex,rf.lastApplied)
			rf.mu.Unlock()
			return
		}
		if rf.LastIndex() < args.PrevLogIndex {	
			log.Printf("peer:%d  lastindex %d < prevlogIndex %d   lastapplied %d", rf.me,rf.LastIndex(),args.PrevLogIndex,rf.lastApplied)
			rf.mu.Unlock()
			return
		}
		log.Printf("peer %d  Len  %d  realIndex %d   prevlogIndex  %d", rf.me,len(rf.log),rf.ScalerIndexToReal(args.PrevLogIndex+1),args.PrevLogIndex)
		
		if rf.ScalerTermToReal(args.PrevLogIndex) != args.PrevLogTerm {
			log.Printf("peer:%d  rf.log[rf.ScalerIndexToReal(args.PrevLogIndex)].Term   %d  != args.PrevLogTerm %d", rf.me,rf.log[rf.ScalerIndexToReal(args.PrevLogIndex)].Term,args.PrevLogTerm)
			rf.log = rf.log[:rf.ScalerIndexToReal(args.PrevLogIndex + 1)]
			rf.mu.Unlock()
			return
		}
		if args.PrevLogIndex < rf.lastApplied - 1{
			log.Printf("peer:%d  PrevLogIndex %d < rf.lastApplied - 1 %d", rf.me,args.PrevLogIndex,rf.lastApplied - 1)
			rf.mu.Unlock()
			return
		}
		log.Printf("prev %d  len(log) %d   end %d", args.PrevLogIndex,len(rf.log),rf.ScalerIndexToReal(args.PrevLogIndex+ 1) )
		

		existingEntries := rf.log[rf.ScalerIndexToReal(args.PrevLogIndex+1):]
		reply.IsRepet = false
		if len(existingEntries)>= len(args.Entries) && len(existingEntries)>0{
			flag := false
			for i := 0 ; i < len(args.Entries);i++{
				if existingEntries[i].Term != args.Entries[i].Term{
					flag = true
					break
				}

			}
			if !flag{
				reply.UpNextIndex = rf.LastLength()
				log.Printf("peer:%d  has longer log %d  data %d", rf.me,len(existingEntries),len(args.Entries))
				reply.IsRepet = true
				reply.Success = true
				rf.mu.Unlock()
				return
			}
		}

		rf.log = rf.log[:rf.ScalerIndexToReal(args.PrevLogIndex + 1) ]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		if args.LeaderCommit > rf.commitIndex{
			log.Printf("peer: %d change commitIndex %d", rf.me,rf.commitIndex)
			//log.Println(rf.log)
			rf.commitIndex = Min(args.LeaderCommit, rf.LastIndex())
			log.Printf("peer: %d change commitIndex %d", rf.me,rf.commitIndex)
		}
		log.Printf("lastindex %d   len  %d  lastLength  %d", rf.LastIncludedIndex,len(rf.log),rf.LastLength())
		reply.UpNextIndex = rf.LastLength()
		reply.Success = true
		rf.mu.Unlock()
		return

	}else{
		log.Println("beat2")
		reply.Success = false
		rf.mu.Unlock()
		return
	}
}


// 处理心跳，对leader，candidate和follower
func (rf *Raft) Timer() {
	
	for {
		randTime := rand.Intn(50)
		time.Sleep(time.Duration(75+randTime) * time.Millisecond)
		rf.mu.Lock()
		
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.killed(){
			rf.mu.Unlock()
			return
		}

		if rf.state == Leader || rf.state == Candidate{
			rf.followerOut = 0
			rf.mu.Unlock()
			continue
		}else if rf.state == Follower{
			rf.followerOut += 1
			if rf.followerOut >= FTimeOut{
				log.Printf("peer %d going to be candidate", rf.me)
				rf.followerOut = 0
				rf.state = Candidate
				rf.mu.Unlock()
				go rf.BeCandidate()
			}else{
				rf.mu.Unlock()
			}
			
		}
	}
}

func (rf *Raft)ApplyEntries(){
	for{
		time.Sleep(10 * time.Millisecond)
		if rf.killed(){
			return
		}
		
		if rf.state == Leader{
			rf.mu.Lock()
			middleMatch :=make([]int,len(rf.matchIndex))
			copy(middleMatch,rf.matchIndex)
			middleMatch[rf.me] = rf.LastIndex()
			sort.Ints(middleMatch)
			midIndex := len(middleMatch)/2
			log.Printf("leader:%d wangt update commit", rf.me)
			log.Println(rf.matchIndex)
			log.Printf("leader:%d trying to update commit %d", rf.me,middleMatch[midIndex])
			if middleMatch[midIndex] == -1 || middleMatch[midIndex] == rf.commitIndex{
				//rf.mu.Unlock()
				//continue
			}else if len(rf.log) >= 0 && middleMatch[midIndex] > rf.LastIncludedIndex{
				log.Println("gong to update commitedindex")
				log.Println(len(rf.log))
				log.Println(rf.matchIndex,rf.LastIncludedIndex,rf.me)
				if rf.ScalerTermToReal(middleMatch[midIndex]) == rf.currentTerm{
					rf.commitIndex = Max(rf.commitIndex,middleMatch[midIndex])
					//log.Printf("leader:%d update commit: %d", rf.me,rf.commitIndex)
					rf.SendHeartBeat()

				}
						
			}
			rf.mu.Unlock()
		}
		rf.mu.Lock()
		rf.isCommit = true
		for rf.lastApplied <= rf.commitIndex && rf.lastApplied < rf.LastLength(){
			log.Printf("peer %d applied:%d    commitIndex:%d   astLength(%d  reallength %d  InceludeIndex  %d", rf.me,rf.lastApplied,rf.commitIndex,rf.LastLength(),rf.RealLength(),rf.LastIncludedIndex)
			rf.HasSnapShot = false
			//rf.ScalerTermToReal()
			if rf.RealLength() == 0{
				// continue
				log.Println("1")
				break
			}
			if rf.lastApplied <= rf.LastIncludedIndex{
				//rf.lastApplied = rf.LastIncludedIndex + 1
				log.Println("2")
				break
			}
			if rf.log[rf.ScalerIndexToReal(rf.lastApplied)].IsEmpty == true{
				rf.lastApplied++
				log.Printf("753 line %d",rf.lastApplied)
				log.Println("3")
				continue
			}else{
				if rf.IsSnapShot {
					break
				}
				tempapply := rf.lastApplied
				message := ApplyMsg{true,rf.log[rf.ScalerIndexToReal(rf.lastApplied)].Command,rf.lastApplied,false,[]byte{'a'},0,0}
				
				rf.mu.Unlock()
				rf.applyMessage <- message
				rf.mu.Lock()
				log.Printf("peer:%d already applied :%d and commited %d and temapply %d",rf.me,rf.lastApplied,rf.commitIndex,tempapply)
				log.Println(message)
				
				if tempapply != rf.lastApplied || rf.lastApplied > rf.commitIndex || rf.HasSnapShot|| rf.IsSnapShot{
					break
				}
				//log.Printf("peer:%d update apply with applied :%d and commited %d", rf.me,rf.lastApplied,rf.commitIndex)
			}
			rf.lastApplied++
			log.Printf("peer:%d update apply with applied :%d and commited %d", rf.me,rf.lastApplied,rf.commitIndex)
		}
		rf.isCommit = false
		rf.mu.Unlock()

	}

}

func (rf *Raft) SendHeartBeat(){
	for i := 0;i<len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		args:=&AppendEntriesArgs{
			Term:rf.currentTerm,
			LeaderId:rf.me,
			PrevLogIndex:rf.LastIndex(),
			PrevLogTerm:rf.LastTerm(),
			Entries:make([]Entry,0),
			LeaderCommit:rf.commitIndex,
			IsEmpty:true,
		}

		reply:=&AppendEntriesReply{
			Term:0,
			Success:false,
			UpNextIndex:0,
		}
		peer := i
		go func(server int){
			//log.Printf("Peer %d(%d) : Be Leader and Sending void entry to %d", rf.me,rf.currentTerm,server)
			rf.sendAppendEntry(server, args, reply)
		}(peer)	
	}
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs,reply *InstallSnapShotReply){
	if rf.killed(){
		return
	}
	log.Printf("peer %d receive snapshot ", rf.me)
	rf.mu.Lock()
	
	switch{
		case args.Term < rf.currentTerm:
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		case args.Term > rf.currentTerm:
			rf.mu.Unlock()
			rf.BeFollower(args.Term, NILL)	
		case args.Term == rf.currentTerm:
			rf.mu.Unlock()
			rf.BeFollower(args.Term, args.LeaderId)
	}

	rf.mu.Lock()

	rf.followerOut = 0
	reply.Term = rf.currentTerm

	
	if rf.LastIncludedIndex >= args.LastIncludedIndex{
		rf.mu.Unlock()
		return
	}

	index := args.LastIncludedIndex
	tempLog := make([]Entry,0)

	for i := rf.ScalerIndexToReal(index + 1);i<=rf.RealIndex();i++{
		tempLog = append(tempLog,rf.log[i])
	}


	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.log = tempLog

	rf.commitIndex = Max(args.LastIncludedIndex, rf.commitIndex)
	rf.lastApplied = index + 1
	log.Printf("peer %d, 856 line,%d commit %d;%d",rf.me,rf.lastApplied,rf.commitIndex,args.LastIncludedIndex)
	
	
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.raftState(),args.Data)
	rf.IsSnapShot = true
	rf.HasSnapShot = true
	rf.mu.Unlock()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.LastIncludedTerm, 
		SnapshotIndex: rf.LastIncludedIndex,
	}
	if rf.isCommit{
		time.Sleep(1 * time.Millisecond)
	}
	rf.applyMessage <- msg

	rf.mu.Lock()

	rf.IsSnapShot = false
	rf.mu.Unlock()
	log.Printf("peer %d handle snapshot  and end", rf.me)
	//log.Println(rf)
	//log.Println(args)
	//log.Println(reply)

}

func (rf *Raft) SnapShotSender(server int){
	log.Printf("leader :%d send snapshot to peer %d", rf.me,server)
	if rf.killed(){
		return
	}

	if rf.state != Leader{
		return
	}
	rf.mu.Lock()
	
	args := &InstallSnapShotArgs{
		Term:rf.currentTerm,
		LeaderId:rf.me,
		LastIncludedIndex:rf.LastIncludedIndex,
		LastIncludedTerm:rf.LastIncludedTerm,
		Data:rf.persister.ReadSnapshot(),
	}
	reply := &InstallSnapShotReply{
		Term: -1,
	}
	rf.mu.Unlock()
	ok := false
	done := make(chan bool)
	time_out := make(chan bool)
	go func(){
		ok = rf.sendSnapShot(server, args, reply)
		done <- true
	}()

	go func(){
		time.Sleep(1000 * time.Millisecond)
		time_out<- true			
	}()
	
	select{
		case <- done :
		case <- time_out :
	}
	rf.mu.Lock()
	if ok{

		if args.Term != rf.currentTerm || rf.state != Leader{
			rf.mu.Unlock()
			return
		}


		if reply.Term > rf.currentTerm{
			rf.followerOut = 0
			rf.mu.Unlock()
			rf.BeFollower(reply.Term, NILL)	
			return
		}

		log.Printf("leader %d finish snapshot with rf.matchIndex%d  args.LastIncludedIndex %d  rf.nextIndex %d",rf.me, rf.matchIndex[server],args.LastIncludedIndex,rf.nextIndex[server])
		log.Println(rf.currentTerm,args.Term,ok)
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1

	}
	rf.mu.Unlock()
	log.Printf("leader :%d send snapshot to peer %d end", rf.me,server)
	return
}


func (rf *Raft) SyncAppendEntry(peer int){

	for{
		
		if rf.killed(){
			return
		}
		for{
			rf.mu.Lock()
			if rf.state != Leader{
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[peer] <= rf.LastIncludedIndex{
				rf.mu.Unlock()
				go rf.SnapShotSender(peer)
				log.Printf("leader: %d send snapshot to peer %d with len(log) %d,  snapshot  %d ,  nextindex %d ", rf.me,peer,len(rf.log),rf.LastIncludedIndex,rf.nextIndex[peer])
				time.Sleep(50 * time.Millisecond)
				break
			}

			if rf.nextIndex[peer]>=rf.LastLength() && true{
				args:=&AppendEntriesArgs{
					Term:rf.currentTerm,
					LeaderId:rf.me,
					PrevLogIndex:rf.LastIndex(),
					PrevLogTerm:rf.LastTerm(),
					Entries:make([]Entry,0),
					LeaderCommit:rf.commitIndex,
					IsEmpty:true,
				}
				
				reply:=&AppendEntriesReply{
					Term:0,
					Success:false,
					UpNextIndex:0,
				}
				rf.mu.Unlock()
				append_done := make(chan bool)
				time_out := make(chan bool)
				ok := false
				go func(server int){
					log.Printf("Peer %d(%d) : Be Leader and Sending void entry to %d ,next index %d  len(log) %d", rf.me,rf.currentTerm,server,rf.nextIndex[peer],rf.LastLength())
					ok = rf.sendAppendEntry(server, args, reply)
					append_done <-true
				}(peer)	

				go func(){
					time.Sleep(100 * time.Millisecond)
					time_out<- true			
				}()

				select{
					case <- append_done:
						//log.Println("append_done")
						//log.Printf("nextIndex %d" , rf.nextIndex[peer])
					case <- time_out:
				}
				
				if ok{

					if args.Term != rf.currentTerm{
						break
					}

					rf.mu.Lock()

					if reply.Term<=rf.currentTerm{

						if len(args.Entries) ==0 && rf.nextIndex[peer] >0{
							log.Println(args)
							log.Println(reply)
							rf.nextIndex[peer] = Max(reply.UpNextIndex,rf.matchIndex[peer] + 1)
							rf.matchIndex[peer] = Max(reply.UpNextIndex - 1,rf.matchIndex[peer])
							
						}else{
							log.Println("只是心跳 ")
						}
						rf.mu.Unlock()

					}else{
						rf.mu.Unlock()
						rf.BeFollower(reply.Term, NILL)
						return
					}
				}
				break
			}

			
			var data = make([]Entry,rf.LastLength()-rf.nextIndex[peer])
			if len(data) > 0{
				log.Printf("leader:%d(%d)   len(data):%d  nextIndex:%d   len(log):%d  for %d",rf.me,rf.currentTerm,len(data),rf.nextIndex[peer],rf.LastLength(),peer)
				copy(data,rf.log[rf.ScalerIndexToReal(rf.nextIndex[peer]):rf.RealLength()])
			}

			args:=&AppendEntriesArgs{
				Term:rf.currentTerm,
				LeaderId:rf.me,
				PrevLogIndex:rf.nextIndex[peer] - 1,
				PrevLogTerm:rf.ScalerTermToReal(rf.nextIndex[peer] - 1),
				Entries:data,
				LeaderCommit:rf.commitIndex,
				IsEmpty:false,
			}
			if args.PrevLogIndex<=0 {
				args.PrevLogIndex = 0
			}
			reply:=&AppendEntriesReply{
				Term:0,
				Success:false,
				UpNextIndex:0,
			}
			rf.mu.Unlock()


			append_done := make(chan bool)
			time_out := make(chan bool)
			ok := false
			go func(){
				
				ok = rf.sendAppendEntry(peer, args, reply)
				log.Printf("Peer %d(%d) : Be Leader and Sending not void entry to %d  ", rf.me,rf.currentTerm,peer)
				log.Println(args)
				log.Println(reply,ok)
				append_done<- true			
			}()


			go func(){
				time.Sleep(100 * time.Millisecond)
				time_out<- true			
			}()

			select{
				case <- append_done:
					//log.Println("append_done")
				case <- time_out:
			}

			if ok{

				if args.Term != rf.currentTerm{
					break
				}				

				rf.mu.Lock()

				if reply.Success{
					
				log.Printf("nextIndex  %d, matchIndex  %d, len %d ", rf.nextIndex[peer],rf.matchIndex[peer],len(data) + args.PrevLogIndex + 1)
					rf.nextIndex[peer]  = Max(len(data) + args.PrevLogIndex + 1,rf.nextIndex[peer])
					rf.matchIndex[peer] = Max(rf.nextIndex[peer] - 1,	rf.matchIndex[peer])
					


					log.Printf("leader:%d log success for %d and matchInde has been %d", rf.me,peer,rf.matchIndex[peer])
					rf.mu.Unlock()
					
				}else{
					if reply.Term<=rf.currentTerm{
						if len(args.Entries) !=0 && rf.nextIndex[peer] >0{
							rf.nextIndex[peer] = reply.UpNextIndex
							
						}else{
							///log.Println("只是心跳 ")
						}
						rf.mu.Unlock()
					}else{
						log.Printf("leadder:%d receive higher term %d be follower", rf.me,reply.Term)
						rf.mu.Unlock() 
						rf.BeFollower(reply.Term, NILL)
						return
					}
				}

			}else{
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}


func (rf * Raft) BeFollower(term int,voteFor int){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = voteFor
	rf.candidateOut = 0
	//rf.followerOut = 0
	rf.persist()
}

func (rf * Raft) BeLeader(){
	rf.mu.Lock()
	
	rf.state = Leader
	//rf.votedFor = rf.me
	rf.candidateOut = 0
	rf.followerOut = 0

	for i:=0;i<len(rf.peers);i++{
		rf.nextIndex[i] = rf.LastLength()
		rf.matchIndex[i] = 0
	}
	
	rf.mu.Unlock()
	rf.SendHeartBeat()
	for i:=0;i<len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		go rf.SyncAppendEntry(i)
	}
}


func (rf *Raft) BeCandidate()  { 
	
	for {
		if rf.killed() {
			return
		}

		if rf.state != Candidate{
			
			return
		}


		go func(){

			//log.Printf("Peer %d(%d,%d) : Being Candidate", rf.me,rf.currentTerm,rf.state)
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.state != Candidate{
				rf.mu.Unlock()
				return
			}
		
			rf.currentTerm += 1
			rf.votedFor = rf.me

			rf.persist()


			rf.followerOut = 0
			voteForMe := 1
			totalVote := 1
			Term := rf.currentTerm
			rf.mu.Unlock()
			waitFlag := sync.WaitGroup{}
			//log.Printf("Peer %d(%d,%d) : start RequestVote", rf.me,rf.currentTerm,rf.state)
			//time1 := time.Now()
				//log.Println(time1)
			for i := 0;i<len(rf.peers);i++{

				if rf.votedFor == i{
					continue
				}else{
					args := &RequestVoteArgs{
						Term : rf.currentTerm,
						CandidateId : rf.me,
						LastLogTerm : rf.LastTerm(),
						LastLogIndex :  rf.LastIndex(),
					}
					reply := &RequestVoteReply{
						Term : rf.currentTerm,
						VoteGranted : false,
					}

					waitFlag.Add(1)
					go func(server int){
						//log.Printf("Peer %d(%d,%d) : RequestVote to Peer %d", rf.me,rf.currentTerm,rf.state,server)
						ok := rf.sendRequestVote(server, args, reply)
						
						rf.mu.Lock()
						if ok {
							if reply.VoteGranted{
								voteForMe++
							}else{
								if reply.Term > rf.currentTerm{
									rf.mu.Unlock()
									rf.BeFollower(reply.Term,NILL)
									
									return
								}else if reply.Term == rf.currentTerm{
									// pass
								}else if reply.Term < rf.currentTerm{
									// pass
								}
							}
						}
						totalVote++
						waitFlag.Done()
						rf.mu.Unlock()
					}(i)
					
				}
			}
			time_out := make(chan bool)
			vote_done := make(chan bool)
			vote_succ := make(chan bool)
			// 检查是否超时
			go func(){
				time.Sleep(CTimeOut * 200 *  time.Millisecond)
				time_out <- true
			}()
			// 检查是否所有requestVote都返回
			go func(){
				waitFlag.Wait()
				vote_done <- true	
				
			}()
			//检查是否已经可以成为Leader
			go func(){
				for readtime := 0;readtime < CTimeOut * 5;readtime++{
					time.Sleep(100 * time.Millisecond)
					if voteForMe*2 >= len(rf.peers){
					vote_succ <- true	
					}
				}
			}()

			select{
			case <- time_out:
				//log.Printf("Peer %d(%d,%d) : timeout,totalVote: %d:,voteForMe:%d", rf.me,rf.currentTerm,rf.state,totalVote,voteForMe)

			case <- vote_done:
				//log.Printf("Peer %d(%d,%d)%d : Vote_Done", rf.me,rf.currentTerm,rf.state,voteForMe)

			case <- vote_succ:
				//log.Printf("Peer %d(%d,%d) : Vote_Success", rf.me,rf.currentTerm,rf.state)	

			}

			rf.mu.Lock()
			if rf.state == Follower || Term != rf.currentTerm{
				log.Printf("Peer %d(%d,%d)%d : has been follower", rf.me,rf.currentTerm,rf.state,Term)	
						//time1 := time.Now().Second()
				//log.Println(time1)
				rf.mu.Unlock()
				rf.BeFollower(rf.currentTerm, NILL)
				return
			}
			if voteForMe*2 >= len(rf.peers){
				
				//log.Printf("Peer %d(%d,%d) : BeLeader", rf.me,rf.currentTerm,rf.state)	
				rf.mu.Unlock()
				go rf.BeLeader()
				return
			}else{
				rf.mu.Unlock()

			}
		}()
		randTime := rand.Intn(600)
		time.Sleep(time.Duration(600+randTime) * time.Millisecond)
		
	}
}


func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = NILL
	rf.log = make([]Entry,0)
	rf.log = append(rf.log,Entry{0,-1,true})



	rf.commitIndex = 0
	rf.lastApplied = 1

	rf.nextIndex = make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))
	for i:=0;i<len(rf.peers);i++{
		rf.nextIndex[i] = 0;
		rf.matchIndex[i] = 0;
	}

	rf.state = Follower
	rf.candidateOut = 0
	rf.followerOut = 0
	rf.LastIncludedIndex =  0
	rf.LastIncludedTerm = 0

	rf.applyMessage = applyCh
	rf.isCommit = false
	rf.IsSnapShot = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.LastIncludedIndex != 0{
		rf.lastApplied = rf.LastIncludedIndex + 1
	}

	// start ticker goroutine to start elections
	go rf.Timer()

	go rf.ApplyEntries()

	log.Printf("start a server %d",rf.me)
	return rf
}




func (rf *Raft) AppendNow(command interface{}){
	for i := 0;i<len(rf.peers);i++{
		if i == rf.me{
			continue
		}

		peer := i
		var data = make([]Entry,0)
		data = append(data, Entry{command,rf.currentTerm,false})

		args:=&AppendEntriesArgs{
			Term:rf.currentTerm,
			LeaderId:rf.me,
			PrevLogIndex:rf.LastIndex() - 1,
			PrevLogTerm:rf.ScalerTermToReal(rf.LastIndex() - 1),
			Entries:data,
			LeaderCommit:rf.commitIndex,
			IsEmpty:false,
		}
		reply:=&AppendEntriesReply{
			Term:0,
			Success:false,
			UpNextIndex:0,
		}

		ok := false
		go func(){
			ok = rf.sendAppendEntry(peer, args, reply)
			if ok{
				if args.Term != rf.currentTerm{
					return
				}				
				rf.mu.Lock()
				if reply.Success{
					rf.nextIndex[peer]  = Max(len(data) + args.PrevLogIndex + 1,rf.nextIndex[peer])
					rf.matchIndex[peer] = Max(rf.nextIndex[peer] - 1,	rf.matchIndex[peer])
					//log.Println("now",rf.nextIndex,len(data),peer)
					rf.mu.Unlock()
					
				}else{
					if reply.Term<=rf.currentTerm{
						if len(args.Entries) !=0 && rf.nextIndex[peer] >0{
							rf.nextIndex[peer] = reply.UpNextIndex
						}
						rf.mu.Unlock()
					}else{
						rf.mu.Unlock()
						rf.BeFollower(reply.Term, NILL)
						return
					}
				}
			}
		}()
	}
}

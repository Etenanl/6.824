package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
	"log"
	"sort"
	"fmt"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	Time_Out = iota
	Vote_Done
	Vote_Success
)
const (
	CTimeOut = 3
	FTimeOut = 4
)



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

type Entry struct {
	Command	interface{}
	Term	int
	Index	int
	IsEmpty	bool
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

	appliedLog	[]Entry


	// 目前不知道什么用
	applyMessage	chan ApplyMsg


}

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
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.   
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


}


func MMM(){
	fmt.Println("format, a")
}


func (rf *Raft)LastTerm() int{
	lastTerm := 0
	if  len(rf.log) > 0{
		lastTerm = rf.log[len(rf.log)-1].Term
	}else{
		lastTerm = rf.currentTerm
	}	
	return lastTerm
}



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	///log.Printf("Peer %d(%d,%d) : receive RequestVote", rf.me,rf.currentTerm,rf.state)

	
	rf.mu.Lock()


	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 如果收到的term<=自己的term，说明对方开始竞选前的term小于自己
	if args.Term < rf.currentTerm{
		rf.mu.Unlock()
		return 
	}else if args.Term >= rf.currentTerm{

		if args.Term > rf.currentTerm {
			rf.mu.Unlock()
			rf.BeFollower(args.Term, NILL)
			rf.mu.Lock()
		}


		if (rf.votedFor == NILL || args.CandidateId == rf.votedFor) && 
		(args.LastLogTerm>rf.LastTerm()|| 
		(args.LastLogTerm==rf.LastTerm()&& args.LastLogIndex>=len(rf.log)) ){
			// 如果候选者的log比receiver新
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.followerOut = 0
			rf.mu.Unlock()
			rf.BeFollower(args.Term, args.CandidateId)
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
	reply.UpNextIndex = 1
	rf.mu.Lock()
	
	if  args.Term > rf.currentTerm{
		rf.followerOut = 0
		rf.mu.Unlock()
		rf.BeFollower(args.Term, args.LeaderId)

		return
	}else if args.Term == rf.currentTerm{

		
		rf.followerOut = 0
		rf.mu.Unlock()
		rf.BeFollower(args.Term, args.LeaderId)
		rf.mu.Lock()
		// 心跳，直接返回
		if len(args.Entries) == 0{

			if rf.LastTerm() == args.Term{
				if args.LeaderCommit > rf.commitIndex{
					rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
				}
			}
			reply.UpNextIndex = rf.lastApplied
			rf.mu.Unlock()
			return
		}
		if len(rf.log) - 1 < args.PrevLogIndex {
			reply.UpNextIndex = rf.lastApplied
			rf.mu.Unlock()
			return
		}

		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.UpNextIndex = rf.lastApplied
			rf.mu.Unlock()
			return
		}

		if args.PrevLogIndex < rf.lastApplied - 1{
			reply.UpNextIndex = rf.lastApplied
			rf.mu.Unlock()
			return
		}



		rf.log = rf.log[:args.PrevLogIndex + 1]
		rf.log = append(rf.log, args.Entries...)

		if args.LeaderCommit > rf.commitIndex{
			rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		}

		reply.UpNextIndex = len(rf.log)
		reply.Success = true
		rf.mu.Unlock()
		return

	}else{
		reply.Success = false
		rf.mu.Unlock()
		return
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)

	return ok
}



//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
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
	}


	rf.log = append(rf.log, logEntry)
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.persist()

	return index,term,isLeader

}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//



func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The Timer go routine starts a new election if this peer hasn't received
// heartsbeats recently.
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
		time.Sleep(30 * time.Millisecond)
		if rf.killed(){
			return
		}
		

		if rf.state == Leader{
			rf.mu.Lock()
			middleMatch :=make([]int,len(rf.matchIndex))
			copy(middleMatch,rf.matchIndex)
			middleMatch[rf.me] = len(rf.log)-1
			sort.Ints(middleMatch)
			midIndex := len(middleMatch)/2
			if middleMatch[midIndex] == -1 || middleMatch[midIndex] == rf.commitIndex{
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			if len(rf.log) > 0{
				if rf.log[middleMatch[midIndex]].Term == rf.currentTerm{
					rf.commitIndex = Max(rf.commitIndex,middleMatch[midIndex])
					log.Printf("leader:%d update commit: %d", rf.me,rf.commitIndex)
					
					rf.SendHeartBeat()

				}			
			}
			
		}
		rf.mu.Lock()
		for rf.lastApplied <= rf.commitIndex{

			if rf.log[rf.lastApplied].IsEmpty == true{
				rf.lastApplied++
				continue
			}else{
				message := ApplyMsg{true,rf.log[rf.lastApplied].Command,rf.lastApplied,false,[]byte{'a'},0,0}
				rf.applyMessage <- message
				rf.appliedLog = append(rf.appliedLog,rf.log[rf.lastApplied])

			}
			rf.lastApplied++


		}
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
			PrevLogIndex:len(rf.log),
			PrevLogTerm:rf.LastTerm(),
			Entries:make([]Entry,0),
			LeaderCommit:rf.commitIndex,
		}

		reply:=&AppendEntriesReply{
			Term:0,
			Success:false,
			UpNextIndex:0,
		}
		peer := i
		go func(server int){
			rf.sendAppendEntry(server, args, reply)
		}(peer)	
	}
}


func (rf *Raft) SyncAppendEntry(peer int){

	for{
		
		if rf.killed(){
			return
		}
		//rf.nextIndex[peer] = len(rf.log) - 1
		for{
			rf.mu.Lock()
			if rf.state != Leader{
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[peer]>=len(rf.log) && true{

				args:=&AppendEntriesArgs{
					Term:rf.currentTerm,
					LeaderId:rf.me,
					PrevLogIndex:len(rf.log),
					PrevLogTerm:rf.LastTerm(),
					Entries:make([]Entry,0),
					LeaderCommit:rf.commitIndex,
				}
				
				reply:=&AppendEntriesReply{
					Term:0,
					Success:false,
					UpNextIndex:0,
				}
				rf.mu.Unlock()
				append_done := make(chan bool)
				ok := false
				go func(server int){
					ok = rf.sendAppendEntry(server, args, reply)
					append_done <-true
				}(peer)	

				select{
					case <- append_done:
				}
				
				if ok{

				rf.mu.Lock()

					if reply.Term<=rf.currentTerm{

						if len(args.Entries) ==0 && rf.nextIndex[peer] >0{
							if reply.UpNextIndex == 0{
							}
							rf.nextIndex[peer] = Max(reply.UpNextIndex,rf.matchIndex[peer] + 1)
							
						}else{
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

			
			var data = make([]Entry,len(rf.log)-rf.nextIndex[peer])

			if len(data) > 0{
				copy(data,rf.log[rf.nextIndex[peer]:len(rf.log)])
			
			}else{
			}
			if rf.nextIndex[peer] - 1 < 0{
			}

			args:=&AppendEntriesArgs{
				Term:rf.currentTerm,
				LeaderId:rf.me,
				PrevLogIndex:rf.nextIndex[peer] - 1,
				PrevLogTerm:rf.log[rf.nextIndex[peer] - 1].Term,
				Entries:data,
				LeaderCommit:rf.commitIndex,
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

			time_out := make(chan bool)
			append_done := make(chan bool)
			ok := false
			go func(){
				ok = rf.sendAppendEntry(peer, args, reply)
				append_done<- true
			}()


			select{
				case <- time_out:
					//log.Println("timeout")
				case <- append_done:
					//log.Println("append_done")
			}

			if ok{

				rf.mu.Lock()

				if reply.Success{
					rf.nextIndex[peer] += len(data)
					rf.matchIndex[peer] = Max(rf.matchIndex[peer],rf.nextIndex[peer]-1)
					rf.mu.Unlock()
					
				}else{

					if reply.Term<=rf.currentTerm{

						if len(args.Entries) !=0 && rf.nextIndex[peer] >0{
							if reply.UpNextIndex == 0{
							}
							rf.nextIndex[peer] = reply.UpNextIndex
							
						}else{
							///fmt.Println("只是心跳 ")
						}
						rf.mu.Unlock()

					}else{
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

}



func (rf * Raft) BeLeader(){
	rf.mu.Lock()
	
	rf.state = Leader
	rf.votedFor = rf.me
	rf.candidateOut = 0
	rf.followerOut = 0

	for i:=0;i<len(rf.peers);i++{
		rf.nextIndex[i] = len(rf.log)
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
		rf.mu.Lock()
		if rf.state != Candidate{
			rf.mu.Unlock()
			return
		}
	
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.followerOut = 0
		voteForMe := 1
		totalVote := 1
		Term := rf.currentTerm
		rf.mu.Unlock()
		waitFlag := sync.WaitGroup{}
		for i := 0;i<len(rf.peers);i++{

			if rf.votedFor == i{
				continue
			}else{
				args := &RequestVoteArgs{
					Term : rf.currentTerm,
					CandidateId : rf.me,
					LastLogTerm : rf.LastTerm(),
					LastLogIndex :  len(rf.log),
				}
				reply := &RequestVoteReply{
					Term : rf.currentTerm,
					VoteGranted : false,
				}

				waitFlag.Add(1)
				go func(server int){
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
			time.Sleep(CTimeOut * 100 *  time.Millisecond)
			time_out <- true
		}()
		// 检查是否所有requestVote都返回
		go func(){
			waitFlag.Wait()
			vote_done <- true	
			
		}()
		// 检查是否已经可以成为Leader
		go func(){
			for readtime := 0;readtime < CTimeOut;readtime++{
				time.Sleep(100 * time.Millisecond)
				if voteForMe*2 >= len(rf.peers){
				vote_succ <- true	
				}
			}
		}()

		select{
		case <- time_out:

		case <- vote_done:

		case <- vote_succ:

		}

		rf.mu.Lock()
		if rf.state == Follower || Term != rf.currentTerm{
			rf.mu.Unlock()
			rf.BeFollower(rf.currentTerm, NILL)
			return
		}
		if voteForMe*2 >= len(rf.peers){
			
			rf.mu.Unlock()
			go rf.BeLeader()
			return
		}else{
			rf.mu.Unlock()

		}
	time.Sleep(250 * time.Millisecond)	
	}
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
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
	rf.log = append(rf.log,Entry{0,-1,0,true})

	rf.appliedLog = make([]Entry,0)

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


	rf.applyMessage = applyCh


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.Timer()

	go rf.ApplyEntries()

	return rf
}

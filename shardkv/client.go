package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	me 		int64
	leader	int
	OPID	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.me = nrand()
	ck.leader = 0
	ck.OPID = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num : num,
		ClientID : ck.me,
		OPID : ck.OPID,
	}
	ck.OPID++
	serverID := ck.leader
	// Your code here.

	for i := 0 ;i< ReTry;{
		reply := &QueryReply{}
		// try each known server.
		ok := ck.servers[serverID].Call("ShardCtrler.Query", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown{
			if !ok{
				i++
			}else{
				i=0
			}
			serverID = (serverID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection{
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = serverID
		return	reply.Config
	}
	return  Config{Num: -1}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers : servers,
		ClientID : ck.me,
		OPID : ck.OPID,
	}

	ck.OPID++
	serverID := ck.leader
	// Your code here.

	for {
		reply := &JoinReply{}
		// try each known server.
		ok := ck.servers[serverID].Call("ShardCtrler.Join", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown{
			serverID = (serverID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection{
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = serverID
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs : gids,
		ClientID : ck.me,
		OPID : ck.OPID,
	}

	ck.OPID++
	serverID := ck.leader
	// Your code here.

	for {
		reply := &LeaveReply{}
		// try each known server.
		ok := ck.servers[serverID].Call("ShardCtrler.Leave", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown{
			serverID = (serverID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection{
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = serverID
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard : shard,
		GID : gid,
		ClientID : ck.me,
		OPID : ck.OPID,
	}

	ck.OPID++
	serverID := ck.leader
	// Your code here.

	for {
		reply := &MoveReply{}
		// try each known server.
		ok := ck.servers[serverID].Call("ShardCtrler.Move", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown{
			serverID = (serverID + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection{
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = serverID
		return
	}
}

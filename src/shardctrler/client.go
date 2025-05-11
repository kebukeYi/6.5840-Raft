package shardctrler

//
// Shardctrler clerk.
//

import "raftCourse/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	clientId int64
	seqId    int64
	// Your data here.
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
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.Servers = servers
	for {
		// try each known server.
		var joinReply JoinReply
		// try each known server.
		server := ck.servers[ck.leaderId]
		ok := server.Call("ShardCtrler.Join", args, &joinReply)
		if !ok || joinReply.Err == ErrWrongLeader || joinReply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	for {
		var moveReply MoveReply
		// try each known server.
		end := ck.servers[ck.leaderId]
		ok := end.Call("ShardCtrler.Move", args, &moveReply)
		if !ok || moveReply.Err == ErrWrongLeader || moveReply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{ClientId: ck.clientId, SeqId: ck.seqId}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		var leaveReply LeaveReply
		// try each known server.
		server := ck.servers[ck.leaderId]
		ok := server.Call("ShardCtrler.Leave", args, &leaveReply)
		if !ok || leaveReply.Err == ErrWrongLeader || leaveReply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.seqId++
		return
	}
}

func (ck *Clerk) Query(configId int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ConfigId = configId
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		return reply.Config
	}
}

package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "raftCourse/labrpc"
import "crypto/rand"
import "math/big"
import "raftCourse/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk             // 获取元数据的客户端,单例
	config   shardctrler.Config             // 获得的元数据,多例
	make_end func(string) *labrpc.ClientEnd // 创建网络连接接口,单例
	// You will have to modify this struct.
	leaderIds map[int]int // <GroupId, leaderId>
	clientId  int64
	seqId     int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.leaderIds = make(map[int]int)
	// ck.config = shardctrler.DefaultConfig()
	ck.clientId = nrand()
	ck.seqId = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	for {
		shard := key2shard(key)        // 根据key 获得hash
		gid := ck.config.Shards[shard] // 根据 hash 获得 group
		if servers, ok := ck.config.Groups[gid]; ok {
			// 尝试获得指定组的 leader;
			if _, ok := ck.leaderIds[gid]; !ok {
				// 没有相关组的leader, 那就从0开始探索查找;
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			// try each server for the shard.
			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				// 找错组了,可能正在组数据迁移,跳出循环,重新获得最新配置;
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if oldLeaderId == ck.leaderIds[gid] {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// PutAppend shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}
	args.Key = key
	args.Value = value
	args.Op = op
	shard := key2shard(key)
	for {
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			if _, ok := ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			for {
				srv := ck.make_end(servers[ck.leaderIds[gid]])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					ck.seqId++
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					if oldLeaderId == ck.leaderIds[gid] {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

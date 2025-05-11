package shardkv

import (
	"bytes"
	"fmt"
	"raftCourse/labrpc"
	"raftCourse/shardctrler"
	"sync/atomic"
	"time"
)
import "raftCourse/raft"
import "sync"
import "raftCourse/labgob"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // 当前server所属的组;
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead           int32 // set by Kill()
	lastApplyIndex int
	shards         map[int]*MemoryKVStateMachine
	notifyCh       map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo // <clientId, {SeqId}>
	currentConfig  shardctrler.Config          // 日志,多例
	prevConfig     shardctrler.Config
	ctrlerPeers    []*labrpc.ClientEnd // 元数据中心对端 节点数组
	mck            *shardctrler.Clerk  // 元数据中心的请求端
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 调用 raft, 将请求存储到 raft 日志中并进行同步;内部有锁
	logIndex, _, isLeader := kv.rf.Start(RaftLogCommand{
		OpType: ClientOperation,
		Data:   OP{Key: args.Key, OpType: OpGet},
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyChannel := kv.getNotifyChannel(logIndex)
	kv.mu.Unlock()

	select {
	case result := <-notifyChannel:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.cleanNotifyChannel(logIndex)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) matchGroup(key string) bool {
	shard := key2shard(key)
	status := kv.shards[shard].Status
	return kv.currentConfig.Shards[shard] == kv.gid &&
		(status == GC || status == Normal)
}
func (s *ShardKV) getNotifyChannel(logIndex int) chan *OpReply {
	if _, ok := s.notifyCh[logIndex]; !ok {
		s.notifyCh[logIndex] = make(chan *OpReply, 1)
	}
	return s.notifyCh[logIndex]
}
func (kv *ShardKV) cleanNotifyChannel(logIndex int) {
	delete(kv.notifyCh, logIndex)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	// 判断请求 key 是否所属当前 Group
	if !kv.matchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.isDuplicateRequest(args.ClientId, args.SeqId) {
		operationInfo := kv.duplicateTable[args.ClientId]
		reply.Err = operationInfo.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	logIndex, _, isLeader := kv.rf.Start(RaftLogCommand{
		OpType: ClientOperation,
		Data: OP{
			ClientId: args.ClientId,
			Key:      args.Key,
			OpType:   getOpType(args.Op),
			SeqId:    args.SeqId,
			Val:      args.Value}},
	)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyChannel := kv.getNotifyChannel(logIndex)
	kv.mu.Unlock()

	select {
	case result := <-notifyChannel:
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.cleanNotifyChannel(logIndex)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, seqId int64) bool {
	if _, ok := kv.duplicateTable[clientId]; ok {
		if kv.duplicateTable[clientId].SeqId >= seqId {
			return true
		}
	}
	return false
}

func (kv *ShardKV) applyStateMachine(op OP) *OpReply {
	var value string
	var err Err
	shard := key2shard(op.Key)
	switch op.OpType {
	case OpGet:
		value, err = kv.shards[shard].Get(op.Key)
	case OpPut:
		err = kv.shards[shard].Put(op.Key, op.Val)
	case OpAppend:
		err = kv.shards[shard].Append(op.Key, op.Val)
	}
	return &OpReply{Value: value, Err: err}
}

// StartServer servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OP{})
	labgob.Register(RaftLogCommand{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperation{})
	labgob.Register(ShardOperationReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlerPeers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlerPeers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplyIndex = 0
	kv.shards = make(map[int]*MemoryKVStateMachine)
	kv.notifyCh = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.prevConfig = shardctrler.DefaultConfig()

	kv.recoveryFromSnapShot(persister.ReadSnapshot())

	// 1,应用,用户的操作;
	// 2,应用,系统内部 shard 涉及的操作;
	go kv.applyTicker()

	// 后台定时获得最新配置,更改 shard 状态即可; 只有 shard_leader 可以执行;
	go kv.fetchConfigTask()

	// 根据上面的数据状态变更, 进行数据获取; 只有 shard_leader 可以执行;
	go kv.shardMigrationTask()

	// 数据获取完毕后, 数据需要清除; 只有 shard_leader 可以执行;
	go kv.shardGCTask()

	return kv
}
func (kv *ShardKV) makeSnapShot(index int) {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(kv.shards)
	if err != nil {
		fmt.Printf("encode kv.shards err:%s", err)
		return
	}
	err = encoder.Encode(kv.duplicateTable)
	if err != nil {
		fmt.Printf("encode duplicateTable err:%s", err)
		return
	}
	err = encoder.Encode(kv.currentConfig)
	if err != nil {
		fmt.Printf("encode currentConfig err:%s", err)
		return
	}
	err = encoder.Encode(kv.prevConfig)
	if err != nil {
		fmt.Printf("encode prevConfig err:%s", err)
		return
	}
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) recoveryFromSnapShot(snapShot []byte) {
	if len(snapShot) == 0 {
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = NewMemoryKVStateMachine()
			}
		}
		return
	}

	buffer := bytes.NewBuffer(snapShot)
	dec := labgob.NewDecoder(buffer)

	var stateMachine map[int]*MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	var currentConfig shardctrler.Config
	var prevConfig shardctrler.Config

	if dec.Decode(&stateMachine) != nil ||
		dec.Decode(&dupTable) != nil ||
		dec.Decode(&currentConfig) != nil ||
		dec.Decode(&prevConfig) != nil {
		panic("failed to restore state from snapshpt")
	}

	kv.shards = stateMachine
	kv.duplicateTable = dupTable
	kv.currentConfig = currentConfig
	kv.prevConfig = prevConfig
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

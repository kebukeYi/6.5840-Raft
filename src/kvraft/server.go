package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"raftCourse/labgob"
	"raftCourse/labrpc"
	"raftCourse/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.

	stateMachine   *MemoryKVStateMachine
	notifyCh       map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo // <clientId, {SeqId}>
	lastApplyIndex int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	logIndex, _, isLeader := kv.rf.Start(OP{
		Key:    args.Key,
		OpType: OpGet,
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
		reply.Err = ErrTimeOut
	}

	go func() {
		kv.mu.Lock()
		kv.cleanNotifyChannel(logIndex)
		kv.mu.Unlock()
	}()
}
func (s *KVServer) getNotifyChannel(logIndex int) chan *OpReply {
	if _, ok := s.notifyCh[logIndex]; !ok {
		s.notifyCh[logIndex] = make(chan *OpReply, 1)
	}
	return s.notifyCh[logIndex]
}
func (kv *KVServer) cleanNotifyChannel(logIndex int) {
	delete(kv.notifyCh, logIndex)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.isDuplicateRequest(args.ClientId, args.SeqId) {
		operationInfo := kv.duplicateTable[args.ClientId]
		reply.Value = operationInfo.Reply.Value
		reply.Err = operationInfo.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	logIndex, _, isLeader := kv.rf.Start(OP{
		ClientId: args.ClientId,
		Key:      args.Key,
		OpType:   getOpType(args.Op),
		SeqId:    args.SeqId,
		Val:      args.Value,
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
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeOut
	}

	go func() {
		kv.mu.Lock()
		kv.cleanNotifyChannel(logIndex)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicateRequest(clientId int64, seqId int64) bool {
	if _, ok := kv.duplicateTable[clientId]; ok {
		if kv.duplicateTable[clientId].SeqId >= seqId {
			return true
		}
	}
	return false
}

func (kv *KVServer) applyTicker() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if kv.lastApplyIndex >= msg.CommandIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplyIndex = msg.CommandIndex

				op := msg.Command.(OP)
				reply := &OpReply{}
				if op.OpType != OpGet && kv.isDuplicateRequest(op.ClientId, op.SeqId) {
					reply = kv.duplicateTable[op.ClientId].Reply
				} else {
					reply = kv.applyStateMachine(&op)
					if op.OpType != OpGet {
						kv.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: reply,
						}
					}
				}

				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyChannel := kv.getNotifyChannel(msg.CommandIndex)
					notifyChannel <- reply
				}

				if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
					kv.makeSnapShot(msg.CommandIndex)
				}

				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				kv.recoveryFromSnapShot(msg.Snapshot)
				kv.lastApplyIndex = msg.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) applyStateMachine(op *OP) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.stateMachine.Get(op.Key)
	case OpPut:
		err = kv.stateMachine.Put(op.Key, op.Val)
	case OpAppend:
		err = kv.stateMachine.Append(op.Key, op.Val)
	}
	return &OpReply{Value: value, Err: err}
}

func (kv *KVServer) makeSnapShot(index int) {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	err := encoder.Encode(kv.stateMachine)
	if err != nil {
		fmt.Printf("encode stateMachine err:%s", err)
		return
	}
	err = encoder.Encode(kv.duplicateTable)
	if err != nil {
		fmt.Printf("encode duplicateTable err:%s", err)
		return
	}
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *KVServer) recoveryFromSnapShot(snapShot []byte) {
	if len(snapShot) == 0 {
		return
	}
	buffer := bytes.NewBuffer(snapShot)
	decoder := labgob.NewDecoder(buffer)
	var stateMachine MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	if decoder.Decode(&stateMachine) != nil || decoder.Decode(&dupTable) != nil {
		panic("failed to restore state from snapshot")
	}

	kv.stateMachine = &stateMachine
	kv.duplicateTable = dupTable
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OP{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.dead = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.stateMachine = NewMemoryKVStateMachine()
	kv.notifyCh = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)
	kv.lastApplyIndex = 0

	kv.recoveryFromSnapShot(persister.ReadSnapshot())

	go kv.applyTicker()

	return kv
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

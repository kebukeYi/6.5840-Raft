package shardctrler

import (
	"raftCourse/raft"
	"sync/atomic"
	"time"
)
import "raftCourse/labrpc"
import "sync"
import "raftCourse/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	//configs []Config // indexed by config num
	configs        map[int]Config // indexed by config num
	stateMachine   *CtrlerStateMachine
	dead           int32
	lastApplied    int
	notifyChannel  map[int]chan *OpReply       // <log_index, reply>
	duplicateTable map[int64]LastOperationInfo // < clientId, op>
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType:   OpJoin,
		Servers:  args.Servers,
		ClientID: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType:    OpLeave,
		GroupsIds: args.GIDs,
		ClientID:  args.ClientId,
		SeqId:     args.SeqId,
	}, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType:   OpMove,
		Shard:    args.Shard,
		GroupsId: args.GID,
		ClientID: args.ClientId,
		SeqId:    args.SeqId,
	}, &opReply)
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var opReply OpReply
	sc.command(Op{
		OpType:   OpQuery,
		ConfigId: args.ConfigId,
	}, &opReply)
	reply.Config = opReply.Result
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) command(args Op, reply *OpReply) {
	sc.mu.Lock()
	if args.OpType != OpQuery && sc.requestDuplicate(args.ClientID, args.SeqId) {
		info := sc.duplicateTable[args.ClientID].Reply
		reply.Err = info.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	logIndex, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	notifyChannel := sc.getNotifyChannel(logIndex)
	sc.mu.Unlock()

	select {
	case result := <-notifyChannel:
		reply.Result = result.Result
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.cleanNotifyChannel(logIndex)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) getNotifyChannel(logIndex int) chan *OpReply {
	if _, ok := sc.notifyChannel[logIndex]; !ok {
		sc.notifyChannel[logIndex] = make(chan *OpReply, 1)
	}
	return sc.notifyChannel[logIndex]
}

func (sc *ShardCtrler) cleanNotifyChannel(index int) {
	delete(sc.notifyChannel, index)
}

func (sc *ShardCtrler) requestDuplicate(clientId, seqId int64) bool {
	info, ok := sc.duplicateTable[clientId]
	return ok && info.SeqId >= seqId
}

func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				var opReply *OpReply
				if op.OpType != OpQuery && sc.requestDuplicate(op.ClientID, op.SeqId) {
					opReply = sc.duplicateTable[op.ClientID].Reply
				} else {
					opReply = sc.applyToStateMachine(op)
					if op.OpType != OpQuery {
						sc.duplicateTable[op.ClientID] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				if _, isLeader := sc.rf.GetState(); isLeader {
					notifyChannel := sc.getNotifyChannel(msg.CommandIndex)
					notifyChannel <- opReply
				}
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyToStateMachine(args Op) *OpReply {
	var err Err
	var cfg Config
	switch args.OpType {
	case OpQuery:
		cfg, err = sc.stateMachine.Query(args.ConfigId)
	case OpMove:
		err = sc.stateMachine.Move(args.Shard, args.GroupsId)
	case OpJoin:
		err = sc.stateMachine.Join(args.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(args.GroupsIds)
	}
	return &OpReply{Result: cfg, Err: err}
}

// Kill the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make(map[int]Config, 1)
	c := sc.configs[0]
	c.Groups = make(map[int][]string)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.lastApplied = 0
	sc.notifyChannel = make(map[int]chan *OpReply)
	sc.stateMachine = NewCtrlerStateMachine()
	sc.duplicateTable = make(map[int64]LastOperationInfo)

	go sc.applyTask()
	return sc
}

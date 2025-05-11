package shardkv

import (
	"fmt"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrWrongConfig = "ErrWrongConfig"
	ErrNotReady    = "ErrNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

const ClientRequestTimeout = 500 * time.Millisecond
const FetchTimeoutInterval = 100 * time.Millisecond
const ShardMigrationInterval = 50 * time.Millisecond
const ShardGCInterval = 50 * time.Millisecond

type OperationType uint8

const (
	OpGet OperationType = iota
	OpPut
	OpAppend
)

func getOpType(op string) OperationType {
	switch op {
	case "Put":
		return OpPut
	case "Append":
		return OpAppend
	default:
		panic(fmt.Sprintf("unknown operation type %s", op))
	}
}

type OP struct {
	Key      string
	Val      string
	ClientId int64
	SeqId    int64
	OpType   OperationType
}

type OpReply struct {
	Err   Err
	Value string
}

type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}

func (op *LastOperationInfo) copyData() LastOperationInfo {
	return LastOperationInfo{
		SeqId: op.SeqId,
		Reply: &OpReply{
			Err:   op.Reply.Err,
			Value: op.Reply.Value,
		},
	}
}

// RaftLogCommandOpType 保留在Raft日志中的 日志具体类型
type RaftLogCommandOpType uint8

const (
	ClientOperation RaftLogCommandOpType = iota
	ConfigChange                         // 系统内部操作
	ShardMigration                       // 系统内部操作
	ShardGC
)

type RaftLogCommand struct {
	OpType RaftLogCommandOpType
	Data   interface{}
}

type ShardState uint8

const (
	Normal ShardState = iota
	MoveIn
	MoveOut
	GC
)

type ShardOperation struct {
	ConfigId int
	Shards   []int
}

type ShardOperationReply struct {
	Err            Err
	ConfigId       int
	ShardData      map[int]map[string]string
	DuplicateTable map[int64]LastOperationInfo
}

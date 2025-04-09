package kvraft

import (
	"fmt"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
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
	Value string
	Err   Err
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

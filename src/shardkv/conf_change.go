package shardkv

import (
	"raftCourse/shardctrler"
	"time"
)

func (kv *ShardKV) handleConfigCommand(command RaftLogCommand, reply *OpReply) {
	// 调用 raft, 将请求 存储到 raft 日志中并进行同步
	index, _, isLeader := kv.rf.Start(command)

	// 如果当前节点不是 Leader 的话, 直接返回错误
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待结果
	kv.mu.Lock()
	notifyCh := kv.getNotifyChannel(index)
	kv.mu.Unlock()

	select {
	case result := <-notifyCh:
		reply.Value = result.Value
		reply.Err = result.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.cleanNotifyChannel(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) handleConfigChangeMessage(raftCommand RaftLogCommand) *OpReply {
	switch raftCommand.OpType {
	case ConfigChange:
		newConfig := raftCommand.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	case ShardMigration:
		shardData := raftCommand.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
	case ShardGC:
		operation := raftCommand.Data.(ShardOperation)
		return kv.applyShardGC(&operation)
	default:
		panic("unknown config change type")
	}
	return nil
}

// 配置变更, 将涉及变更的shard 更改状态, 等待后台线程进行 shardMigrationTask() 数据判断和迁移 shardMigration;
func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	if kv.currentConfig.ConfigId+1 == newConfig.ConfigId {
		// 判断分片 有没有 发生变化
		for i := 0; i < shardctrler.NShards; i++ {
			// 1 2 3 4 : 1
			// 5 6 7 8 : 2
			// 2 3 4 : 1
			// 1 6 7 8 9 :2
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				if kv.currentConfig.Shards[i] != 0 {
					kv.shards[i].Status = MoveIn
				}
			}
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				if newConfig.Shards[i] != 0 {
					kv.shards[i].Status = MoveOut
				}
			}
		}
		kv.prevConfig = kv.currentConfig
		kv.currentConfig = newConfig
		return &OpReply{Err: OK}
	}
	return &OpReply{Err: ErrWrongConfig}
}

// 对面传输过来的数据,需要本次存储起来;
func (kv *ShardKV) applyShardMigration(shardDataReply *ShardOperationReply) *OpReply {
	if kv.currentConfig.ConfigId == shardDataReply.ConfigId {
		for shard, shardData := range shardDataReply.ShardData {
			state := kv.shards[shard]
			if state.Status == MoveIn {
				for k, v := range shardData {
					state.Put(k, v)
				}
				// 状态置为 GC, 说明本地传输完毕, 源头随后,可以删除了;
				state.Status = GC
			} else {
				break
				//continue
			}
		}

		for clientId, operationInfo := range shardDataReply.DuplicateTable {
			info, ok := kv.duplicateTable[clientId]
			if !ok || info.SeqId < operationInfo.SeqId {
				kv.duplicateTable[clientId] = operationInfo
			}
		}
	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardGC(shardsInfo *ShardOperation) *OpReply {
	if kv.currentConfig.ConfigId == shardsInfo.ConfigId {
		for _, shardId := range shardsInfo.Shards {
			shard := kv.shards[shardId]
			if shard.Status == GC {
				shard.Status = Normal
			} else if shard.Status == MoveOut {
				kv.shards[shardId] = NewMemoryKVStateMachine()
			} else {
				//continue
				break
			}
		}
	}
	return &OpReply{Err: OK}
}

package shardkv

import (
	"sync"
	"time"
)

func (kv *ShardKV) applyTicker() {
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

				var opReply *OpReply
				raftCommand := msg.Command.(RaftLogCommand)
				if raftCommand.OpType == ClientOperation {
					opReply = kv.handleClientOperation(raftCommand)
				} else {
					opReply = kv.handleConfigChangeMessage(raftCommand)
				}

				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyChannel := kv.getNotifyChannel(msg.CommandIndex)
					notifyChannel <- opReply
				}

				// 判断是否需要 snapshot
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

func (kv *ShardKV) handleClientOperation(raftCommand RaftLogCommand) *OpReply {
	op := raftCommand.Data.(OP)
	if kv.matchGroup(op.Key) {
		var reply *OpReply
		if op.OpType != OpGet && kv.isDuplicateRequest(op.ClientId, op.SeqId) {
			reply = kv.duplicateTable[op.ClientId].Reply
		} else {
			reply = kv.applyStateMachine(op)
			if op.OpType != OpGet {
				kv.duplicateTable[op.ClientId] = LastOperationInfo{
					SeqId: op.SeqId,
					Reply: reply,
				}
			}
		}
		return reply
	}
	return &OpReply{Err: ErrWrongGroup}
}

func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			needFetch := true
			for _, machine := range kv.shards {
				if machine.Status != Normal {
					needFetch = false
					break
				}
			}
			kv.mu.Unlock()
			// 当前 Leader 下的shard 都处于 正常状态,可以进行获取配置信息;
			if needFetch {
				currentConfigId := kv.currentConfig.ConfigId
				newConfig := kv.mck.Query(currentConfigId + 1)
				// 成功获得配置信息, 传入 raft 模块进行同步, 等待 Leader follower 的消费
				if newConfig.ConfigId == currentConfigId+1 {
					kv.handleConfigCommand(RaftLogCommand{
						OpType: ConfigChange,
						Data:   newConfig,
					}, &OpReply{})
				}
			}
		}
		time.Sleep(FetchTimeoutInterval)
	}
}

// 后台定时根据改变的shard状态,去搜寻数据;
func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 获得将要 迁移的 shard 所属的组, 再根据组获得 具体ip;
			gidToShards := kv.getShardByStatus(MoveIn)
			var wg sync.WaitGroup
			// <g1,[1,2,3]>  <g2, [4]> <g3,[5,6]>
			// <g1,[ip1,ip2,ip3]>  <g2,[ip1,ip2,ip3]> <g3,[ip1,ip2,ip3]>
			for gid, shards := range gidToShards {
				wg.Add(1)
				go func(servers []string, configId int, shards []int) {
					defer wg.Done()
					// 遍历该 Group 中每一个节点(不知道哪一个是leader), 然后从 Leader 返回读取到对应的 shard 数据;
					getShardArgs := ShardOperation{configId, shards}
					for _, server := range servers {
						var reply ShardOperationReply
						ok := kv.make_end(server).Call("ShardKV.GetShardsData", &getShardArgs, &reply)
						if ok && reply.Err == OK {
							kv.handleConfigCommand(RaftLogCommand{
								OpType: ShardMigration,
								Data:   reply}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.ConfigId, shards)
			}
			// 没有释放锁
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(ShardMigrationInterval)
	}
}

// 不是本地删除, 是要向源节点, 发送RPC消息(GC消息), 让其删除;
func (kv *ShardKV) shardGCTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// 只有刚 MoveIn 的 被设置成 GC, GC的含义是向之前的节点发送 删除数据请求;
			gidToShards := kv.getShardByStatus(GC)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					wg.Done()
					shardGCArgs := ShardOperation{configNum, shardIds}
					for _, server := range servers {
						var shardGCReply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.DeleteShardsData", &shardGCArgs, &shardGCReply)
						if ok && shardGCReply.Err == OK {
							// 发送完毕后, 我也要把本地处于 GC 的状态, 改为 Normal;
							kv.handleConfigCommand(RaftLogCommand{
								ShardGC,
								shardGCArgs},
								&OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.ConfigId, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}
		time.Sleep(ShardGCInterval)
	}
}

// 得到 某个状态的 shard 以及 所属 gid;
func (kv *ShardKV) getShardByStatus(status ShardState) map[int][]int {
	gidToShards := make(map[int][]int)
	for shard, machine := range kv.shards {
		if machine.Status == status {
			gid := kv.prevConfig.Shards[shard]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], shard)
			}
		}
	}
	return gidToShards
}

// GetShardsData for RPC
func (kv *ShardKV) GetShardsData(args *ShardOperation, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.currentConfig.ConfigId < args.ConfigId {
		reply.Err = ErrNotReady
		return
	}

	// 拷贝 shard 数据
	reply.ShardData = make(map[int]map[string]string)
	for _, shard := range args.Shards {
		reply.ShardData[shard] = kv.shards[shard].copyData()
	}

	// 拷贝 去重表数据
	reply.DuplicateTable = make(map[int64]LastOperationInfo)
	for clientId, op := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = op.copyData()
	}

	//reply.ConfigId = kv.currentConfig.ConfigId
	reply.ConfigId = args.ConfigId
	reply.Err = OK
}

// DeleteShardsData for RPC
func (kv *ShardKV) DeleteShardsData(args *ShardOperation, reply *ShardOperationReply) {
	// 只需要从 Leader 获取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.currentConfig.ConfigId > args.ConfigId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OpReply
	// 接收到 对方的GC, 说明对方已经复制完毕, 我这里可以删除处于 MoveOut 的 shard 了
	// 发送到 raft 一致性模块中, 方便之后follower也执行删除操作
	kv.handleConfigCommand(RaftLogCommand{ShardGC, *args}, &opReply)

	reply.Err = opReply.Err
}

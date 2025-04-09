package shardctrler

import "sort"

type CtrlerStateMachine struct {
	Configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{Configs: make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (c *CtrlerStateMachine) Join(groups map[int][]string) Err {
	// Your code here.
	index := len(c.Configs)
	lastConfig := c.Configs[index-1]
	newConfig := Config{
		ConfigId: index,
		Groups:   copyGroups(lastConfig.Groups),
		Shards:   lastConfig.Shards,
	}

	// 将新的 Group[] 加入到 Groups[] 中
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newConfig.Groups[gid] = make([]string, len(servers))
			copy(newConfig.Groups[gid], servers)
		}
	}

	gidToShard := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShard[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShard[gid] = append(gidToShard[gid], shard)
	}

	for {
		maxGid, minGid := getMaxGidWithShard(gidToShard), getMinGidWithShard(gidToShard)
		if maxGid != 0 && len(gidToShard[maxGid])-len(gidToShard[minGid]) <= 1 {
			break
		}
		gidToShard[minGid] = append(gidToShard[minGid], gidToShard[maxGid][0])
		gidToShard[maxGid] = gidToShard[maxGid][1:]
	}

	var newShards [NShards]int
	for gid, shards := range gidToShard {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	newConfig.Shards = newShards
	c.Configs = append(c.Configs, newConfig)
	return OK

}

func (c *CtrlerStateMachine) Leave(GIds []int) Err {
	// Your code here.
	// 新配置
	index := len(c.Configs)
	lastConfig := c.Configs[index-1]
	newConfig := Config{
		ConfigId: index,
		Shards:   lastConfig.Shards,
		Groups:   copyGroups(lastConfig.Groups),
	}

	// 为当前每一组group构建映射 <gid, [1,2,3]>, 包括将要被删除的组;
	gidToShard := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShard[gid] = make([]int, 0)
	}
	//for gid, shard := range newConfig.Shards {
	for shard, gid := range newConfig.Shards {
		gidToShard[gid] = append(gidToShard[gid], shard)
	}

	// 将要分配的 shard;
	var unusedShards []int
	for _, gid := range GIds {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := gidToShard[gid]; ok {
			unusedShards = append(unusedShards, shards...)
			delete(gidToShard, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		// 为无家可归的 shard 重新分配 gid;
		for _, shard := range unusedShards {
			minGid := getMinGidWithShard(gidToShard)
			gidToShard[minGid] = append(gidToShard[minGid], shard)
		}

		// 重新存储 shards 数组
		for gid, shards := range gidToShard {
			for _, shard := range shards {
				//newShards[gid] = shard
				newShards[shard] = gid
			}
		}
	}

	newConfig.Shards = newShards
	c.Configs = append(c.Configs, newConfig)
	return OK

}

func (c *CtrlerStateMachine) Move(shardId, toGid int) Err {
	// Your code here.
	index := len(c.Configs)
	lastConfig := c.Configs[index-1]
	newConfig := Config{
		ConfigId: index,
		Groups:   copyGroups(lastConfig.Groups),
		Shards:   lastConfig.Shards,
	}
	newConfig.Shards[shardId] = toGid
	c.Configs = append(c.Configs, newConfig)
	return OK
}

func (c *CtrlerStateMachine) Query(configId int) (Config, Err) {
	// Your code here.
	index := len(c.Configs)
	if configId < 0 || configId >= index {
		return c.Configs[index-1], OK
	}
	return c.Configs[configId], OK
}

func getMaxGidWithShard(gidToshards map[int][]int) int {
	if shard, ok := gidToshards[0]; ok && len(shard) > 0 {
		return 0
	}

	var gids []int
	for gid := range gidToshards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	maxGid, maxShards := -1, -1
	// for gid, shards := range gidToshards {
	for _, gid := range gids {
		if len(gidToshards[gid]) > maxShards {
			maxGid, maxShards = gid, len(gidToshards[gid])
		}
	}
	return maxGid
}
func getMinGidWithShard(gidToshards map[int][]int) int {
	var gids []int
	for gid := range gidToshards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToshards[gid]) < minShards {
			minGid, minShards = gid, len(gidToshards[gid])
		}
	}
	return minGid
}

func copyGroups(groups map[int][]string) map[int][]string {
	newGroup := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		newGroup[gid] = make([]string, len(servers))
		copy(newGroup[gid], servers)
	}
	return newGroup
}

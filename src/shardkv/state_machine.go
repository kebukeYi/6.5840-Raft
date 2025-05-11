package shardkv

type MemoryKVStateMachine struct {
	KV     map[string]string
	Status ShardState
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV:     make(map[string]string),
		Status: Normal,
	}
}

func (m *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := m.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (m *MemoryKVStateMachine) Put(key, value string) Err {
	m.KV[key] = value
	return OK
}

func (m *MemoryKVStateMachine) Append(key, value string) Err {
	m.KV[key] += value
	return OK
}

func (m *MemoryKVStateMachine) copyData() map[string]string {
	newKV := make(map[string]string)
	for k, v := range m.KV {
		newKV[k] = v
	}
	return newKV
}

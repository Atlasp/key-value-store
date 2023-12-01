package concurrency

import (
	"crypto/sha1"
	"sync"
)

type Shard struct {
	sync.RWMutex
	m map[string]interface{}
}

type ShardMap []*Shard

func NewShardMap(nshards int) ShardMap {
	shards := make([]*Shard, nshards)

	for i := 0; i < nshards; i++ {
		shard := make(map[string]interface{})
		shards[i] = &Shard{m: shard}
	}

	return shards
}

func (m ShardMap) getShardIndex(key string) int {
	checksum := sha1.Sum([]byte(key))
	hash := int(checksum[17])

	return hash % len(m)
}

func (m ShardMap) getShard(key string) *Shard {
	index := m.getShardIndex(key)
	return m[index]
}

func (m ShardMap) Get(key string) interface{} {
	shard := m.getShard(key)
	shard.RLock()
	defer shard.RUnlock()

	return shard.m[key]
}

func (m ShardMap) Set(key string, value interface{}) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()

	shard.m[key] = value
}

func (m ShardMap) Keys() []string {
	keys := make([]string, 0)

	mutex := sync.Mutex{}

	wg := sync.WaitGroup{}
	wg.Add(len(m))

	for _, shard := range m {
		go func(s *Shard) {
			s.RLock()

			for key := range s.m {
				mutex.Lock()
				keys = append(keys, key)
				mutex.Unlock()
			}

			s.RUnlock()
			wg.Done()
		}(shard)
	}
	wg.Wait()

	return keys
}

// RWMutex provides methods to establish both read and write locks, as demonstrated in the following.
// Using this method, any number of processes can establish simultaneous read locks as long as there are no open write locks;
// a process can establish a write lock only when there are no existing read or write locks.
// Attempts to establish additional locks will block until any locks ahead of it are released:

var items = struct {
	sync.RWMutex
	m map[string]int
}{m: make(map[string]int)}

func ThreadSafeRead(key string) int {
	items.RLock()
	value := items.m[key]
	items.RUnlock()

	return value
}

func ThreadSafeWrite(key string, value int) {
	items.Lock()
	items.m[key] = value
	items.Unlock()
}

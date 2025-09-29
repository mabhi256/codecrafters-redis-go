package main

type SortedSet struct {
	skipList *SkipList
	hashmap  map[string]float64
}

// Implement RedisValue interface
func (ss *SortedSet) Type() string {
	return "zset"
}

func NewSortedSet() *SortedSet {
	zset := &SortedSet{
		skipList: NewSkipList(),
		hashmap:  make(map[string]float64),
	}

	return zset
}

func (ss *SortedSet) Insert(member string, score float64) {
	ss.skipList.Insert(member, score)
	ss.hashmap[member] = score
}

func (ss *SortedSet) Remove(member string) {
	score, exists := ss.hashmap[member]

	if exists {
		ss.skipList.Remove(member, score)
		delete(ss.hashmap, member)
	}
}

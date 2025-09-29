package main

import "math/rand"

const SKIPLIST_MAXLEVEL = 8 // (indices 0-7) Redis uses 32, but this is enough for us
const SKIPLIST_P = 0.5      // Redis uses 0.25, higher P, more prob of higher levels, more memory, less search time

type SkipListNode struct {
	key   string
	value float64
	next  []*SkipListNode // pointer to next node at each level
	span  []int           // span[level] = number of nodes from current node to next[level]
}

type SkipList struct {
	header      *SkipListNode
	maxLevelIdx int
	size        int
}

func NewSkipList() *SkipList {
	// Sentinel node
	header := &SkipListNode{
		key:   "",
		value: 0,
		next:  make([]*SkipListNode, SKIPLIST_MAXLEVEL),
		span:  make([]int, SKIPLIST_MAXLEVEL),
	}

	return &SkipList{
		header:      header,
		maxLevelIdx: 0,
	}
}

func (sk *SkipList) shouldMoveRight(curr *SkipListNode, targetScore float64, targetKey string) bool {
	if curr == nil {
		return false
	}

	if curr.value < targetScore {
		return true
	}

	if curr.value > targetScore {
		return false
	}

	// value is same, compare keys lexicographically
	return curr.key < targetKey
}

func (sk *SkipList) Search(score float64, key string) *SkipListNode {
	level := sk.maxLevelIdx
	node := sk.header

	for level >= 0 {

		for sk.shouldMoveRight(node.next[level], score, key) {
			node = node.next[level]
		}

		nxt := node.next[level]
		if nxt != nil && nxt.value == score && nxt.key == key {
			return nxt
		}

		level--
	}

	return nil
}

// randomLevelIdx returns the number of levels a new node should span.
// Uses geometric distribution (P=0.5) to create a pyramid like structure
// level 1 -> 50%, level 2 -> 25%, level 3 -> 12.5% ...
//
// Level 2: [header] ---------→ [20.0]-------------------→ [NULL]
// Level 1: [header] ---------→ [20.0] ---------→ [40.0] → [NULL]
// Level 0: [header] → [10.0] → [20.0] → [30.0] → [40.0] → [NULL]
func randomLevelIdx() int {
	level := 0

	// To reach level N we need 0s (heads) N times and 1 (tails) 1 time
	// Probability to reach level N = (0.5)^N × 0.5
	for rand.Float64() < SKIPLIST_P && level < SKIPLIST_MAXLEVEL-1 {
		level++
	}

	return level // return 0-7 with a geometric distribution
}

// Ranks:    0    1    2    3
// Head:     a    b    c    d
// Score:    10   20   30   40
//
// Level 1: [head] --------------------span=3-------------------→ [c/30] -----------span=2----------→ [NULL]
//
// Level 0: [head] --span=1-→ [a/10] --span=1-→ [b/20] --span=1-→ [c/30] --span=1-→ [d/40] --span=1-→ [NULL]
//
// Insert 25 (e) between b and c
// Let's assume randomLevel() generated newLevelIdx = 1, so it gets level [0,1]
// update[1] = head, rank[1] = 0 // Start at level 1, head.next[1] is c (30), but 30 > 25, so don't move right
//
// move down to level 0,
// node = a, rank[0] += head.span[0] = 0 + 1 = 1, head.next[0] = a (score 10 < 25), move right
// node = b, rank[0] += a.span[0] = 1 + 1 = 2, a.next[0] = b (score 20 < 25), move right
// update[0] = b, rank[0] = 2 // moved right 2 times
//
// level 0
// before:
// prev = b	b.span[0] = 1 (b -> c)
//
// after:
// e.span[0] = b.span[0] - (rank[0]-rank[0]) = 1 (e -> c)
// b.span[0] = (rank[0]-rank[0]) + 1 = 1 (b -> e)
//
// level 1
// before:
// prev = head	head.span[1] = 3 (head -> c)
//
// after:
// e.span[1] = 3 - (rank[0]-rank[1]) = 3-(2-0) = 1 (e -> c)
// head.span[1] = (rank[0]-rank[1]) + 1 = 3 (head -> e)
func (sk *SkipList) Insert(key string, score float64) {
	sk.size++
	newLevel := randomLevelIdx()
	if newLevel > sk.maxLevelIdx {
		sk.maxLevelIdx = newLevel
	}

	update := make([]*SkipListNode, SKIPLIST_MAXLEVEL) // Track which nodes will point to our new node at each level
	rank := make([]int, SKIPLIST_MAXLEVEL)             // rank[i] = number of nodes passed at level i

	level := sk.maxLevelIdx
	node := sk.header

	for level >= 0 {
		// move right on this level as long as possible
		for sk.shouldMoveRight(node.next[level], score, key) {
			rank[level] += node.span[level]
			node = node.next[level]
		}

		// This node will point to the new inserted node
		update[level] = node
		level--
	}

	newNode := &SkipListNode{
		key:   key,
		value: score,
		next:  make([]*SkipListNode, newLevel+1),
		span:  make([]int, newLevel+1),
	}

	for i := 0; i <= newLevel; i++ {
		prev := update[i]
		newNode.next[i] = prev.next[i]
		prev.next[i] = newNode

		newNode.span[i] = prev.span[i] - (rank[0] - rank[i])
		prev.span[i] = rank[0] - rank[i] + 1
	}

	// handle levels where the newNode is not present
	for i := newLevel + 1; i <= sk.maxLevelIdx; i++ {
		update[i].span[i]++
	}
}

func (sk *SkipList) Remove(key string, score float64) bool {
	level := sk.maxLevelIdx
	node := sk.header
	update := make([]*SkipListNode, SKIPLIST_MAXLEVEL)

	for level >= 0 {
		// move right on this level as long as possible
		for sk.shouldMoveRight(node.next[level], score, key) {
			node = node.next[level]
		}
		update[level] = node
		level--
	}

	// check if target exists
	target := node.next[0]
	if target == nil || target.value != score || target.key != key {
		return false // Not found
	}

	// Remove node and update spans at each level
	for i := 0; i <= sk.maxLevelIdx; i++ {
		if update[i].next[i] == target {
			// Target exists at this level - remove it and merge spans
			update[i].next[i] = target.next[i]
			update[i].span[i] += target.span[i] - 1
		} else {
			// Node doesn't exist at this level - just decrement span
			update[i].span[i]--
		}
	}

	// Decrement max level if header's next was pointing to the deleted node
	for sk.maxLevelIdx > 0 && sk.header.next[sk.maxLevelIdx] == nil {
		sk.maxLevelIdx--
	}

	sk.size--
	return true
}

func (sk *SkipList) Range(start, stop int) ([]string, []float64) {
	if start < 0 {
		start = max(0, start+sk.size)
	}
	if stop < 0 {
		stop = max(0, stop+sk.size)
	}
	if stop > sk.size {
		stop = sk.size - 1
	}

	if start >= sk.size || start > stop {
		return nil, nil
	}

	keys := []string{}
	scores := []float64{}

	level := sk.maxLevelIdx
	node := sk.header
	currRank := 0

	for level >= 0 {
		// Move right until we reach predecessor of start position
		for node.next[level] != nil && currRank+node.span[level] < start {
			currRank += node.span[level]
			node = node.next[level]
		}
		level--
	}

	// Collect elements from start to stop
	count := stop - start + 1
	for count > 0 && node != nil {
		node = node.next[0]
		keys = append(keys, node.key)
		scores = append(scores, node.value)
		count--
	}

	return keys, scores
}

func (sk *SkipList) Rank(key string, score float64) int {
	rank := 0
	level := sk.maxLevelIdx
	node := sk.header

	for level >= 0 {
		// Accumulate spans while moving right
		for sk.shouldMoveRight(node.next[level], score, key) {
			rank += node.span[level]
			node = node.next[level]
		}
	}

	// After traversal, node.next[0] should be our target (if it exists)
	target := node.next[0]
	if target != nil && target.value == score && target.key == key {
		return rank
	}

	return -1
}

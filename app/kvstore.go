package main

import (
	"sync"
	"time"
)

type RedisValue interface {
	Type() string // redis also keeps other metadata like lru/lfu and encoding type
}

type StringEntry struct {
	value string
}

func (e *StringEntry) Type() string {
	return "string"
}

type ListEntry struct {
	value []string
}

func (e *ListEntry) Type() string {
	return "list"
}

type RedisCache struct {
	mu             sync.RWMutex
	data           map[string]RedisValue
	expiry         map[string]int64
	blocking       chan BlockingItem
	txnQueue       map[string][][]string // connID -> array of args
	execAbortQueue map[string]bool
}

func NewRedisCache() *RedisCache {
	rc := &RedisCache{
		data:           make(map[string]RedisValue),
		expiry:         make(map[string]int64),
		blocking:       make(chan BlockingItem, 100),
		txnQueue:       make(map[string][][]string),
		execAbortQueue: make(map[string]bool),
	}

	return rc
}

func (rc *RedisCache) CheckExpiry(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			// Redis uses a 25ms time limit, we are using a iteration counter to simplify it
			rc.cleanupExpired(25)
		}
	}()
}

func (rc *RedisCache) cleanupExpired(maxIterations int) {
	if maxIterations <= 0 {
		return
	}

	now := time.Now().UnixMilli()
	// Redis samples 20 random TTL keys every 100ms, and if > 25% are expired,
	// it repeats the process again until < 25% are expired

	rc.mu.RLock()
	sampled := 0
	keys := make([]string, 20)
	expired := 0

	for key := range rc.expiry { // Go randomizes map iteration order on each iteration
		keys = append(keys, key)
		sampled++

		if sampled > 20 {
			break
		}
	}
	rc.mu.RUnlock()

	for _, key := range keys {
		rc.mu.Lock()
		if exp, exists := rc.expiry[key]; exists && now > exp {
			delete(rc.data, key)
			delete(rc.expiry, key)
			expired++
		}
		rc.mu.Unlock()
	}

	if expired > 5 { // 25% of 20
		rc.cleanupExpired(maxIterations - 1)
	}
}

func (rc *RedisCache) Get(key string) (RedisValue, bool) {
	rc.mu.RLock()

	// Lazy expiration - remove when checking for Get()
	exp, expExists := rc.expiry[key]
	if expExists && time.Now().UnixMilli() > exp {
		rc.mu.RUnlock()
		rc.mu.Lock() // upgrade to write lock

		// double check if the key is not upgraded/deleted by another goroutine
		exp2, expExists2 := rc.expiry[key]
		if expExists2 && exp2 == exp {
			delete(rc.data, key)
			delete(rc.expiry, key)
		}
		rc.mu.Unlock()

		return nil, false
	}

	entry, exists := rc.data[key]
	rc.mu.RUnlock()

	return entry, exists
}

func (rc *RedisCache) Set(key string, value RedisValue, exp int64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.data[key] = value

	if exp > 0 {
		rc.expiry[key] = exp
	}
}

func (rc *RedisCache) Del(key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	delete(rc.data, key)
	delete(rc.expiry, key)
}

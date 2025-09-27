package main

import (
	"sync"
	"time"
)

type RedisValue interface {
	Type() string
	IsExpired() bool
}

type StringEntry struct {
	value  string
	expiry int64 // in ms
}

func (e *StringEntry) Type() string {
	return "string"
}

func (e *StringEntry) IsExpired() bool {
	return e.expiry != -1 && time.Now().UnixMilli() > e.expiry
}

type ListEntry struct {
	value  []string
	expiry int64 // in ms
}

func (e *ListEntry) Type() string {
	return "list"
}

func (e *ListEntry) IsExpired() bool {
	return e.expiry != -1 && time.Now().UnixMilli() > e.expiry
}

type StreamEntry struct {
	root    *RadixNode
	startID string
	lastID  string
}

func (e *StreamEntry) Type() string {
	return "stream"
}

func (e *StreamEntry) IsExpired() bool {
	return false
}

type RedisCache struct {
	mu       sync.RWMutex
	data     map[string]RedisValue
	blocking chan BlockingItem
}

func (rc *RedisCache) Get(key string) (RedisValue, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	entry, exists := rc.data[key]
	return entry, exists
}

func (rc *RedisCache) Set(key string, value RedisValue) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.data[key] = value
}

func (rc *RedisCache) Del(key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	delete(rc.data, key)
}

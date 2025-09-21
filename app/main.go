package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
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
	value   *RadixNode
	startID string
	lastID  string
}

func (e *StreamEntry) Type() string {
	return "stream"
}

func (e *StreamEntry) IsExpired() bool {
	return false
}

type BlockingItem struct {
	listName string
	value    string
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	cache := make(map[string]RedisValue)
	blocking := make(chan BlockingItem, 1)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleRequest(conn, cache, blocking)
	}
}

func handleRequest(conn net.Conn, cache map[string]RedisValue, blocking chan BlockingItem) {
	defer conn.Close()

	for {
		args, err := receiveCommand(conn)
		if err != nil && err != io.EOF {
			fmt.Println("Error receiving data:", err.Error())
			os.Exit(1)
		}
		if err == io.EOF {
			return
		}

		command := args[0]

		switch command {
		case "PING":
			_, err = sendSimpleString(conn, "PONG")
			if err != nil {
				fmt.Println("Error sending simple string:", err.Error())
				os.Exit(1)
			}

		case "ECHO":
			if len(args) != 2 {
				fmt.Println("Expecting 'redis-cli ECHO <value>', got:", args)
				os.Exit(1)
			}

			_, err = sendBulkString(conn, args[1])
			if err != nil {
				fmt.Println("Error sending bulk string:", err.Error())
				os.Exit(1)
			}

		case "SET":
			if len(args) < 3 {
				fmt.Println("Expecting 'redis-cli SET <key> <value>', got:", args)
				os.Exit(1)
			}

			expiryMs := int64(-1)
			if len(args) == 5 {
				switch strings.ToUpper(args[3]) {
				case "EX":
					expiry, err := strconv.Atoi(args[4])
					if err != nil {
						fmt.Println("Expecting 'redis-cli SET <key> <value> EX <seconds>', got:", args)
						os.Exit(1)
					}
					expiryMs = time.Now().UnixMilli() + int64(expiry)*1000
				case "PX":
					expiry, err := strconv.Atoi(args[4])
					if err != nil {
						fmt.Println("Expecting 'redis-cli SET <key> <value> PX <millis>', got:", args)
						os.Exit(1)
					}
					expiryMs = time.Now().UnixMilli() + int64(expiry)
				}
			}

			cache[args[1]] = &StringEntry{
				value:  args[2],
				expiry: expiryMs,
			}

			_, err = sendSimpleString(conn, "OK")
			if err != nil {
				fmt.Println("Error sending simple string:", err.Error())
				os.Exit(1)
			}

		case "GET":
			if len(args) != 2 {
				fmt.Println("Expecting 'redis-cli GET <key>', got:", args)
				os.Exit(1)
			}

			key := args[1]
			entry, exists := cache[key]
			// If not found, send null bulk string
			var err error
			if !exists || entry.IsExpired() {
				if exists {
					delete(cache, key)
				}
				_, err = sendNullBulkString(conn)
			} else {
				_, err = sendBulkString(conn, entry.(*StringEntry).value)
			}

			if err != nil {
				fmt.Println("Error sending bulk string:", err.Error())
				os.Exit(1)
			}

		case "RPUSH":
			if len(args) < 3 {
				fmt.Println("Expecting 'redis-cli RPUSH <list-name> <values>...', got:", args)
				os.Exit(1)
			}
			key := args[1]
			list, exists := cache[key]

			var entry *ListEntry
			if exists {
				entry = list.(*ListEntry)
			} else {
				entry = &ListEntry{value: []string{}, expiry: -1}
				cache[key] = entry
			}

			for _, item := range args[2:] {
				entry.value = append(entry.value, item)
				go func() {
					blocking <- BlockingItem{listName: key, value: item}
				}()
			}

			_, err = sendInteger(conn, len(entry.value))
			if err != nil {
				fmt.Println("Error sending integer:", err.Error())
				os.Exit(1)
			}

		case "LPUSH":
			if len(args) < 3 {
				fmt.Println("Expecting 'redis-cli LPUSH <list-name> <values>...', got:", args)
				os.Exit(1)
			}
			key := args[1]
			list, exists := cache[key]

			var entry *ListEntry
			if exists {
				entry = list.(*ListEntry)
			} else {
				entry = &ListEntry{value: []string{}, expiry: -1}
				cache[key] = entry
			}

			for _, item := range args[2:] {
				entry.value = append([]string{item}, entry.value...)
				go func() {
					blocking <- BlockingItem{listName: key, value: item}
				}()
			}

			_, err = sendInteger(conn, len(entry.value))
			if err != nil {
				fmt.Println("Error sending integer:", err.Error())
				os.Exit(1)
			}

		case "LRANGE":
			if len(args) < 4 {
				fmt.Println("Expecting 'redis-cli LRANGE <list-name> <start> <stop>', got:", args)
				os.Exit(1)
			}
			key := args[1]
			start, err := strconv.Atoi(args[2])
			if err != nil {
				fmt.Println("Error parsing start value:", err.Error())
				os.Exit(1)
			}
			stop, err := strconv.Atoi(args[3])
			if err != nil {
				fmt.Println("Error parsing stop value:", err.Error())
				os.Exit(1)
			}

			list, exists := cache[key]
			var entry *ListEntry
			if exists {
				entry = list.(*ListEntry)
			}

			if start < 0 {
				start = max(0, len(entry.value)+start)
			}
			if stop < 0 {
				stop = len(entry.value) + stop
			}

			fmt.Println("start:", start, ", stop:", stop)
			if !exists || start > stop || start >= len(entry.value) {
				_, err = sendArray(conn, []string{})

			} else {
				stop = min(len(entry.value), stop+1) // change stop to exclusive boundary
				_, err = sendArray(conn, entry.value[start:stop])
			}

			if err != nil {
				fmt.Println("Error sending array:", err.Error())
				os.Exit(1)
			}

		case "LLEN":
			if len(args) != 2 {
				fmt.Println("Expecting 'redis-cli LLEN <list-name>', got:", args)
				os.Exit(1)
			}
			key := args[1]
			list, exists := cache[key]
			var entry *ListEntry
			if exists {
				entry = list.(*ListEntry)
			}

			var err error
			if exists {
				_, err = sendInteger(conn, len(entry.value))
			} else {
				_, err = sendInteger(conn, 0)
			}

			if err != nil {
				fmt.Println("Error sending integer:", err.Error())
				os.Exit(1)
			}

		case "LPOP":
			if len(args) < 2 {
				fmt.Println("Expecting 'redis-cli LPOP <list-name> (<pop-count>)', got:", args)
				os.Exit(1)
			}
			key := args[1]
			count := 1
			if len(args) == 3 {
				count, err = strconv.Atoi(args[2])
				if err != nil {
					fmt.Println("Error reading pop count:", err.Error())
					os.Exit(1)
				}
			}

			list, exists := cache[key]
			var entry *ListEntry
			if exists {
				entry = list.(*ListEntry)
			}

			var err error
			if len(entry.value) == 0 {
				_, err = sendNullBulkString(conn)
			} else if count == 1 {
				peek := entry.value[0]
				entry.value = entry.value[1:]
				_, err = sendBulkString(conn, peek)
			} else {
				count = min(len(entry.value), count)
				values := entry.value[:count]

				if count == len(entry.value) {
					delete(cache, key)
				} else {
					entry.value = entry.value[count:]
				}

				_, err = sendArray(conn, values)
			}

			if err != nil {
				fmt.Println("Error sending bulk string:", err.Error())
				os.Exit(1)
			}

		case "BLPOP":
			if len(args) != 3 {
				fmt.Println("Expecting 'redis-cli BLPOP <list-name> <timeout>', got:", args)
				os.Exit(1)
			}
			key := args[1]
			timeout, err := strconv.ParseFloat(args[2], 64)
			if err != nil {
				fmt.Println("Error reading pop count:", err.Error())
				os.Exit(1)
			}

			if timeout > 0 {
				// fmt.Println("timeout:", timeout)

				timer := time.After(time.Duration(timeout*1000) * time.Millisecond)

				for {
					select {
					case item := <-blocking:
						list, exists := cache[key]
						if exists && item.listName == key {
							entry := list.(*ListEntry)
							peek := entry.value[0]
							entry.value = entry.value[1:]
							_, err = sendArray(conn, []string{key, peek})
							if err != nil {
								fmt.Println("Error sending BLPOP element:", err.Error())
								os.Exit(1)
							}
							return
						}

					case <-timer:
						_, err = sendNullArray(conn)
						if err != nil {
							fmt.Println("Error sending BLPOP element:", err.Error())
							os.Exit(1)
						}
						return
					}
				}
			} else {
				for {
					item := <-blocking
					list, exists := cache[key]
					if exists && item.listName == key {
						entry := list.(*ListEntry)
						peek := entry.value[0]
						entry.value = entry.value[1:]
						_, err = sendArray(conn, []string{key, peek})
						if err != nil {
							fmt.Println("Error sending BLPOP element:", err.Error())
							os.Exit(1)
						}
						return
					}
				}
			}

		case "TYPE":
			if len(args) != 2 {
				fmt.Println("Expecting 'redis-cli TYPE <key>', got:", args)
				os.Exit(1)
			}
			key := args[1]

			entry, exists := cache[key]
			var err error
			if exists {
				_, err = sendSimpleString(conn, entry.Type())
			} else {
				_, err = sendSimpleString(conn, "none")
			}

			if err != nil {
				fmt.Println("Error sending simple string:", err.Error())
				os.Exit(1)
			}

		case "XADD":
			if len(args) < 3 {
				fmt.Println("Expecting 'redis-cli XADD <stream-key> <entry-id> <key1> <value1> ...', got:", args)
				os.Exit(1)
			}

			idx := 1
			key := args[idx]
			idx++

			entryID := args[idx]
			idx++

			// Verify the entryId is well formed: <ms>-<seq>
			parts := strings.SplitN(entryID, "-", 2)

			for i, part := range parts {
				if len(parts) == 1 && part == "*" {
					break
				}
				if i == 1 && part == "*" {
					fmt.Println("part[1] = *")
					break
				}

				for _, ch := range part {
					if ch < '0' || ch > '9' {
						_, err = sendSimpleError(conn, "ERR Invalid stream ID specified as stream command argument")
						if err != nil {
							fmt.Println("Error sending error:", err.Error())
							os.Exit(1)
						}
					}
				}
			}

			// check if "x-y" format
			if entryID == "0-0" {
				_, err = sendSimpleError(conn, "ERR The ID specified in XADD must be greater than 0-0")
				if err != nil {
					fmt.Println("Error sending error:", err.Error())
					os.Exit(1)
				}
				continue
			}

			list, exists := cache[key]
			var entry *StreamEntry
			if exists {
				entry = list.(*StreamEntry)
				// if same time part, incr seq byt 1
				prevIDParts := strings.SplitN(entry.lastID, "-", 2)

				if parts[0] < prevIDParts[0] {
					_, err = sendSimpleError(conn, "ERR The ID specified in XADD is equal or smaller than the target stream top item")
					if err != nil {
						fmt.Println("Error sending error:", err.Error())
						os.Exit(1)
					}
					continue
				}

				if parts[0] == prevIDParts[0] {
					if parts[1] == "*" {
						prevSeq, err := strconv.Atoi(prevIDParts[1])
						if err != nil {
							fmt.Println("Error parsing entryID")
							os.Exit(1)
						}
						entryID = parts[0] + "-" + strconv.Itoa(prevSeq+1)
					} else if parts[1] <= prevIDParts[1] {
						_, err = sendSimpleError(conn, "ERR The ID specified in XADD is equal or smaller than the target stream top item")
						if err != nil {
							fmt.Println("Error sending error:", err.Error())
							os.Exit(1)
						}
						continue
					}
				}

				if parts[0] > prevIDParts[0] && parts[1] == "*" {
					entryID = parts[0] + "-0"
				}
			} else {
				if parts[1] == "*" {
					if parts[0] == "0" {
						entryID = "0-1"
					} else {
						entryID = parts[0] + "-0"
					}
				}
				entry = &StreamEntry{
					value:   &RadixNode{},
					startID: entryID,
					lastID:  "",
				}
				cache[key] = entry
			}

			value := make(map[string]string)
			for idx < len(args) {
				entryKey := args[idx]
				idx++
				entryValue := args[idx]
				idx++

				value[entryKey] = entryValue
			}

			entry.value.Insert(entryID, value)
			entry.lastID = entryID

			_, err = sendBulkString(conn, entryID)
			if err != nil {
				fmt.Println("Error sending bulk string:", err.Error())
				os.Exit(1)
			}

		default:
			fmt.Println("Unknown command:", command)
			os.Exit(1)
		}
	}
}

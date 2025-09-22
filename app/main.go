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
			if len(args) < 5 || len(args)%2 == 0 {
				fmt.Println("Expecting 'redis-cli XADD <stream-key> <entry-id> <key1> <value1> ...', got:", args)
				os.Exit(1)
			}

			streamKey := args[1]
			entryID := args[2]

			list, exists := cache[streamKey]
			var entry *StreamEntry
			if exists {
				entry = list.(*StreamEntry)

				if err := ValidateStreamID(entryID, entry.lastID); err != nil {
					if streamErr, ok := err.(StreamIDError); ok {
						_, err := sendSimpleError(conn, streamErr.message)
						if err != nil {
							fmt.Println("Error sending error:", err.Error())
							os.Exit(1)
						}
					}
					continue
				}
				entryID, err = GenerateStreamID(entryID, entry.lastID)
			} else {
				entryID, err = GenerateStreamID(entryID, "")
				entry = &StreamEntry{
					root:    &RadixNode{},
					startID: entryID,
					lastID:  "",
				}
				cache[streamKey] = entry
			}
			if err != nil {
				fmt.Println("Error generating streamID:", err.Error())
				os.Exit(1)
			}

			idx := 3
			value := []string{}
			for idx < len(args) {
				value = append(value, args[idx])
				idx++
			}
			entry.root.Insert(entryID, value)
			entry.lastID = entryID

			_, err = sendBulkString(conn, entryID)
			if err != nil {
				fmt.Println("Error sending bulk string:", err.Error())
				os.Exit(1)
			}

		case "XRANGE":
			if len(args) != 4 {
				fmt.Println("Expecting 'redis-cli XRANGE <stream-key> <start-id> <end-id>', got:", args)
				os.Exit(1)
			}

			streamKey := args[1]
			startID := args[2]
			endID := args[3]

			// If no sequence number provided for the end, change it to end+1, to cover all sequences with end ms
			endParts := strings.SplitN(endID, "-", 2)
			if endID != "+" && len(endParts) != 2 {
				endIDms, err := strconv.ParseInt(endID, 10, 64)
				if err != nil {
					fmt.Println("Error parsing endID:", err.Error())
					os.Exit(1)
				}
				endID = fmt.Sprintf("%d", endIDms+1)
			}

			// The sequence number doesn't need to be included in the start and end IDs
			// If not provided, XRANGE defaults to a sequence number of 0 for the start and
			// the maximum sequence number for the end.
			list, exists := cache[streamKey]
			var stream *StreamEntry
			var response []any
			if exists {
				stream = list.(*StreamEntry)

				if startID == "-" {
					startID = stream.startID
				}
				if endID == "+" {
					endID = stream.lastID
				}

				res := stream.root.RangeQuery(startID, endID)

				for _, item := range res {
					response = append(response, []any{item.ID, item.Data})
				}

				_, err = sendAnyArray(conn, response)
				if err != nil {
					fmt.Println("Error sending bulk string:", err.Error())
					os.Exit(1)
				}
			}

		default:
			fmt.Println("Unknown command:", command)
			os.Exit(1)
		}
	}
}

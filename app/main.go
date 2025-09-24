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
	key string
	// value any
}

func validateCommand(args []string) error {
	command := args[0]

	switch command {
	case "ECHO":
		if len(args) != 2 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "SET":
		if len(args) < 3 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}
		if len(args) == 5 {
			if strings.ToUpper(args[3]) != "EX" && strings.ToUpper(args[3]) != "PX" {
				return fmt.Errorf("ERR syntax error")
			}
		}

	case "GET":
		if len(args) != 2 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "INCR":
		if len(args) != 2 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "RPUSH":
		if len(args) < 3 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "LPUSH":
		if len(args) < 3 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "LRANGE":
		if len(args) < 4 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "LLEN":
		if len(args) != 2 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "LPOP":
		if len(args) < 2 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "BLPOP":
		if len(args) != 3 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "TYPE":
		if len(args) != 2 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "XADD":
		if len(args) < 5 || len(args)%2 == 0 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "XRANGE":
		if len(args) != 4 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "XREAD":
		if len(args) < 4 || len(args)%2 != 0 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}

	case "DISCARD":
		if len(args) != 1 {
			return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
		}
	}

	return nil
}

func execute(args []string, conn net.Conn,
	cache map[string]RedisValue, blocking chan BlockingItem,
	txnQueue map[string][][]string, execAbortQueue map[string]bool,
) {
	command := args[0]
	connID := fmt.Sprintf("%p", conn)
	var err error
	var res any

	switch command {
	case "PING":
		_, err = sendSimpleString(conn, "PONG")
		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}

	case "ECHO":
		// ECHO <value>
		_, err = sendBulkString(conn, args[1])
		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}

	case "SET":
		// SET <key> <value> (EX/PX <timeout-sec/ms>)
		expiryMs := int64(-1)
		if len(args) == 5 {
			switch strings.ToUpper(args[3]) {
			case "EX":
				expiry, err := strconv.Atoi(args[4])
				if err != nil {
					fmt.Println("Expecting 'SET <key> <value> EX <seconds>', got:", args)
					os.Exit(1)
				}
				expiryMs = time.Now().UnixMilli() + int64(expiry)*1000
			case "PX":
				expiry, err := strconv.Atoi(args[4])
				if err != nil {
					fmt.Println("Expecting 'SET <key> <value> PX <millis>', got:", args)
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
		// GET <key>
		key := args[1]
		entry, exists := cache[key]

		if !exists {
			res = nil
		} else if entry.IsExpired() {
			delete(cache, key)
			res = nil
		} else {
			res = entry
		}

		if res == nil {
			_, err = sendNullBulkString(conn)
		} else {
			_, err = sendBulkString(conn, res.(*StringEntry).value)
		}

		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}

	case "INCR":
		// INCR <key>
		key := args[1]
		entry, exists := cache[key]

		if !exists {
			cache[key] = &StringEntry{
				value:  "1",
				expiry: -1,
			}
			res = 1
		} else if entry.IsExpired() {
			delete(cache, key)
			cache[key] = &StringEntry{
				value:  "1",
				expiry: -1,
			}
			res = 1
		} else {
			entryStr := entry.(*StringEntry)
			entryInt, err2 := strconv.Atoi(entryStr.value)
			if err2 != nil {
				_, err = sendSimpleError(conn, "ERR value is not an integer or out of range")
				if err != nil {
					fmt.Println("Error sending response:", err.Error())
					os.Exit(1)
				}
				return
			}

			entryInt++
			cache[key] = &StringEntry{
				value:  strconv.Itoa(entryInt),
				expiry: entryStr.expiry,
			}
			res = entryInt
		}

		_, err = sendInteger(conn, res.(int))
		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}

	case "RPUSH":
		// RPUSH <list-name> <values>...
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
				blocking <- BlockingItem{key: key /* , value: item */}
			}()
		}

		_, err = sendInteger(conn, len(entry.value))
		if err != nil {
			fmt.Println("Error sending integer:", err.Error())
			os.Exit(1)
		}

	case "LPUSH":
		// LPUSH <list-name> <values>...
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
				blocking <- BlockingItem{key: key /* , value: item */}
			}()
		}

		_, err = sendInteger(conn, len(entry.value))
		if err != nil {
			fmt.Println("Error sending integer:", err.Error())
			os.Exit(1)
		}

	case "LRANGE":
		// LRANGE <list-name> <start> <stop>
		key := args[1]
		start, err := strconv.Atoi(args[2])
		if err != nil {
			res = nil
		}
		stop, err := strconv.Atoi(args[3])
		if err != nil {
			res = nil
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

		if !exists || start > stop || start >= len(entry.value) {
			res = []string{}
		} else {
			stop = min(len(entry.value), stop+1) // change stop to exclusive boundary
			res = entry.value[start:stop]
		}

		if res == nil {
			_, err = sendSimpleError(conn, "ERR value is not an integer or out of range")
		} else {
			_, err = sendArray(conn, res.([]string))
		}

		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}

	case "LLEN":
		// LLEN <list-name>
		key := args[1]
		list, exists := cache[key]
		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		}

		if exists {
			res = len(entry.value)
		} else {
			res = 0
		}

		_, err = sendInteger(conn, res.(int))
		if err != nil {
			fmt.Println("Error sending integer:", err.Error())
			os.Exit(1)
		}

	case "LPOP":
		// LPOP <list-name> (<pop-count>)
		key := args[1]
		count := 1
		var err error
		if len(args) == 3 {
			count, err = strconv.Atoi(args[2])
			if err != nil {
				_, err = sendSimpleError(conn, "ERR value is not an integer or out of range")
				if err != nil {
					fmt.Println("Error sending error:", err.Error())
					os.Exit(1)
				}
			}
		}

		list, exists := cache[key]
		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		}

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
		// BLPOP <list-name> <timeout>
		key := args[1]
		timeout, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			_, err = sendSimpleError(conn, "ERR value is not an integer or out of range")
			if err != nil {
				fmt.Println("Error sending response:", err.Error())
				os.Exit(1)
			}
			return
		}

		var timer <-chan time.Time
		if timeout > 0 {
			timer = time.After(time.Duration(timeout*1000) * time.Millisecond)
		}

		for {
			select {
			case item := <-blocking:
				list, exists := cache[key]
				if exists && item.key == key {
					entry := list.(*ListEntry)
					peek := entry.value[0]
					entry.value = entry.value[1:]
					_, err = sendArray(conn, []string{key, peek})
					if err != nil {
						fmt.Println("Error sending response:", err.Error())
						os.Exit(1)
					}
					return
				}

			case <-timer:
				_, err = sendNullArray(conn)
				if err != nil {
					fmt.Println("Error sending response:", err.Error())
					os.Exit(1)
				}
				return
			}
		}

	case "TYPE":
		// TYPE <key>
		key := args[1]
		entry, exists := cache[key]

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
		// XADD <stream-key> <entry-id> <key1> <value1> ...
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
				return
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

		go func() {
			blocking <- BlockingItem{key: streamKey}
			fmt.Println("Sending to channel:", streamKey, ", entryID:", entryID)
		}()

		_, err = sendBulkString(conn, entryID)
		if err != nil {
			fmt.Println("Error sending bulk string:", err.Error())
			os.Exit(1)
		}

	case "XRANGE":
		// XRANGE <stream-key> <start-id> <end-id>
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

	case "XREAD":
		// XREAD (block <block-ms>) streams <stream-key1> <stream-key2>... <entry-id1> <entry-id2>
		isBlocking := args[1] == "block"
		blockMS := -1
		streamNameIdx := 2
		if isBlocking {
			blockMS, err = strconv.Atoi(args[2])
			if err != nil {
				fmt.Println("Error parsing block value:", err.Error())
				os.Exit(1)
			}
			streamNameIdx = 4
		}

		streamCount := (len(args) - streamNameIdx) / 2
		streamKeys := args[streamNameIdx : streamNameIdx+streamCount]
		entryIDs := args[streamNameIdx+streamCount:]

		// Return immediately if any stream has data, else wait till timeout
		found := false
		var response []any
		startIDs := make([]string, streamCount)

		for i, streamKey := range streamKeys {
			var entries []any
			list, exists := cache[streamKey]
			if exists {
				stream := list.(*StreamEntry)

				if entryIDs[i] == "$" {
					entryIDs[i] = stream.lastID
				}
				parts := strings.SplitN(entryIDs[i], "-", 2)

				seq, err := strconv.Atoi(parts[1])
				if err != nil {
					fmt.Println("Error parsing entryID sequence:", err.Error())
					os.Exit(1)
				}
				startIDs[i] = fmt.Sprintf("%s-%d", parts[0], seq+1)

				if stream.lastID >= startIDs[i] {
					fmt.Println("Querying:", startIDs[i], "to", stream.lastID)
					res := stream.root.RangeQuery(startIDs[i], stream.lastID)

					for _, item := range res {
						entries = append(entries, []any{item.ID, item.Data})
					}

					found = true
				}
			}
			response = append(response, []any{streamKey, entries})
		}

		if found {
			fmt.Println("Found")
			_, err = sendAnyArray(conn, response)
			if err != nil {
				fmt.Println("Error sending bulk string:", err.Error())
				os.Exit(1)
			}
			return
		}

		if isBlocking {
			var timer <-chan time.Time

			if blockMS > 0 {
				timer = time.After(time.Duration(blockMS) * time.Millisecond)
			}

		blockingLoop:
			for {
				select {
				case item := <-blocking:
					found := false
					var blockingResponse []any

					for i, streamKey := range streamKeys {
						var entries []any
						list, exists := cache[streamKey]

						if exists && item.key == streamKey {
							stream := list.(*StreamEntry)

							if stream.lastID >= startIDs[i] {
								fmt.Println("Querying:", startIDs[i], "to", stream.lastID)
								res := stream.root.RangeQuery(startIDs[i], stream.lastID)

								for _, item := range res {
									entries = append(entries, []any{item.ID, item.Data})
								}

								found = true
							}
						}
						blockingResponse = append(blockingResponse, []any{streamKey, entries})
					}

					if found {
						_, err = sendAnyArray(conn, blockingResponse)
						if err != nil {
							fmt.Println("Error sending bulk string:", err.Error())
							os.Exit(1)
						}
						break blockingLoop
					}

				case <-timer:
					_, err = sendNullArray(conn)
					if err != nil {
						fmt.Println("Error sending Null Array:", err.Error())
						os.Exit(1)
					}
					break blockingLoop
				}
			}
		}

	case "MULTI":
		txnQueue[connID] = [][]string{}
		_, err = sendSimpleString(conn, "OK")
		if err != nil {
			fmt.Println("Error sending simple string:", err.Error())
			os.Exit(1)
		}

	case "EXEC":
		tasks, exists := txnQueue[connID]

		if !exists {
			_, err = sendSimpleError(conn, "ERR EXEC without MULTI")
			if err != nil {
				fmt.Println("Error sending simple error:", err.Error())
				os.Exit(1)
			}
			return
		}
		delete(txnQueue, connID)

		if execAbortQueue[connID] {
			delete(execAbortQueue, connID)
			_, err = sendSimpleError(conn, "EXECABORT Transaction discarded because of previous errors")
			if err != nil {
				fmt.Println("Error sending simple error:", err.Error())
				os.Exit(1)
			}
			return
		}

		response := fmt.Sprintf("*%d\r\n", len(tasks))
		_, err := conn.Write([]byte(response))
		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}

		for _, task := range tasks {
			execute(task, conn, cache, blocking, txnQueue, execAbortQueue)
		}

	case "DISCARD":
		_, exists := txnQueue[connID]

		if !exists {
			_, err = sendSimpleError(conn, "ERR DISCARD without MULTI")
			if err != nil {
				fmt.Println("Error sending simple error:", err.Error())
				os.Exit(1)
			}
			return
		}
		delete(txnQueue, connID)

		_, err = sendSimpleString(conn, "OK")
		if err != nil {
			fmt.Println("Error sending simple string:", err.Error())
			os.Exit(1)
		}

	default:
		fmt.Println("Unknown command:", command)
		os.Exit(1)
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	var l net.Listener
	var err error

	if len(os.Args) == 3 && os.Args[1] == "--port" {
		l, err = net.Listen("tcp", "0.0.0.0:"+os.Args[2])
	} else {
		l, err = net.Listen("tcp", "0.0.0.0:6379")
	}
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	cache := make(map[string]RedisValue)
	blocking := make(chan BlockingItem, 1)
	txnQueue := make(map[string][][]string) // connID -> array of args
	execAbortQueue := make(map[string]bool)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleRequest(conn, cache, blocking, txnQueue, execAbortQueue)
	}
}

func handleRequest(conn net.Conn,
	cache map[string]RedisValue, blocking chan BlockingItem,
	txnQueue map[string][][]string, execAbortQueue map[string]bool,
) {
	defer conn.Close()

	connID := fmt.Sprintf("%p", conn)

	for {
		args, err := receiveCommand(conn)
		if err != nil && err != io.EOF {
			fmt.Println("Error receiving data:", err.Error())
			os.Exit(1)
		}
		if err == io.EOF {
			return
		}

		err = validateCommand(args)
		if err != nil {
			execAbortQueue[connID] = true
			_, err = sendSimpleError(conn, err.Error())
			if err != nil {
				fmt.Println("Error sending error:", err.Error())
				os.Exit(1)
			}
		}

		command := args[0]

		_, exists := txnQueue[connID]
		if exists && command != "EXEC" && command != "DISCARD" {
			txnQueue[connID] = append(txnQueue[connID], args)
			_, err = sendSimpleString(conn, "QUEUED")
			if err != nil {
				fmt.Println("Error sending simple string:", err.Error())
				os.Exit(1)
			}
			continue
		}

		execute(args, conn, cache, blocking, txnQueue, execAbortQueue)
	}
}

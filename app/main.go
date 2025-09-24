package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type RedisServer struct {
	host       string
	role       string
	master     string
	replID     string
	replOffset int
}

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

func execute(args []string, conn net.Conn, server RedisServer,
	cache map[string]RedisValue, blocking chan BlockingItem,
	txnQueue map[string][][]string, execAbortQueue map[string]bool,
) string {
	command := args[0]
	connID := fmt.Sprintf("%p", conn)
	var err error

	switch command {
	case "PING":
		return encodeSimpleString("PONG")

	case "ECHO":
		// ECHO <value>
		return encodeBulkString(args[1])

	case "SET":
		// SET <key> <value> (EX/PX <timeout-sec/ms>)
		expiryMs := int64(-1)
		if len(args) == 5 {
			expiry, err := strconv.Atoi(args[4])
			if err != nil {
				return encodeSimpleError("ERR value is not an integer or out of range")
			}

			switch strings.ToUpper(args[3]) {
			case "EX":
				expiryMs = time.Now().UnixMilli() + int64(expiry)*1000
			case "PX":
				expiryMs = time.Now().UnixMilli() + int64(expiry)
			}
		}

		cache[args[1]] = &StringEntry{
			value:  args[2],
			expiry: expiryMs,
		}

		return encodeSimpleString("OK")

	case "GET":
		// GET <key>
		key := args[1]
		entry, exists := cache[key]

		if !exists {
			return encodeNullString()
		} else if entry.IsExpired() {
			delete(cache, key)
			return encodeNullString()
		} else {
			return encodeBulkString(entry.(*StringEntry).value)
		}

	case "INCR":
		// INCR <key>
		key := args[1]
		entry, exists := cache[key]

		if !exists || entry.IsExpired() {
			cache[key] = &StringEntry{
				value:  "1",
				expiry: -1,
			}
			return encodeInteger(1)
		} else {
			entryStr := entry.(*StringEntry)
			entryInt, err2 := strconv.Atoi(entryStr.value)
			if err2 != nil {
				return encodeSimpleError("ERR value is not an integer or out of range")
			}

			entryInt++
			cache[key] = &StringEntry{
				value:  strconv.Itoa(entryInt),
				expiry: entryStr.expiry,
			}
			return encodeInteger(entryInt)
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

		return encodeInteger(len(entry.value))

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

		return encodeInteger(len(entry.value))

	case "LRANGE":
		// LRANGE <list-name> <start> <stop>
		key := args[1]
		start, err := strconv.Atoi(args[2])
		if err != nil {
			return encodeSimpleError("ERR value is not an integer or out of range")
		}
		stop, err := strconv.Atoi(args[3])
		if err != nil {
			return encodeSimpleError("ERR value is not an integer or out of range")
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
			return encodeStringArray([]string{})
		} else {
			stop = min(len(entry.value), stop+1) // change stop to exclusive boundary
			return encodeStringArray(entry.value[start:stop])
		}

	case "LLEN":
		// LLEN <list-name>
		key := args[1]
		list, exists := cache[key]
		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		}

		length := 0
		if exists {
			length = len(entry.value)
		}

		return encodeInteger(length)

	case "LPOP":
		// LPOP <list-name> (<pop-count>)
		key := args[1]
		count := 1
		var err error
		if len(args) == 3 {
			count, err = strconv.Atoi(args[2])
			if err != nil {
				return encodeSimpleError("ERR value is not an integer or out of range")
			}
		}

		list, exists := cache[key]
		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		}

		if len(entry.value) == 0 {
			return encodeNullString()
		} else if count == 1 {
			peek := entry.value[0]
			entry.value = entry.value[1:]
			return encodeBulkString(peek)
		} else {
			count = min(len(entry.value), count)
			values := entry.value[:count]

			if count == len(entry.value) {
				delete(cache, key)
			} else {
				entry.value = entry.value[count:]
			}

			return encodeStringArray(values)
		}

	case "BLPOP":
		// BLPOP <list-name> <timeout>
		key := args[1]
		timeout, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return encodeSimpleError("ERR value is not an integer or out of range")
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
					return encodeStringArray([]string{key, peek})
				}

			case <-timer:
				return encodeNullArray()
			}
		}

	case "TYPE":
		// TYPE <key>
		key := args[1]
		entry, exists := cache[key]

		if exists {
			return encodeSimpleString(entry.Type())
		} else {
			return encodeSimpleString("none")
		}

	case "XADD":
		// XADD <stream-key> <entry-id> <key1> <value1> ...
		streamKey := args[1]
		entryID := args[2]

		list, exists := cache[streamKey]
		var entry *StreamEntry
		if exists {
			entry = list.(*StreamEntry)

			if err = ValidateStreamID(entryID, entry.lastID); err != nil {
				return encodeSimpleError(err.(StreamIDError).message)
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
		}()

		return encodeBulkString(entryID)

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
				return encodeSimpleError("ERR value is not an integer or out of range")
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

			return encodeAnyArray(response)
		}

	case "XREAD":
		// XREAD (block <block-ms>) streams <stream-key1> <stream-key2>... <entry-id1> <entry-id2>
		isBlocking := args[1] == "block"
		blockMS := -1
		streamNameIdx := 2
		if isBlocking {
			blockMS, err = strconv.Atoi(args[2])
			if err != nil {
				return encodeSimpleError("ERR value is not an integer or out of range")
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
					return encodeSimpleError("ERR value is not an integer or out of range")
				}
				startIDs[i] = fmt.Sprintf("%s-%d", parts[0], seq+1)

				if stream.lastID >= startIDs[i] {
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
			return encodeAnyArray(response)
		}

		if isBlocking {
			var timer <-chan time.Time

			if blockMS > 0 {
				timer = time.After(time.Duration(blockMS) * time.Millisecond)
			}

			// blockingLoop:
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
						return encodeAnyArray(blockingResponse)
						// break blockingLoop
					}

				case <-timer:
					return encodeNullArray()
					// break blockingLoop
				}
			}
		}

	case "MULTI":
		txnQueue[connID] = [][]string{}
		return encodeSimpleString("OK")

	case "EXEC":
		tasks, exists := txnQueue[connID]

		if !exists {
			return encodeSimpleError("ERR EXEC without MULTI")
		}
		delete(txnQueue, connID)

		if execAbortQueue[connID] {
			delete(execAbortQueue, connID)
			return encodeSimpleError("EXECABORT Transaction discarded because of previous errors")
		}

		response := fmt.Sprintf("*%d\r\n", len(tasks))

		for _, task := range tasks {
			response += execute(task, conn, server, cache, blocking, txnQueue, execAbortQueue)
		}

		return response

	case "DISCARD":
		_, exists := txnQueue[connID]

		if !exists {
			return encodeSimpleError("ERR DISCARD without MULTI")
		}
		delete(txnQueue, connID)

		return encodeSimpleString("OK")

	case "INFO":
		// INFO replication
		if args[1] == "replication" {
			response := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				server.role, server.replID, server.replOffset)

			return encodeBulkString(response)
		}

	default:
		fmt.Println("Unknown command:", command)
		os.Exit(1)
	}

	return ""
}

// Redis uses 40-character hexadecimal strings (0-9, a-f) for replication ID
func GenerateReplID() (string, error) {
	bytes := make([]byte, 20) // 20 bytes = 40 hex chars
	_, err := rand.Read(bytes)
	if err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %v", err)
	}

	return fmt.Sprintf("%x", bytes), nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	var l net.Listener
	var err error

	redisServer := RedisServer{
		host: "0.0.0.0:6379",
		role: "master",
	}

	redisServer.replID, err = GenerateReplID()
	if err != nil {
		fmt.Println("Failed to generate replication ID")
		os.Exit(1)
	}

	if len(os.Args) >= 3 && os.Args[1] == "--port" {
		redisServer.host = "0.0.0.0:" + os.Args[2]
	}

	l, err = net.Listen("tcp", redisServer.host)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	if len(os.Args) == 5 && os.Args[3] == "--replicaof" {
		parts := strings.SplitN(os.Args[4], " ", 2)
		if parts[0] == "localhost" {
			parts[0] = "0.0.0.0"
		}
		redisServer.master = parts[0] + ":" + parts[1]

		redisServer.role = "slave"
	}

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

		go handleRequest(conn, redisServer, cache, blocking, txnQueue, execAbortQueue)
	}
}

func handleRequest(conn net.Conn, redisServer RedisServer,
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
			response := encodeSimpleError(err.Error())
			_, err = conn.Write([]byte(response))
			if err != nil {
				fmt.Println("Error sending response:", err.Error())
				os.Exit(1)
			}
		}

		command := args[0]

		_, exists := txnQueue[connID]
		if exists && command != "EXEC" && command != "DISCARD" {
			txnQueue[connID] = append(txnQueue[connID], args)
			response := encodeSimpleString("QUEUED")
			_, err = conn.Write([]byte(response))
			if err != nil {
				fmt.Println("Error sending response:", err.Error())
				os.Exit(1)
			}
			continue
		}

		response := execute(args, conn, redisServer, cache, blocking, txnQueue, execAbortQueue)
		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("Error sending response:", err.Error())
			os.Exit(1)
		}
	}
}

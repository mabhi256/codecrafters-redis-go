package main

import (
	"bufio"
	"crypto/rand"
	_ "embed"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type RedisServer struct {
	port            string
	host            string
	role            string
	master          string
	replID          string
	replOffset      int
	slaves          map[net.Conn]int // slave -> offset
	lastWriteOffset int
	waitCh          chan int
	dir             string
	dbFilename      string
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

func (server *RedisServer) execute(args []string, respCommand string, conn net.Conn,
	cache *RedisCache,
	txnQueue map[string][][]string, execAbortQueue map[string]bool, rdbCache map[string]string,
) string {
	command := args[0]
	connID := fmt.Sprintf("%p", conn)
	isWrite := false
	var response string
	var err error

	switch command {
	case "PING":
		response = encodeSimpleString("PONG")

	case "ECHO":
		// ECHO <value>
		response = encodeBulkString(args[1])

	case "SET":
		// SET <key> <value> (EX/PX <timeout-sec/ms>)
		isWrite = true
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

		cache.Set(args[1], &StringEntry{
			value:  args[2],
			expiry: expiryMs,
		})

		response = encodeSimpleString("OK")

	case "GET":
		// GET <key>
		key := args[1]

		if rdbCache != nil {
			entry, exists := rdbCache[key]
			if !exists {
				response = encodeNullString()
			} else {
				response = encodeBulkString(entry)
			}
		} else {
			entry, exists := cache.Get(key)

			if !exists {
				response = encodeNullString()
			} else if entry.IsExpired() {
				cache.Del(key)
				response = encodeNullString()
			} else {
				response = encodeBulkString(entry.(*StringEntry).value)
			}
		}

	case "INCR":
		// INCR <key>
		isWrite = true
		key := args[1]
		entry, exists := cache.Get(key)

		if !exists || entry.IsExpired() {
			cache.Set(key, &StringEntry{
				value:  "1",
				expiry: -1,
			})
			response = encodeInteger(1)
		} else {
			entryStr := entry.(*StringEntry)
			entryInt, err2 := strconv.Atoi(entryStr.value)
			if err2 != nil {
				response = encodeSimpleError("ERR value is not an integer or out of range")
				break
			}

			entryInt++
			cache.Set(key, &StringEntry{
				value:  strconv.Itoa(entryInt),
				expiry: entryStr.expiry,
			})
			response = encodeInteger(entryInt)
		}

	case "RPUSH":
		// RPUSH <list-name> <values>...
		isWrite = true
		key := args[1]
		list, exists := cache.Get(key)

		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		} else {
			entry = &ListEntry{value: []string{}, expiry: -1}
			cache.Set(key, entry)
		}

		for _, item := range args[2:] {
			entry.value = append(entry.value, item)
			go func() {
				cache.blocking <- BlockingItem{key: key /* , value: item */}
			}()
		}

		response = encodeInteger(len(entry.value))

	case "LPUSH":
		// LPUSH <list-name> <values>...
		isWrite = true
		key := args[1]
		list, exists := cache.Get(key)

		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		} else {
			entry = &ListEntry{value: []string{}, expiry: -1}
			cache.Set(key, entry)
		}

		for _, item := range args[2:] {
			entry.value = append([]string{item}, entry.value...)
			go func() {
				cache.blocking <- BlockingItem{key: key /* , value: item */}
			}()
		}

		response = encodeInteger(len(entry.value))

	case "LRANGE":
		// LRANGE <list-name> <start> <stop>
		key := args[1]
		start, err := strconv.Atoi(args[2])
		if err != nil {
			response = encodeSimpleError("ERR value is not an integer or out of range")
			break
		}
		stop, err := strconv.Atoi(args[3])
		if err != nil {
			response = encodeSimpleError("ERR value is not an integer or out of range")
			break
		}

		list, exists := cache.Get(key)
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
			response = encodeStringArray([]string{})
		} else {
			stop = min(len(entry.value), stop+1) // change stop to exclusive boundary
			response = encodeStringArray(entry.value[start:stop])
		}

	case "LLEN":
		// LLEN <list-name>
		key := args[1]
		list, exists := cache.Get(key)
		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		}

		length := 0
		if exists {
			length = len(entry.value)
		}

		response = encodeInteger(length)

	case "LPOP":
		// LPOP <list-name> (<pop-count>)
		isWrite = true
		key := args[1]
		count := 1
		var err error
		if len(args) == 3 {
			count, err = strconv.Atoi(args[2])
			if err != nil {
				response = encodeSimpleError("ERR value is not an integer or out of range")
				break
			}
		}

		list, exists := cache.Get(key)
		var entry *ListEntry
		if exists {
			entry = list.(*ListEntry)
		}

		if len(entry.value) == 0 {
			response = encodeNullString()
		} else if count == 1 {
			peek := entry.value[0]
			entry.value = entry.value[1:]
			response = encodeBulkString(peek)
		} else {
			count = min(len(entry.value), count)
			values := entry.value[:count]

			if count == len(entry.value) {
				cache.Del(key)
			} else {
				entry.value = entry.value[count:]
			}

			response = encodeStringArray(values)
		}

	case "BLPOP":
		// BLPOP <list-name> <timeout>
		isWrite = true
		key := args[1]
		timeout, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			response = encodeSimpleError("ERR value is not an integer or out of range")
			break
		}

		var timer <-chan time.Time
		if timeout > 0 {
			timer = time.After(time.Duration(timeout*1000) * time.Millisecond)
		}

	blockLoop:
		for {
			select {
			case item := <-cache.blocking:
				list, exists := cache.Get(key)
				if exists && item.key == key {
					entry := list.(*ListEntry)
					peek := entry.value[0]
					entry.value = entry.value[1:]
					response = encodeStringArray([]string{key, peek})
					break blockLoop
				}

			case <-timer:
				response = encodeNullArray()
				break blockLoop
			}
		}

	case "TYPE":
		// TYPE <key>
		key := args[1]
		entry, exists := cache.Get(key)

		if exists {
			response = encodeSimpleString(entry.Type())
		} else {
			response = encodeSimpleString("none")
		}

	case "XADD":
		// XADD <stream-key> <entry-id> <key1> <value1> ...
		isWrite = true
		streamKey := args[1]
		entryID := args[2]

		list, exists := cache.Get(streamKey)
		var entry *StreamEntry
		if exists {
			entry = list.(*StreamEntry)

			if err = ValidateStreamID(entryID, entry.lastID); err != nil {
				response = encodeSimpleError(err.(StreamIDError).message)
				break
			}
			entryID, err = GenerateStreamID(entryID, entry.lastID)
		} else {
			entryID, err = GenerateStreamID(entryID, "")
			entry = &StreamEntry{
				root:    &RadixNode{},
				startID: entryID,
				lastID:  "",
			}
			cache.Set(streamKey, entry)
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
			cache.blocking <- BlockingItem{key: streamKey}
		}()

		response = encodeBulkString(entryID)

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
				response = encodeSimpleError("ERR value is not an integer or out of range")
				break
			}
			endID = fmt.Sprintf("%d", endIDms+1)
		}

		// The sequence number doesn't need to be included in the start and end IDs
		// If not provided, XRANGE defaults to a sequence number of 0 for the start and
		// the maximum sequence number for the end.
		list, exists := cache.Get(streamKey)
		var stream *StreamEntry
		var rangeRes []any
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
				rangeRes = append(rangeRes, []any{item.ID, item.Data})
			}

			response = encodeAnyArray(rangeRes)
		}

	case "XREAD":
		// XREAD (block <block-ms>) streams <stream-key1> <stream-key2>... <entry-id1> <entry-id2>
		isBlocking := args[1] == "block"
		blockMS := -1
		streamNameIdx := 2
		if isBlocking {
			blockMS, err = strconv.Atoi(args[2])
			if err != nil {
				response = encodeSimpleError("ERR value is not an integer or out of range")
				break
			}
			streamNameIdx = 4
		}

		streamCount := (len(args) - streamNameIdx) / 2
		streamKeys := args[streamNameIdx : streamNameIdx+streamCount]
		entryIDs := args[streamNameIdx+streamCount:]

		// Return immediately if any stream has data, else wait till timeout
		found := false
		var readRes []any
		startIDs := make([]string, streamCount)

		for i, streamKey := range streamKeys {
			var entries []any
			list, exists := cache.Get(streamKey)
			if exists {
				stream := list.(*StreamEntry)

				if entryIDs[i] == "$" {
					entryIDs[i] = stream.lastID
				}
				parts := strings.SplitN(entryIDs[i], "-", 2)

				seq, err := strconv.Atoi(parts[1])
				if err != nil {
					response = encodeSimpleError("ERR value is not an integer or out of range")
					break
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
			readRes = append(readRes, []any{streamKey, entries})
		}

		if found {
			response = encodeAnyArray(readRes)
			break
		}

		if isBlocking {
			var timer <-chan time.Time

			if blockMS > 0 {
				timer = time.After(time.Duration(blockMS) * time.Millisecond)
			}

		blockingLoop:
			for {
				select {
				case item := <-cache.blocking:
					found := false
					var blockingReadRes []any

					for i, streamKey := range streamKeys {
						var entries []any
						list, exists := cache.Get(streamKey)

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
						blockingReadRes = append(blockingReadRes, []any{streamKey, entries})
					}

					if found {
						response = encodeAnyArray(blockingReadRes)
						break blockingLoop
					}

				case <-timer:
					response = encodeNullArray()
					break blockingLoop
				}
			}
		}

	case "MULTI":
		txnQueue[connID] = [][]string{}
		response = encodeSimpleString("OK")

	case "EXEC":
		tasks, exists := txnQueue[connID]

		if !exists {
			response = encodeSimpleError("ERR EXEC without MULTI")
			break
		}
		delete(txnQueue, connID)

		if execAbortQueue[connID] {
			delete(execAbortQueue, connID)
			response = encodeSimpleError("EXECABORT Transaction discarded because of previous errors")
			break
		}

		response = fmt.Sprintf("*%d\r\n", len(tasks))

		for _, task := range tasks {
			response += server.execute(task, respCommand, conn, cache, txnQueue, execAbortQueue, rdbCache)
		}

	case "DISCARD":
		_, exists := txnQueue[connID]

		if !exists {
			response = encodeSimpleError("ERR DISCARD without MULTI")
			break
		}
		delete(txnQueue, connID)

		response = encodeSimpleString("OK")

	case "INFO":
		// INFO replication
		if args[1] == "replication" {
			rep := fmt.Sprintf("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d",
				server.role, server.replID, server.replOffset)

			response = encodeBulkString(rep)
		}

	case "REPLCONF":
		// REPLCONF listening-port <PORT>
		// REPLCONF capa psync2
		// REPLCONF GETACK *
		if server.role == "slave" && args[1] == "GETACK" && args[2] == "*" {
			offset := fmt.Sprintf("%d", server.replOffset)
			response = encodeStringArray([]string{"REPLCONF", "ACK", offset})
			break
		}

		if server.role == "master" && args[1] == "ACK" {
			offset, err := strconv.Atoi(args[2])
			if err == nil {
				server.slaves[conn] = offset
				server.waitCh <- offset
			}

			response = "" // Dont sent response for ACK
			break
		}

		response = encodeSimpleString("OK")

	case "PSYNC":
		if server.role == "master" {
			offset := server.replOffset
			if args[2] == "-1" {
				offset = 0
			}
			server.slaves[conn] = offset
			res := fmt.Sprintf("FULLRESYNC %s %d", server.replID, offset)
			response = encodeSimpleString(res)
		}

	case "WAIT":
		// WAIT <numreplicas> <timeout-ms>
		if server.role == "master" {
			numReplicas, err := strconv.Atoi(args[1])
			if err != nil {
				response = encodeSimpleError("ERR value is not an integer or out of range")
				break
			}
			timeout, err := strconv.Atoi(args[2])
			if err != nil {
				response = encodeSimpleError("ERR value is not an integer or out of range")
				break
			}

			// if numReplicas == 0 {
			// 	response = encodeInteger(numReplicas)
			// 	break
			// }

			if server.lastWriteOffset < 0 {
				response = encodeInteger(len(server.slaves))
				break
			}

			duration := time.Duration(timeout) * time.Millisecond
			deadline := time.Now().Add(duration)

			for slave := range server.slaves {
				go func(slaveConn net.Conn) {
					getAck(slaveConn, deadline)
				}(slave)
			}

			var numAck int

		waitLoop:
			for numAck <= len(server.slaves) {
				select {
				case slaveOffset := <-server.waitCh:
					if slaveOffset >= server.lastWriteOffset {
						numAck++
					}
					if numAck == numReplicas {
						response = encodeInteger(numAck)
						break waitLoop
					}

				case <-time.After(duration):
					response = encodeInteger(numAck)
					break waitLoop
				}
			}
		}

	case "CONFIG":
		if args[1] == "GET" {
			switch args[2] {
			case "dir":
				response = encodeStringArray([]string{"dir", server.dir})
			case "dbfilename":
				response = encodeStringArray([]string{"dbfilename", server.dbFilename})
			}
		}

	case "KEYS":
		if args[1] == "*" {
			var keys []string
			for key := range rdbCache {
				keys = append(keys, key)
			}
			response = encodeStringArray(keys)
		}

	default:
		fmt.Println("Unknown command:", command)
		os.Exit(1)
	}

	if server.role == "master" && isWrite {
		server.replOffset += len(respCommand)
		server.lastWriteOffset = server.replOffset
		for slave := range server.slaves {
			go slave.Write([]byte(respCommand))
		}
	} else if server.role == "slave" {
		server.replOffset += len(respCommand)
	}

	return response
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

	cache := &RedisCache{
		data:     make(map[string]RedisValue),
		blocking: make(chan BlockingItem, 100),
	}
	txnQueue := make(map[string][][]string) // connID -> array of args
	execAbortQueue := make(map[string]bool)

	argIdx := 1
	port := "6379"
	role := "master"
	var replicaOf, dir, dbFilename string
	for argIdx < len(os.Args) {
		switch os.Args[argIdx] {
		case "--port":
			port = os.Args[argIdx+1]

		case "--replicaof":
			role = "slave"
			replicaOf = os.Args[argIdx+1]

		case "--dir":
			dir = os.Args[argIdx+1]

		case "--dbfilename":
			dbFilename = os.Args[argIdx+1]
		}
		argIdx += 2
	}

	server := &RedisServer{
		port:            port,
		host:            "0.0.0.0:" + port,
		role:            role,
		slaves:          make(map[net.Conn]int),
		lastWriteOffset: -1,
		waitCh:          make(chan int, 1),
		dir:             dir,
		dbFilename:      dbFilename,
	}

	var rdbCache map[string]string
	// var rdbExpiry map[string]int64
	if dbFilename != "" {
		rdbCache, _, err = server.ParseRdb()
		if err != nil {
			fmt.Println("rdb file error:", err.Error())
		}
		// fmt.Println("Cache:")
		// for key, value := range rdbCache {
		// 	if expiry, exists := rdbExpiry[key]; exists {
		// 		fmt.Printf("%s -> %v {expiry: %d}\n", key, value, expiry)
		// 	} else {
		// 		fmt.Printf("%s -> %v\n", key, value)
		// 	}
		// }
		// fmt.Println()
	}

	if role == "master" {
		server.replID, err = GenerateReplID()
		if err != nil {
			fmt.Println("Failed to generate replication ID")
			os.Exit(1)
		}
	}

	if role == "slave" {
		parts := strings.SplitN(replicaOf, " ", 2)
		if parts[0] == "localhost" {
			parts[0] = "127.0.0.1"
		}
		server.master = parts[0] + ":" + parts[1]

		server.replID = "?"
		server.replOffset = -1

		masterConn, err := server.handshake()
		if err != nil && err != io.EOF {
			fmt.Println("Error handshaking master:", err.Error())
			os.Exit(1)
		}
		defer masterConn.Close()

		// execute replication commands from master
		go handleRequest(masterConn, server, cache, txnQueue, execAbortQueue, rdbCache)
	}

	l, err = net.Listen("tcp", server.host)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleRequest(conn, server, cache, txnQueue, execAbortQueue, rdbCache)
	}
}

func handleRequest(conn net.Conn, redisServer *RedisServer,
	cache *RedisCache,
	txnQueue map[string][][]string, execAbortQueue map[string]bool,
	rdbCache map[string]string,
) {
	if redisServer.role == "master" {
		defer conn.Close()
	}

	connID := fmt.Sprintf("%p", conn)
	reader := bufio.NewReader(conn)

	for {
		args, respCommand, err := receiveCommand(reader)
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

		response := redisServer.execute(args, respCommand, conn, cache, txnQueue, execAbortQueue, rdbCache)

		// fmt.Printf("Processed [%s] command: %v, Updating offset to: %d\n", redisServer.role, args, redisServer.replOffset)
		if redisServer.role == "master" ||
			(redisServer.role == "slave" && conn.RemoteAddr().String() != redisServer.master) ||
			(redisServer.role == "slave" && conn.RemoteAddr().String() == redisServer.master && args[0] == "REPLCONF" && args[1] == "GETACK") {
			_, err = conn.Write([]byte(response))
			if err != nil {
				fmt.Println("Error sending response:", err.Error())
				os.Exit(1)
			}
		}

		if command == "PSYNC" {
			redisServer.propagateEmptyRDB(conn)
		}
	}
}

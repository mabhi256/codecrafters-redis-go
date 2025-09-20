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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleRequest(conn, cache)
	}
}

func handleRequest(conn net.Conn, cache map[string]RedisValue) {
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
				fmt.Println("Expecting 'redis-cli RPUSH <list-name> <value>', got:", args)
				os.Exit(1)
			}
			key := args[1]
			list, exists := cache[key]

			var entry *ListEntry
			if exists {
				entry = list.(*ListEntry)
			} else {
				entry = &ListEntry{value: []string{}, expiry: -1}
			}

			entry.value = append(entry.value, args[2])
			cache[key] = entry

			_, err = sendInteger(conn, len(entry.value))
			if err != nil {
				fmt.Println("Error sending integer:", err.Error())
				os.Exit(1)
			}

		default:
			fmt.Println("Unknown command:", command)
			os.Exit(1)
		}
	}
}

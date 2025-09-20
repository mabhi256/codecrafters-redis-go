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

type CacheValue struct {
	value  string
	expiry int64 // in ms
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

	cache := make(map[string]CacheValue)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleRequest(conn, cache)
	}
}

func handleRequest(conn net.Conn, cache map[string]CacheValue) {
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

		fmt.Println("args:", args)
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

			cache[args[1]] = CacheValue{
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
			if !exists {
				_, err = sendNullBulkString(conn)
				if err != nil {
					fmt.Println("Error sending null bulk string:", err.Error())
					os.Exit(1)
				}
			}

			// If expired, send null bulk string
			now := time.Now().UnixMilli()

			if entry.expiry != -1 && entry.expiry < now {
				delete(cache, key)
				_, err = sendNullBulkString(conn)
				if err != nil {
					fmt.Println("Error sending null bulk string:", err.Error())
					os.Exit(1)
				}
			} else {
				_, err = sendBulkString(conn, entry.value)
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

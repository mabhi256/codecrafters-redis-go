package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	// fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	keyVal := make(map[string]string)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleRequest(conn, keyVal)
	}
}

func handleRequest(conn net.Conn, keyVal map[string]string) {
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
			if len(args) != 3 {
				fmt.Println("Expecting 'redis-cli SET <key> <value>', got:", args)
				os.Exit(1)
			}

			keyVal[args[1]] = args[2]

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

			value, exists := keyVal[args[1]]
			if !exists {
				_, err = sendNullBulkString(conn)
				if err != nil {
					fmt.Println("Error sending null bulk string:", err.Error())
					os.Exit(1)
				}
			}

			_, err = sendBulkString(conn, value)
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

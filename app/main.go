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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
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
			conn.Write([]byte("+PONG\r\n"))

		case "ECHO":
			_, err = sendBulkString(conn, args[1])
			if err != nil {
				fmt.Println("Error sending data:", err.Error())
				os.Exit(1)
			}

		default:
			fmt.Println("Unknown command:", command)
			os.Exit(1)
		}
	}
}

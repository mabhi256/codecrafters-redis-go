package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func (server *RedisServer) handshake() {
	conn, err := net.Dial("tcp", server.master)
	if err != nil {
		fmt.Println("Failed to connect to master")
		os.Exit(1)
	}

	ping := encodeStringArray([]string{"PING"})
	_, err = conn.Write([]byte(ping))
	if err != nil {
		fmt.Println("Failed to ping master")
		os.Exit(1)
	}

	reader := bufio.NewReader(conn)
	receiveLine(reader) // receive PONG

	replconf := encodeStringArray([]string{"REPLCONF", "listening-port", server.port})
	_, err = conn.Write([]byte(replconf))
	if err != nil {
		fmt.Println("Failed to replconf")
		os.Exit(1)
	}
	receiveLine(reader) // receive OK

	psync2 := encodeStringArray([]string{"REPLCONF", "capa", "psync2"})
	_, err = conn.Write([]byte(psync2))
	if err != nil {
		fmt.Println("Failed to replconf")
		os.Exit(1)
	}
	receiveLine(reader) // receive OK

	conn.Close()
}

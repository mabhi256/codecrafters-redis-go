package main

import (
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

	conn.Close()
}

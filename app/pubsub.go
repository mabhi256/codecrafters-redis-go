package main

import (
	"net"
	"slices"
)

func (server *RedisServer) IsSubscribed(conn net.Conn) bool {
	_, subExists := server.subscribers[conn]
	return subExists
}

func IsSubscribedModeCommand(command string) bool {
	writeCommands := []string{"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT"}

	return slices.Contains(writeCommands, command)
}

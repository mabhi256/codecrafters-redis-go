package main

import (
	"fmt"
	"net"
	"strconv"
)

func receive(conn net.Conn) (string, error) {
	msg := ""

	CR := false
	LF := false
	for !CR || !LF {
		bytes := make([]byte, 1)
		_, err := conn.Read(bytes)
		if err != nil {
			return "", err
		}

		recv := string(bytes)
		switch recv {
		case "\r":
			CR = true
		case "\n":
			LF = true
		default:
			msg += recv
		}
	}

	return msg, nil
}

// Receive command from client as an array of bulk string
func receiveCommand(conn net.Conn) ([]string, error) {
	lengthData, err := receive(conn)
	if err != nil {
		return nil, err
	}
	if len(lengthData) <= 0 || lengthData[0] != '*' {
		return nil, fmt.Errorf("expecting first byte * for command")
	}

	lengthStr := lengthData[1:]
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return nil, err
	}

	args := []string{}
	for range length {
		argLengthData, err := receive(conn)
		if err != nil {
			return nil, err
		}

		if len(argLengthData) <= 0 || argLengthData[0] != '$' {
			return nil, fmt.Errorf("expecting byte $ for command arg")
		}

		argLengthStr := argLengthData[1:]
		argLength, err := strconv.Atoi(argLengthStr)
		if err != nil {
			return nil, err
		}

		arg, err := receive(conn)
		if err != nil {
			return nil, err
		}
		if len(arg) != argLength {
			return nil, fmt.Errorf("expecting arg keyword of length: %d, received %s", argLength, arg)
		}

		args = append(args, arg)
	}

	return args, nil
}

func sendSimpleString(conn net.Conn, value string) (int, error) {
	response := fmt.Sprintf("+%s\r\n", value)

	return conn.Write([]byte(response))
}

func sendBulkString(conn net.Conn, value string) (int, error) {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)

	return conn.Write([]byte(response))
}

func sendNullBulkString(conn net.Conn) (int, error) {
	return conn.Write([]byte("$-1\r\n"))
}

func sendInteger(conn net.Conn, value int) (int, error) {
	response := fmt.Sprintf(":%d\r\n", value)

	return conn.Write([]byte(response))
}

func sendArray(conn net.Conn, values []string) (int, error) {
	if len(values) == 0 {
		return conn.Write([]byte("*0\r\n"))
	}

	response := fmt.Sprintf("*%d\r\n", len(values))
	_, err := conn.Write([]byte(response))
	if err != nil {
		return 0, err
	}

	for _, value := range values {
		_, err := sendBulkString(conn, value)
		if err != nil {
			return 0, err
		}
	}

	return 0, err
}

func sendNullArray(conn net.Conn) (int, error) {
	return conn.Write([]byte("*-1\r\n"))
}

func sendSimpleError(conn net.Conn, msg string) (int, error) {
	response := fmt.Sprintf("-%s\r\n", msg)

	return conn.Write([]byte(response))
}

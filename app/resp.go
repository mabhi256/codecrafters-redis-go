package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

func receiveLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}

// Receive command from client as an array of bulk string
func receiveCommand(reader *bufio.Reader) ([]string, string, error) {
	var respCommand string

	lengthData, err := receiveLine(reader)
	if err != nil {
		return nil, "", err
	}
	if len(lengthData) <= 0 || lengthData[0] != '*' {
		return nil, "", fmt.Errorf("expecting first byte * for command")
	}
	respCommand = lengthData
	lengthData = strings.TrimRight(lengthData, "\r\n")

	lengthStr := lengthData[1:]
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return nil, "", err
	}

	args := []string{}
	for range length {
		argLengthData, err := receiveLine(reader)
		if err != nil {
			return nil, "", err
		}
		respCommand += argLengthData
		argLengthData = strings.TrimRight(argLengthData, "\r\n")

		if len(argLengthData) <= 0 || argLengthData[0] != '$' {
			return nil, "", fmt.Errorf("expecting byte $ for command arg")
		}

		argLengthStr := argLengthData[1:]
		argLength, err := strconv.Atoi(argLengthStr)
		if err != nil {
			return nil, "", err
		}

		arg, err := receiveLine(reader)
		if err != nil {
			return nil, "", err
		}

		respCommand += arg
		arg = strings.TrimRight(arg, "\r\n")
		if len(arg) != argLength {
			return nil, "", fmt.Errorf("expecting arg keyword of length: %d, received %s", argLength, arg)
		}

		args = append(args, arg)
	}

	return args, respCommand, nil
}

func encodeSimpleString(value string) string {
	return fmt.Sprintf("+%s\r\n", value)
}

func encodeBulkString(value string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func encodeNullString() string {
	return "$-1\r\n"
}

func encodeInteger(value int) string {
	return fmt.Sprintf(":%d\r\n", value)
}

func encodeStringArray(values []string) string {
	if len(values) == 0 {
		return "*0\r\n"
	}

	encoded := fmt.Sprintf("*%d\r\n", len(values))
	for _, value := range values {
		encoded += encodeBulkString(value)
	}

	return encoded
}

func encodeAnyArray(values []any) string {
	if len(values) == 0 {
		return "*0\r\n"
	}

	encoded := fmt.Sprintf("*%d\r\n", len(values))
	for _, value := range values {
		switch value := value.(type) {
		case string:
			encoded += encodeBulkString(value)
		case []string:
			encoded += encodeStringArray(value)
		case []any:
			encoded += encodeAnyArray(value)
		}
	}

	return encoded
}

func encodeNullArray() string {
	return "*-1\r\n"
}

func encodeSimpleError(msg string) string {
	return fmt.Sprintf("-%s\r\n", msg)
}

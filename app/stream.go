package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type StreamEntry struct {
	root    *RadixNode
	startID string
	lastID  string
}

func (e *StreamEntry) Type() string {
	return "stream"
}

type StreamIDError struct {
	message string
}

func (e StreamIDError) Error() string {
	return e.message
}

func ValidateStreamID(entryID string, lastID string) error {
	if entryID == "*" {
		return nil
	}

	if entryID == "0-0" {
		return StreamIDError{"ERR The ID specified in XADD must be greater than 0-0"}
	}

	newParts := strings.SplitN(entryID, "-", 2)
	if len(newParts) != 2 {
		return StreamIDError{"ERR Invalid stream ID specified as stream command argument"}
	}

	for i, part := range newParts {
		if i == 1 && part == "*" {
			return nil
		}

		for _, ch := range part {
			if ch < '0' || ch > '9' {
				return StreamIDError{"ERR Invalid stream ID specified as stream command argument"}
			}
		}
	}

	if lastID == "" {
		return nil
	}

	prevParts := strings.SplitN(lastID, "-", 2)
	if newParts[0] < prevParts[0] {
		return StreamIDError{"ERR The ID specified in XADD is equal or smaller than the target stream top item"}
	}

	if newParts[0] == prevParts[0] && newParts[1] <= prevParts[1] {
		return StreamIDError{"ERR The ID specified in XADD is equal or smaller than the target stream top item"}
	}

	return nil
}

func GenerateStreamID(entryID, lastID string) (string, error) {
	prevParts := strings.SplitN(lastID, "-", 2)
	newParts := strings.SplitN(entryID, "-", 2)
	now := time.Now().UnixMilli()

	if entryID == "*" {
		if lastID == "" {
			return fmt.Sprintf("%d-0", now), nil
		}

		nowStr := fmt.Sprintf("%d", now)
		if nowStr == prevParts[0] {
			prevSeq, err := strconv.Atoi(prevParts[1])
			if err != nil {
				return "", fmt.Errorf("error parsing previous sequence: %w", err)
			}
			return fmt.Sprintf("%d-%d", now, prevSeq+1), nil
		} else {
			return fmt.Sprintf("%d-0", now), nil
		}
	}

	if newParts[1] == "*" {
		if lastID == "" {
			if newParts[0] == "0" {
				return "0-1", nil
			} else {
				return newParts[0] + "-0", nil
			}
		}

		if newParts[0] == prevParts[0] {
			prevSeq, err := strconv.Atoi(prevParts[1])
			if err != nil {
				fmt.Println("Error parsing entryID")
				os.Exit(1)
			}
			return fmt.Sprintf("%s-%d", newParts[0], prevSeq+1), nil
		} else {
			return fmt.Sprintf("%s-0", newParts[0]), nil
		}
	}

	return entryID, nil
}

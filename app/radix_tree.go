package main

import "strings"

type RadixNode struct {
	label    string
	children [11]*RadixNode // indices 0-9 for '0'-'9', index 10 for '-'
	data     map[string]string
}

type StreamResult struct {
	ID   string
	Data map[string]string
}

func findMismatchIndex(label, insert string) int {
	idx := 0
	for idx < len(label) && idx < len(insert) {
		if label[idx] != insert[idx] {
			return idx
		}

		idx++
	}

	return idx
}

func charToIndex(c byte) int {
	if c >= '0' && c <= '9' {
		return int(c - '0')
	}
	if c == '-' {
		return 10
	}
	return -1
}

func (node *RadixNode) Insert(key string, data map[string]string) {
	idx := findMismatchIndex(node.label, key)

	switch idx {
	case len(node.label):
		if idx == len(key) {
			// Exact match - just update the data
			node.data = data
		} else {
			// label is a prefix of the key being inserted
			child := node.children[charToIndex(key[idx])]
			if child == nil {
				node.children[charToIndex(key[idx])] = &RadixNode{
					label: key[idx:],
					data:  data,
				}
			} else {
				child.Insert(key[idx:], data)
			}
		}

	case len(key):
		// key is a prefix of the label
		childLabel := node.label[idx:]
		childData := node.data
		childChildren := node.children

		node.label = key
		node.data = data
		node.children = [11]*RadixNode{}
		node.children[charToIndex(childLabel[0])] = &RadixNode{
			label:    childLabel,
			children: childChildren,
			data:     childData,
		}
	default:
		// deverge in middle
		label1 := node.label[idx:]
		children1 := node.children
		data1 := node.data
		label2 := key[idx:]

		node.label = node.label[:idx]
		node.data = nil // it is now an internal node
		node.children = [11]*RadixNode{}
		node.children[charToIndex(label1[0])] = &RadixNode{
			label:    label1,
			children: children1,
			data:     data1,
		}
		node.children[charToIndex(label2[0])] = &RadixNode{
			label:    label2,
			children: [11]*RadixNode{},
			data:     data,
		}
	}
}

func (node *RadixNode) Search(key string) (map[string]string, bool) {
	idx := findMismatchIndex(node.label, key)

	if idx != len(node.label) {
		return nil, false
	}

	if idx == len(key) {
		return node.data, node.data != nil
	}

	childIdx := charToIndex(key[idx])
	if childIdx == -1 {
		return nil, false
	}
	child := node.children[childIdx]
	if child != nil {
		return child.Search(key[idx:])
	}

	return nil, false
}

func (node *RadixNode) RangeQuery(startID, endID string) []StreamResult {
	var results []StreamResult
	node.traverse("", startID, endID, &results)
	return results
}

func (node *RadixNode) traverse(currentPath string, startID string, endID string, result *[]StreamResult) {
	fullPath := currentPath + node.label

	if fullPath > endID {
		return
	}

	// Collect current node if it's in range
	if node.data != nil && fullPath >= startID && fullPath <= endID {
		*result = append(*result, StreamResult{ID: fullPath, Data: node.data})
	}

	// Traverse children in order
	chars := "0123456789-"
	for i := range 11 {
		if node.children[i] != nil {
			childPrefix := fullPath + string(chars[i])
			if childPrefix >= startID || strings.HasPrefix(startID, childPrefix) {
				node.children[i].traverse(fullPath, startID, endID, result)
			}
		}
	}
}

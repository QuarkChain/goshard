package common

import (
	"errors"
	"fmt"
)

const (
	TokenBase  = uint64(36)
	TokenIDMax = uint64(4873763662273663091) // ZZZZZZZZZZZZ
	TokenMax   = "ZZZZZZZZZZZZ"
)

// TokenIDEncode encodes a token name string to a uint64 token ID
// using 36-base (digits 0-9 = 0-9, letters A-Z = 10-35).
func TokenIDEncode(str string) uint64 {
	if len(str) >= 13 {
		panic(errors.New("name too long"))
	}

	id := TokenCharEncode(str[len(str)-1])
	base := TokenBase

	for index := len(str) - 2; index >= 0; index-- {
		id += base * (TokenCharEncode(str[index]) + 1)
		base *= TokenBase
	}
	return id
}

// TokenIDDecode decodes a uint64 token ID back to its name string.
func TokenIDDecode(id uint64) (string, error) {
	if id > TokenIDMax {
		return "", errors.New("token ID exceeds maximum")
	}
	name := make([]byte, 0)
	t, err := TokenCharDecode(id % TokenBase)
	if err != nil {
		return "", err
	}
	name = append(name, t)
	if id/TokenBase < 1 {
		return string(name), nil
	}
	id = id/TokenBase - 1
	for id >= 0 {
		t, err := TokenCharDecode(id % TokenBase)
		if err != nil {
			return "", err
		}
		name = append(name, t)
		if id/TokenBase < 1 {
			break
		}
		id = id/TokenBase - 1
	}
	return reverseString(string(name)), nil
}

func TokenCharEncode(char byte) uint64 {
	if char >= byte('A') && char <= byte('Z') {
		return 10 + uint64(char-byte('A'))
	}
	if char >= byte('0') && char <= byte('9') {
		return uint64(char - byte('0'))
	}
	panic(fmt.Errorf("unknown character %v", byte(char)))
}

func TokenCharDecode(id uint64) (byte, error) {
	if !(id < TokenBase && id >= 0) {
		return byte(0), fmt.Errorf("invalid char %v", id)
	}
	if id < 10 {
		return byte('0' + id), nil
	}
	return byte('A' + id - 10), nil
}

func reverseString(s string) string {
	b := []byte(s)
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
	return string(b)
}

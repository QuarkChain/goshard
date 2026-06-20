package common

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestTokenIDEncode(t *testing.T) {
	if got := TokenIDEncode("QKC"); got != 35760 {
		t.Fatalf("QKC: want 35760, got %d", got)
	}
	// QKCUP: U=30, P=25, C=12, K=20, Q=26
	// result = 25 + 36*(30+1) + 36^2*(12+1) + 36^3*(20+1) + 36^4*(26+1)
	if got := TokenIDEncode("QKCUP"); got != 46347397 {
		t.Fatalf("QKCUP: want 46347397, got %d", got)
	}
	s, err := TokenIDDecode(35760)
	if err != nil || s != "QKC" {
		t.Fatalf("decode 35760: want QKC, got %q %v", s, err)
	}
}

func TestTokenCharEncode(t *testing.T) {
	EncodedValues := make(map[string]uint64)
	EncodedValues["0"] = 0
	EncodedValues["Z"] = 35
	EncodedValues["00"] = 36
	EncodedValues["0Z"] = 71
	EncodedValues["1Z"] = 107
	EncodedValues["20"] = 108
	EncodedValues["ZZ"] = 1331
	EncodedValues["QKC"] = 35760
	EncodedValues[TokenMax] = TokenIDMax

	for key, value := range EncodedValues {
		if value != TokenIDEncode(key) {
			t.Fatalf("key:%v should: %v is %v", key, value, TokenIDEncode(key))
		}
	}
}

func TestRandomToken(t *testing.T) {
	count := 100000
	for index := 0; index < count; index++ {
		data := rand.Intn(int(TokenIDMax))

		deData, err := TokenIDDecode(uint64(data))
		if err != nil {
			fmt.Println("data", data)
			panic(err)
		}
		newData := TokenIDEncode(deData)
		if newData != uint64(data) {
			t.Fatalf("data:%v newData:%v", data, newData)
		}
	}
}

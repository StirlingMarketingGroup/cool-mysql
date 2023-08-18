package mysql

import (
	"fmt"
)

func decodeHex(s string) (string, error) {
	var result []byte
	for i := 0; i < len(s); i++ {
		if s[i] == '0' && i+2 < len(s) && s[i+1] == 'x' {
			hexByte, err := decodeSingleHex(s[i+2 : i+4])
			if err != nil {
				return "", err
			}
			result = append(result, hexByte)
			i += 3 // skip the "0x" and two hex digits
		} else {
			result = append(result, s[i])
		}
	}
	return string(result), nil
}

func decodeSingleHex(s string) (byte, error) {
	var value byte
	for i := 0; i < 2; i++ {
		char := s[i]
		value *= 16
		if char >= '0' && char <= '9' {
			value += char - '0'
		} else if char >= 'a' && char <= 'f' {
			value += char - 'a' + 10
		} else if char >= 'A' && char <= 'F' {
			value += char - 'A' + 10
		} else {
			return 0, fmt.Errorf("invalid hex digit: %c", char)
		}
	}
	return value, nil
}

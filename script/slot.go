package script

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import "strings"

// slotCount is the number of hash slots in a Redis Cluster.
const slotCount = 16384

// Slot returns the Redis Cluster hash slot for key, the same value CLUSTER
// KEYSLOT reports. When the key contains a hash tag (the text between the
// first "{" and the next "}", if that text is not empty) only the tag is
// hashed, which is how related keys are kept on one node.
func Slot(key string) int {
	return int(crc16(hashTag(key))) % slotCount
}

// hashTag returns the part of key that decides its slot.
func hashTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	end := strings.IndexByte(key[open+1:], '}')
	if end <= 0 {
		// No closing brace, or an empty tag "{}": hash the whole key.
		return key
	}
	return key[open+1 : open+1+end]
}

// crc16 is CRC-16/XMODEM (polynomial 0x1021, initial value 0), the checksum
// Redis Cluster uses for key slots.
func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

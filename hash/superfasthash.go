/*
 * Copyright (c) 2010, Paul Hsieh
 * Copyright (c) 2017, Steven Giacomelli (stevegiacomelli@gmail.com) - GO port
 *
 * All rights reserved.  Redistribution and use in source and binary forms,
 * with or without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither my name, Paul Hsieh, nor the names of any other contributors to
 *    the code use may not be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package hash

func get16bits(data []byte, index int) (bits uint32) {
	v := uint32(data[index+1])<<8 + uint32(data[index])
	return v
}

func calcSuperfasthash(data []byte) Type {

	if len(data) == 0 {
		return 0
	}

	hash := uint32(len(data))
	rem := len(data) & 3
	index := 0

	for i := len(data) >> 2; i > 0; i-- {
		hash = hash + get16bits(data, index)
		tmp := (get16bits(data, index+2) << 11) ^ hash
		hash = (hash << 16) ^ tmp
		index += 2
		hash += hash >> 11
	}

	switch rem {
	case 3:
		hash = hash + get16bits(data, index)
		hash = hash ^ (hash << 16)
		hash = hash ^ (uint32(data[index+2]))<<18
		hash = hash + (hash >> 11)
	case 2:
		hash = hash + get16bits(data, index)
		hash = hash ^ (hash << 11)
		hash = hash + (hash >> 17)
	case 1:
		hash = hash + uint32(data[index])
		hash = hash ^ (hash << 10)
		hash = hash + (hash >> 1)
	}
	hash = hash ^ (hash << 3)
	hash = hash + (hash >> 5)
	hash = hash ^ (hash << 4)
	hash = hash + (hash >> 17)
	hash = hash ^ (hash << 25)
	hash = hash + (hash >> 6)

	return Type(hash)
}

// https://gist.github.com/swgiacomelli/862de5d20fa843055a433f62e29abe02

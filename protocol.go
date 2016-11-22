// Copyright 2016 The Mangos Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use file except in compliance with the License.
// You may obtain a copy of the license at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zmq

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

var (
	errGreeting      = errors.New("zmq: invalid greeting received")
	errSecMech       = errors.New("zmq: invalid security mechanism")
	errBadSec        = errors.New("zmq: invalid or unsupported security mechanism")
	errBadCmd        = errors.New("zmq: invalid command name")
	errBadFrame      = errors.New("zmq: invalid frame")
	errOverflow      = errors.New("zmq: overflow")
	errEmptyAppMDKey = errors.New("zmq: empty application metadata key")
	errDupAppMDKey   = errors.New("zmq: duplicate application metadata key")
	errMoreCmd       = errors.New("zmq: MORE not supported") // FIXME(sbinet)
)

const (
	sigHeader = 0xFF
	sigFooter = 0x7F
)

var (
	defaultVersion = [2]uint8{3, 0}
)

const (
	maxUint   = ^uint(0)
	maxInt    = int(maxUint >> 1)
	maxUint64 = ^uint64(0)
	maxInt64  = int64(maxUint64 >> 1)
)

type greeting struct {
	Sig struct {
		Header byte
		_      [8]byte
		Footer byte
	}
	Version   [2]uint8
	Mechanism [20]byte
	Server    byte
	_         [31]byte
}

func (g *greeting) read(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, g)
	if err != nil {
		return err
	}

	if g.Sig.Header != sigHeader {
		return errGreeting
	}

	if g.Sig.Footer != sigFooter {
		return errGreeting
	}

	// FIXME(sbinet): handle version negotiations as per
	// https://rfc.zeromq.org/spec:23/ZMTP/#version-negotiation
	if g.Version != defaultVersion {
		return errGreeting
	}

	return nil
}

// command is a ZMTP command as per:
//  https://rfc.zeromq.org/spec:23/ZMTP/#formal-grammar
type command struct {
	Name string
	Body []byte
}

func (cmd *command) unmarshalZMTP(data []byte) error {
	if len(data) == 0 {
		return io.ErrUnexpectedEOF
	}
	n := int(data[0])
	if n > len(data)-1 {
		return errBadCmd
	}
	cmd.Name = string(data[1 : n+1])
	cmd.Body = data[n+1:]
	return nil
}

// ZMTP commands as per:
//  https://rfc.zeromq.org/spec:23/ZMTP/#commands
const (
	cmdCancel    = "CANCEL"
	cmdError     = "ERROR"
	cmdHello     = "HELLO"
	cmdPing      = "PING"
	cmdPong      = "PONG"
	cmdReady     = "READY"
	cmdSubscribe = "SUBSCRIBE"
)

type metaData struct {
	k string
	v string
}

func (md metaData) Read(data []byte) (n int, err error) {
	klen := len(md.k)
	data[n] = byte(klen)
	n++
	n += copy(data[n:n+klen], md.k)
	vlen := len(md.v)
	binary.BigEndian.PutUint32(data[n:n+4], uint32(vlen))
	n += 4
	n += copy(data[n:], md.v)
	return n, io.EOF
}

func (md *metaData) Write(data []byte) (n int, err error) {
	klen := int(data[n])
	n++
	if klen > len(data) {
		return n, io.ErrUnexpectedEOF
	}

	md.k = strings.ToLower(string(data[n : n+klen]))
	n += klen

	v := binary.BigEndian.Uint32(data[n : n+4])
	n += 4
	if uint64(v) > uint64(maxInt) {
		return n, errOverflow
	}

	vlen := int(v)
	if n+vlen > len(data) {
		return n, io.ErrUnexpectedEOF
	}

	md.v = string(data[n : n+vlen])
	n += vlen
	return n, nil
}

type flag byte

func (f flag) hasMore() bool    { return f == 0x1 || f == 0x3 }
func (fl flag) isLong() bool    { return fl == 0x2 || fl == 0x3 || fl == 0x6 }
func (fl flag) isCommand() bool { return fl == 0x4 || fl == 0x6 }

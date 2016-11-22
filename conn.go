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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/go-mangos/mangos"
	"github.com/sbinet-alice/zmq/zmtp"
)

// conn implements the Pipe interface on top of net.Conn.  The
// assumption is that transports using this have similar wire protocols,
// and conn is meant to be used as a building block.
type conn struct {
	c      net.Conn
	proto  mangos.Protocol
	sock   mangos.Socket
	open   bool
	props  map[string]interface{}
	maxrx  int64
	secu   zmtp.Security
	server bool
	peer   struct {
		server bool
		md     map[string]string
	}
}

// Recv implements the Pipe Recv method.  The message received is expected as
// a 64-bit size (network byte order) followed by the message itself.
func (p *conn) Recv() (*mangos.Message, error) {

	iscmd, body, err := p.read()
	if err != nil {
		return nil, err
	}
	if iscmd {
		var cmd command
		err := cmd.unmarshalZMTP(body)
		if err != nil {
			return nil, err
		}
		// FIXME(sbinet) implement commands
		return nil, fmt.Errorf("command [%s] not handled yet", cmd.Name)
	}
	msg := mangos.NewMessage(len(body))
	msg.Body = body
	return msg, nil
}

// Send implements the Pipe Send method.  The message is sent as a 64-bit
// size (network byte order) followed by the message itself.
func (p *conn) Send(msg *mangos.Message) error {

	if msg.Expired() {
		msg.Free()
		return nil
	}

	if err := p.send(false, msg.Body); err != nil {
		return err
	}
	msg.Free()
	return nil
}

// LocalProtocol returns our local protocol number.
func (p *conn) LocalProtocol() uint16 {
	return p.proto.Number()
}

// RemoteProtocol returns our peer's protocol number.
func (p *conn) RemoteProtocol() uint16 {
	return p.proto.PeerNumber()
}

// Close implements the Pipe Close method.
func (p *conn) Close() error {
	p.open = false
	return p.c.Close()
}

// IsOpen implements the PipeIsOpen method.
func (p *conn) IsOpen() bool {
	return p.open
}

func (p *conn) GetProp(n string) (interface{}, error) {
	if v, ok := p.props[n]; ok {
		return v, nil
	}
	return nil, mangos.ErrBadProperty
}

// NewConnPipe allocates a new Pipe using the supplied net.Conn, and
// initializes it.  It performs the handshake required at the SP layer,
// only returning the Pipe once the SP layer negotiation is complete.
//
// Stream oriented transports can utilize this to implement a Transport.
// The implementation will also need to implement PipeDialer, PipeAccepter,
// and the Transport enclosing structure.   Using this layered interface,
// the implementation needn't bother concerning itself with passing actual
// SP messages once the lower layer connection is established.
func NewConnPipe(c net.Conn, sock mangos.Socket, props ...interface{}) (mangos.Pipe, error) {
	p := &conn{c: c, proto: sock.GetProtocol(), sock: sock, secu: &zmtp.SecurityNull{}}

	if err := p.handshake(props); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *conn) handshake(props []interface{}) error {
	var err error

	p.props = make(map[string]interface{})
	p.props[mangos.PropLocalAddr] = p.c.LocalAddr()
	p.props[mangos.PropRemoteAddr] = p.c.RemoteAddr()

	for len(props) > 2 {
		switch name := props[0].(type) {
		case string:
			p.props[name] = props[1]
		default:
			return mangos.ErrBadProperty
		}
		props = props[2:]
	}

	if v, e := p.sock.GetOption(mangos.OptionMaxRecvSize); e == nil {
		// socket guarantees this is an integer
		p.maxrx = int64(v.(int))
	}

	for _, f := range []func() error{
		p.sendGreeting,
		p.recvGreeting,
		p.secu.Handshake,
		func() error { return p.sendMD(p.proto, nil) },
	} {
		err := f()
		if err != nil {
			return mangos.ErrBadHeader
		}
	}

	peerMD, err := p.recvMD()
	if err != nil {
		return err
	}
	p.peer.md = peerMD

	// FIXME(sbinet): if security mechanism does not define a client/server
	// topology, enforce that p.server == p.peer.server == 0
	// as per:
	//  https://rfc.zeromq.org/spec:23/ZMTP/#topology

	p.open = true

	return nil
}

func (c *conn) sendGreeting() error {
	g := greeting{Version: defaultVersion}
	g.Sig.Header = sigHeader
	g.Sig.Footer = sigFooter

	kind := string(c.secu.Kind())
	if len(kind) > len(g.Mechanism) {
		return errSecMech
	}
	copy(g.Mechanism[:], kind)

	return binary.Write(c.c, byteOrder, &g)
}

func (c *conn) recvGreeting() error {
	var g greeting

	err := g.read(c.c)
	if err != nil {
		return errGreeting
	}

	var (
		peerKind = asString(g.Mechanism[:])
		kind     = string(c.secu.Kind())
	)
	if peerKind != kind {
		return errBadSec
	}

	c.peer.server, err = asBool(g.Server)
	if err != nil {
		return err
	}
	return nil
}

func (c *conn) sendMD(proto mangos.Protocol, appMD map[string]string) error {
	buf := new(bytes.Buffer)
	keys := make(map[string]struct{})

	for k, v := range appMD {
		if len(k) == 0 {
			return errEmptyAppMDKey
		}

		key := strings.ToLower(k)
		if _, dup := keys[key]; dup {
			return errDupAppMDKey
		}

		keys[key] = struct{}{}
		if _, err := io.Copy(buf, metaData{k: "x-" + key, v: v}); err != nil {
			return err
		}
	}

	if _, err := io.Copy(buf, metaData{k: "socket-type", v: strings.ToUpper(proto.Name())}); err != nil {
		return err
	}
	return c.sendCmd(cmdReady, buf.Bytes())
}

func (c *conn) recvMD() (map[string]string, error) {
	isCommand, body, err := c.read()
	if err != nil {
		return nil, err
	}

	if !isCommand {
		return nil, errBadFrame
	}

	var cmd command
	err = cmd.unmarshalZMTP(body)
	if err != nil {
		return nil, err
	}

	if cmd.Name != cmdReady {
		return nil, errBadCmd
	}

	sysMetadata := make(map[string]string)
	appMetadata := make(map[string]string)
	i := 0
	for i < len(cmd.Body) {
		var kv metaData
		n, err := kv.Write(cmd.Body[i:])
		if err != nil {
			return nil, err
		}
		i += n

		if strings.HasPrefix(kv.k, "x-") {
			appMetadata[kv.k[2:]] = kv.v
		} else {
			sysMetadata[kv.k] = kv.v
		}
	}

	peer := strings.ToLower(sysMetadata["socket-type"])
	if c.proto.PeerName() != peer {
		return nil, mangos.ErrBadProto
	}

	return appMetadata, nil
}

// sendCmd sends a ZMTP command over the wire.
func (c *conn) sendCmd(name string, body []byte) error {
	if len(name) > 255 {
		return errBadCmd
	}

	n := len(name)
	buf := make([]byte, 0, 1+n+len(body))
	buf = append(buf, byte(n))
	buf = append(buf, []byte(name)...)
	buf = append(buf, body...)

	return c.send(true, buf)
}

// sendMsg sends a ZMTP message over the wire.
func (c *conn) sendMsg(body []byte) error {
	return c.send(false, body)
}

func (c *conn) send(isCommand bool, body []byte) error {
	var flag byte

	// Long flag
	size := len(body)
	isLong := size > 255
	switch {
	case isLong && isCommand:
		flag = 0x6
	case !isLong && isCommand:
		flag = 0x4
	case isLong && !isCommand:
		flag = 0x2
	case !isLong && !isCommand:
		flag = 0x0
	}

	// Write out the message itself
	if _, err := c.c.Write([]byte{flag}); err != nil {
		return err
	}

	if isLong {
		if err := binary.Write(c.c, byteOrder, int64(size)); err != nil {
			return err
		}
	} else {
		if err := binary.Write(c.c, byteOrder, uint8(size)); err != nil {
			return err
		}
	}

	if _, err := c.secu.Encrypt(c.c, body); err != nil {
		return err
	}

	return nil
}

// read returns the isCommand flag, the body of the message, and optionally an error
func (c *conn) read() (bool, []byte, error) {
	var header [2]byte

	// Read out the header
	_, err := io.ReadFull(c.c, header[:])
	if err != nil {
		return false, nil, err
	}

	fl := flag(header[0])

	// FIXME(sbinet): implement MORE commands
	if fl.hasMore() {
		return false, nil, errMoreCmd
	}

	// Determine the actual length of the body
	size := uint64(header[1])
	if fl.isLong() {
		var longHeader [8]byte
		// We read 2 bytes of the header already
		// In case of a long message, the length is bytes 2-8 of the header
		// We already have the first byte, so assign it, and then read the rest
		longHeader[0] = header[1]

		_, err = io.ReadFull(c.c, longHeader[1:])
		if err != nil {
			return false, nil, err
		}

		size = byteOrder.Uint64(longHeader[:])
	}

	if size > uint64(maxInt64) {
		return false, nil, errOverflow
	}

	body := make([]byte, size)
	_, err = io.ReadFull(c.c, body)
	if err != nil {
		return false, nil, err
	}

	return fl.isCommand(), body, nil
}

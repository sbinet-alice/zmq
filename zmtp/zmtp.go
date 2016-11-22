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

package zmtp

import "io"

// SecurityKind denotes kinds of ZMTP security mechanisms
type SecurityKind string

const (
	// NullSecurityKind is an empty security mechanism
	// that does no authentication nor encryption.
	NullSecurityKind SecurityKind = "NULL"

	// PlainSecurityKind is a security mechanism that uses
	// plaintext passwords. It is a reference implementation and
	// should not be used to anything important.
	PlainSecurityKind SecurityKind = "PLAIN"

	// CurveSecurityKind uses ZMQ_CURVE for authentication
	// and encryption.
	CurveSecurityKind SecurityKind = "CURVE"
)

// Security is an interface for ZMTP security mechanisms
type Security interface {
	Kind() SecurityKind
	// Handshake implements the ZMTP security handshake according to
	// this security mechanism.
	// see:
	//  https://rfc.zeromq.org/spec:23/ZMTP/
	//  https://rfc.zeromq.org/spec:24/ZMTP-PLAIN/
	//  https://rfc.zeromq.org/spec:25/ZMTP-CURVE/
	Handshake() error
	Encrypt(w io.Writer, data []byte) (int, error)
}

// SecurityNull implements the NullSecurityMechanismType
type SecurityNull struct{}

// Kind returns the security mechanisms type
func (s *SecurityNull) Kind() SecurityKind {
	return NullSecurityKind
}

// Handshake performs the ZMTP handshake for this
// security mechanism
func (s *SecurityNull) Handshake() error {
	return nil
}

// Encrypt encrypts a []byte to w
func (s *SecurityNull) Encrypt(w io.Writer, data []byte) (int, error) {
	return w.Write(data)
}

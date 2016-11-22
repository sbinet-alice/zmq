package tcp

import (
	"net"

	"github.com/go-mangos/mangos"
	"github.com/sbinet-alice/zmq"
)

type options map[string]interface{}

func newOptions() options {
	o := make(options)
	o[mangos.OptionNoDelay] = true
	o[mangos.OptionKeepAlive] = true
	return o
}

// get retrieves an option value.
func (o options) get(name string) (interface{}, error) {
	v, ok := o[name]
	if !ok {
		return nil, mangos.ErrBadOption
	}
	return v, nil
}

// set sets an option.
func (o options) set(name string, val interface{}) error {
	switch name {
	case mangos.OptionNoDelay:
		fallthrough
	case mangos.OptionKeepAlive:
		switch v := val.(type) {
		case bool:
			o[name] = v
			return nil
		default:
			return mangos.ErrBadValue
		}
	}
	return mangos.ErrBadOption
}

func (o options) configTCP(conn *net.TCPConn) error {
	if v, ok := o[mangos.OptionNoDelay]; ok {
		if err := conn.SetNoDelay(v.(bool)); err != nil {
			return err
		}
	}
	if v, ok := o[mangos.OptionKeepAlive]; ok {
		if err := conn.SetKeepAlive(v.(bool)); err != nil {
			return err
		}
	}
	return nil
}

type dialer struct {
	addr *net.TCPAddr
	sock mangos.Socket
	opts options
}

func (d *dialer) Dial() (mangos.Pipe, error) {

	conn, err := net.DialTCP("tcp", nil, d.addr)
	if err != nil {
		return nil, err
	}
	if err = d.opts.configTCP(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return zmq.NewConnPipe(conn, d.sock)
}

func (d *dialer) SetOption(n string, v interface{}) error {
	return d.opts.set(n, v)
}

func (d *dialer) GetOption(n string) (interface{}, error) {
	return d.opts.get(n)
}

type listener struct {
	addr     *net.TCPAddr
	bound    net.Addr
	sock     mangos.Socket
	listener *net.TCPListener
	opts     options
}

func (l *listener) Accept() (mangos.Pipe, error) {

	if l.listener == nil {
		return nil, mangos.ErrClosed
	}
	conn, err := l.listener.AcceptTCP()
	if err != nil {
		return nil, err
	}
	if err = l.opts.configTCP(conn); err != nil {
		conn.Close()
		return nil, err
	}
	return zmq.NewConnPipe(conn, l.sock)
}

func (l *listener) Listen() (err error) {
	l.listener, err = net.ListenTCP("tcp", l.addr)
	if err == nil {
		l.bound = l.listener.Addr()
	}
	return
}

func (l *listener) Address() string {
	if b := l.bound; b != nil {
		return "tcp://" + b.String()
	}
	return "tcp://" + l.addr.String()
}

func (l *listener) Close() error {
	l.listener.Close()
	return nil
}

func (l *listener) SetOption(n string, v interface{}) error {
	return l.opts.set(n, v)
}

func (l *listener) GetOption(n string) (interface{}, error) {
	return l.opts.get(n)
}

type tcpTran struct {
	localAddr net.Addr
}

func (t *tcpTran) Scheme() string {
	return "tcp"
}

func (t *tcpTran) NewDialer(addr string, sock mangos.Socket) (mangos.PipeDialer, error) {
	var err error
	d := &dialer{sock: sock, opts: newOptions()}

	if addr, err = mangos.StripScheme(t, addr); err != nil {
		return nil, err
	}

	if d.addr, err = mangos.ResolveTCPAddr(addr); err != nil {
		return nil, err
	}
	return d, nil
}

func (t *tcpTran) NewListener(addr string, sock mangos.Socket) (mangos.PipeListener, error) {
	var err error
	l := &listener{sock: sock, opts: newOptions()}

	if addr, err = mangos.StripScheme(t, addr); err != nil {
		return nil, err
	}

	if l.addr, err = mangos.ResolveTCPAddr(addr); err != nil {
		return nil, err
	}

	return l, nil
}

// NewTransport allocates a new TCP transport.
func NewTransport() mangos.Transport {
	return &tcpTran{}
}

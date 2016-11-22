package zmq_test

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mangos/mangos"
	"github.com/sbinet-alice/zmq/protocol/pub"
	"github.com/sbinet-alice/zmq/protocol/sub"
	"github.com/sbinet-alice/zmq/zmtp/tcp"
	czmq "github.com/zeromq/goczmq"
)

func TestPubSub(t *testing.T) {
	for _, tt := range []struct {
		name     string
		pub, sub func(t *testing.T, port string, n int, done chan int)
	}{
		{
			name: "zmq/zmq",
			pub:  testCZMQPub,
			sub:  testCZMQSub,
		},
		{
			name: "mangos/mangos",
			pub:  testMangosPub,
			sub:  testMangosSub,
		},
		{
			name: "zmq/mangos",
			pub:  testCZMQPub,
			sub:  testMangosSub,
		},
		{
			name: "mangos/zmq",
			pub:  testMangosPub,
			sub:  testCZMQSub,
		},
	} {
		tt := tt
		const N = 100

		port, err := getTCPPort()
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan int)
		t.Run("pubsub="+tt.name, func(t *testing.T) {
			go tt.pub(t, port, N, done)
			tt.sub(t, port, N, done)
		})
	}
}

func getTCPPort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return "", err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}

func testCZMQSub(t *testing.T, port string, N int, done chan int) {
	ep := "tcp://127.0.0.1:" + port
	sum := 0

	sub, err := czmq.NewSub(ep, "")
	if err != nil {
		panic(err)
	}

	defer sub.Destroy()

	sub.Connect(ep)

	for i := 0; i < N; i++ {
		msg, _, err := sub.RecvFrame()
		if err != nil {
			t.Fatalf("error receiving frame[%d]: %v\n", i, err)
		}

		weatherData := strings.Split(string(msg), " ")
		temperature, err := strconv.ParseInt(weatherData[1], 10, 64)
		if err == nil {
			sum += int(temperature)
		}
	}
	done <- 1
}

func testCZMQPub(t *testing.T, port string, N int, done chan int) {
	ep := "tcp://127.0.0.1:" + port
	pub, err := czmq.NewPub(ep)
	if err != nil {
		t.Fatal(err)
	}

	defer pub.Destroy()
	pub.Bind(ep)

loop:
	for {
		select {
		case <-done:
			break loop
		default:
			temperature := rand.Intn(100) - 50
			relHumidity := rand.Intn(50) + 10

			msg := fmt.Sprintf("%d %d", temperature, relHumidity)
			err := pub.SendFrame([]byte(msg), 0)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func testMangosPub(t *testing.T, port string, N int, done chan int) {
	ep := "tcp://127.0.0.1:" + port
	pub, err := pub.NewSocket()
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	pub.AddTransport(tcp.NewTransport())

	err = pub.Listen(ep)
	if err != nil {
		t.Fatal(err)
	}

loop:
	for {
		select {
		case <-done:
			break loop
		default:
			temperature := rand.Intn(100) - 50
			relHumidity := rand.Intn(50) + 10

			msg := fmt.Sprintf("%d %d", temperature, relHumidity)
			err := pub.Send([]byte(msg))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func testMangosSub(t *testing.T, port string, N int, done chan int) {
	ep := "tcp://127.0.0.1:" + port
	sum := 0

	sub, err := sub.NewSocket()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	sub.AddTransport(tcp.NewTransport())

	err = sub.SetOption(mangos.OptionSubscribe, []byte(""))
	if err != nil {
		t.Fatalf("subscription error: %v\n", err)
	}

	err = sub.Dial(ep)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		msg, err := sub.Recv()
		if err != nil {
			t.Fatal(err)
		}

		weatherData := strings.Split(string(msg), " ")
		temperature, err := strconv.ParseInt(weatherData[1], 10, 64)
		if err == nil {
			sum += int(temperature)
		}
	}
	done <- 1
}

package cloudhub

import (
	"fmt"
	"net"
	"os"
	"time"
)

// UnixDomainSocket struct
type UnixDomainSocket struct {
	filename string
	bufsize  int
	handler  func(string) string
}

// NewUnixDomainSocket create new socket
func NewUnixDomainSocket(filename string, size ...int) *UnixDomainSocket {
	size1 := 10480
	if size != nil {
		size1 = size[0]
	}
	us := UnixDomainSocket{filename: filename, bufsize: size1}
	return &us
}

// SetContextHandler set handler for [Server]
func (us *UnixDomainSocket) SetContextHandler(f func(string) string) {
	us.handler = f
}

// StartServer start for [Server]
func (us *UnixDomainSocket) StartServer() {
	proto, addr, err := parseEndpoint(us.filename)
	if err != nil {
		panic("Failed to parseEndpoint: " + err.Error())
	}
	if proto == "unix" {
		addr = "/" + addr
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) { //nolint: vetshadow
			panic("Failed to remove addr" + err.Error())
		}
	}

	// Listen
	listener, err := net.Listen(proto, addr)
	if err != nil {
		panic("Failed to listen" + err.Error())
	}
	defer listener.Close()
	fmt.Println("Listening on", listener.Addr())

	for {
		c, err := listener.Accept()
		fmt.Printf("Connected from %v", c)
		if err != nil {
			panic("Accept: " + err.Error())
		}
		go us.handleServerConn(c)
	}
}

// handleServerConn handler for [Server]
func (us *UnixDomainSocket) handleServerConn(c net.Conn) {
	defer c.Close()
	buf := make([]byte, us.bufsize)
	nr, err := c.Read(buf)
	if err != nil {
		panic("Read: " + err.Error())
	}
	result := us.handleServerContext(string(buf[0:nr]))
	_, err = c.Write([]byte(result))
	if err != nil {
		panic("Writes failed.")
	}
}

// HandleServerContext handler for [Server]
func (us *UnixDomainSocket) handleServerContext(context string) string {
	if us.handler != nil {
		return us.handler(context)
	}
	now := time.Now().String()
	return now
}

// Connect connect for [Client]
func (us *UnixDomainSocket) Connect() net.Conn {
	// parse
	proto, addr, err := parseEndpoint(us.filename)
	if err != nil {
		panic("Failed to parseEndpoint: " + err.Error())
	}

	// dial
	c, err := net.Dial(proto, addr)
	if err != nil {
		panic("Dial failed.")
	}
	return c
}

// Send msg for [Client]
func (us *UnixDomainSocket) Send(c net.Conn, context string) string {
	// send
	_, err := c.Write([]byte(context))
	if err != nil {
		panic("Writes failed.")
	}

	// read
	buf := make([]byte, us.bufsize)
	nr, err := c.Read(buf)
	if err != nil {
		panic("Read: " + err.Error())
	}
	return string(buf[0:nr])
}

package zmq4

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// Prefix used for socket-pair endpoint names in MakePair.
var PairPrefix = "socket-pair-"

var counter = uint64(0)

// ErrTimeout represents a timeout error
var ErrTimeout = errors.New("zmq timeout")

func nextID() uint64 {
	atomic.AddUint64(&counter, 1)
	return counter
}

// default HWM for the internal bridge sockets
const internalHWM = 1

// Channels provides a method for using Go channels to send and receive messages on a Socket. This is useful not only
// because it allows one to use select for sockets, but also because Sockets by themselves are not thread-safe (ie. one
// should not Send and Recv on the same socket from different threads).
type Channels struct {
	stopch   chan bool
	wg       sync.WaitGroup
	socket   *Socket       // Target socket
	insock   *Socket       // Read-end of outgoing messages socket
	outsock  *Socket       // Write-end of outgoing messages socket
	closein  *Socket       // Read-end of closing socket
	closeout *Socket       // Write-end of closing socket
	in       chan [][]byte // Incoming messages
	out      chan [][]byte // Outgoing messages
	errors   chan error    // Error notification channel
}

// MakePair creates a pair of connected inproc sockets that can be used for safe inter-thread communication.
// Returns both sockets.
func (c *Context) MakePair() (a *Socket, b *Socket) {
	var err error
	addr := fmt.Sprintf("inproc://%s-%d", PairPrefix, nextID())
	if a, err = c.NewSocket(PAIR); err != nil {
		goto Error
	}
	// set the recv high water mark to low values to prevent msgs from queuing within the socket
	// this is desirable, as we'd prefer that the channel be the place for msgs to queue up,
	// as the caller has direct control over the depth of the channel.
	// note HWMs must be set prior to binding/connecting
	a.SetRcvhwm(internalHWM)
	if err = a.Bind(addr); err != nil {
		goto Error
	}

	if b, err = c.NewSocket(PAIR); err != nil {
		goto Error
	}
	// set the send high water mark to low values to prevent msgs from queuing within the socket
	// this is desirable, as we'd prefer that the channel be the place for msgs to queue up,
	// as the caller has direct control over the depth of the channel.
	// note HWMs must be set prior to binding/connecting
	b.SetSndhwm(internalHWM)
	if err = b.Connect(addr); err != nil {
		goto Error
	}
	return

Error:
	if a != nil {
		a.Close()
	}
	if b != nil {
		b.Close()
	}
	panic(err)
}

// ChannelsBuffer creates a new Channels object with the given channel buffer size.
func (s *Socket) ChannelsBuffer(chanbuf int) (c *Channels) {
	s.SetRcvtimeo(0)
	s.SetSndtimeo(0)
	c = &Channels{
		stopch: make(chan bool),
		socket: s,
		in:     make(chan [][]byte, chanbuf),
		out:    make(chan [][]byte, chanbuf),
		errors: make(chan error, 2),
	}
	c.insock, c.outsock = s.ctx.MakePair()
	c.closein, c.closeout = s.ctx.MakePair()
	c.wg.Add(2)
	go c.processOutgoing()
	go c.processSockets()
	return
}

// Channels creates a new Channels object with the default channel buffer size (zero).
func (s *Socket) Channels() *Channels {
	return s.ChannelsBuffer(0)
}

// Close closes the Channels object. This will ensure that a number of internal sockets are closed, and that worker goroutines
// are stopped cleanly.
func (c *Channels) Close() {
	close(c.stopch)
	c.wg.Wait()
}

// In represents channel input
func (c *Channels) In() <-chan [][]byte {
	return c.in
}

// Out represents channel output
func (c *Channels) Out() chan<- [][]byte {
	return c.out
}

// Errors represents error channel
func (c *Channels) Errors() <-chan error {
	return c.errors
}

func (c *Channels) processOutgoing() {
	runtime.LockOSThread()
	defer c.wg.Done()
	defer c.outsock.Close()
	defer func() {
		c.closeout.SendMessage([]byte{})
		//c.closeout.SendPart([]byte{}, false)
		c.closeout.Close()
	}()
	for {
		select {
		case <-c.stopch:
			return
		case msg := <-c.out:
			if _, err := c.outsock.SendMessage(msg); err != nil {
				c.errors <- err
				goto Error
			}
		}
	}
Error:
	for {
		select {
		case <-c.stopch:
			return
		case _ = <-c.out:
			/* discard outgoing messages */
		}
	}
}

func (c *Channels) processSockets() {
	runtime.LockOSThread()
	defer c.wg.Done()
	defer c.insock.Close()
	defer c.closein.Close()
	defer close(c.in)

	poller := NewPoller()
	idxSock := poller.Add(c.socket, POLLIN)
	idxPairOut := poller.Add(c.insock, POLLIN)
	idxClose := poller.Add(c.closein, POLLIN)

	var sending [][]byte
	var sockets []Polled
	for {

		if sending == nil {
			poller.Update(0, POLLIN) // Don't monitor main socket for send events
			poller.Update(1, POLLIN) // Monitor the outgoing messages socket
		} else {
			poller.Update(0, POLLIN|POLLOUT) // Monitor the main socket for send events
			poller.Update(1, NONE)           // Don't monitor the outgoing messages socket, we're waiting for sending to go through
		}

		var err error
		if sockets, err = poller.PollAll(-1); err != nil {
			c.errors <- err
			goto Error
		}

		if sockets[idxSock].Events&POLLIN != 0 {
			// Receive a new incoming message
			incoming, err := c.socket.RecvMessageBytes(0)
			if err != nil {
				if err != ErrTimeout {
					c.errors <- err
					goto Error
				}
			} else {
				select {
				case c.in <- incoming:
				case <-c.stopch:
				}
			}
		}
		if sockets[idxSock].Events&POLLOUT != 0 && sending != nil {
			if _, err := c.socket.SendMessage(sending); err != nil {
				if err != ErrTimeout {
					c.errors <- err
					goto Error
				}
			} else {
				sending = nil
			}
		}

		if sockets[idxPairOut].Events&POLLIN != 0 && sending == nil {
			// Receive a new outgoing message
			outgoing, err := c.insock.RecvMessageBytes(0)
			if err != nil {
				c.errors <- err
				goto Error
			}
			if sending != nil {
				panic("sending should be nil")
			}
			sending = outgoing
		}

		if sockets[idxClose].Events&POLLIN != 0 {
			// Check for close message
			_, err := c.closein.RecvMessageBytes(0)
			if err != nil && err != ErrTimeout {
				c.errors <- err
				goto Error
			} else if err == nil {
				// We're done
				if sending != nil {
					c.sendFinal(sending)
				}
				return
			}
		}

	}
	return

Error:
	poller.Update(0, POLLIN)
	poller.Update(1, POLLIN)
	poller.Update(2, POLLIN)

	for {
		_, err := poller.Poll(-1)
		if err != nil {
			return
		}
		if len(sockets)-1 >= idxSock && sockets[idxSock].Events&POLLIN != 0 {
			// Discard new incoming message
			if _, err = c.socket.RecvMessageBytes(0); err != nil && err != ErrTimeout {
				return
			}
		}
		if len(sockets)-1 >= idxPairOut && sockets[idxPairOut].Events&POLLIN != 0 {
			// Discard outgoing message
			if _, err = c.insock.RecvMessageBytes(0); err != nil && err != ErrTimeout {
				return
			}
		}
		if len(sockets)-1 >= idxClose && sockets[idxClose].Events&POLLIN != 0 {
			_, err = c.closein.RecvMessageBytes(0)
			if err != nil && err != ErrTimeout {
				return
			} else if err == nil {
				return
			}
		}
	}

}

func (c *Channels) sendFinal(msg [][]byte) {
	linger, _ := c.socket.GetLinger()
	c.socket.SetSndtimeo(linger)
	c.socket.SendMessage(msg)
}

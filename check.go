package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	// The HTTP method used for each test
	HTTP_METHOD = "HEAD"
	// The HTTP URI
	HTTP_URI = "/CloudHealthCheck"
	// HTTP Host header
	HTTP_HOST = "ping"
	// Cloud Provider
	PROVIDER = "cloudControl"
	// Check the URL every 5 seconds
	CHECK_INTERVAL = 5
	// If the test keeps the same state for 5 min, stop it
	CHECK_DURATION = 300
	// Check every 1 minute if we break the check
	CHECK_BREAK_INTERVAL = 60
	// Connection timeout is 20 seconds by default
	CONNECTION_TIMEOUT = 20
	// IO timeout applies after the connection
	IO_TIMEOUT = 20
)

var (
	httpTransport      *http.Transport
	httpMethod         string
	httpUri            string
	httpHost           string
	httpUserAgent      string
	checkInterval      time.Duration
	checkDuration      = time.Duration(CHECK_DURATION) * time.Second
	checkBreakInterval = time.Duration(CHECK_BREAK_INTERVAL) * time.Second
	connectionTimeout  time.Duration
	ioTimeout          time.Duration
)

type Check struct {
	BackendUrl         string
	BackendId          int
	BackendGroupLength int
	FrontendKey        string

	// Goroutine unique signature
	routineSig string

	// Called when backend dies
	deadCallback func() bool
	// Called when the backend comes back to life
	aliveCallback func() bool
	// Called every CHECK_BREAK_INTERVAL to stop the routine if returned true
	checkIfBreakCallback func() bool
	// Called when the check exits
	exitCallback func()
}

func NewCheck(line string) (*Check, error) {
	parts := strings.Split(strings.TrimSpace(line), ";")
	if len(parts) != 4 {
		return nil, errors.New("Invalid check line")
	}
	u, err := url.Parse(parts[1])
	if err != nil {
		return nil, err
	}
	backendUrl := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	backendId, _ := strconv.Atoi(parts[2])
	backendGroupLength, _ := strconv.Atoi(parts[3])
	c := &Check{BackendUrl: backendUrl, BackendId: backendId,
		BackendGroupLength: backendGroupLength, FrontendKey: parts[0]}
	if len(httpUserAgent) == 0 {
		httpUserAgent = fmt.Sprintf("%s-HealthCheck/%s %s", PROVIDER, VERSION,
			runtime.Version())
	}
	return c, nil
}

func (c *Check) SetDeadCallback(callback func() bool) {
	c.deadCallback = callback
}

func (c *Check) SetAliveCallback(callback func() bool) {
	c.aliveCallback = callback
}

func (c *Check) SetCheckIfBreakCallback(callback func() bool) {
	c.checkIfBreakCallback = callback
}

func (c *Check) SetExitCallback(callback func()) {
	c.exitCallback = callback
}

func (c *Check) doHttpRequest() (*http.Response, error) {
	if httpTransport == nil {
		httpDial := func(proto string, addr string) (net.Conn, error) {
			conn, err := net.DialTimeout(proto, addr, connectionTimeout)
			if err != nil {
				return nil, err
			}
			conn.SetDeadline(time.Now().Add(ioTimeout))
			return conn, nil
		}
		httpTransport = &http.Transport{
			DisableKeepAlives:  true,
			DisableCompression: true,
			Dial:               httpDial,
		}
	}
	req, _ := http.NewRequest(httpMethod, c.BackendUrl, nil)
	req.URL.Path = httpUri
	req.Host = httpHost
	req.Header.Add("User-Agent", httpUserAgent)
	req.Close = true
	log.Println(c.FrontendKey, "Requesting", req.URL, "...")
	return httpTransport.RoundTrip(req)
}

func (c *Check) PingUrl(ch chan int) {
	// Current status, true for alive, false for dead
	var (
		lastDeadCall    time.Time
		lastStateChange = time.Now()
		status          = false
		newStatus       = true
		firstCheck      = true
		healthy         = false
		i               = time.Duration(0)
		n               = 1
	)
	for {
		select {
		case <-ch:
			// If we added a frontend to the mapping, we consider it's the
			// first check
			firstCheck = true
		default:
		}
		log.Println(c.FrontendKey, "Checking", c.BackendUrl, "for", n, "time.", time.Since(lastStateChange), "since last status change.")
		resp, err := c.doHttpRequest()
		if err != nil {
			// TCP error
			newStatus = false
			healthy = false
			log.Println(c.FrontendKey, "Response from", c.BackendUrl, "... TCP error:", err.Error())
		} else {
			// No TCP error, checking HTTP code
			if resp.StatusCode >= 500 && resp.StatusCode < 600 &&
				resp.StatusCode != 503 {
				newStatus = false
				healthy = false
				log.Println(c.FrontendKey, "Response from", c.BackendUrl, "... HTTP error:", resp.Status)
			} else {
				newStatus = true
				healthy = true
				log.Println(c.FrontendKey, "Response from", c.BackendUrl, "... OK", resp.StatusCode)
			}
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		// Check if the status changed before updating Redis
		if newStatus != status || firstCheck == true {
			lastStateChange = time.Now()
			if newStatus == true {
				if c.aliveCallback != nil {
					if r := c.aliveCallback(); r == false {
						log.Println(c.FrontendKey, "Backend", c.BackendUrl, "not found in Redis")
						break
					}
				}
				lastDeadCall = time.Time{}
			} else {
				if c.deadCallback != nil {
					if r := c.deadCallback(); r == false {
						log.Println(c.FrontendKey, "Backend", c.BackendUrl, "not found in Redis")
						break
					}
				}
				lastDeadCall = time.Now()
			}
		} else if newStatus == false {
			// Backend is still dead. Mark it as dead every 30 seconds to keep
			// it dead despite the Redis TTL
			if lastDeadCall.IsZero() == false &&
				time.Since(lastDeadCall) >=
					(time.Duration(30)*time.Second) {
				if c.deadCallback != nil {
					if r := c.deadCallback(); r == false {
						log.Println(c.FrontendKey, "Backend", c.BackendUrl, "not found in Redis")
						break
					}
				}
				lastDeadCall = time.Now()
			}
		}
		status = newStatus
		firstCheck = false
		time.Sleep(checkInterval)
		i += checkInterval
		n += 1
		// At longer interval, we check if still have the lock on the backend
		if i >= checkBreakInterval {
			if c.checkIfBreakCallback != nil &&
				c.checkIfBreakCallback() == true {
				log.Println(c.FrontendKey, "Backend", c.BackendUrl, " lost the lock")
				break
			}
			// Let's see if the check is in the same state for a while
			if time.Since(lastStateChange) >= checkDuration && healthy {
				log.Println(c.FrontendKey, "Backend", c.BackendUrl, " state is stable and healthy")
				break
			}
			i = time.Duration(0)
		}
	}
	if c.exitCallback != nil {
		log.Println(c.FrontendKey, "Removed check for backend", c.BackendUrl, "| ", runningCheckers-1, "backends being checked.")
		c.exitCallback()
	}
}

// Package consul is a consul implementation of lock
// fork from github.com/go-micro/plugins/v4/sync/consul. since the official implementation has wait time bug. and the pr has not been merged yet.
package syncr

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path"
	"strings"
	gosync "sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"github.com/hashicorp/go-hclog"
	"go-micro.dev/v4/sync"
)

type consulSync struct {
	options sync.Options
	path    string
	c       *api.Client

	mtx   gosync.Mutex
	locks map[string]*api.Lock
}

type consulElected struct {
	c   *api.Client
	l   *api.Lock
	id  string
	key string

	mtx gosync.RWMutex
	rv  <-chan struct{}
}

func (c *consulElected) Reelect() error {
	rv, err := c.l.Lock(nil)
	if err != nil {
		return err
	}

	c.mtx.Lock()
	c.rv = rv
	c.mtx.Unlock()
	return nil
}

func (c *consulElected) Revoked() chan bool {
	ch := make(chan bool, 1)
	c.mtx.RLock()
	rv := c.rv
	c.mtx.RUnlock()

	go func() {
		<-rv
		ch <- true
		close(ch)
	}()

	return ch
}

func (c *consulElected) Resign() error {
	return c.l.Unlock()
}

func (c *consulElected) Status() chan bool {
	ch := make(chan bool, 1)

	p, err := watch.Parse(map[string]interface{}{
		"type": "key",
		"key":  c.key,
	})
	if err != nil {
		return ch
	}
	p.Handler = func(idx uint64, raw interface{}) {
		if raw == nil {
			ch <- true
			return // ignore
		}
		v, ok := raw.(*api.KVPair)
		if !ok || v == nil || string(v.Value) != c.id {
			ch <- true
			return // ignore
		}
	}

	go p.RunWithClientAndHclog(c.c, hclog.New(&hclog.LoggerOptions{
		Name:   "consul",
		Output: os.Stdout,
	}))
	return ch
}

func (c *consulSync) Leader(id string, opts ...sync.LeaderOption) (sync.Leader, error) {
	var options sync.LeaderOptions
	for _, o := range opts {
		o(&options)
	}

	key := path.Join(c.path, strings.Replace(c.options.Prefix+id, "/", "-", -1))

	lc, err := c.c.LockOpts(&api.LockOptions{
		Key:   key,
		Value: []byte(id),
	})
	if err != nil {
		return nil, err
	}

	rv, err := lc.Lock(nil)
	if err != nil {
		return nil, err
	}

	return &consulElected{
		c:   c.c,
		key: c.path,
		rv:  rv,
		id:  id,
		l:   lc,
	}, nil
}

func (c *consulSync) Init(opts ...sync.Option) error {
	for _, o := range opts {
		o(&c.options)
	}
	return nil
}

func (c *consulSync) Options() sync.Options {
	return c.options
}

func (c *consulSync) Lock(id string, opts ...sync.LockOption) error {
	var options sync.LockOptions
	for _, o := range opts {
		o(&options)
	}

	if options.Wait < time.Duration(0) {
		options.Wait = 24 * time.Hour // wait long time if wait < 0, similar to forever
	} else if options.Wait == time.Duration(0) {
		// 0 will be replaced with default(15s), so use a small value instead
		// see github.com/hashicorp/consul/api@v1.25.1/lock.go:119
		options.Wait = 10 * time.Millisecond
	}

	rawKey := strings.Replace(c.options.Prefix+id, "/", "-", -1)
	rawKey = strings.Replace(rawKey, ",", "-", -1)
	rawKey = strings.Replace(rawKey, ":", "-", -1)
	key := path.Join(c.path, rawKey)

	l, err := c.c.LockOpts(&api.LockOptions{
		Key:          key,
		LockWaitTime: options.Wait,
		LockTryOnce:  true, // try until LockWaitTime
	})

	if err != nil {
		return err
	}

	ch, err := l.Lock(nil)
	if err != nil {
		return err
	}
	if ch == nil {
		// no err, no ch, means timeout
		return sync.ErrLockTimeout
	}

	c.mtx.Lock()
	c.locks[id] = l
	c.mtx.Unlock()

	if options.TTL > 0 {
		go func(currentLk *api.Lock) {
			// auto unlock after ttl
			time.Sleep(options.TTL)
			c.mtx.Lock()
			restore := c.locks[id]
			c.mtx.Unlock()

			// auto unlock only if the lock is still the same
			if restore == currentLk {
				_ = c.Unlock(id)
			}
		}(l)
	}

	return nil
}

func (c *consulSync) Unlock(id string) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	l, ok := c.locks[id]
	if !ok {
		return errors.New("lock not found")
	}
	err := l.Unlock()
	delete(c.locks, id)
	return err
}

func (c *consulSync) String() string {
	return "consul"
}

func NewConsulSync(opts ...sync.Option) sync.Sync {
	var options sync.Options
	for _, o := range opts {
		o(&options)
	}

	config := api.DefaultConfig()

	// set host
	// config.Host something
	// check if there are any addrs
	if len(options.Nodes) > 0 {
		addr, port, err := net.SplitHostPort(options.Nodes[0])
		if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
			port = "8500"
			config.Address = fmt.Sprintf("%s:%s", options.Nodes[0], port)
		} else if err == nil {
			config.Address = fmt.Sprintf("%s:%s", addr, port)
		}
	}

	client, _ := api.NewClient(config)

	return &consulSync{
		c:       client,
		options: options,
		locks:   make(map[string]*api.Lock),
	}
}

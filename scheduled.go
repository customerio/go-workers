package workers

import (
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type scheduled struct {
	keys   []string
	closed chan bool
	exit   chan bool
	wg     sync.WaitGroup
}

func (s *scheduled) start() {
	s.wg.Add(1)
	go (func() {
		defer s.wg.Done()
		for {
			s.poll()

			select {
			case <-s.closed:
				return
			case <-time.After(time.Duration(Config.PollInterval) * time.Second):
			}

		}
	})()
}

func (s *scheduled) quit() {
	close(s.closed)
	s.wg.Wait()
}

func (s *scheduled) poll() {
	conn := Config.Pool.Get()

	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = Config.Namespace + key
		for {
			messages, _ := redis.Strings(conn.Do("zrangebyscore", key, "-inf", now, "limit", 0, 1))

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsgFromString(messages[0])

			if removed, _ := redis.Bool(conn.Do("zrem", key, messages[0])); removed {
				queue := strings.TrimPrefix(message.queue, Config.Namespace)
				message.enqueuedAt = nowToSecondsWithNanoPrecision()
				conn.Do("lpush", Config.Namespace+"queue:"+queue, message.ToJson())
			}
		}
	}

	conn.Close()
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{
		keys:   keys,
		closed: make(chan bool),
		exit:   make(chan bool),
	}
}

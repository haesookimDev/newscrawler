package sessionstate

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	defaultRedisKey     = "xgen:sessions"
	defaultRedisTimeout = 5 * time.Second
)

// RedisStore implements Store using a minimal RESP client.
type RedisStore struct {
	addr     string
	password string
	db       int
	key      string
	timeout  time.Duration
}

// NewRedisStore creates a store backed by Redis.
func NewRedisStore(cfg RedisConfig) (Store, error) {
	if strings.TrimSpace(cfg.Host) == "" {
		return nil, fmt.Errorf("redis host is required")
	}
	port := cfg.Port
	if port == "" {
		port = "6379"
	}
	key := cfg.Key
	if key == "" {
		key = defaultRedisKey
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = defaultRedisTimeout
	}
	store := &RedisStore{
		addr:     net.JoinHostPort(cfg.Host, port),
		password: cfg.Password,
		db:       cfg.DB,
		key:      key,
		timeout:  timeout,
	}
	return store, nil
}

func (s *RedisStore) Close() error {
	return nil
}

func (s *RedisStore) Save(ctx context.Context, snap Snapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return s.withConn(ctx, func(conn *redisConn) error {
		if err := conn.send("HSET", s.key, snap.SessionID, string(data)); err != nil {
			return err
		}
		_, err := conn.read()
		return err
	})
}

func (s *RedisStore) Remove(ctx context.Context, sessionID string) error {
	return s.withConn(ctx, func(conn *redisConn) error {
		if err := conn.send("HDEL", s.key, sessionID); err != nil {
			return err
		}
		_, err := conn.read()
		return err
	})
}

func (s *RedisStore) Exists(ctx context.Context, sessionID string) (bool, error) {
	var exists bool
	err := s.withConn(ctx, func(conn *redisConn) error {
		if err := conn.send("HEXISTS", s.key, sessionID); err != nil {
			return err
		}
		reply, err := conn.read()
		if err != nil {
			return err
		}
		switch v := reply.(type) {
		case int64:
			exists = v == 1
		default:
			return fmt.Errorf("unexpected response: %T", v)
		}
		return nil
	})
	return exists, err
}

func (s *RedisStore) Get(ctx context.Context, sessionID string) (Snapshot, bool, error) {
	var snap Snapshot
	var found bool
	err := s.withConn(ctx, func(conn *redisConn) error {
		if err := conn.send("HGET", s.key, sessionID); err != nil {
			return err
		}
		reply, err := conn.read()
		if err != nil {
			return err
		}
		switch v := reply.(type) {
		case nil:
			found = false
		case string:
			if err := json.Unmarshal([]byte(v), &snap); err != nil {
				return err
			}
			found = true
		default:
			return fmt.Errorf("unexpected response type %T", v)
		}
		return nil
	})
	return snap, found, err
}

func (s *RedisStore) List(ctx context.Context) ([]Snapshot, error) {
	snapshots := []Snapshot{}
	err := s.withConn(ctx, func(conn *redisConn) error {
		if err := conn.send("HGETALL", s.key); err != nil {
			return err
		}
		reply, err := conn.read()
		if err != nil {
			return err
		}
		arr, ok := reply.([]interface{})
		if !ok {
			return nil
		}
		for i := 0; i+1 < len(arr); i += 2 {
			value, ok := arr[i+1].(string)
			if !ok {
				continue
			}
			var snap Snapshot
			if err := json.Unmarshal([]byte(value), &snap); err != nil {
				continue
			}
			snapshots = append(snapshots, snap)
		}
		return nil
	})
	return snapshots, err
}

func (s *RedisStore) withConn(ctx context.Context, fn func(*redisConn) error) error {
	conn, err := newRedisConn(ctx, s.addr, s.timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.initialize(s.password, s.db); err != nil {
		return err
	}
	return fn(conn)
}

type redisConn struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func newRedisConn(ctx context.Context, addr string, timeout time.Duration) (*redisConn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	c, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &redisConn{
		conn:   c,
		reader: bufio.NewReader(c),
		writer: bufio.NewWriter(c),
	}, nil
}

func (c *redisConn) initialize(password string, db int) error {
	if password != "" {
		if err := c.send("AUTH", password); err != nil {
			return err
		}
		if _, err := c.read(); err != nil {
			return err
		}
	}
	if db != 0 {
		if err := c.send("SELECT", strconv.Itoa(db)); err != nil {
			return err
		}
		if _, err := c.read(); err != nil {
			return err
		}
	}
	return nil
}

func (c *redisConn) send(cmd string, args ...string) error {
	if _, err := fmt.Fprintf(c.writer, "*%d\r\n", len(args)+1); err != nil {
		return err
	}
	if err := writeBulk(c.writer, strings.ToUpper(cmd)); err != nil {
		return err
	}
	for _, arg := range args {
		if err := writeBulk(c.writer, arg); err != nil {
			return err
		}
	}
	return c.writer.Flush()
}

func writeBulk(w *bufio.Writer, value string) error {
	if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(value), value); err != nil {
		return err
	}
	return nil
}

func (c *redisConn) read() (interface{}, error) {
	prefix, err := c.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '+':
		line, err := readLine(c.reader)
		if err != nil {
			return nil, err
		}
		return line, nil
	case '-':
		line, err := readLine(c.reader)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(line)
	case ':':
		line, err := readLine(c.reader)
		if err != nil {
			return nil, err
		}
		val, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, err
		}
		return val, nil
	case '$':
		line, err := readLine(c.reader)
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(c.reader, buf); err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*':
		line, err := readLine(c.reader)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return nil, err
		}
		if count == -1 {
			return nil, nil
		}
		items := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			item, err := c.read()
			if err != nil {
				return nil, err
			}
			items = append(items, item)
		}
		return items, nil
	default:
		return nil, fmt.Errorf("unexpected redis prefix %q", prefix)
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line, nil
}

func (c *redisConn) Close() error {
	return c.conn.Close()
}

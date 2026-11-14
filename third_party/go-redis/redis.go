// Package redis provides a Redis 7+ client supporting RESP2 over TCP, pipelines, Lua scripts, and an in-memory engine for tests.
package redis

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Nil is returned when a Redis key does not exist.
const Nil = RedisError("redis: nil")

// RedisError represents an error returned by Redis.
type RedisError string

func (e RedisError) Error() string { return string(e) }

// Options configures a Redis Client.
type Options struct {
	Addr         string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

// Scripter is implemented by Client and Pipeliner to execute Lua scripts.
type Scripter interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) *Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *Cmd
}

// Script represents a Redis Lua script with automatic SHA1 computation.
type Script struct {
	src  string
	hash string
}

// NewScript creates a new Lua Script instance.
func NewScript(src string) *Script {
	h := sha1.Sum([]byte(src))
	return &Script{
		src:  src,
		hash: hex.EncodeToString(h[:]),
	}
}

// Hash returns the SHA1 hex digest of the Lua script.
func (s *Script) Hash() string {
	return s.hash
}

// Run executes the script using Eval.
func (s *Script) Run(ctx context.Context, c Scripter, keys []string, args ...any) *Cmd {
	return c.Eval(ctx, s.src, keys, args...)
}

// Client is a concurrency-safe Redis client.
type Client struct {
	opt   *Options
	inMem *inMemoryStore
}

// NewClient creates a Redis client targeting opt.Addr.
func NewClient(opt *Options) *Client {
	if opt == nil {
		opt = &Options{Addr: "localhost:6379"}
	}
	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}
	return &Client{opt: opt}
}

// NewInMemoryClient creates an isolated, concurrency-safe in-memory Redis client for unit tests.
func NewInMemoryClient() *Client {
	return &Client{
		opt:   &Options{Addr: "in-memory"},
		inMem: newInMemoryStore(),
	}
}

// Close closes the client.
func (c *Client) Close() error {
	return nil
}

// Ping checks connectivity to Redis.
func (c *Client) Ping(ctx context.Context) *StatusCmd {
	if c.inMem != nil {
		return &StatusCmd{val: "PONG"}
	}
	res, err := c.doRESP(ctx, "PING")
	if err != nil {
		return &StatusCmd{err: err}
	}
	return &StatusCmd{val: fmt.Sprint(res)}
}

// Get retrieves the string value of key.
func (c *Client) Get(ctx context.Context, key string) *StringCmd {
	if c.inMem != nil {
		val, ok := c.inMem.get(key)
		if !ok {
			return &StringCmd{err: Nil}
		}
		return &StringCmd{val: val}
	}
	res, err := c.doRESP(ctx, "GET", key)
	if err != nil {
		return &StringCmd{err: err}
	}
	if res == nil {
		return &StringCmd{err: Nil}
	}
	return &StringCmd{val: fmt.Sprint(res)}
}

// Set sets key to value with optional expiration.
func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) *StatusCmd {
	strVal := fmt.Sprint(value)
	if c.inMem != nil {
		c.inMem.set(key, strVal, expiration)
		return &StatusCmd{val: "OK"}
	}
	args := []any{"SET", key, strVal}
	if expiration > 0 {
		ms := expiration.Milliseconds()
		if ms <= 0 {
			ms = 1
		}
		args = append(args, "PX", strconv.FormatInt(ms, 10))
	}
	res, err := c.doRESP(ctx, args...)
	if err != nil {
		return &StatusCmd{err: err}
	}
	return &StatusCmd{val: fmt.Sprint(res)}
}

// SetNX sets key to value if key does not exist, with optional expiration.
func (c *Client) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *BoolCmd {
	strVal := fmt.Sprint(value)
	if c.inMem != nil {
		ok := c.inMem.setNX(key, strVal, expiration)
		return &BoolCmd{val: ok}
	}
	args := []any{"SET", key, strVal, "NX"}
	if expiration > 0 {
		ms := expiration.Milliseconds()
		if ms <= 0 {
			ms = 1
		}
		args = append(args, "PX", strconv.FormatInt(ms, 10))
	}
	res, err := c.doRESP(ctx, args...)
	if err != nil {
		if errors.Is(err, Nil) {
			return &BoolCmd{val: false}
		}
		return &BoolCmd{err: err}
	}
	return &BoolCmd{val: res != nil}
}

// Del deletes one or more keys.
func (c *Client) Del(ctx context.Context, keys ...string) *IntCmd {
	if len(keys) == 0 {
		return &IntCmd{val: 0}
	}
	if c.inMem != nil {
		n := c.inMem.del(keys...)
		return &IntCmd{val: n}
	}
	args := make([]any, 0, 1+len(keys))
	args = append(args, "DEL")
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := c.doRESP(ctx, args...)
	if err != nil {
		return &IntCmd{err: err}
	}
	n, _ := toInt64(res)
	return &IntCmd{val: n}
}

// Exists returns the count of existing keys.
func (c *Client) Exists(ctx context.Context, keys ...string) *IntCmd {
	if len(keys) == 0 {
		return &IntCmd{val: 0}
	}
	if c.inMem != nil {
		n := c.inMem.exists(keys...)
		return &IntCmd{val: n}
	}
	args := make([]any, 0, 1+len(keys))
	args = append(args, "EXISTS")
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := c.doRESP(ctx, args...)
	if err != nil {
		return &IntCmd{err: err}
	}
	n, _ := toInt64(res)
	return &IntCmd{val: n}
}

// Incr increments the integer value of key by 1.
func (c *Client) Incr(ctx context.Context, key string) *IntCmd {
	if c.inMem != nil {
		val, err := c.inMem.incr(key)
		return &IntCmd{val: val, err: err}
	}
	res, err := c.doRESP(ctx, "INCR", key)
	if err != nil {
		return &IntCmd{err: err}
	}
	n, _ := toInt64(res)
	return &IntCmd{val: n}
}

// RPush appends one or more values to a list.
func (c *Client) RPush(ctx context.Context, key string, values ...any) *IntCmd {
	if c.inMem != nil {
		n := c.inMem.rpush(key, values...)
		return &IntCmd{val: n}
	}
	args := make([]any, 0, 2+len(values))
	args = append(args, "RPUSH", key)
	args = append(args, values...)
	res, err := c.doRESP(ctx, args...)
	if err != nil {
		return &IntCmd{err: err}
	}
	n, _ := toInt64(res)
	return &IntCmd{val: n}
}

// LPush prepends one or more values to a list.
func (c *Client) LPush(ctx context.Context, key string, values ...any) *IntCmd {
	if c.inMem != nil {
		n := c.inMem.lpush(key, values...)
		return &IntCmd{val: n}
	}
	args := make([]any, 0, 2+len(values))
	args = append(args, "LPUSH", key)
	args = append(args, values...)
	res, err := c.doRESP(ctx, args...)
	if err != nil {
		return &IntCmd{err: err}
	}
	n, _ := toInt64(res)
	return &IntCmd{val: n}
}

// LRange returns the specified elements of the list stored at key.
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd {
	if c.inMem != nil {
		vals := c.inMem.lrange(key, int(start), int(stop))
		return &StringSliceCmd{val: vals}
	}
	res, err := c.doRESP(ctx, "LRANGE", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10))
	if err != nil {
		return &StringSliceCmd{err: err}
	}
	slice, _ := toStringSlice(res)
	return &StringSliceCmd{val: slice}
}

// LTrim trims an existing list so that it will contain only the specified range of elements.
func (c *Client) LTrim(ctx context.Context, key string, start, stop int64) *StatusCmd {
	if c.inMem != nil {
		c.inMem.ltrim(key, int(start), int(stop))
		return &StatusCmd{val: "OK"}
	}
	res, err := c.doRESP(ctx, "LTRIM", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10))
	if err != nil {
		return &StatusCmd{err: err}
	}
	return &StatusCmd{val: fmt.Sprint(res)}
}

// LLen returns the length of the list stored at key.
func (c *Client) LLen(ctx context.Context, key string) *IntCmd {
	if c.inMem != nil {
		return &IntCmd{val: c.inMem.llen(key)}
	}
	res, err := c.doRESP(ctx, "LLEN", key)
	if err != nil {
		return &IntCmd{err: err}
	}
	n, _ := toInt64(res)
	return &IntCmd{val: n}
}

// Expire sets a timeout on key.
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *BoolCmd {
	if c.inMem != nil {
		ok := c.inMem.expire(key, expiration)
		return &BoolCmd{val: ok}
	}
	sec := int64(expiration.Seconds())
	if sec <= 0 {
		sec = 1
	}
	res, err := c.doRESP(ctx, "EXPIRE", key, strconv.FormatInt(sec, 10))
	if err != nil {
		return &BoolCmd{err: err}
	}
	n, _ := toInt64(res)
	return &BoolCmd{val: n == 1}
}

// Eval executes a Lua script on the Redis server or in-memory engine.
func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...any) *Cmd {
	if c.inMem != nil {
		val, err := c.inMem.evalLua(script, keys, args...)
		return &Cmd{val: val, err: err}
	}
	cmdArgs := make([]any, 0, 2+len(keys)+len(args))
	cmdArgs = append(cmdArgs, "EVAL", script, strconv.Itoa(len(keys)))
	for _, k := range keys {
		cmdArgs = append(cmdArgs, k)
	}
	cmdArgs = append(cmdArgs, args...)
	res, err := c.doRESP(ctx, cmdArgs...)
	return &Cmd{val: res, err: err}
}

// EvalSha executes a cached Lua script by its SHA1 hash.
func (c *Client) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *Cmd {
	cmdArgs := make([]any, 0, 2+len(keys)+len(args))
	cmdArgs = append(cmdArgs, "EVALSHA", sha1, strconv.Itoa(len(keys)))
	for _, k := range keys {
		cmdArgs = append(cmdArgs, k)
	}
	cmdArgs = append(cmdArgs, args...)
	res, err := c.doRESP(ctx, cmdArgs...)
	return &Cmd{val: res, err: err}
}

// Pipeliner queues multiple Redis operations for batch execution.
type Pipeliner interface {
	Scripter
	RPush(ctx context.Context, key string, values ...any) *IntCmd
	LPush(ctx context.Context, key string, values ...any) *IntCmd
	LTrim(ctx context.Context, key string, start, stop int64) *StatusCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *BoolCmd
	Del(ctx context.Context, keys ...string) *IntCmd
	Set(ctx context.Context, key string, value any, expiration time.Duration) *StatusCmd
	Exec(ctx context.Context) ([]Cmder, error)
}

// Pipeline returns a new Pipeliner.
func (c *Client) Pipeline() Pipeliner {
	return &pipeline{client: c}
}

// TxPipeline returns a new transactional Pipeliner.
func (c *Client) TxPipeline() Pipeliner {
	return &pipeline{client: c}
}

type pipeline struct {
	client *Client
	ops    []func(ctx context.Context) Cmder
}

func (p *pipeline) Eval(ctx context.Context, script string, keys []string, args ...any) *Cmd {
	cmd := &Cmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.Eval(c, script, keys, args...)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *Cmd {
	cmd := &Cmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.EvalSha(c, sha1, keys, args...)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) RPush(_ context.Context, key string, values ...any) *IntCmd {
	cmd := &IntCmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.RPush(c, key, values...)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) LPush(_ context.Context, key string, values ...any) *IntCmd {
	cmd := &IntCmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.LPush(c, key, values...)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) LTrim(_ context.Context, key string, start, stop int64) *StatusCmd {
	cmd := &StatusCmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.LTrim(c, key, start, stop)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) Expire(_ context.Context, key string, expiration time.Duration) *BoolCmd {
	cmd := &BoolCmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.Expire(c, key, expiration)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) Del(_ context.Context, keys ...string) *IntCmd {
	cmd := &IntCmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.Del(c, keys...)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) Set(_ context.Context, key string, value any, expiration time.Duration) *StatusCmd {
	cmd := &StatusCmd{}
	p.ops = append(p.ops, func(c context.Context) Cmder {
		res := p.client.Set(c, key, value, expiration)
		*cmd = *res
		return cmd
	})
	return cmd
}

func (p *pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	cmds := make([]Cmder, 0, len(p.ops))
	var firstErr error
	for _, op := range p.ops {
		cmd := op(ctx)
		cmds = append(cmds, cmd)
		if cmd.Err() != nil && firstErr == nil {
			firstErr = cmd.Err()
		}
	}
	p.ops = nil
	return cmds, firstErr
}

// Cmder is the common interface implemented by all Redis command results.
type Cmder interface {
	Err() error
}

// StatusCmd holds a status string result.
type StatusCmd struct {
	val string
	err error
}

func (c *StatusCmd) Val() string               { return c.val }
func (c *StatusCmd) Err() error                { return c.err }
func (c *StatusCmd) Result() (string, error)   { return c.val, c.err }

// StringCmd holds a string result.
type StringCmd struct {
	val string
	err error
}

func (c *StringCmd) Val() string               { return c.val }
func (c *StringCmd) Err() error                { return c.err }
func (c *StringCmd) Result() (string, error)   { return c.val, c.err }
func (c *StringCmd) Int64() (int64, error) {
	if c.err != nil {
		return 0, c.err
	}
	return strconv.ParseInt(c.val, 10, 64)
}

// BoolCmd holds a boolean result.
type BoolCmd struct {
	val bool
	err error
}

func (c *BoolCmd) Val() bool                 { return c.val }
func (c *BoolCmd) Err() error                { return c.err }
func (c *BoolCmd) Result() (bool, error)     { return c.val, c.err }

// IntCmd holds an int64 result.
type IntCmd struct {
	val int64
	err error
}

func (c *IntCmd) Val() int64                { return c.val }
func (c *IntCmd) Err() error                { return c.err }
func (c *IntCmd) Result() (int64, error)    { return c.val, c.err }

// StringSliceCmd holds a []string result.
type StringSliceCmd struct {
	val []string
	err error
}

func (c *StringSliceCmd) Val() []string              { return c.val }
func (c *StringSliceCmd) Err() error                 { return c.err }
func (c *StringSliceCmd) Result() ([]string, error)  { return c.val, c.err }

// Cmd holds an arbitrary Redis command result (such as from EVAL).
type Cmd struct {
	val any
	err error
}

func (c *Cmd) Val() any                  { return c.val }
func (c *Cmd) Err() error                { return c.err }
func (c *Cmd) Result() (any, error)      { return c.val, c.err }
func (c *Cmd) Int64() (int64, error) {
	if c.err != nil {
		return 0, c.err
	}
	return toInt64(c.val)
}
func (c *Cmd) StringSlice() ([]string, error) {
	if c.err != nil {
		return nil, c.err
	}
	return toStringSlice(c.val)
}
func (c *Cmd) Slice() ([]any, error) {
	if c.err != nil {
		return nil, c.err
	}
	if s, ok := c.val.([]any); ok {
		return s, nil
	}
	if ss, ok := c.val.([]string); ok {
		out := make([]any, len(ss))
		for i, v := range ss {
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("redis: unexpected slice type %T", c.val)
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		return 0, fmt.Errorf("redis: cannot convert %T to int64", v)
	}
}

func toStringSlice(v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	switch val := v.(type) {
	case []string:
		return val, nil
	case []any:
		res := make([]string, 0, len(val))
		for _, item := range val {
			if item != nil {
				res = append(res, fmt.Sprint(item))
			}
		}
		return res, nil
	default:
		return nil, fmt.Errorf("redis: cannot convert %T to []string", v)
	}
}

// RESP2 wire protocol implementation
func (c *Client) doRESP(ctx context.Context, args ...any) (any, error) {
	var d net.Dialer
	if c.opt.DialTimeout > 0 {
		d.Timeout = c.opt.DialTimeout
	}
	conn, err := d.DialContext(ctx, "tcp", c.opt.Addr)
	if err != nil {
		return nil, fmt.Errorf("redis: dial %s: %w", c.opt.Addr, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}

	br := bufio.NewReader(conn)

	if c.opt.Password != "" {
		if err := writeRESPCommand(conn, "AUTH", c.opt.Password); err != nil {
			return nil, err
		}
		if _, err := readRESPValue(br); err != nil {
			return nil, err
		}
	}
	if c.opt.DB > 0 {
		if err := writeRESPCommand(conn, "SELECT", strconv.Itoa(c.opt.DB)); err != nil {
			return nil, err
		}
		if _, err := readRESPValue(br); err != nil {
			return nil, err
		}
	}

	if err := writeRESPCommand(conn, args...); err != nil {
		return nil, err
	}
	return readRESPValue(br)
}

func writeRESPCommand(w io.Writer, args ...any) error {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, a := range args {
		s := fmt.Sprint(a)
		b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func readRESPValue(br *bufio.Reader) (any, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 {
		return nil, errors.New("redis: empty RESP line")
	}
	prefix := line[0]
	payload := line[1:]
	switch prefix {
	case '+':
		return payload, nil
	case '-':
		return nil, RedisError(payload)
	case ':':
		return strconv.ParseInt(payload, 10, 64)
	case '$':
		length, err := strconv.Atoi(payload)
		if err != nil {
			return nil, err
		}
		if length < 0 {
			return nil, Nil
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*':
		count, err := strconv.Atoi(payload)
		if err != nil {
			return nil, err
		}
		if count < 0 {
			return nil, Nil
		}
		arr := make([]any, count)
		for i := 0; i < count; i++ {
			v, err := readRESPValue(br)
			if err != nil && !errors.Is(err, Nil) {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("redis: unknown RESP prefix %q", prefix)
	}
}

// In-memory store for deterministic unit testing without external Redis daemon
type memEntry struct {
	strVal    string
	listVal   []string
	isList    bool
	expiresAt time.Time
}

type inMemoryStore struct {
	mu   sync.Mutex
	data map[string]*memEntry
}

func newInMemoryStore() *inMemoryStore {
	return &inMemoryStore{
		data: make(map[string]*memEntry),
	}
}

func (m *inMemoryStore) purgeIfExpiredLocked(key string) (*memEntry, bool) {
	e, ok := m.data[key]
	if !ok {
		return nil, false
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		delete(m.data, key)
		return nil, false
	}
	return e, true
}

func (m *inMemoryStore) get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok || e.isList {
		return "", false
	}
	return e.strVal, true
}

func (m *inMemoryStore) set(key, val string, exp time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var expAt time.Time
	if exp > 0 {
		expAt = time.Now().Add(exp)
	}
	m.data[key] = &memEntry{strVal: val, expiresAt: expAt}
}

func (m *inMemoryStore) setNX(key, val string, exp time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.purgeIfExpiredLocked(key); ok {
		return false
	}
	var expAt time.Time
	if exp > 0 {
		expAt = time.Now().Add(exp)
	}
	m.data[key] = &memEntry{strVal: val, expiresAt: expAt}
	return true
}

func (m *inMemoryStore) del(keys ...string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var count int64
	for _, k := range keys {
		if _, ok := m.purgeIfExpiredLocked(k); ok {
			delete(m.data, k)
			count++
		}
	}
	return count
}

func (m *inMemoryStore) exists(keys ...string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var count int64
	for _, k := range keys {
		if _, ok := m.purgeIfExpiredLocked(k); ok {
			count++
		}
	}
	return count
}

func (m *inMemoryStore) incr(key string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok {
		m.data[key] = &memEntry{strVal: "1"}
		return 1, nil
	}
	n, err := strconv.ParseInt(e.strVal, 10, 64)
	if err != nil {
		return 0, err
	}
	n++
	e.strVal = strconv.FormatInt(n, 10)
	return n, nil
}

func (m *inMemoryStore) rpush(key string, values ...any) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok {
		e = &memEntry{isList: true}
		m.data[key] = e
	}
	for _, v := range values {
		e.listVal = append(e.listVal, fmt.Sprint(v))
	}
	return int64(len(e.listVal))
}

func (m *inMemoryStore) lpush(key string, values ...any) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok {
		e = &memEntry{isList: true}
		m.data[key] = e
	}
	for _, v := range values {
		e.listVal = append([]string{fmt.Sprint(v)}, e.listVal...)
	}
	return int64(len(e.listVal))
}

func (m *inMemoryStore) lrange(key string, start, stop int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lrangeLocked(key, start, stop)
}

func (m *inMemoryStore) lrangeLocked(key string, start, stop int) []string {
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok || !e.isList || len(e.listVal) == 0 {
		return []string{}
	}
	n := len(e.listVal)
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return []string{}
	}
	out := make([]string, stop-start+1)
	copy(out, e.listVal[start:stop+1])
	return out
}

func (m *inMemoryStore) ltrim(key string, start, stop int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.lrangeLocked(key, start, stop)
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok || !e.isList {
		return
	}
	if len(kept) == 0 {
		delete(m.data, key)
		return
	}
	e.listVal = kept
}

func (m *inMemoryStore) llen(key string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok || !e.isList {
		return 0
	}
	return int64(len(e.listVal))
}

func (m *inMemoryStore) expire(key string, exp time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.purgeIfExpiredLocked(key)
	if !ok {
		return false
	}
	e.expiresAt = time.Now().Add(exp)
	return true
}

// evalLua atomically executes the Redis Lua scripts used by the debouncer and session store.
func (m *inMemoryStore) evalLua(script string, keys []string, args ...any) (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	upper := strings.ToUpper(script)

	// Case 1: Push script (INCR seqKey, RPUSH msgsKey payload, EXPIRE both, return {seq, count})
	if strings.Contains(upper, "INCR") && strings.Contains(upper, "RPUSH") && len(keys) >= 2 && len(args) >= 2 {
		seqKey := keys[0]
		msgsKey := keys[1]
		payload := fmt.Sprint(args[0])
		ttlSec, _ := strconv.Atoi(fmt.Sprint(args[1]))
		if ttlSec <= 0 {
			ttlSec = 120
		}
		expAt := time.Now().Add(time.Duration(ttlSec) * time.Second)

		seqEntry, ok := m.purgeIfExpiredLocked(seqKey)
		var seq int64 = 1
		if ok {
			n, _ := strconv.ParseInt(seqEntry.strVal, 10, 64)
			seq = n + 1
		}
		m.data[seqKey] = &memEntry{strVal: strconv.FormatInt(seq, 10), expiresAt: expAt}

		msgEntry, ok := m.purgeIfExpiredLocked(msgsKey)
		if !ok {
			msgEntry = &memEntry{isList: true}
			m.data[msgsKey] = msgEntry
		}
		msgEntry.listVal = append(msgEntry.listVal, payload)
		msgEntry.expiresAt = expAt

		return []any{seq, int64(len(msgEntry.listVal))}, nil
	}

	// Case 2: Flush script (check seqKey == expectedSeq or unconditional force flush, LRANGE msgsKey 0 -1, DEL seqKey msgsKey)
	if strings.Contains(upper, "LRANGE") && strings.Contains(upper, "DEL") && len(keys) >= 2 {
		seqKey := keys[0]
		msgsKey := keys[1]
		if len(args) >= 1 {
			expectedSeq := fmt.Sprint(args[0])
			if expectedSeq != "" && expectedSeq != "0" {
				seqEntry, ok := m.purgeIfExpiredLocked(seqKey)
				if !ok || seqEntry.strVal != expectedSeq {
					return []any{}, nil
				}
			}
		}
		msgs := m.lrangeLocked(msgsKey, 0, -1)
		delete(m.data, seqKey)
		delete(m.data, msgsKey)
		out := make([]any, len(msgs))
		for i, msg := range msgs {
			out[i] = msg
		}
		return out, nil
	}

	return nil, errors.New("redis: unsupported lua script in in-memory mode")
}

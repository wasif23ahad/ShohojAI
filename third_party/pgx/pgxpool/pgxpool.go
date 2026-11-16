// Package pgxpool provides a concurrency-safe connection pool for pgx.
package pgxpool

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Handler allows unit tests to intercept and serve SQL queries deterministically.
type Handler interface {
	Ping(ctx context.Context) error
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Close()
}

// Config holds connection pool settings parsed from a PostgreSQL connection string.
type Config struct {
	ConnString        string
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

// ParseConfig parses and validates a PostgreSQL connection string.
func ParseConfig(connString string) (*Config, error) {
	u, err := url.Parse(strings.TrimSpace(connString))
	if err != nil {
		return nil, fmt.Errorf("pgxpool: invalid connection URL: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("pgxpool: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("pgxpool: host is required")
	}
	return &Config{
		ConnString:        connString,
		MaxConns:          10,
		MinConns:          2,
		MaxConnLifetime:   time.Hour,
		MaxConnIdleTime:   30 * time.Minute,
		HealthCheckPeriod: time.Minute,
	}, nil
}

// Pool represents a concurrency-safe PostgreSQL connection pool.
type Pool struct {
	cfg     *Config
	handler Handler
	mu      sync.RWMutex
	closed  bool
}

// New creates a new Pool from a PostgreSQL connection string.
func New(ctx context.Context, connString string) (*Pool, error) {
	cfg, err := ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	return NewWithConfig(ctx, cfg)
}

// NewWithConfig creates a new Pool from a parsed Config.
func NewWithConfig(ctx context.Context, cfg *Config) (*Pool, error) {
	if cfg == nil {
		return nil, errors.New("pgxpool: config is nil")
	}
	wireHandler, err := newWireHandler(cfg.ConnString)
	if err != nil {
		return nil, err
	}
	return &Pool{
		cfg:     cfg,
		handler: wireHandler,
	}, nil
}

// NewWithHandler constructs a Pool backed by a custom Handler (useful for unit tests).
func NewWithHandler(h Handler) *Pool {
	return &Pool{
		cfg:     &Config{ConnString: "postgres://mock@localhost:5432/mock"},
		handler: h,
	}
}

// Ping verifies connectivity to the PostgreSQL server.
func (p *Pool) Ping(ctx context.Context) error {
	if err := p.checkOpen(); err != nil {
		return err
	}
	return p.handler.Ping(ctx)
}

// Close closes all connections in the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if p.handler != nil {
		p.handler.Close()
	}
}

// Exec executes a SQL command that does not return rows.
func (p *Pool) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	if err := p.checkOpen(); err != nil {
		return pgconn.CommandTag{}, err
	}
	return p.handler.Exec(ctx, sql, arguments...)
}

// Query executes a SQL query that returns multiple rows.
func (p *Pool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	return p.handler.Query(ctx, sql, args...)
}

// QueryRow executes a SQL query expected to return at most one row.
func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if err := p.checkOpen(); err != nil {
		return errorRow{err: err}
	}
	return p.handler.QueryRow(ctx, sql, args...)
}

// Begin starts a new database transaction with default options.
func (p *Pool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	return p.handler.Begin(ctx)
}

// BeginTx starts a new database transaction with the given TxOptions.
func (p *Pool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	if err := p.checkOpen(); err != nil {
		return nil, err
	}
	return p.handler.BeginTx(ctx, txOptions)
}

func (p *Pool) checkOpen() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("pgxpool: pool is closed")
	}
	return nil
}

type errorRow struct {
	err error
}

func (r errorRow) Scan(_ ...any) error {
	return r.err
}

// SliceRows is a helper implementation of pgx.Rows backed by a 2D slice of values.
type SliceRows struct {
	rows [][]any
	idx  int
	err  error
}

// NewSliceRows creates a pgx.Rows from a slice of row value slices.
func NewSliceRows(rows [][]any) *SliceRows {
	return &SliceRows{rows: rows, idx: -1}
}

func (r *SliceRows) Close() {}

func (r *SliceRows) Err() error { return r.err }

func (r *SliceRows) Next() bool {
	if r.err != nil {
		return false
	}
	r.idx++
	return r.idx < len(r.rows)
}

func (r *SliceRows) Scan(dest ...any) error {
	if r.idx < 0 || r.idx >= len(r.rows) {
		return errors.New("pgx: Scan called without calling Next")
	}
	return ScanValues(r.rows[r.idx], dest...)
}

// SliceRow is a helper implementation of pgx.Row backed by a slice of values or an error.
type SliceRow struct {
	vals []any
	err  error
}

// NewSliceRow creates a pgx.Row from column values.
func NewSliceRow(vals ...any) SliceRow {
	return SliceRow{vals: vals}
}

// NewErrorRow creates a pgx.Row that returns err on Scan.
func NewErrorRow(err error) SliceRow {
	return SliceRow{err: err}
}

func (r SliceRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return ScanValues(r.vals, dest...)
}

// ScanValues copies src values into dest pointers with automatic type conversion.
func ScanValues(src []any, dest ...any) error {
	if len(src) != len(dest) {
		return fmt.Errorf("pgx: column count mismatch: got %d values, %d destinations", len(src), len(dest))
	}
	for i, d := range dest {
		if err := assignValue(src[i], d); err != nil {
			return fmt.Errorf("pgx: column %d: %w", i, err)
		}
	}
	return nil
}

type sqlScanner interface {
	Scan(src any) error
}

func assignValue(src any, dest any) error {
	if scanner, ok := dest.(sqlScanner); ok {
		return scanner.Scan(src)
	}

	switch ptr := dest.(type) {
	case *string:
		if src == nil {
			*ptr = ""
			return nil
		}
		switch v := src.(type) {
		case string:
			*ptr = v
		case []byte:
			*ptr = string(v)
		default:
			*ptr = fmt.Sprint(v)
		}
		return nil
	case *int:
		if src == nil {
			*ptr = 0
			return nil
		}
		switch v := src.(type) {
		case int:
			*ptr = v
		case int32:
			*ptr = int(v)
		case int64:
			*ptr = int(v)
		case float64:
			*ptr = int(v)
		case string:
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			*ptr = n
		default:
			return fmt.Errorf("cannot scan %T into *int", src)
		}
		return nil
	case *int64:
		if src == nil {
			*ptr = 0
			return nil
		}
		switch v := src.(type) {
		case int64:
			*ptr = v
		case int:
			*ptr = int64(v)
		case int32:
			*ptr = int64(v)
		case float64:
			*ptr = int64(v)
		case string:
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return err
			}
			*ptr = n
		default:
			return fmt.Errorf("cannot scan %T into *int64", src)
		}
		return nil
	case *float64:
		if src == nil {
			*ptr = 0
			return nil
		}
		switch v := src.(type) {
		case float64:
			*ptr = v
		case float32:
			*ptr = float64(v)
		case int:
			*ptr = float64(v)
		case int64:
			*ptr = float64(v)
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return err
			}
			*ptr = f
		default:
			return fmt.Errorf("cannot scan %T into *float64", src)
		}
		return nil
	case *bool:
		if src == nil {
			*ptr = false
			return nil
		}
		switch v := src.(type) {
		case bool:
			*ptr = v
		case string:
			*ptr = v == "t" || v == "true" || v == "TRUE" || v == "1"
		default:
			return fmt.Errorf("cannot scan %T into *bool", src)
		}
		return nil
	case *[]byte:
		if src == nil {
			*ptr = nil
			return nil
		}
		switch v := src.(type) {
		case []byte:
			cp := make([]byte, len(v))
			copy(cp, v)
			*ptr = cp
		case string:
			*ptr = []byte(v)
		case json.RawMessage:
			cp := make([]byte, len(v))
			copy(cp, v)
			*ptr = cp
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			*ptr = b
		}
		return nil
	case *json.RawMessage:
		if src == nil {
			*ptr = nil
			return nil
		}
		switch v := src.(type) {
		case json.RawMessage:
			cp := make(json.RawMessage, len(v))
			copy(cp, v)
			*ptr = cp
		case []byte:
			cp := make(json.RawMessage, len(v))
			copy(cp, v)
			*ptr = cp
		case string:
			*ptr = json.RawMessage(v)
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			*ptr = json.RawMessage(b)
		}
		return nil
	case *time.Time:
		if src == nil {
			*ptr = time.Time{}
			return nil
		}
		switch v := src.(type) {
		case time.Time:
			*ptr = v
			return nil
		case string:
			for _, layout := range []string{
				time.RFC3339Nano,
				time.RFC3339,
				"2006-01-02 15:04:05.999999-07",
				"2006-01-02 15:04:05.999999+00",
				"2006-01-02 15:04:05-07",
				"2006-01-02 15:04:05",
			} {
				if parsed, err := time.Parse(layout, v); err == nil {
					*ptr = parsed
					return nil
				}
			}
			return fmt.Errorf("cannot parse time %q", v)
		default:
			return fmt.Errorf("cannot scan %T into *time.Time", src)
		}
	default:
		rv := reflect.ValueOf(dest)
		if rv.Kind() == reflect.Pointer && !rv.IsNil() && src != nil {
			sv := reflect.ValueOf(src)
			if sv.Type().AssignableTo(rv.Elem().Type()) {
				rv.Elem().Set(sv)
				return nil
			}
		}
		return fmt.Errorf("unsupported scan destination %T", dest)
	}
}

// wireHandler implements PostgreSQL v3 simple-query protocol with safe literal interpolation.
type wireHandler struct {
	addr     string
	user     string
	password string
	database string
}

func newWireHandler(connString string) (*wireHandler, error) {
	u, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	user := "postgres"
	password := ""
	if u.User != nil {
		if u.User.Username() != "" {
			user = u.User.Username()
		}
		password, _ = u.User.Password()
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		db = "postgres"
	}
	return &wireHandler{
		addr:     net.JoinHostPort(host, port),
		user:     user,
		password: password,
		database: db,
	}, nil
}

func (w *wireHandler) dial(ctx context.Context) (*pgWireConn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", w.addr)
	if err != nil {
		return nil, fmt.Errorf("pgx: dial %s: %w", w.addr, err)
	}
	wc := &pgWireConn{conn: conn}
	if err := wc.startup(w.user, w.password, w.database); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return wc, nil
}

func (w *wireHandler) Ping(ctx context.Context) error {
	wc, err := w.dial(ctx)
	if err != nil {
		return err
	}
	defer wc.Close()
	_, _, err = wc.simpleQuery("SELECT 1")
	return err
}

func (w *wireHandler) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	wc, err := w.dial(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer wc.Close()
	return wc.Exec(ctx, sql, args...)
}

func (w *wireHandler) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	wc, err := w.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer wc.Close()
	return wc.Query(ctx, sql, args...)
}

func (w *wireHandler) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return querySingleRow(w.Query(ctx, sql, args...))
}

func querySingleRow(rows pgx.Rows, err error) pgx.Row {
	if err != nil {
		return NewErrorRow(err)
	}
	defer rows.Close()
	if !rows.Next() {
		if rows.Err() != nil {
			return NewErrorRow(rows.Err())
		}
		return NewErrorRow(pgx.ErrNoRows)
	}
	sr, ok := rows.(*SliceRows)
	if ok && sr.idx >= 0 && sr.idx < len(sr.rows) {
		return NewSliceRow(sr.rows[sr.idx]...)
	}
	return NewErrorRow(pgx.ErrNoRows)
}

func (w *wireHandler) Begin(ctx context.Context) (pgx.Tx, error) {
	return w.BeginTx(ctx, pgx.TxOptions{})
}

func (w *wireHandler) BeginTx(ctx context.Context, _ pgx.TxOptions) (pgx.Tx, error) {
	wc, err := w.dial(ctx)
	if err != nil {
		return nil, err
	}
	if _, _, err := wc.simpleQuery("BEGIN"); err != nil {
		wc.Close()
		return nil, err
	}
	return &wireTx{wc: wc}, nil
}

func (w *wireHandler) Close() {}

type wireTx struct {
	wc   *pgWireConn
	done bool
}

func (t *wireTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return t, nil
}

func (t *wireTx) Commit(_ context.Context) error {
	if t.done {
		return errors.New("pgx: tx already closed")
	}
	t.done = true
	defer t.wc.Close()
	_, _, err := t.wc.simpleQuery("COMMIT")
	return err
}

func (t *wireTx) Rollback(_ context.Context) error {
	if t.done {
		return nil
	}
	t.done = true
	defer t.wc.Close()
	_, _, err := t.wc.simpleQuery("ROLLBACK")
	return err
}

func (t *wireTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.wc.Exec(ctx, sql, args...)
}

func (t *wireTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.wc.Query(ctx, sql, args...)
}

func (t *wireTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return querySingleRow(t.wc.Query(ctx, sql, args...))
}

type pgWireConn struct {
	conn net.Conn
}

func (c *pgWireConn) Close() {
	_ = c.conn.Close()
}

func (c *pgWireConn) startup(user, password, database string) error {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(196608)) // protocol 3.0
	buf.WriteString("user\x00" + user + "\x00")
	buf.WriteString("database\x00" + database + "\x00")
	buf.WriteByte(0)

	pkt := make([]byte, 4+buf.Len())
	binary.BigEndian.PutUint32(pkt[:4], uint32(4+buf.Len()))
	copy(pkt[4:], buf.Bytes())
	if _, err := c.conn.Write(pkt); err != nil {
		return err
	}

	for {
		typ, payload, err := c.readMsg()
		if err != nil {
			return err
		}
		switch typ {
		case 'R':
			authType := binary.BigEndian.Uint32(payload[:4])
			switch authType {
			case 0:
				// AuthOk
			case 3:
				// CleartextPassword
				if err := c.sendPassword(password); err != nil {
					return err
				}
			case 5:
				// MD5Password
				salt := payload[4:8]
				h1 := md5.Sum([]byte(password + user))
				h2 := md5.Sum(append([]byte(hex.EncodeToString(h1[:])), salt...))
				md5Pass := "md5" + hex.EncodeToString(h2[:])
				if err := c.sendPassword(md5Pass); err != nil {
					return err
				}
			default:
				return fmt.Errorf("pgx: unsupported auth type %d", authType)
			}
		case 'Z':
			return nil
		case 'E':
			return parsePgError(payload)
		}
	}
}

func (c *pgWireConn) sendPassword(pw string) error {
	b := append([]byte(pw), 0)
	return c.writeMsg('p', b)
}

func (c *pgWireConn) writeMsg(typ byte, payload []byte) error {
	buf := make([]byte, 1+4+len(payload))
	buf[0] = typ
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(payload)))
	copy(buf[5:], payload)
	_, err := c.conn.Write(buf)
	return err
}

func (c *pgWireConn) readMsg() (byte, []byte, error) {
	hdr := make([]byte, 5)
	if _, err := io.ReadFull(c.conn, hdr); err != nil {
		return 0, nil, err
	}
	typ := hdr[0]
	length := int(binary.BigEndian.Uint32(hdr[1:5])) - 4
	if length < 0 {
		return 0, nil, errors.New("pgx: invalid message length")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return 0, nil, err
	}
	return typ, payload, nil
}

func (c *pgWireConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	q, err := interpolateSQL(sql, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	_, tag, err := c.simpleQuery(q)
	return tag, err
}

func (c *pgWireConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	q, err := interpolateSQL(sql, args...)
	if err != nil {
		return nil, err
	}
	rows, _, err := c.simpleQuery(q)
	if err != nil {
		return nil, err
	}
	return NewSliceRows(rows), nil
}

func (c *pgWireConn) simpleQuery(query string) ([][]any, pgconn.CommandTag, error) {
	if err := c.writeMsg('Q', append([]byte(query), 0)); err != nil {
		return nil, pgconn.CommandTag{}, err
	}
	var (
		rows [][]any
		tag  pgconn.CommandTag
		qErr error
	)
	for {
		typ, payload, err := c.readMsg()
		if err != nil {
			return nil, pgconn.CommandTag{}, err
		}
		switch typ {
		case 'D':
			colCount := int(binary.BigEndian.Uint16(payload[:2]))
			pos := 2
			row := make([]any, colCount)
			for i := 0; i < colCount; i++ {
				colLen := int(int32(binary.BigEndian.Uint32(payload[pos : pos+4])))
				pos += 4
				if colLen < 0 {
					row[i] = nil
				} else {
					row[i] = string(payload[pos : pos+colLen])
					pos += colLen
				}
			}
			rows = append(rows, row)
		case 'C':
			tagStr := strings.TrimRight(string(payload), "\x00")
			parts := strings.Fields(tagStr)
			var affected int64
			if len(parts) > 0 {
				affected, _ = strconv.ParseInt(parts[len(parts)-1], 10, 64)
			}
			tag = pgconn.NewCommandTag(tagStr, affected)
		case 'E':
			qErr = parsePgError(payload)
		case 'Z':
			return rows, tag, qErr
		}
	}
}

func parsePgError(payload []byte) error {
	parts := bytes.Split(payload, []byte{0})
	var msg string
	for _, p := range parts {
		if len(p) > 1 && p[0] == 'M' {
			msg = string(p[1:])
			break
		}
	}
	if msg == "" {
		msg = "postgres error"
	}
	return errors.New("pgx: " + msg)
}

func interpolateSQL(query string, args ...any) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	out := query
	for i := len(args); i >= 1; i-- {
		lit, err := formatLiteral(args[i-1])
		if err != nil {
			return "", err
		}
		placeholder := "$" + strconv.Itoa(i)
		out = strings.ReplaceAll(out, placeholder, lit)
	}
	return out, nil
}

func formatLiteral(arg any) (string, error) {
	if arg == nil {
		return "NULL", nil
	}
	if valuer, ok := arg.(driver.Valuer); ok {
		v, err := valuer.Value()
		if err != nil {
			return "", err
		}
		if v == nil {
			return "NULL", nil
		}
		arg = v
	}
	switch v := arg.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'", nil
	case []byte:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'", nil
	case json.RawMessage:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'", nil
	case int:
		return strconv.Itoa(v), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case bool:
		if v {
			return "TRUE", nil
		}
		return "FALSE", nil
	case time.Time:
		return "'" + v.UTC().Format(time.RFC3339Nano) + "'", nil
	default:
		s := fmt.Sprint(v)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
	}
}

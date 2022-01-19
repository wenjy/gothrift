package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// @see https://github.com/go-redis/redis/blob/master/internal/pool/pool.go

var (
	ErrClosed      = errors.New("rpc: client is closed")
	ErrPoolTimeout = errors.New("rpc: connection pool timeout")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats 连接池的状态信息和统计信息
type Stats struct {
	Hits     uint32 // 命中数，连接池中找到空闲连接的次数
	Misses   uint32 // 未命中数，连接池中未找到空闲连接的次数
	Timeouts uint32 // 创建超时，等待令牌超时的次数

	TotalConns uint32 // 总连接，连接池总连接数
	IdleConns  uint32 // 空闲连接，连接池空闲连接数
	StaleConns uint32 // 释放连接数，被删除的旧连接数
}

// Pooler 连接池接口
type Pooler interface {
	NewConn(context.Context) (*Conn, error)
	CloseConn(*Conn) error

	Get(context.Context) (*Conn, error)
	Put(context.Context, *Conn)
	Remove(context.Context, *Conn)

	Len() int
	IdleLen() int
	Stats() *Stats

	Close() error
}

// Options 参数配置
type Options struct {
	Dialer  func(context.Context) (*thrift.TSocket, error) // 创建连接调用
	OnClose func(*Conn) error                              // 连接关闭回调

	PoolSize           int           // 连接池大小
	MinIdleConns       int           // 最小闲置的连接数量
	MaxConnAge         time.Duration // 连接的最大寿命
	PoolTimeout        time.Duration // 从连接池获取连接超时时间，为0不超时
	IdleTimeout        time.Duration // 闲置连接的超时时间，超过则会被检测删除，为0不超时
	IdleCheckFrequency time.Duration // 闲置连接检测频率
}

type lastDialErrorWrap struct {
	err error
}

// ConnPool 连接池
type ConnPool struct {
	opt *Options

	dialErrorsNum uint32 // atomic

	lastDialError atomic.Value

	queue chan struct{}

	connsMu      sync.Mutex
	conns        []*Conn // 所有的连接
	idleConns    []*Conn // 空闲连接
	poolSize     int
	idleConnsLen int

	stats Stats

	_closed  uint32 // atomic
	closedCh chan struct{}
}

// 检测ConnPool是否实现接口Pooler
var _ Pooler = (*ConnPool)(nil)

// NewConnPool 实例化连接池
func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
		closedCh:  make(chan struct{}),
	}

	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()

	// 空闲连接检测
	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

// 连接池初始化的时候就会往连接池填满空闲的连接
func (p *ConnPool) checkMinIdleConns() {
	if p.opt.MinIdleConns == 0 {
		return
	}
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns {
		p.poolSize++
		p.idleConnsLen++
		go func() {
			err := p.addIdleConn()
			if err != nil {
				p.connsMu.Lock()
				p.poolSize--
				p.idleConnsLen--
				p.connsMu.Unlock()
			}
		}()
	}
}

// 添加一个空闲连接
func (p *ConnPool) addIdleConn() error {
	cn, err := p.dialConn(context.TODO(), true)
	if err != nil {
		return err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	p.connsMu.Unlock()
	return nil
}

// NewConn 创建新连接
func (p *ConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.newConn(ctx, false)
}

func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	if pooled {
		// 如果连接池满了，会在后面移除
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}
	p.connsMu.Unlock()

	return cn, nil
}

func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewConn(netConn)

	cn.pooled = pooled
	return cn, nil
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get 从连接池返回一个已存在的连接或者新建一个连接
func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	// 排队获取令牌
	err := p.waitTurn(ctx)
	if err != nil {
		return nil, err
	}

	for {
		p.connsMu.Lock()
		// 从空闲连接里面先获取一个空闲连接
		cn := p.popIdle()
		p.connsMu.Unlock()

		// 没有空闲连接时候直接跳出循环
		if cn == nil {
			break
		}

		// 判断是否已经过时，是的话close掉了然后继续取出。
		if p.isStaleConn(cn) {
			_ = p.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	// 如果没有空闲连接的话，这边就直接新建连接了
	newcn, err := p.newConn(ctx, true)
	if err != nil {
		// 归还令牌
		p.freeTurn()
		return nil, err
	}

	return newcn, nil
}

// 拿令牌
func (p *ConnPool) getTurn() {
	p.queue <- struct{}{}
}

// 等待拿令牌，超时返回错误
func (p *ConnPool) waitTurn(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

// 释放令牌
func (p *ConnPool) freeTurn() {
	<-p.queue
}

// 取一个空闲连接，并保持最小空闲连接数
func (p *ConnPool) popIdle() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	idx := len(p.idleConns) - 1
	cn := p.idleConns[idx]
	p.idleConns = p.idleConns[:idx]
	p.idleConnsLen--
	p.checkMinIdleConns()
	return cn
}

// Put 把连接放入空闲连接里
func (p *ConnPool) Put(ctx context.Context, cn *Conn) {
	// 前面标记过不要入池的，这边就删除了
	if !cn.pooled {
		p.Remove(ctx, cn)
		return
	}

	p.connsMu.Lock()
	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
	p.connsMu.Unlock()
	p.freeTurn()
}

// Remove 删除指定的连接
func (p *ConnPool) Remove(ctx context.Context, cn *Conn) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

// CloseConn 关闭连接
func (p *ConnPool) CloseConn(cn *Conn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

func (p *ConnPool) removeConnWithLock(cn *Conn) {
	p.connsMu.Lock()
	p.removeConn(cn)
	p.connsMu.Unlock()
}

// 删除连接，并保持最小空闲连接数
func (p *ConnPool) removeConn(cn *Conn) {
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			if cn.pooled {
				p.poolSize--
				p.checkMinIdleConns()
			}
			return
		}
	}
}

// 关闭连接，并触发关闭回调
func (p *ConnPool) closeConn(cn *Conn) error {
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	return cn.Close()
}

// Len 当前连接总数
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen 空闲连接数
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

// Stats 连接池的状态
func (p *ConnPool) Stats() *Stats {
	idleLen := p.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

// 连接池是否关闭
func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

// Filter 过滤连接池
func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	var firstErr error
	for _, cn := range p.conns {
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Close 关闭连接池
func (p *ConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	close(p.closedCh)

	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

// 检测空闲连接，周期性的淘汰连接池里面要被淘汰的连接
func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			_, err := p.ReapStaleConns()
			if err != nil {
				continue
			}
		case <-p.closedCh:
			return
		}
	}
}

// ReapStaleConns 删除过期的连接
func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		p.getTurn()

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()

		p.freeTurn()

		if cn != nil {
			_ = p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	return n, nil
}

// 执行删除过期的连接
func (p *ConnPool) reapStaleConn() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		return nil
	}

	p.idleConns = append(p.idleConns[:0], p.idleConns[1:]...)
	p.idleConnsLen--
	p.removeConn(cn)

	return cn
}

// 是否过期
func (p *ConnPool) isStaleConn(cn *Conn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		return true
	}

	return false
}

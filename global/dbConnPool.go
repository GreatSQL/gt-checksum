package global

import (
	"database/sql"
	"fmt"
	"sync"
)

type Pool struct {
	mu      sync.Mutex
	minConn int          // 最小连接数
	maxConn int          // 最大连接数
	numConn int          // 池已申请的连接数
	conns   chan *sql.DB //当前池中空闲连接实例
	close   bool
	drive   string //数据库类型
}

// 初始化池实例
func NewPool(min int, db []*sql.DB, logThreadSeq int, drive string) *Pool {
	var (
		vlog string
	)
	defer func() {
		if err := recover(); err != nil {
			// Error output removed - use logs instead
		}
	}()
	p := &Pool{
		minConn: min,
		maxConn: min,
		numConn: 1,
		conns:   make(chan *sql.DB, min),
		close:   false,
		drive:   drive,
	}
	vlog = fmt.Sprintf("(%d) Initializing %s DB connection pool...", logThreadSeq, p.drive)
	Wlog.Debug(vlog)
	for i := 0; i < min; i++ {
		p.conns <- db[i]
		//p.conns <- dbconn
	}
	vlog = fmt.Sprintf("(%d) %s DB connection pool initialized successfully", logThreadSeq, p.drive)
	Wlog.Debug(vlog)
	vlog = fmt.Sprintf("(%d) The current number of %s DB session connection pools is [%d]", logThreadSeq, p.drive, len(p.conns))
	Wlog.Debug(vlog)
	return p
}

type DBConn struct {
	db       *sql.DB
	idleTime int // 标记该数据库连接空闲时间
}

// 从池中取出连接
func (p *Pool) Get(logThreadSeq int64) *sql.DB {
	var (
		vlog string
		d    *sql.DB
	)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	//vlog = fmt.Sprintf("(%d) Get a session connection from the %s DB session connection pool ...", logThreadSeq, p.drive)
	//Wlog.Debug(vlog)

	// p.close 的读取与写入（Close() 中）都在 mutex 保护下，避免数据竞态
	p.mu.Lock()
	if p.close {
		p.mu.Unlock()
		return nil
	}
	if p.numConn >= p.minConn {
		vlog = fmt.Sprintf("(%d) The current %s DB session connection pool is full. use session [%d], total session [%d], no memory available, please wait...", logThreadSeq, p.drive, p.numConn, p.minConn)
		Wlog.Warn(vlog)
	}
	p.mu.Unlock()

	// 必须在释放 mutex 之后再阻塞等待 channel，否则 Put() 也需要 mutex 而造成死锁。
	// 使用两值接收：若 Close() 在此期间关闭了 channel，ok==false，不返回无效连接，不更新 numConn
	var ok bool
	d, ok = <-p.conns // 若池中没有可取的连接，则等待其他请求返回连接至池中再取
	if !ok {
		return nil
	}
	p.mu.Lock()
	p.numConn++
	p.mu.Unlock()
	//vlog = fmt.Sprintf("(%d) Obtain a connection successfully, the current %s DB connection pool status, the number of applied connections is [%d], and the remaining number is [%d].", logThreadSeq, p.drive, p.minConn-len(p.conns), len(p.conns))
	//Wlog.Debug(vlog)
	return d
}

// 将连接返回池中
func (p *Pool) Put(d *sql.DB, logThreadSeq int64) {
	//var (
	//	vlog string
	//)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	//vlog = fmt.Sprintf("(%d) Put a session connection into the %s DB session connection pool ...", logThreadSeq, p.drive)
	//Wlog.Debug(vlog)
	if p.close {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns <- d
	p.numConn--
	//vlog = fmt.Sprintf("(%d) The connection is put in successfully, the %s DB current connection pool status, the number of applied connections is [%d], and the remaining number is [%d].", logThreadSeq, p.drive, p.minConn-len(p.conns), len(p.conns))
	//Wlog.Debug(vlog)
}

// 关闭池
func (p *Pool) Close(logThreadSeq int) {
	var (
		vlog string
	)
	p.mu.Lock()
	defer p.mu.Unlock()
	vlog = fmt.Sprintf("(%d) Start closing the %s DB session connection pool ...", logThreadSeq, p.drive)
	Wlog.Debug(vlog)
	// 先设置关闭标志，再关闭 channel。
	// Get() 在持锁时读取此标志，确保新的 Get() 调用在 channel 关闭前就能感知到关闭状态并提前返回 nil
	p.close = true
	close(p.conns)
	for d := range p.conns {
		d.Close()
	}
	vlog = fmt.Sprintf("(%d) %s DB Session connection pool closed successfully.", logThreadSeq, p.drive)
	Wlog.Debug(vlog)
}

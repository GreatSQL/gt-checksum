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
}

// 初始化池实例
func NewPool(min, max int, db []*sql.DB) *Pool {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	p := &Pool{
		minConn: min,
		maxConn: max,
		numConn: min,
		conns:   make(chan *sql.DB, max),
		close:   false,
	}
	for i := 0; i < min; i++ {
		p.conns <- db[i]
		//p.conns <- dbconn
	}
	return p
}

type DBConn struct {
	db       *sql.DB
	idleTime int // 标记该数据库连接空闲时间
}

// 新建数据库连接
//func NewDBConn(dbcon *sql.DB) *DBConn {
//	return &DBConn{
//		db:       dbcon,
//		idleTime: 0,
//	}
//}

// 从池中取出连接
func (p *Pool) Get() *sql.DB {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	var d *sql.DB
	if p.close {
		close(p.conns)
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.numConn >= p.minConn || len(p.conns) > 0 { // 保证了池申请连接数量不超过最大连接数
		d = <-p.conns // 若池中没有可取的连接，则等待其他请求返回连接至池中再取
	}
	p.numConn++
	return d
	//return NewDBConn() //申请新的连接
}

// 将连接返回池中
func (p *Pool) Put(d *sql.DB) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	if p.close {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns <- d
}

// 关闭池
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	close(p.conns)
	for d := range p.conns {
		d.Close()
	}
	p.close = true
}

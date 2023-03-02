package global

//
//import (
//	"errors"
//	"sync"
//	"time"
//)
//
//// goroutine pool
//type GoroutinePool struct {
//	c  chan struct{}
//	wg *sync.WaitGroup
//}
//
//// 采用有缓冲channel实现,当channel满的时候阻塞
//func NewGoroutinePool(maxSize int) *GoroutinePool {
//	if maxSize <= 0 {
//		panic("max size too small")
//	}
//	return &GoroutinePool{
//		c:  make(chan struct{}, maxSize),
//		wg: new(sync.WaitGroup),
//	}
//}
//
//// add
//func (g *GoroutinePool) Add(delta int) {
//	g.wg.Add(delta)
//	for i := 0; i < delta; i++ {
//		g.c <- struct{}{}
//	}
//
//}
//
//// done
//func (g *GoroutinePool) Done() {
//	<-g.c
//	g.wg.Done()
//}
//
//// wait
//func (g *GoroutinePool) Wait() {
//	g.wg.Wait()
//}
//
//func testGoroutineWithTimeOut() error {
//	done := make(chan struct{})
//	// 新增阻塞chan
//	errChan := make(chan error)
//	var err error
//	pool := NewGoroutinePool(10)
//	for i := 0; i < 10; i++ {
//		pool.Add(1)
//		go func() {
//			pool.Done()
//			if err != nil {
//				errChan <- errors.New("error")
//			}
//		}()
//	}
//	go func() {
//		pool.Wait()
//		close(done)
//	}()
//
//	select {
//	// 错误快返回,适用于get接口
//	case err := <-errChan:
//		return err
//	case <-done:
//	case <-time.After(500 * time.Millisecond):
//	}
//	return nil
//}

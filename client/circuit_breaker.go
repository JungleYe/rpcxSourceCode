package client

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrBreakerOpen    = errors.New("breaker open")
	ErrBreakerTimeout = errors.New("breaker time out")
)

// ConsecCircuitBreaker is window sliding CircuitBreaker with failure threshold.
//断路器的结构体
//断路器的基本原理：fn调用正常时断路器一路绿灯，且清除错误计数；
// 当fn调用出错的时候，就累加错误次数，当错误次数达到了设置的failureThreshold时，断路器变成红灯，fn的调用不执行，直接返回错误；
//红灯不能一直持续下去，所以有个window时间窗口，当上一次的业务失败lastFailureTime经过了window时间之后，会再次清除错误计数并开启绿灯；
type ConsecCircuitBreaker struct {
	lastFailureTime  time.Time
	failures         uint64
	failureThreshold uint64
	window           time.Duration
}

// NewConsecCircuitBreaker returns a new ConsecCircuitBreaker.
//创建一个断路器
func NewConsecCircuitBreaker(failureThreshold uint64, window time.Duration) *ConsecCircuitBreaker {
	return &ConsecCircuitBreaker{
		failureThreshold: failureThreshold,
		window:           window,
	}
}

// Call Circuit function
//以过断路器的方式进行fn的调用
func (cb *ConsecCircuitBreaker) Call(fn func() error, d time.Duration) error {
	var err error

	if !cb.ready() {
		return ErrBreakerOpen
	}

	if d == 0 {
		err = fn()
	} else {
		c := make(chan error, 1)
		go func() {
			c <- fn()
			close(c)
		}()

		t := time.NewTimer(d)
		select {
		case e := <-c:
			err = e
		case <-t.C:
			err = ErrBreakerTimeout
		}
		t.Stop()
	}

	if err == nil {
		cb.success()
	} else {
		cb.fail()
	}

	return err
}

//查看断路器的红绿灯状态true为绿灯，false为红灯
func (cb *ConsecCircuitBreaker) ready() bool {
	if time.Since(cb.lastFailureTime) > cb.window {
		cb.reset()
		return true
	}

	failures := atomic.LoadUint64(&cb.failures)
	return failures < cb.failureThreshold
}

//设置绿灯
func (cb *ConsecCircuitBreaker) success() {
	cb.reset()
}
//设置红灯
func (cb *ConsecCircuitBreaker) fail() {
	atomic.AddUint64(&cb.failures, 1)
	cb.lastFailureTime = time.Now()
}

func (cb *ConsecCircuitBreaker) Success() {
	cb.success()
}
func (cb *ConsecCircuitBreaker) Fail() {
	cb.fail()
}

func (cb *ConsecCircuitBreaker) Ready() bool {
	return cb.ready()
}

//设置绿灯时清空错误计数，并设置时间
func (cb *ConsecCircuitBreaker) reset() {
	atomic.StoreUint64(&cb.failures, 0)
	cb.lastFailureTime = time.Now()
}

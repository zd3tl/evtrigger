package evtrigger

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"
)

var (
	defaultWorkerSize = 1
	defaultBufferSize = 128
)

var (
	ErrCallbackNil   = errors.New("Trigger: callback nil")
	ErrCallbackExist = errors.New("Trigger: callback exist")
	ErrEventIllegal  = errors.New("Trigger: event error")
)

// https://github.com/grpc/grpc-go/blob/689f7b154ee8a3f3ab6a6107ff7ad78189baae06/internal/transport/controlbuf.go#L40
type itemNode struct {
	it   interface{}
	next *itemNode
}

type itemList struct {
	head *itemNode
	tail *itemNode
}

func (il *itemList) enqueue(i interface{}) {
	n := &itemNode{it: i}
	if il.tail == nil {
		il.head, il.tail = n, n
		return
	}
	il.tail.next = n
	il.tail = n
}

func (il *itemList) dequeue() interface{} {
	if il.head == nil {
		return nil
	}
	i := il.head.it
	il.head = il.head.next
	if il.head == nil {
		il.tail = nil
	}
	return i
}

func (il *itemList) ForEach(visitor func(it interface{}) error) error {
	if il.head == nil {
		return nil
	}

	// 保持head位置不变，临时变量ih走到tail，逐个调用visitor
	ih := il.head
	for ih != il.tail {
		if err := visitor(ih.it); err != nil {
			return err
		}
	}
	return nil
}

type TriggerEvent struct {
	Key   string
	Value interface{}
}

// internalEvent 是 Trigger 内部传递的数据，在dispatch时，把callback得到，下发给goroutine，减少callbacks的竞争压力，
// 放在worker去竞争，随着worker的goroutine数量增加，竞争会比较激烈
type internalEvent struct {
	event        *TriggerEvent
	callbackFunc TriggerCallback
}

type Trigger struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	// wg 管理goroutine退出
	wg sync.WaitGroup
	lg *zap.Logger

	listMu sync.Mutex
	// list 无边界的缓存，使用chan需要有边界，存储外部事件。
	// 这里的 itemList 没有按照key做区分，同key的事件在goroutine池有资源的情况下，
	// 可能并发执行，所以调用方提供的callback需要保证threadsafe，即便做到同key顺序下发，
	// 不同key的配置更新，对于调用方的callback方法也可能访问相同的资源。
	list *itemList

	// ch 和 consumerWaiting 的设定参考：
	// https://github.com/grpc/grpc-go/blob/689f7b154ee8a3f3ab6a6107ff7ad78189baae06/internal/transport/controlbuf.go#L286
	// ch 触发等待 list 中新进元素的goroutine
	ch chan struct{}
	// consumerWaiting 在grpc-go中的设定是loopyWriter的run在调用get(获取事件)这个事情上花费的时间相比网络io较少，所以不能让事件的进入
	// 卡在ch的写入上。用ch做串联，即便提供一定的buffer，还在response写出和回复数据的进入上做了一定耦合。
	consumerWaiting bool

	callbackMu sync.Mutex
	// callbacks 记录需要callback的event
	callbacks map[string]TriggerCallback

	// buffer 存储提交给goroutine池的
	buffer chan *internalEvent
}

// TriggerCallback 把event的value给到调用方
type TriggerCallback func(key string, value interface{}) error

type triggerOptions struct {
	// workerSize 处理callback的goroutine数量
	workerSize int

	lg *zap.Logger
}

type TriggerOption func(options *triggerOptions)

func WithWorkerSize(v int) TriggerOption {
	return func(options *triggerOptions) {
		options.workerSize = v
	}
}

func WithLogger(v *zap.Logger) TriggerOption {
	return func(options *triggerOptions) {
		options.lg = v
	}
}

func NewTrigger(opts ...TriggerOption) (*Trigger, error) {
	ops := &triggerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	var lg *zap.Logger
	if ops.lg != nil {
		lg = ops.lg
	} else {
		lg, _ = zap.NewProduction()
	}

	// ctx和cancel
	ctx, cancel := context.WithCancel(context.Background())
	tgr := Trigger{
		ctx:        ctx,
		cancelFunc: cancel,
		wg:         sync.WaitGroup{},
		lg:         lg,

		list:   &itemList{},
		buffer: make(chan *internalEvent, defaultBufferSize),
		ch:     make(chan struct{}, 1),
	}

	// 运行worker
	workerSize := ops.workerSize
	if ops.workerSize <= 0 {
		workerSize = defaultWorkerSize
	}
	for i := 0; i < workerSize; i++ {
		tgr.wg.Add(1)
		go tgr.run()
	}

	// 运行event获取
	tgr.wg.Add(1)
	go tgr.get()

	return &tgr, nil
}

func (tgr *Trigger) Register(key string, callback TriggerCallback) error {
	if callback == nil {
		return ErrCallbackNil
	}

	tgr.callbackMu.Lock()
	defer tgr.callbackMu.Unlock()
	_, ok := tgr.callbacks[key]
	if ok {
		tgr.lg.Warn(
			"key already exist",
			zap.String("key", key),
		)
		return ErrCallbackExist
	}
	tgr.callbacks[key] = callback
	return nil
}

func (tgr *Trigger) Unregister(key string) {
	tgr.callbackMu.Lock()
	defer tgr.callbackMu.Unlock()
	delete(tgr.callbacks, key)
}

func (tgr *Trigger) Close() {
	if tgr.cancelFunc != nil {
		tgr.cancelFunc()
	}
	tgr.wg.Wait()
}

func (tgr *Trigger) Put(event *TriggerEvent) error {
	if event == nil || event.Key == "" {
		return ErrEventIllegal
	}

	var wakeUp bool
	tgr.listMu.Lock()
	if tgr.consumerWaiting {
		wakeUp = true
		tgr.consumerWaiting = false
	}
	tgr.list.enqueue(event)
	tgr.listMu.Unlock()
	if wakeUp {
		select {
		case tgr.ch <- struct{}{}:
		default:
		}
	}
	return nil
}

func (tgr *Trigger) get() {
	defer tgr.wg.Done()
	for {
		tgr.listMu.Lock()
		h := tgr.list.dequeue()
		if h != nil {
			tgr.listMu.Unlock()
			ev := h.(*TriggerEvent)

			tgr.callbackMu.Lock()
			callback := tgr.callbacks[ev.Key]
			tgr.callbackMu.Unlock()
			if callback == nil {
				tgr.lg.Error(
					"nil callback",
					zap.String("key", ev.Key),
				)
			} else {
				tgr.buffer <- &internalEvent{event: ev, callbackFunc: callback}
			}
			continue
		}

		tgr.consumerWaiting = true
		tgr.listMu.Unlock()

		select {
		case <-tgr.ch:
			// 解决通过轮询等待的问题，否则就需要在 Trigger.list 为空的时候，等待一个可配置的事件
		case <-tgr.ctx.Done():
			tgr.lg.Info("get exit")
			return
		}
	}
}

func (tgr *Trigger) run() {
	defer tgr.wg.Done()
	for {
		select {
		case <-tgr.ctx.Done():
			tgr.lg.Info("run exit")
			return
		case ev := <-tgr.buffer:
			if err := ev.callbackFunc(ev.event.Key, ev.event.Value); err != nil {
				tgr.lg.Error(
					"callback error",
					zap.Error(err),
					zap.String("ev-key", ev.event.Key),
				)
			}
		}
	}
}

func (tgr *Trigger) ForEach(visitor func(it interface{}) error) error {
	return tgr.ForEach(visitor)
}

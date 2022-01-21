package evtrigger

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"testing"
	"time"
)

func Test_Register(t *testing.T) {
	var tests = []struct {
		key      string
		callback TriggerCallback
		expect   error
	}{
		{
			key:      "foo",
			callback: nil,
			expect:   ErrCallbackNil,
		},
		{
			key: "foo",
			callback: func(eventValue interface{}) error {
				return nil
			},
			expect: nil,
		},
		{
			key: "foo",
			callback: func(eventValue interface{}) error {
				return nil
			},
			expect: ErrCallbackExist,
		},
	}
	logger, _ := zap.NewDevelopment()
	tgr := trigger{
		lg:        logger,
		callbacks: make(map[string]TriggerCallback),
	}
	for idx, tt := range tests {
		actual := tgr.Register(tt.key, tt.callback)
		if actual != tt.expect {
			t.Errorf("idx %d actual: %+v expect: %+v", idx, actual, tt.expect)
			t.SkipNow()
		}
	}
}

func Test_Put(t *testing.T) {
	var tests = []struct {
		event  *TriggerEvent
		tgr    *trigger
		expect error
	}{
		{
			event: &TriggerEvent{
				Key: "foo",
			},
			tgr: &trigger{
				list:            &itemList{},
				consumerWaiting: true,
			},
			expect: nil,
		},
		{
			event: &TriggerEvent{
				Key: "foo",
			},
			tgr: &trigger{
				list: &itemList{},
			},
			expect: nil,
		},
		{
			event: &TriggerEvent{
				Key:   "",
				Value: "",
			},
			tgr: &trigger{
				list: &itemList{},
			},
			expect: ErrEventIllegal,
		},
	}
	for idx, tt := range tests {
		actual := tt.tgr.Put(tt.event)
		if actual != tt.expect {
			t.Errorf("idx %d actual: %+v expect: %+v", idx, actual, tt.expect)
			t.SkipNow()
		}
	}
}

func Test_get(t *testing.T) {
	ctx, cancal := context.WithCancel(context.Background())
	logger, _ := zap.NewDevelopment()

	var tests = []struct {
		tgr      *trigger
		addEvent bool
	}{
		{
			tgr: &trigger{
				ctx:        ctx,
				cancelFunc: cancal,
				lg:         logger,
				wg:         sync.WaitGroup{},

				list: &itemList{},
				callbacks: map[string]TriggerCallback{
					"foo": func(eventValue interface{}) error {
						return nil
					},
				},
				buffer: make(chan *internalEvent, defaultBufferSize),
			},
			addEvent: true,
		},
		{
			tgr: &trigger{
				wg:         sync.WaitGroup{},
				lg:         logger,
				ctx:        ctx,
				cancelFunc: cancal,

				list: &itemList{},
			},
		},
	}

	for _, tt := range tests {
		if tt.addEvent {
			tt.tgr.Put(&TriggerEvent{Key: "foo"})
		}
		tt.tgr.wg.Add(1)
		go func() {
			tt.tgr.cancelFunc()
		}()
		tt.tgr.get()
	}
}

func Test_run(t *testing.T) {
	ctx, cancal := context.WithCancel(context.Background())
	logger, _ := zap.NewDevelopment()

	tgr := &trigger{
		wg:         sync.WaitGroup{},
		lg:         logger,
		ctx:        ctx,
		cancelFunc: cancal,

		list: &itemList{},
		callbacks: map[string]TriggerCallback{
			"foo": func(eventValue interface{}) error {
				fmt.Println("foo event got")
				return nil
			},
		},
		buffer: make(chan *internalEvent, defaultBufferSize),
	}

	callback := tgr.callbacks["foo"]

	tgr.buffer <- &internalEvent{
		event:        &TriggerEvent{Key: "foo"},
		callbackFunc: callback,
	}

	tgr.wg.Add(1)
	go func() {
		time.Sleep(2 * time.Second)
		tgr.cancelFunc()
	}()
	tgr.run()
	tgr.wg.Wait()
}

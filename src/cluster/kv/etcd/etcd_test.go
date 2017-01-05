// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package etcd

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/coreos/etcd/integration"
	"github.com/gogo/protobuf/proto"
	"github.com/m3db/m3cluster/generated/proto/kvtest"
	"github.com/m3db/m3cluster/kv"
	"github.com/stretchr/testify/assert"
)

func TestGetAndSet(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	value, err := store.Get("foo")
	assert.Error(t, err)
	assert.Equal(t, kv.ErrNotFound, err)
	assert.Nil(t, value)

	version, err := store.Set("foo", genProto("bar1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, version)

	value, err = store.Get("foo")
	assert.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	version, err = store.Set("foo", genProto("bar2"))
	assert.NoError(t, err)
	assert.Equal(t, 2, version)

	value, err = store.Get("foo")
	assert.NoError(t, err)
	verifyValue(t, value, "bar2", 2)
}

func TestSetIfNotExist(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	version, err := store.SetIfNotExists("foo", genProto("bar"))
	assert.NoError(t, err)
	assert.Equal(t, 1, version)

	version, err = store.SetIfNotExists("foo", genProto("bar"))
	assert.Error(t, err)
	assert.Equal(t, kv.ErrAlreadyExists, err)

	value, err := store.Get("foo")
	assert.NoError(t, err)
	verifyValue(t, value, "bar", 1)
}

func TestCheckAndSet(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	version, err := store.CheckAndSet("foo", 1, genProto("bar"))
	assert.Error(t, err)
	assert.Equal(t, kv.ErrVersionMismatch, err)

	version, err = store.SetIfNotExists("foo", genProto("bar"))
	assert.NoError(t, err)
	assert.Equal(t, 1, version)

	version, err = store.CheckAndSet("foo", 1, genProto("bar"))
	assert.NoError(t, err)
	assert.Equal(t, 2, version)

	version, err = store.CheckAndSet("foo", 1, genProto("bar"))
	assert.Error(t, err)
	assert.Equal(t, kv.ErrVersionMismatch, err)

	value, err := store.Get("foo")
	assert.NoError(t, err)
	verifyValue(t, value, "bar", 2)
}

func TestWatchClose(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	_, err := store.Set("foo", genProto("bar1"))
	assert.NoError(t, err)
	w1, err := store.Watch("foo")
	assert.NoError(t, err)
	<-w1.C()
	verifyValue(t, w1.Get(), "bar1", 1)

	c := store.(*client)
	_, ok := c.watchables["foo"]
	assert.True(t, ok)

	// closing w1 will close the go routine for the watch updates
	w1.Close()

	// waits until the original watchable is cleaned up
	for {
		c.RLock()
		_, ok = c.watchables["foo"]
		c.RUnlock()
		if !ok {
			break
		}
	}

	// getting a new watch will create a new watchale and thread to watch for updates
	w2, err := store.Watch("foo")
	assert.NoError(t, err)
	<-w2.C()
	verifyValue(t, w2.Get(), "bar1", 1)

	// verify that w1 will no longer be updated because the original watchable is closed
	_, err = store.Set("foo", genProto("bar2"))
	assert.NoError(t, err)
	<-w2.C()
	verifyValue(t, w2.Get(), "bar2", 2)
	verifyValue(t, w1.Get(), "bar1", 1)

	w1.Close()
	w2.Close()
}

func TestWatchLastVersion(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	w, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Nil(t, w.Get())

	lastVersion := 100
	go func() {
		for i := 1; i <= lastVersion; i++ {
			_, err := store.Set("foo", genProto(fmt.Sprintf("bar%d", i)))
			assert.NoError(t, err)
		}
	}()

	for {
		<-w.C()
		value := w.Get()
		if value.Version() == lastVersion {
			break
		}
	}
	verifyValue(t, w.Get(), fmt.Sprintf("bar%d", lastVersion), lastVersion)

	w.Close()
}

func TestWatchFromExist(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	_, err := store.Set("foo", genProto("bar1"))
	assert.NoError(t, err)
	value, err := store.Get("foo")
	assert.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	w, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Nil(t, w.Get())

	<-w.C()
	assert.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	assert.NoError(t, err)

	<-w.C()
	assert.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar2", 2)

	_, err = store.Set("foo", genProto("bar3"))
	assert.NoError(t, err)

	<-w.C()
	assert.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar3", 3)

	w.Close()
}

func TestWatchFromNotExist(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	w, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(w.C()))
	assert.Nil(t, w.Get())

	_, err = store.Set("foo", genProto("bar1"))
	assert.NoError(t, err)

	<-w.C()
	assert.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	assert.NoError(t, err)

	<-w.C()
	assert.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar2", 2)

	w.Close()
}

func TestMultipleWatchesFromExist(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	_, err := store.Set("foo", genProto("bar1"))
	assert.NoError(t, err)

	w1, err := store.Watch("foo")
	assert.NoError(t, err)

	w2, err := store.Watch("foo")
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar1", 1)

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar2", 2)

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar2", 2)

	_, err = store.Set("foo", genProto("bar3"))
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar3", 3)

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar3", 3)

	w1.Close()
	w2.Close()
}

func TestMultipleWatchesFromNotExist(t *testing.T) {
	store, closeFn := testStore(t)
	defer closeFn()

	w1, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(w1.C()))
	assert.Nil(t, w1.Get())

	w2, err := store.Watch("foo")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(w2.C()))
	assert.Nil(t, w2.Get())

	_, err = store.Set("foo", genProto("bar1"))
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar1", 1)

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	assert.NoError(t, err)

	<-w1.C()
	assert.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar2", 2)

	<-w2.C()
	assert.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar2", 2)

	w1.Close()
	w2.Close()
}

func verifyValue(t *testing.T, v kv.Value, value string, version int) {
	var testMsg kvtest.Foo
	err := v.Unmarshal(&testMsg)
	assert.NoError(t, err)
	assert.Equal(t, value, testMsg.Msg)
	assert.Equal(t, version, v.Version())
}

func genProto(msg string) proto.Message {
	return &kvtest.Foo{Msg: msg}
}

func testStore(t *testing.T) (kv.Store, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	ec := ecluster.Client(rand.Intn(3))

	closer := func() {
		ecluster.Terminate(t)
		ec.Watcher.Close()
	}

	return NewStore(ec, NewOptions().SetWatchChanCheckInterval(10*time.Millisecond)), closer
}
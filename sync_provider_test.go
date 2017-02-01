package godist

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestSimpleLocking(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	dlm := NewSyncProvider(ms).(*dlock_manager)

	lock := dlm.NewMutex(context.TODO(), "val1")
	lock2 := dlm.NewMutex(context.TODO(), "val2")

	_, err := ms.Get(context.TODO(), "val1")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val1 record")
	}

	lock.Lock()
	_, err = ms.Get(context.TODO(), "val1")
	if err != nil {
		t.Fatal("Should be val1 record")
	}
	_, err = ms.Get(context.TODO(), "val2")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val2 record")
	}

	lock2.Lock()
	_, err = ms.Get(context.TODO(), "val2")
	if err != nil {
		t.Fatal("Should be val2 record")
	}
	lock.Unlock()
	lock2.Unlock()

	_, err = ms.Get(context.TODO(), "val1")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val1 record")
	}
	_, err = ms.Get(context.TODO(), "val2")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should be no val2 record")
	}
}

func TestRelock(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	dlm := NewSyncProvider(ms).(*dlock_manager)

	lock := dlm.NewMutex(context.TODO(), "val111")
	started := false

	go func() {
		<-time.After(time.Millisecond * 10)
		started = true
		lock.Unlock()
	}()

	err := lock.Lock()
	if err != nil {
		t.Fatal("Should be locked")
	}
	for err := lock.Lock(); err != nil; {
	}

	if !started {
		t.Fatal("Should panicing")
	}

	lock.Unlock()
}

func TestContextCancelled(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	dlm := NewSyncProvider(ms).(*dlock_manager)

	ctx, cancel := context.WithCancel(context.Background())
	lock := dlm.NewMutex(ctx, "val")

	go func() {
		<-time.After(time.Millisecond * 10)
		cancel()
	}()

	err := lock.Lock()
	if err != nil {
		t.Fatal("Should be locked")
	}

	err = lock.Lock()
	if !CheckError(err, DLErrClosed) {
		t.Fatal("Should be closed context")
	}
}

func TestConcurrentLocking(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	dlm := NewSyncProvider(ms).(*dlock_manager)

	lock1 := dlm.NewMutex(context.TODO(), "val")
	lock2 := dlm.NewMutex(context.TODO(), "val")

	lock1.Lock()
	start := time.Now()

	go func() {
		<-time.After(time.Millisecond * 50)
		lock1.Unlock()
	}()

	lock2.Lock()
	if time.Now().Sub(start) <= time.Millisecond*50 {
		t.Fatal("Something goes wrong should be blocked at least 50ms")
	}

	lock2.Unlock()
}

func TestDistributeLocking(t *testing.T) {
	dropAll()
	ms1 := NewMemStorage().(*mem_storage)
	ms2 := NewMemStorage().(*mem_storage)
	dlm1 := NewSyncProvider(ms1).(*dlock_manager)
	dlm2 := NewSyncProvider(ms2).(*dlock_manager)

	lock1 := dlm1.NewMutex(context.TODO(), "val")
	lock2 := dlm2.NewMutex(context.TODO(), "val")

	lock1.Lock()
	start := time.Now()

	go func() {
		<-time.After(time.Millisecond * 50)
		ms1.Close()
	}()

	err := lock2.Lock()
	if time.Now().Sub(start) <= time.Millisecond*50 {
		t.Fatal("Something goes wrong should be blocked at least 50ms")
	}
	if err != nil {
		t.Fatal("err should be nil")
	}
	lock2.Unlock()
}

func TestManyLockalLocking(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	dlm := NewSyncProvider(ms).(*dlock_manager)

	var wg sync.WaitGroup
	var lock sync.Mutex
	lock.Lock()

	mxs := make([]Mutex, 0)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		lck := dlm.NewMutex(context.TODO(), "val")
		mxs = append(mxs, lck)
		go func(l Mutex) {
			err := l.Lock()
			if err != nil {
				t.Fatal("Must not error!")
			}
			lock.Lock()
			l.Unlock()
			lock.Unlock()
			wg.Done()
		}(lck)
	}

	for {
		<-time.After(time.Millisecond)
		ll := dlm.llocks["val"]
		if ll == nil {
			continue
		}
		if len(ll.sigChannels) == 9 {
			break
		}
	}

	lock.Unlock()
	wg.Wait()
	ll := dlm.llocks["val"]
	if ll != nil {
		t.Fatal("Must be no llocks")
	}
}

func TestManyCancelCtx(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	dlm := NewSyncProvider(ms).(*dlock_manager)

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	var lock sync.Mutex
	failed := 0
	good := 0
	mxs := make([]Mutex, 0)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		lck := dlm.NewMutex(ctx, "val")
		mxs = append(mxs, lck)
		go func(l Mutex) {
			defer wg.Done()

			err := l.Lock()
			lock.Lock()
			defer lock.Unlock()

			if err == nil {
				select {
				case <-ctx.Done():
					l.Unlock()
				}
				good++
			} else {
				failed++
			}

		}(lck)
	}

	for {
		<-time.After(time.Millisecond)
		ll := dlm.llocks["val"]
		if ll == nil {
			continue
		}
		if len(ll.sigChannels) == 9 {
			break
		}
	}

	cancel()

	wg.Wait()

	ll := dlm.llocks["val"]
	if ll != nil {
		t.Fatal("Must be no llocks")
	}

	if good+failed != 10 || good == 0 {
		t.Fatal("expecting good=1 or more(" + strconv.Itoa(good) + "), and good+failed=10( failed=" + strconv.Itoa(failed) + ")")
	}
}

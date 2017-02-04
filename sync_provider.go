package godist

import (
	"errors"
	"sync"

	"github.com/jrivets/gorivets"
	"golang.org/x/net/context"
)

type dlock struct {
	name string
	dlm  *dlock_manager
	ctx  context.Context
}

func (dl *dlock) Lock() error {
	return dl.dlm.lockGlobal(dl.ctx, dl.name)
}

func (dl *dlock) Unlock() {
	dl.dlm.unlockGlobal(dl.ctx, dl.name)
}

var logger gorivets.Logger = gorivets.NewNilLoggerProvider()("pff")

type dlock_manager struct {
	storage StorageConnector
	lock    sync.Mutex
	llocks  map[string]*local_lock
}

type local_lock struct {
	sigChannels []chan bool
}

func (dlm *dlock_manager) NewMutex(ctx context.Context, name string) Locker {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	logger.Debug("New mutex - ", name)
	return &dlock{name: name, dlm: dlm, ctx: ctx}
}

func (dlm *dlock_manager) lockGlobal(ctx context.Context, name string) error {
	err := dlm.lockLocal(ctx, name)
	if err != nil {
		return err
	}

	leaseId := dlm.storage.GetProcessLeaseId()
	for {
		r, err := dlm.storage.Create(ctx, &Record{name, string(leaseId), 0, leaseId})

		if err == nil {
			logger.Debug("lockGlobal(): ", name, " OK")
			return nil
		}

		if !CheckError(err, DLErrAlreadyExists) {
			dlm.unlockLocal(name)
			return err
		}

		r, err = dlm.storage.WaitForVersionChange(ctx, name, r.Version)
		if !CheckError(err, DLErrNotFound) && err != nil {
			dlm.unlockLocal(name)
			return err
		}
	}
}

func (dlm *dlock_manager) unlockGlobal(ctx context.Context, name string) {
	defer dlm.unlockLocal(name)
	r, err := dlm.storage.Get(ctx, name)

	procLeaseId := dlm.storage.GetProcessLeaseId()
	for r != nil && !CheckError(err, DLErrNotFound) {
		if r.Lease != procLeaseId {
			panic(errors.New("FATAL internal error: unlocking object which is locked by other locker. " +
				"expected owner=" + string(procLeaseId) +
				", but returned one is " + string(r.Lease)))
		}

		r, err = dlm.storage.Delete(ctx, r)
	}
}

func (dlm *dlock_manager) lockLocal(ctx context.Context, name string) error {
	logger.Debug("lockLocal(): ", name)
	ch := dlm.leaseLocalLock(name)

	if ch == nil {
		logger.Debug("lockLocal(): ", name, " OK")
		return nil
	}

	var err error = nil
	select {
	case <-ch:
		logger.Debug("lockLocal(): ", name, " received wakeup notification")
	case <-ctx.Done():
		logger.Debug("lockLocal(): ", name, " context closed")
		dlm.releaseLocalLock(name, ch)
		err = Error(DLErrClosed)
	}

	return err
}

func (dlm *dlock_manager) leaseLocalLock(name string) <-chan bool {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	ll := dlm.llocks[name]
	if ll == nil {
		ll = &local_lock{make([]chan bool, 0)}
		dlm.llocks[name] = ll
		return nil
	}
	ch := make(chan bool)
	ll.sigChannels = append(ll.sigChannels, ch)

	return ch
}

func (dlm *dlock_manager) releaseLocalLock(name string, ch <-chan bool) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	ll := dlm.llocks[name]
	for i, c := range ll.sigChannels {
		if c == ch {
			lastIdx := len(ll.sigChannels) - 1
			ll.sigChannels[i] = ll.sigChannels[lastIdx]
			ll.sigChannels = ll.sigChannels[:lastIdx]
			return
		}
	}

	// we holded channel, but could not find it is in the list. It means
	// that it was closed when we did not try to read out of there. Give the next
	// waiter a chance to execute then
	dlm.kickLocalWaiter(name, ll)
}

func (dlm *dlock_manager) unlockLocal(name string) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	dlm.kickLocalWaiter(name, dlm.llocks[name])
}

func (dlm *dlock_manager) kickLocalWaiter(name string, ll *local_lock) {
	if len(ll.sigChannels) == 0 {
		delete(dlm.llocks, name)
	} else {
		ch := ll.sigChannels[0]
		ll.sigChannels = ll.sigChannels[1:]
		close(ch)
	}
}

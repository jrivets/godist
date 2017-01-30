package godist

import (
	"errors"
	"sync"

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

type dlock_manager struct {
	storage Storage
	lock    sync.Mutex
	llocks  map[string]*local_lock
}

type local_lock struct {
	sigChannel chan bool
	counter    int
}

func (dlm *dlock_manager) NewMutex(ctx context.Context, name string) Mutex {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	return &dlock{name: name, dlm: dlm, ctx: ctx}
}

func (dlm *dlock_manager) lockGlobal(ctx context.Context, name string) error {
	err := dlm.lockLocal(ctx, name)
	if err != nil {
		return err
	}

	myProcId := dlm.storage.MyProcId()
	for {
		r, err := dlm.storage.Create(ctx, &Record{name, string(myProcId), 0, myProcId})

		if err == nil {
			return nil
		}

		if !CheckError(err, DLErrAlreadyExists) {
			dlm.unlockLocal(name)
			return err
		}

		r, err = dlm.storage.WaitVersionChange(ctx, name, r.Version)
		if !CheckError(err, DLErrNotFound) && err != nil {
			dlm.unlockLocal(name)
			return err
		}
	}
}

func (dlm *dlock_manager) unlockGlobal(ctx context.Context, name string) {
	defer dlm.unlockLocal(name)
	r, err := dlm.storage.Get(ctx, name)

	for r != nil && !CheckError(err, DLErrNotFound) {
		if r.Owner != dlm.storage.MyProcId() {
			panic(errors.New("FATAL internal error: unlocking object which is locked by other locker. " +
				"expected owner=" + string(dlm.storage.MyProcId()) +
				", but returned one is " + string(r.Owner)))
		}

		r, err = dlm.storage.Delete(ctx, r)
	}
}

func (dlm *dlock_manager) lockLocal(ctx context.Context, name string) error {
	ll, err := dlm.leaseLocalLock(name)
	if err != nil {
		return err
	}

	if ll.counter == 1 {
		return nil
	}

	err = nil
	select {
	case _, ok := <-ll.sigChannel:
		if !ok {
			err = error(DLErrClosed)
		}
	case <-ctx.Done():
		err = error(DLErrClosed)
	}

	if err != nil {
		dlm.releaseLocalLock(name, ll)
	}

	return err
}

func (dlm *dlock_manager) leaseLocalLock(name string) (*local_lock, error) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	ll := dlm.llocks[name]
	if ll == nil {
		ll = &local_lock{counter: 0, sigChannel: make(chan bool)}
		dlm.llocks[name] = ll
	}
	ll.counter++

	return ll, nil
}

func (dlm *dlock_manager) releaseLocalLock(name string, ll *local_lock) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	dlm.unlockLocalUnsafe(name, ll, false)
}

func (dlm *dlock_manager) unlockLocal(name string) {
	dlm.lock.Lock()
	defer dlm.lock.Unlock()

	ll := dlm.llocks[name]
	if ll == nil {
		return
	}

	dlm.unlockLocalUnsafe(name, ll, true)
}

func (dlm *dlock_manager) unlockLocalUnsafe(name string, ll *local_lock, signal bool) {
	ll.counter--
	if ll.counter == 0 {
		delete(dlm.llocks, name)
	}

	if signal && ll.counter > 0 {
		ll.sigChannel <- true
	}
}

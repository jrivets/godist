package godist

import (
	"sync"

	"github.com/satori/go.uuid"
	"golang.org/x/net/context"
)

type ms_record struct {
	key     string
	value   string
	version Version
	procId  ProcId
	waitChs []chan bool
}

type mem_storage struct {
	data   map[string]*ms_record
	procId ProcId
}

var (
	mss      []*mem_storage
	mss_lock sync.Mutex
)

func NewMemStorage() Storage {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	if mss == nil {
		mss = make([]*mem_storage, 0)
	}

	var data map[string]*ms_record
	if len(mss) > 0 {
		data = mss[0].data
	} else {
		data = make(map[string]*ms_record)
	}

	ms := newMemStorage(data)
	mss = append(mss, ms)
	return ms
}

func dropAll() {
	mss_lock.Lock()
	defer mss_lock.Unlock()
	mss = make([]*mem_storage, 0)
}

func newMemStorage(data map[string]*ms_record) *mem_storage {
	return &mem_storage{data: data, procId: ProcId(uuid.NewV4().String())}
}

func (ms *mem_storage) MyProcId() ProcId {
	return ms.procId
}

func (ms *mem_storage) IsValid(procId ProcId) bool {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	for _, ms1 := range mss {
		if ms1.procId == procId {
			return true
		}
	}
	return false
}

func (ms *mem_storage) Create(ctx context.Context, record *Record) (*Record, error) {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	if r, ok := ms.data[record.Key]; ok {
		return toRecord(r), Error(DLErrAlreadyExists)
	}

	if record.Owner != NIL_OWNER && record.Owner != ms.procId {
		panic("Incorrect usage: record.Owner=" + record.Owner + " should be NIL_OWNER or " + ms.procId)
	}

	r := to_ms_record(record)
	r.version = 1
	ms.data[r.key] = r
	return toRecord(r), nil
}

func (ms *mem_storage) Get(ctx context.Context, key string) (*Record, error) {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	if r, ok := ms.data[key]; ok {
		return toRecord(r), nil
	}

	return nil, Error(DLErrNotFound)
}

func (ms *mem_storage) CasByVersion(ctx context.Context, record *Record) (*Record, error) {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	r, ok := ms.data[record.Key]
	if !ok {
		return nil, Error(DLErrNotFound)
	}

	if record.Owner != NIL_OWNER && record.Owner != ms.procId {
		panic("Incorrect usage: record.Owner=" + record.Owner + " should be NIL_OWNER or " + ms.procId)
	}

	if r.version != record.Version {
		return toRecord(r), Error(DLErrWrongVersion)
	}

	r1 := to_ms_record(record)
	r1.version = r.version + 1
	r1.procId = r.procId
	ms.data[r.key] = r1
	r.notifyChans()
	return toRecord(r1), nil
}

func (ms *mem_storage) Delete(ctx context.Context, record *Record) (*Record, error) {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	r, ok := ms.data[record.Key]
	if !ok {
		return nil, Error(DLErrNotFound)
	}

	if r.version != record.Version {
		return toRecord(r), Error(DLErrWrongVersion)
	}

	delete(ms.data, record.Key)
	r.notifyChans()
	return nil, nil
}

func (ms *mem_storage) WaitVersionChange(ctx context.Context, key string, version Version) (*Record, error) {
	ch, err := ms.newChan(key, version)
	if err != nil {
		r, _ := ms.Get(ctx, key)
		return r, err
	}

	// will do this after ms.dropChan() (see below), what guarantees no write after the call
	defer close(ch)

	err = nil
	select {
	case <-ctx.Done():
		err = DLErrClosed
	case <-ch:
	}
	ms.dropChan(key, ch)

	if err != nil {
		return nil, err
	}

	return ms.Get(ctx, key)
}

func (ms *mem_storage) Close() {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	for k, v := range ms.data {
		if v.procId == ms.procId {
			delete(ms.data, k)
			v.notifyChans()
		}
	}

	for i, v := range mss {
		if v == ms {
			mss[i] = mss[len(mss)-1]
			mss = mss[:len(mss)-1]
		}
	}

	ms.procId = NIL_OWNER
}

func (ms *mem_storage) newChan(key string, version Version) (chan bool, error) {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	r, ok := ms.data[key]
	if !ok {
		return nil, Error(DLErrNotFound)
	}

	if r.version != version {
		return nil, Error(DLErrWrongVersion)
	}

	ch := make(chan bool, 1)
	r.waitChs = append(r.waitChs, ch)
	return ch, nil
}

func (ms *mem_storage) dropChan(key string, ch chan bool) {
	mss_lock.Lock()
	defer mss_lock.Unlock()

	msr, _ := ms.data[key]
	if msr == nil {
		return
	}

	for i, c := range msr.waitChs {
		if c == ch {
			l := len(msr.waitChs)
			msr.waitChs[l-1], msr.waitChs[i] = msr.waitChs[i], msr.waitChs[l-1]
			msr.waitChs = msr.waitChs[:l-1]
			return
		}
	}
}

func (msr *ms_record) notifyChans() {
	for _, ch := range msr.waitChs {
		ch <- true
	}
	msr.waitChs = make([]chan bool, 0)
}

func toRecord(r *ms_record) *Record {
	return &Record{Key: r.key, Value: r.value, Version: r.version, Owner: r.procId}
}

func to_ms_record(r *Record) *ms_record {
	return &ms_record{r.Key, r.Value, r.Version, r.Owner, make([]chan bool, 0)}
}

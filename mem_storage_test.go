package godist

import (
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestOpenClose(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	ms1 := NewMemStorage().(*mem_storage)

	ms.Create(context.TODO(), &Record{"k", "v1", 0, ms.GetProcessLeaseId()})
	ms1.Create(context.TODO(), &Record{"k1", "v1", 0, ms1.GetProcessLeaseId()})
	ms1.Create(context.TODO(), &Record{"k2", "v1", 0, NO_LEASE_ID})

	if len(mss) != 2 {
		t.Fatal("2 ms instances should be created")
	}
	if len(ms.data) != 3 || len(ms1.data) != 3 {
		t.Fatal("2 keys must be in the storage")
	}

	ms.Close()
	if ms.IsValidLeaseId(ms.GetProcessLeaseId()) || !ms.IsValidLeaseId(ms1.GetProcessLeaseId()) {
		t.Fatal("ms1 should be valid, but ms is invalid")
	}
	if len(mss) != 1 {
		t.Fatal("1 ms instance should be created")
	}
	if len(ms.data) != 2 || len(ms1.data) != 2 {
		t.Fatal("1 keys must be in the storage")
	}
	if mss[0] != ms1 {
		t.Fatal("only ms1 should be there")
	}

	ms1.Close()
	if len(mss) != 0 {
		t.Fatal("0 ms instances should be created")
	}
	if len(ms.data) != 1 || len(ms1.data) != 1 {
		t.Fatal("1 keys must be in the storage")
	}
}

func TestCreateGet(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	r0 := &Record{"K", "v1", 10, NO_LEASE_ID}
	ms.Create(context.TODO(), r0)
	if len(ms.data) != 1 {
		t.Fatal("*ms must contain one element")
	}

	_, err := ms.Create(context.TODO(), r0)
	if !CheckError(err, DLErrAlreadyExists) {
		t.Fatal("K must be already exist")
	}

	r, _ := ms.Get(context.TODO(), "K")
	if r == nil || r.Key != "K" || r.Value != "v1" || r.Version != 1 {
		t.Fatalf("Wrong record r0=%v, and found r=%v", r0, r)
	}

	_, err = ms.Get(context.TODO(), "k")
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Should not found ")
	}
}

func TestImmutableGet(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	r0 := &Record{"K", "v1", 10, NO_LEASE_ID}
	r, _ := ms.Create(context.TODO(), r0)
	if len(ms.data) != 1 {
		t.Fatal("*ms must contain one element")
	}

	if r.Value != "v1" {
		t.Fatal("We read something wrong ", r.Value)
	}
	r.Value = "val2"

	r, _ = ms.Get(context.TODO(), "K")
	if r.Value != "v1" {
		t.Fatal("We read something wrong val=", r.Value)
	}

	ms.Close()
}

func TestCasByVersion(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	r0 := &Record{"K", "v1", 10, NO_LEASE_ID}
	r, err := ms.Create(context.TODO(), r0)
	if err != nil {
		t.Fatal("K must be created, but err=", err)
	}
	if r.Version != 1 {
		t.Fatalf("K version must be 1, but it is %v", r)
	}

	r.Value = "v2"
	r, err = ms.CasByVersion(context.TODO(), r)
	if r.Version != 2 {
		t.Fatalf("K version must be 2, but it is %v", r)
	}
	if err != nil {
		t.Fatal("CAS must work well err=", err)
	}

	r2, err := ms.CasByVersion(context.TODO(), r0)
	if !CheckError(err, DLErrWrongVersion) {
		t.Fatal("Should report about wrong version err=", err)
	}
	if r2.Version != r.Version {
		t.Fatalf("K version (%v) must be same as in %v", r2, r)
	}

	ms.Close()
}

func TestWaitForVersionChange(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	ms1 := NewMemStorage().(*mem_storage)
	r0 := &Record{"K", "v1", 10, NO_LEASE_ID}
	r, _ := ms.Create(context.TODO(), r0)

	rxx, err := ms.WaitForVersionChange(context.TODO(), "Kfff", 234)
	if rxx != nil || !CheckError(err, DLErrNotFound) {
		t.Fatal("(nil, DLErrNotFound) is expected")
	}

	go func(r *Record) {
		// be sure we reached WaitVersion
		msr := ms.data["K"]
		for len(msr.waitChs) == 0 {
			time.After(time.Millisecond)
		}

		r.Value = "val2"
		ms1.CasByVersion(context.TODO(), r)
	}(r)

	r, _ = ms.WaitForVersionChange(context.TODO(), "K", r.Version)
	if r.Value != "val2" || r.Version != 2 {
		t.Fatal("Expecting updated version and value")
	}

	_, err = ms.WaitForVersionChange(context.TODO(), "K", r.Version-1)
	if !CheckError(err, DLErrWrongVersion) {
		t.Fatal("Expecting DLErrWrongVersion")
	}

	msr := ms.data["K"]
	if len(msr.waitChs) != 0 {
		t.Fatal("Expecting no opened channels in the record")
	}
	go func() {
		// be sure we reached WaitVersion
		msr := ms.data["K"]
		for len(msr.waitChs) == 0 {
			time.After(time.Millisecond)
		}
		ms1.Delete(context.TODO(), r)
	}()

	r, err = ms.WaitForVersionChange(context.TODO(), "K", r.Version)
	if r != nil || err != nil {
		t.Fatal("The value is deleted, why not (nil, nil)?")
	}

	ms.Close()
	ms1.Close()

	_, err = ms.WaitForVersionChange(context.TODO(), "K", 123452345)
	if !CheckError(err, DLErrNotFound) {
		t.Fatal("Expecting DLErrNotFound")
	}

}

func TestWaitForVersionChange2(t *testing.T) {
	dropAll()
	ms := NewMemStorage().(*mem_storage)
	r0 := &Record{"K", "v1", 10, NO_LEASE_ID}
	r, _ := ms.Create(context.TODO(), r0)

	ctx, cancel := context.WithCancel(context.Background())
	go func(r *Record) {
		cancel()
		time.Sleep(time.Millisecond)
		r.Value = "val2"
		ms.CasByVersion(context.TODO(), r)
	}(r)

	r, err := ms.WaitForVersionChange(ctx, "K", r.Version)
	if !CheckError(err, DLErrClosed) {
		t.Fatal("Expecting Closed channel but err=", err)
	}

	ms.Close()

}

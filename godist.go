package godist

import (
	"github.com/jrivets/gorivets"
	"golang.org/x/net/context"
)

type (
	Version int64

	// LeaseId is a record lease identifier. Every record in the
	// storage can have a lease id or has NO_LEASE_ID. Records that have a lease
	// will be automatically deleted if the lease is expired. Lease scope
	// is a time-frame when the lease is valid.
	LeaseId string

	// LeaseProvider an interface which allows to manage leasese in conjuction
	// with the StorageConnector
	LeaseProvider interface {

		// Returns the process lease Id. Returned value is always same for the
		// same process and it is considered invalid as soon as the process is
		// over. Based on implementation details, in case of the current
		// process is crashed brutally, its lease Id can be reported to others
		// as valid for some period of time.
		GetProcessLeaseId() LeaseId

		// Returns true if the lease Id is valid.
		IsValidLeaseId(leaseId LeaseId) bool
	}

	// A record that can be stroed in distributed storage
	Record struct {
		Key   string
		Value string

		// A version that identifies the record. It is managed by storage
		Version Version

		// The record lease. If the value is not empty
		// (Lease != NO_LEASE_ID), the record availability is defined by the
		// lease scope. As soon as the lease becomes invalid, the record is
		// Removed from the storage permanently
		//
		// Records with NO_LEASE_ID should be deleted explicitly. The value
		// Can be set only when the record is created and cannot be changed
		// during updates.
		Lease LeaseId
	}

	// The StorageConnector interface defines some operations over the record storage.
	// The record storage allows to keep key-value pairs, and supports a set
	// of operations that allow to implement some distributed primitives
	StorageConnector interface {
		LeaseProvider

		// Creates a new record in the storage. It returns existing record with
		// DLErrAlreadyExists error if it already exists in the storage
		Create(ctx context.Context, record *Record) (*Record, error)

		// Retrieves the record by its key. It will return nil and an error,
		// which will indicate the reason why the operation was not succesful.
		Get(ctx context.Context, key string) (*Record, error)

		// Compare-and-set the record Value if the record stored version is
		// same as in the provided record. The record version will be updated
		// too.
		//
		// Returns updated stored record or stored record together with an error
		// if the operation was not successful
		CasByVersion(ctx context.Context, record *Record) (*Record, error)

		// Tries to delete the record. Operation can fail if stored record
		// version is different than existing one. This case the stored record
		// is returned as well as the appropriate error. If the record is deleted
		// both results will be nil (!)
		Delete(ctx context.Context, record *Record) (*Record, error)

		// Waits for the record version change. The version param contans an
		// expected record version. The call returns immediately if the record
		// is not found (DLErrNotFound will be reported), or the record version
		// is different than expected (no error this case is returned). Otherwise
		// the call will be blocked until one of the following things happens:
		// - context is done
		// - the record version is changed
		// - the record is deleted
		// Updated version of the record will be deleted together with error=nil
		// If the record is deleted (nil, nil) is returned
		WaitForVersionChange(ctx context.Context, key string, version Version) (*Record, error)
	}

	// Errors that can be returned by the package
	Error int

	// Returns a locker for distributed lock.
	Locker interface {

		// Locks the object or returns an error if it is not possible.
		// If error is not nil, then the lock operation considered failed.
		Lock() error

		// Unlocks the object. Behavior can be different for different primitives
		Unlock()
	}

	SyncProvider interface {

		// Returns the distributed mutex by its "name". The following rules are
		// applied:
		// - Different Lockers for the same name address same distributed mutex.
		// - Only one go-routine in the distributed system can lock the mutex in a raise.
		// - Any go-routine of the process which locked the mutex can unlock it.
		NewMutex(ctx context.Context, name string) Locker
	}
)

const (
	// Errors
	DLErrAlreadyExists Error = 1
	DLErrNotFound      Error = 2
	DLErrWrongVersion  Error = 3
	DLErrClosed        Error = 4
	DLErrWrongLeaseId  Error = 5

	NO_LEASE_ID = ""
)

func CheckError(e error, expErr Error) bool {
	if e == nil {
		return false
	}
	err, ok := e.(Error)
	if !ok {
		return false
	}
	return err == expErr
}

func (e Error) Error() string {
	switch e {
	case DLErrAlreadyExists:
		return "Record with the key already exists"
	case DLErrNotFound:
		return "Record with the key is not found"
	case DLErrWrongVersion:
		return "Unexpected record version"
	case DLErrClosed:
		return "The Context is done or storage is closed if supported"
	case DLErrWrongLeaseId:
		return "Unexpected lease Id"
	}
	return ""
}

func NewSyncProvider(storage StorageConnector) SyncProvider {
	return &dlock_manager{storage: storage, llocks: make(map[string]*local_lock)}
}

func SetLogger(log gorivets.Logger) {
	logger = log
}

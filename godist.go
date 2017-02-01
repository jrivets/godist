package godist

import (
	"github.com/jrivets/gorivets"
	"golang.org/x/net/context"
)

type (
	Version int64

	// ProcId identifies a process in the distributed system. It is the
	// storage responsibility to maintain the value unique (see the Storage interface)
	// and guarantee there is no 2 storage instances with same identifier in
	// the distributed system.
	ProcId string

	// A Storage record
	Record struct {
		Key     string
		Value   string
		Version Version

		// Identifies a process which owns the record. If the value is not empty
		// (Owner != NIL_OWNER), the record is owned by the process id, and as
		// soon as the process is over or dies, the record can be automatically
		// deleted from the storage because it is not owned anymore.
		//
		// Records with NIL_OWNER should be deleted explicitly. The value
		// Can be set only when the record is created.
		Owner ProcId
	}

	// The Storage interface defines some operations over the record storage.
	// The record storage allows to keep key-value pairs in the distributed
	// system and supports a set of operations that allow to implement some
	// synchronization distributed objects
	Storage interface {

		// Returns the process unique identifier for the storage object.
		// It is the storage implementation responsibility to keep and support
		// the ProcId uniqueness. When the process is over or storage is closed
		// the implementation guarantees that all records with the ProcId will
		// be deleted or diregarded soon.
		MyProcId() ProcId

		// Returns whether provided procId is valid or not
		IsValid(procId ProcId) bool

		// Creates a new one, or returns existing one with DLErrAlreadyExists
		// error.
		Create(ctx context.Context, record *Record) (*Record, error)

		// Retrieves the record by its key. It will return nil and an error,
		// which will indicate the reason why the operation was not succesful.
		Get(ctx context.Context, key string) (*Record, error)

		// Compare and set the record value if the record stored version is
		// same as in the provided record. The record version will be updated
		// too.
		//
		// Returns updated stored record or stored record and error if the
		// operation was not successful
		CasByVersion(ctx context.Context, record *Record) (*Record, error)

		// Tries to delete the record. Operation can fail if stored record
		// version is different than existing one. This case the stored version
		// is returned as well as the appropriate error. If the record is deleted
		// both results are nil(!)
		Delete(ctx context.Context, record *Record) (*Record, error)

		// Waits for the record version change. The version param contans an
		// expected version.
		WaitVersionChange(ctx context.Context, key string, version Version) (*Record, error)
	}

	Error int

	Mutex interface {
		// Locks the mutex or return error if it is not possible.
		// The method is panicing with incorrect usage (a second attempt to lock
		// the same mutex object without unlocking it
		Lock() error
		Unlock()
	}

	SyncProvider interface {
		NewMutex(ctx context.Context, name string) Mutex
	}
)

const (
	// Errors
	DLErrAlreadyExists Error = 1
	DLErrNotFound      Error = 2
	DLErrWrongVersion  Error = 3
	DLErrClosed        Error = 4

	NIL_OWNER = ""
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
		return "The Distributed Lock Manager is already closed."
	}
	return ""
}

func NewSyncProvider(storage Storage) SyncProvider {
	return &dlock_manager{storage: storage, llocks: make(map[string]*local_lock)}
}

func SetLogger(log gorivets.Logger) {
	logger = log
}

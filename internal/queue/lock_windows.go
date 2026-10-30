//go:build windows

package queue

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	lockfileFailImmediately = 0x1
	lockfileExclusiveLock   = 0x2
)

var procLockFileEx = kernel32.NewProc("LockFileEx")

func lockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	ok, _, err := procLockFileEx.Call(
		file.Fd(),
		lockfileExclusiveLock|lockfileFailImmediately,
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&overlapped)), //nolint:gosec // G103: Required for the Windows API
	)
	if ok == 0 {
		return err
	}
	return nil
}

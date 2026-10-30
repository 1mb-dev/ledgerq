//go:build unix || linux || darwin || freebsd || openbsd || netbsd

package queue

import (
	"os"
	"syscall"
)

func lockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) //nolint:gosec // G115: Fd fits an int
}

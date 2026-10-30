package queue

import (
	"fmt"
	"os"
	"path/filepath"
)

const lockFileName = ".lock"

// acquireDirLock takes the queue's single-process lock. Open repairs segments, so a second
// process must never reach them while another is writing. The OS drops the lock when the holder
// exits, however it exits, so a crash leaves nothing to clean up.
func acquireDirLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockFileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644) //nolint:gosec // G304: Path built from the queue directory
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("queue %s is open in another process, close it first: %w", dir, err)
	}
	return file, nil
}

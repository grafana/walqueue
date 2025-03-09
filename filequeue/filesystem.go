package filequeue

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// FileSystem defines the interface for file operations used by the queue
type FileSystem interface {
	// MkdirAll creates a directory named path, along with any necessary parents
	MkdirAll(path string, perm os.FileMode) error

	// Glob returns the names of all files matching the pattern
	Glob(pattern string) ([]string, error)

	// WriteFile writes data to the named file, creating it if necessary
	WriteFile(name string, data []byte, perm os.FileMode) error

	// ReadFile reads the file named by filename and returns the contents
	ReadFile(name string) ([]byte, error)

	// Remove removes the named file or (empty) directory
	Remove(name string) error
}

// DiskFS implements FileSystem using the actual filesystem
type DiskFS struct{}

func NewDiskFS() *DiskFS {
	return &DiskFS{}
}

func (fs *DiskFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (fs *DiskFS) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

func (fs *DiskFS) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}

func (fs *DiskFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (fs *DiskFS) Remove(name string) error {
	return os.Remove(name)
}

// MemoryFS implements FileSystem using in-memory storage
type MemoryFS struct {
	mu          sync.RWMutex
	files       map[string][]byte
	dirs        map[string]struct{}
	usedBytes   int64            // Tracks total bytes used
	maxBytes    int64            // Memory limit in bytes (0 means unlimited)
	fileModTime map[string]int64 // Tracks file modification times for pruning
}

func NewMemoryFS() *MemoryFS {
	return NewMemoryFSWithLimit(0) // Default to unlimited
}

// NewMemoryFSWithLimit creates a new memory filesystem with a maximum memory limit
// If maxBytes is 0, no limit is applied
func NewMemoryFSWithLimit(maxBytes int64) *MemoryFS {
	return &MemoryFS{
		files:       make(map[string][]byte),
		dirs:        make(map[string]struct{}),
		maxBytes:    maxBytes,
		fileModTime: make(map[string]int64),
	}
}

func (fs *MemoryFS) MkdirAll(path string, _ os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.dirs[path] = struct{}{}
	// Create parent directories too
	dir := filepath.Dir(path)
	for dir != "." && dir != "/" {
		fs.dirs[dir] = struct{}{}
		dir = filepath.Dir(dir)
	}
	return nil
}

func (fs *MemoryFS) Glob(pattern string) ([]string, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var matches []string
	for fileName := range fs.files {
		matched, err := filepath.Match(pattern, fileName)
		if err != nil {
			return nil, err
		}
		if matched {
			matches = append(matches, fileName)
		}
	}
	return matches, nil
}

func (fs *MemoryFS) WriteFile(name string, data []byte, _ os.FileMode) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Create a copy of the data to avoid later mutations
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	// Calculate the change in memory usage
	var byteDelta int64

	// Check if we're replacing an existing file
	if oldData, exists := fs.files[name]; exists {
		byteDelta = int64(len(dataCopy)) - int64(len(oldData))
	} else {
		byteDelta = int64(len(dataCopy))
	}

	// If we have a memory limit and we would exceed it, prune oldest files
	if fs.maxBytes > 0 && (fs.usedBytes+byteDelta) > fs.maxBytes {
		err := fs.pruneOldestFiles(byteDelta)
		if err != nil {
			return err
		}
	}

	// Store the file data
	fs.files[name] = dataCopy

	// Update timestamps and memory usage
	fs.fileModTime[name] = unixNanoNow()
	fs.usedBytes += byteDelta

	// Create parent directory
	dir := filepath.Dir(name)
	fs.dirs[dir] = struct{}{}

	return nil
}

func (fs *MemoryFS) ReadFile(name string) ([]byte, error) {
	fs.mu.Lock() // Using full lock to update modification time
	defer fs.mu.Unlock()

	data, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}

	// Update access time - this helps files being actively read to not get pruned
	fs.fileModTime[name] = unixNanoNow()

	// Return a copy of the data to avoid mutations
	result := make([]byte, len(data))
	copy(result, data)

	return result, nil
}

func (fs *MemoryFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fileData, ok := fs.files[name]
	if !ok {
		_, dirOk := fs.dirs[name]
		if !dirOk {
			return os.ErrNotExist
		}
		delete(fs.dirs, name)
		return nil
	}

	// Update memory usage
	fs.usedBytes -= int64(len(fileData))

	// Delete the file data and metadata
	delete(fs.files, name)
	delete(fs.fileModTime, name)

	return nil
}

// pruneOldestFiles removes the oldest files to free up at least neededBytes
// Caller must hold the mutex
func (fs *MemoryFS) pruneOldestFiles(neededBytes int64) error {
	// If we have no files, we can't free any space
	if len(fs.files) == 0 {
		return fmt.Errorf("memory limit exceeded, but no files to prune")
	}

	// Build a list of files sorted by modification time
	type fileInfo struct {
		name    string
		modTime int64
		size    int64
	}

	files := make([]fileInfo, 0, len(fs.files))
	for name, data := range fs.files {
		modTime, exists := fs.fileModTime[name]
		if !exists {
			modTime = 0 // If we don't have a mod time (shouldn't happen), treat as oldest
		}
		files = append(files, fileInfo{
			name:    name,
			modTime: modTime,
			size:    int64(len(data)),
		})
	}

	// Sort files by modification time (oldest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime < files[j].modTime
	})

	// Remove oldest files until we have freed enough space
	bytesFreed := int64(0)
	for _, file := range files {
		// Don't free more files than needed
		if bytesFreed >= neededBytes {
			break
		}

		// Remove the file data
		bytesFreed += file.size
		fs.usedBytes -= file.size
		delete(fs.files, file.name)
		delete(fs.fileModTime, file.name)
	}

	return nil
}

// unixNanoNow returns the current time as nanoseconds since Unix epoch
func unixNanoNow() int64 {
	return time.Now().UnixNano()
}

// GetUsedBytes returns the current amount of memory used by the filesystem
func (fs *MemoryFS) GetUsedBytes() int64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.usedBytes
}

// GetMaxBytes returns the memory limit of the filesystem (0 means unlimited)
func (fs *MemoryFS) GetMaxBytes() int64 {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.maxBytes
}

// SetMaxBytes updates the memory limit of the filesystem (0 means unlimited)
// If the new limit is lower than current usage, oldest files will be pruned
func (fs *MemoryFS) SetMaxBytes(maxBytes int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Set the new limit
	fs.maxBytes = maxBytes

	// If we have a limit and we exceed it, prune oldest files
	if maxBytes > 0 && fs.usedBytes > maxBytes {
		neededBytes := fs.usedBytes - maxBytes
		return fs.pruneOldestFiles(neededBytes)
	}

	return nil
}

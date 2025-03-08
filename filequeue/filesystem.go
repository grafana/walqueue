package filequeue

import (
	"os"
	"path/filepath"
	"sync"
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
	mu    sync.RWMutex
	files map[string][]byte
	dirs  map[string]struct{}
}

func NewMemoryFS() *MemoryFS {
	return &MemoryFS{
		files: make(map[string][]byte),
		dirs:  make(map[string]struct{}),
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
	
	fs.files[name] = dataCopy
	
	// Create parent directory
	dir := filepath.Dir(name)
	fs.dirs[dir] = struct{}{}
	
	return nil
}

func (fs *MemoryFS) ReadFile(name string) ([]byte, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	
	data, ok := fs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	
	// Return a copy of the data to avoid mutations
	result := make([]byte, len(data))
	copy(result, data)
	
	return result, nil
}

func (fs *MemoryFS) Remove(name string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	
	_, ok := fs.files[name]
	if !ok {
		_, dirOk := fs.dirs[name]
		if !dirOk {
			return os.ErrNotExist
		}
		delete(fs.dirs, name)
		return nil
	}
	
	delete(fs.files, name)
	return nil
}
package filequeue

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
)

var _ types.FileStorage = (*queue)(nil)

// StorageType defines the type of storage to use for the queue
type StorageType string

const (
	// StorageDisk uses the actual filesystem for storage
	StorageDisk StorageType = "disk"
	// StorageMemory uses an in-memory filesystem for storage (data is lost on restart)
	StorageMemory StorageType = "memory"
)

// queue represents a file-based queue. This is a list implemented as files ordered by id with a name pattern: <id>.committed
// Each file contains a byte buffer and an optional metadata map.
// The queue can use either the actual filesystem or an in-memory filesystem.
type queue struct {
	directory string
	maxID     int
	logger    log.Logger
	dataQueue *types.Mailbox[types.Data]
	// Out is where to send data when pulled from queue, it is assumed that it will
	// block until ready for another record.
	out func(ctx context.Context, dh types.DataHandle)
	// files is the list of files found initially.
	files []string
	stats types.StatsHub
	// fs is the filesystem implementation to use (disk or memory)
	fs FileSystem
	// totalBytesOnDisk tracks the total bytes stored in the filesystem
	totalBytesOnDisk int64
}

// QueueOption is a function that can modify a queue.
type QueueOption func(*queue)

// WithStorageType sets the storage type for the queue. Default is StorageDisk.
func WithStorageType(storageType StorageType) QueueOption {
	return func(q *queue) {
		switch storageType {
		case StorageMemory:
			q.fs = NewMemoryFS()
		default:
			q.fs = NewDiskFS()
		}
	}
}

// WithCustomFS sets a custom filesystem implementation for the queue.
func WithCustomFS(fs FileSystem) QueueOption {
	return func(q *queue) {
		q.fs = fs
	}
}

// NewQueue returns an implementation of FileStorage.
func NewQueue(directory string, out func(ctx context.Context, dh types.DataHandle), stats types.StatsHub, logger log.Logger, opts ...QueueOption) (types.FileStorage, error) {
	q := &queue{
		directory: directory,
		logger:    logger,
		out:       out,
		dataQueue: types.NewMailbox[types.Data](),
		files:     make([]string, 0),
		stats:     stats,
		fs:        NewDiskFS(), // Default to disk storage
	}

	// Apply options
	for _, opt := range opts {
		opt(q)
	}

	// Create directory if it doesn't exist
	err := q.fs.MkdirAll(directory, 0777)
	if err != nil {
		return nil, err
	}

	// We dont actually support uncommitted but I think its good to at least have some naming to avoid parsing random files
	// that get installed into the system.
	matches, _ := q.fs.Glob(filepath.Join(directory, "*.committed"))
	ids := make([]int, len(matches))

	// Try and grab the id from each file.
	// e.g. grab 1 from `1.committed`
	for i, fileName := range matches {
		id, err := strconv.Atoi(strings.ReplaceAll(filepath.Base(fileName), ".committed", ""))
		if err != nil {
			level.Error(logger).Log("msg", "unable to convert numeric prefix for committed file", "err", err, "file", fileName)
			continue
		}
		ids[i] = id
	}
	sort.Ints(ids)
	var currentMaxID int
	if len(ids) > 0 {
		currentMaxID = ids[len(ids)-1]
	}
	q.maxID = currentMaxID

	// Save the existing files in `q.existingFiles`, which will have their data pushed to `out` when actor starts.
	for _, id := range ids {
		name := filepath.Join(directory, fmt.Sprintf("%d.committed", id))
		q.files = append(q.files, name)

		// Get file size to track total bytes on disk
		if data, err := q.fs.ReadFile(name); err == nil {
			q.totalBytesOnDisk += int64(len(data))
		}
	}

	// Report initial metric
	q.reportDiskStats()

	return q, nil
}

func (q *queue) Start(ctx context.Context) {
	// Queue up our existing items.
	for _, name := range q.files {
		// Need to make a copy of name for the closure
		fileName := name
		q.out(ctx, types.DataHandle{
			Name: fileName,
			Pop: func() (map[string]string, []byte, error) {
				return q.get(fileName)
			},
		})
	}
	// We only want to process existing files once.
	q.files = nil
	go q.run(ctx)
}

func (q *queue) Stop() {
}

// Store will add records to the dataQueue that will add the data to the filesystem. This is an unbuffered channel.
// Its possible in the future we would want to make it a buffer of 1, but so far it hasnt been an issue in testing.
func (q *queue) Store(ctx context.Context, meta map[string]string, data []byte) error {
	return q.dataQueue.Send(ctx, types.Data{
		Meta: meta,
		Data: data,
	})
}

// get returns the data of the file or an error if something wrong went on.
func (q *queue) get(name string) (map[string]string, []byte, error) {
	defer q.deleteFile(name)
	buf, err := q.readFile(name)
	if err != nil {
		return nil, nil, err
	}
	r := &Record{}
	_, err = r.UnmarshalMsg(buf)
	if err != nil {
		return nil, nil, err
	}
	return r.Meta, r.Data, nil
}

// run allows most of the queue to be single threaded with work only coming in and going out via mailboxes(channels).
func (q *queue) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-q.dataQueue.ReceiveC():
			if !ok {
				return
			}
			name, err := q.add(item.Meta, item.Data)
			if err != nil {
				level.Error(q.logger).Log("msg", "error adding item - dropping data", "err", err)
				continue
			}
			// The idea is that this callee will block/process until the callee is ready for another file.
			// Need to make a copy of name for the closure
			fileName := name
			q.out(ctx, types.DataHandle{
				Name: fileName,
				Pop: func() (map[string]string, []byte, error) {
					return q.get(fileName)
				},
			})
		}
	}
}

// Add a file to the queue (as committed).
func (q *queue) add(meta map[string]string, data []byte) (string, error) {
	if meta == nil {
		meta = make(map[string]string)
	}
	q.maxID++
	name := filepath.Join(q.directory, fmt.Sprintf("%d.committed", q.maxID))
	meta["file_id"] = strconv.Itoa(q.maxID)
	r := &Record{
		Meta: meta,
		Data: data,
	}
	// Not reusing a buffer here since allocs are not bad here and we are trying to reduce memory.
	rBuf, err := r.MarshalMsg(nil)
	if err != nil {
		return "", err
	}
	err = q.writeFile(name, rBuf)
	if err != nil {
		return "", err
	}
	q.stats.SendSerializerStats(types.SerializerStats{
		FileIDWritten: q.maxID,
	})
	return name, nil
}

func (q *queue) writeFile(name string, data []byte) error {
	err := q.fs.WriteFile(name, data, 0644)
	if err == nil {
		// Update totalBytesOnDisk when successfully writing a file
		q.totalBytesOnDisk += int64(len(data))
		q.reportDiskStats()
	}
	return err
}

func (q *queue) deleteFile(name string) {
	// Get file size before deleting to subtract from totalBytesOnDisk
	if data, err := q.fs.ReadFile(name); err == nil {
		q.totalBytesOnDisk -= int64(len(data))
	}

	err := q.fs.Remove(name)
	if err != nil {
		level.Error(q.logger).Log("msg", "unable to delete file", "err", err, "file", name)
	} else {
		q.reportDiskStats()
	}
}

func (q *queue) readFile(name string) ([]byte, error) {
	bb, err := q.fs.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return bb, err
}

// reportDiskStats sends the current total bytes on disk as a metric
func (q *queue) reportDiskStats() {
	q.stats.SendSerializerStats(types.SerializerStats{
		TotalBytesOnDisk: q.totalBytesOnDisk,
	})
}

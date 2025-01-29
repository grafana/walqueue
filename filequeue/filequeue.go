package filequeue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"github.com/vladopajic/go-actor/actor"
)

var _ types.FileStorage = (*queue)(nil)

// queue represents an on-disk queue. This is a list implemented as files ordered by id with a name pattern: <id>.committed
// Each file contains a byte buffer and an optional metatdata map.
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
}

// NewQueue returns a implementation of FileStorage.
func NewQueue(directory string, out func(ctx context.Context, dh types.DataHandle), logger log.Logger) (types.FileStorage, error) {
	err := os.MkdirAll(directory, 0777)
	if err != nil {
		return nil, err
	}

	// We dont actually support uncommitted but I think its good to at least have some naming to avoid parsing random files
	// that get installed into the system.
	matches, _ := filepath.Glob(filepath.Join(directory, "*.committed"))
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
	q := &queue{
		directory: directory,
		maxID:     currentMaxID,
		logger:    logger,
		out:       out,
		dataQueue: types.NewMailbox[types.Data](),
		files:     make([]string, 0),
	}

	// Save the existing files in `q.existingFiles`, which will have their data pushed to `out` when actor starts.
	for _, id := range ids {
		name := filepath.Join(directory, fmt.Sprintf("%d.committed", id))
		q.files = append(q.files, name)
	}
	return q, nil
}

func (q *queue) Start(ctx context.Context) {
	go q.run(ctx)
}

func (q *queue) Stop() {
	q.dataQueue.Close()
}

// Store will add records to the dataQueue that will add the data to the filesystem. This is an unbuffered channel.
// Its possible in the future we would want to make it a buffer of 1, but so far it hasnt been an issue in testing.
func (q *queue) Store(ctx context.Context, meta map[string]string, data []byte) error {
	d := types.Data{
		Meta: meta,
		Data: data,
	}
	return q.dataQueue.Send(ctx, d)
}

// get returns the data of the file or an error if something wrong went on.
func get(logger log.Logger, name string) (map[string]string, []byte, error) {
	defer deleteFile(logger, name)
	buf, err := readFile(name)
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

// DoWork allows most of the queue to be single threaded with work only coming in and going out via mailboxes(channels).
func (q *queue) run(ctx actor.Context) {
	// Queue up our existing items, we cant do this earlier since the actor isnt started.
	for _, name := range q.files {
		q.out(ctx, types.DataHandle{
			Name: name,
			Pop: func() (map[string]string, []byte, error) {
				return get(q.logger, name)
			},
		})
	}
	// We only want to process existing files once.
	q.files = nil
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-q.dataQueue.Receive():
			if !ok {
				return
			}
			name, err := q.add(item.Meta, item.Data)
			if err != nil {
				level.Error(q.logger).Log("msg", "error adding item - dropping data", "err", err)
				continue
			}
			// The idea is that this will callee will block/process until the callee is ready for another file.
			q.out(ctx, types.DataHandle{
				Name: name,
				Pop: func() (map[string]string, []byte, error) {
					return get(q.logger, name)
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
	return name, nil
}

func (q *queue) writeFile(name string, data []byte) error {
	return os.WriteFile(name, data, 0644)
}

func deleteFile(logger log.Logger, name string) {
	err := os.Remove(name)
	if err != nil {
		level.Error(logger).Log("msg", "unable to delete file", "err", err, "file", name)
	}
}
func readFile(name string) ([]byte, error) {
	bb, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return bb, err
}

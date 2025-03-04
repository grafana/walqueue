package types

import (
	"context"
)

type FileStorage interface {
	Start(ctx context.Context)
	Stop()
	Store(ctx context.Context, meta map[string]string, value []byte) error
}

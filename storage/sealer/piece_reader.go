package sealer

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	"go.opencensus.io/stats"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/metrics"
)

// For small read skips, it's faster to "burn" some bytes than to setup new sector reader.
// Assuming 1ms stream seek latency, and 1G/s stream rate, we're willing to discard up to 1 MiB.
var MaxPieceReaderBurnBytes int64 = 1 << 20 // 1M
var ReadBuf = 128 * (127 * 8)               // unpadded(128k)

type pieceGetter func(ctx context.Context, offset uint64) (io.ReadCloser, error)

type pieceReader struct {
	ctx       context.Context
	getReader pieceGetter
	pieceCid  cid.Cid
	len       abi.UnpaddedPieceSize
	onClose   context.CancelFunc

	closed bool
	seqAt  int64 // next byte to be read by io.Reader

	mu   sync.Mutex
	pool *sync.Pool
	lru  *lruCache
}

func (p *pieceReader) init() (_ *pieceReader, err error) {
	stats.Record(p.ctx, metrics.DagStorePRInitCount.M(1))

	// Initialize the pool and LRU cache
	p.pool = &sync.Pool{}
	p.lru = newLRUCache(5) // Adjust the cache size based on your requirements
	p.pool.New = func() interface{} {
		return p.getReader(uint64(p.seqAt))
	}

	return p, nil
}

func (p *pieceReader) check() error {
	if p.closed {
		return xerrors.Errorf("reader closed")
	}

	return nil
}

func (p *pieceReader) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.check(); err != nil {
		return err
	}

	if p.r != nil {
		if err := p.r.Close(); err != nil {
			return err
		}
		if err := p.r.Close(); err != nil {
			return err
		}
		p.r = nil
	}

	p.onClose()

	p.closed = true

	return nil
}

func (p *pieceReader) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.check(); err != nil {
		return 0, err
	}

	n, err := p.readAtUnlocked(b, p.seqAt)
	p.seqAt += int64(n)
	return n, err
}

func (p *pieceReader) Seek(offset int64, whence int) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.check(); err != nil {
		return 0, err
	}

	switch whence {
	case io.SeekStart:
		p.seqAt = offset
	case io.SeekCurrent:
		p.seqAt += offset
	case io.SeekEnd:
		p.seqAt = int64(p.len) + offset
	default:
		return 0, xerrors.Errorf("bad whence")
	}

	return p.seqAt, nil
}

func (p *pieceReader) ReadAt(b []byte, off int64) (n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.readAtUnlocked(b, off)
}

func (p *pieceReader) readAtUnlocked(b []byte, off int64) (n int, err error) {
	if err := p.check(); err != nil {
		return 0, err
	}

	stats.Record(p.ctx, metrics.DagStorePRBytesRequested.M(int64(len(b))))

	// Check if we have an existing reader in the LRU cache
	reader, ok := p.lru.get(off)
	if !ok {
		// Acquire a reader from the pool or create a new one if necessary
		reader = p.pool.Get().(io.ReadCloser)
		_, err = reader.Seek(off, io.SeekStart)
		if err != nil {
			return 0, err
		}
		p.lru.add(off, reader)
	}

	// Create a bufio.Reader to buffer the read
	br := bufio.NewReaderSize(reader, ReadBuf)

	n, err = io.ReadFull(br, b)
	if n < len(b) {
		log.Debugw("pieceReader short read", "piece", p.pieceCid, "at", off, "toEnd", int64(p.len)-off, "n", len(b), "read", n, "err", err)
	}
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	// Update the LRU cache and return the reader to the pool
	p.lru.touch(off)
	p.pool.Put(reader)

	return n, err
}

var _ mount.Reader = (*pieceReader)(nil)

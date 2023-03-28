package sealer

import (
	"bufio"
	"container/list"
	"context"
	"errors"
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

type pieceReader struct {
	ctx      context.Context
	pieceCid cid.Cid
	len      abi.UnpaddedPieceSize
	onClose  context.CancelFunc

	closed bool
	seqAt  int64 // next byte to be read by io.Reader

	mu   sync.Mutex
	pool *sync.Pool
	lru  *lruCache
}

type lruCache struct {
	mu       sync.Mutex
	cache    map[uint64]*list.Element
	lruList  *list.List
	size     int
	getValue func(uint64) (io.ReadCloser, error)
}

type lruCacheEntry struct {
	offset uint64
	reader io.ReadCloser
}

func newLRUCache(size int) *lruCache {
	return &lruCache{
		size:    size,
		cache:   make(map[uint64]*list.Element),
		lruList: list.New(),
	}
}

func (c *lruCache) Get(offset uint64) (io.ReadCloser, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.cache[offset]; ok {
		c.lruList.MoveToFront(el)
		return el.Value.(*lruCacheEntry).reader, true
	}
	return nil, false
}

func (c *lruCache) add(offset uint64, reader io.ReadCloser) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.size == 0 {
		return
	}

	if len(c.cache) >= c.size {
		lastEl := c.lruList.Back()
		delete(c.cache, lastEl.Value.(*lruCacheEntry).offset)
		c.lruList.Remove(lastEl)
	}

	el := c.lruList.PushFront(&lruCacheEntry{
		offset: offset,
		reader: reader,
	})
	c.cache[offset] = el
}

func (c *lruCache) touch(offset uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.cache[offset]; ok {
		c.lruList.MoveToFront(el)
	}
}

func (p *pieceReader) init() (_ *pieceReader, err error) {
	stats.Record(p.ctx, metrics.DagStorePRInitCount.M(1))

	// Initialize the pool and LRU cache
	p.pool = &sync.Pool{}
	p.lru = newLRUCache(5) // Adjust the cache size based on your requirements
	p.pool.New = func() interface{} {
		reader, err := p.getReader(p.ctx, uint64(p.seqAt))
		if err != nil {
			log.Errorw("error creating reader for pool",
				"pieceCid", p.pieceCid,
				"offset", p.seqAt,
				"error", err)
			return nil
		}
		return reader
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

	// Close and release any acquired readers
	for el := p.lru.lruList.Front(); el != nil; el = el.Next() {
		if el.Value != nil {
			if reader, ok := el.Value.(*lruCacheEntry); ok && reader.reader != nil {
				if err := reader.reader.Close(); err != nil {
					return err
				}
			}
		}
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
	reader, ok := p.lru.Get(uint64(off))
	if !ok {
		// Acquire a reader from the pool or create a new one if necessary
		reader = p.pool.Get().(io.ReadCloser)

		// Type assert the reader to an io.Seeker
		seeker, ok := reader.(io.Seeker)
		if !ok {
			return 0, errors.New("reader does not implement io.Seeker")
		}

		// Use the seeker to perform the Seek operation
		_, err = seeker.Seek(off, io.SeekStart)
		if err != nil {
			return 0, err
		}
		p.lru.add(uint64(off), reader)
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
	p.lru.touch(uint64(off))
	p.pool.Put(reader)

	return n, err
}

var _ mount.Reader = (*pieceReader)(nil)

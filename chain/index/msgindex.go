package index

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
)

var log = logging.Logger("msgindex")

var (
	dbName = "msgindex.db"

	coalesceMinDelay      = 100 * time.Millisecond
	coalesceMaxDelay      = time.Second
	coalesceMergeInterval = 100 * time.Millisecond
)

// chain store interface; we could use store.ChainStore directly,
// but this simplifies unit testing.
type ChainStore interface {
	SubscribeHeadChanges(f store.ReorgNotifee)
	MessagesForBlock(ctx context.Context, b *types.BlockHeader) ([]*types.Message, []*types.SignedMessage, error)
	GetHeaviestTipSet() *types.TipSet
	GetTipSetFromKey(ctx context.Context, tsk types.TipSetKey) (*types.TipSet, error)
}

var _ ChainStore = (*store.ChainStore)(nil)

type msgIndex struct {
	cs ChainStore

	db               *sql.DB
	selectMsgStmt    *sql.Stmt
	insertMsgStmt    *sql.Stmt
	deleteTipSetStmt *sql.Stmt

	sema chan struct{}
	mx   sync.Mutex
	pend []headChange

	cancel  func()
	workers sync.WaitGroup
	closeLk sync.RWMutex
	closed  bool
}

var _ MsgIndex = (*msgIndex)(nil)

type headChange struct {
	rev []*types.TipSet
	app []*types.TipSet
}

func NewMsgIndex(basePath string, cs ChainStore) (MsgIndex, error) {
	var (
		mkdb   bool
		dbPath string
		err    error
	)

	err = os.MkdirAll(basePath, 0755)
	if err != nil {
		return nil, xerrors.Errorf("error creating msgindex base directory: %w", err)
	}

	dbPath = path.Join(basePath, dbName)
	_, err = os.Stat(dbPath)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		mkdb = true

	case err != nil:
		return nil, xerrors.Errorf("error stating msgindex database: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		// TODO [nice to have]: automaticaly delete corrupt databases
		//      but for now we can just error and let the operator delete.
		return nil, xerrors.Errorf("error opening msgindex database: %w", err)
	}

	if mkdb {
		err = createTables(db)
		if err != nil {
			return nil, xerrors.Errorf("error creating msgindex database: %w", err)
		}

		// TODO we may consider populating the index in this case.
	} else {
		err = reconcileIndex(db, cs)
		if err != nil {
			return nil, xerrors.Errorf("error reconciling msgindex database: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	msgIndex := &msgIndex{
		db:     db,
		cs:     cs,
		sema:   make(chan struct{}, 1),
		cancel: cancel,
	}

	err = msgIndex.prepareStatements()
	if err != nil {
		err2 := db.Close()
		if err2 != nil {
			log.Errorf("error closing msgindex database: %s", err2)
		}

		return nil, xerrors.Errorf("error preparing msgindex database statements: %w", err)
	}

	rnf := store.WrapHeadChangeCoalescer(
		msgIndex.onHeadChange,
		coalesceMinDelay,
		coalesceMaxDelay,
		coalesceMergeInterval,
	)
	cs.SubscribeHeadChanges(rnf)

	msgIndex.workers.Add(1)
	go msgIndex.background(ctx)

	return msgIndex, nil
}

// init utilities
func createTables(db *sql.DB) error {
	// Just a single table for now; ghetto, but this an index so we denormalize to avoid joins.
	if _, err := db.Exec("CREATE TABLE Messages (cid VARCHAR(80) PRIMARY KEY, tipset VARCHAR(80), xepoch INTEGER, xindex INTEGER)"); err != nil {
		return err
	}

	// TODO Should we add an index for tipset to speed up deletion on revert?
	return nil
}

func reconcileIndex(db *sql.DB, cs ChainStore) error {
	// Invariant: after reconciliation, every tipset in the index is in the current chain; ie either
	//  the chain head or reachable by walking the chain.
	// Algorithm:
	//  1. Count mesages in index; if none, trivially reconciled.
	//     TODO we may consider populating the index in that case
	//  2. Find the minimum tipset in the index; this will mark the end of the reconciliation walk
	//  3. Walk from current tipset until we find a tipset in the index.
	//  4. Delete (revert!) all tipsets above the found tipset.
	//  5. If the walk ends in the boundary epoch, then delete everything.
	//

	row := db.QueryRow("SELECT COUNT(*) FROM Messages")

	var result int64
	if err := row.Scan(&result); err != nil {
		return xerrors.Errorf("error counting messages: %w", err)
	}

	if result == 0 {
		return nil
	}

	row = db.QueryRow("SELECT MIN(xepoch) FROM Messages")
	if err := row.Scan(&result); err != nil {
		return xerrors.Errorf("error finding boundary epoch: %w", err)
	}

	boundaryEpoch := abi.ChainEpoch(result)

	countMsgsStmt, err := db.Prepare("SELECT COUNT(*) FROM Messages WHERE tipset = ?")
	if err != nil {
		return xerrors.Errorf("error preparing statement: %w", err)
	}

	curTs := cs.GetHeaviestTipSet()
	for curTs != nil && curTs.Height() >= boundaryEpoch {
		tsCid, err := curTs.Key().Cid()
		if err != nil {
			return xerrors.Errorf("error computing tipset cid: %w", err)
		}

		key := tsCid.String()
		row = countMsgsStmt.QueryRow(key)
		if err := row.Scan(&result); err != nil {
			return xerrors.Errorf("error counting messages: %w", err)
		}

		if result > 0 {
			// found it!
			boundaryEpoch = curTs.Height() + 1
			break
		}

		// walk up
		parents := curTs.Parents()
		curTs, err = cs.GetTipSetFromKey(context.TODO(), parents)
		if err != nil {
			return xerrors.Errorf("error walking chain: %w", err)
		}
	}

	// delete everything above the minEpoch
	if _, err = db.Exec("DELETE FROM Messages WHERE xepoch >= ?", int64(boundaryEpoch)); err != nil {
		return xerrors.Errorf("error deleting stale reorged out message: %w", err)
	}

	return nil
}

func (x *msgIndex) prepareStatements() error {
	stmt, err := x.db.Prepare("SELECT tipset, xepoch, xindex FROM Messages WHERE cid = ?")
	if err != nil {
		return xerrors.Errorf("prepare selectMsgStmt: %w", err)
	}
	x.selectMsgStmt = stmt

	stmt, err = x.db.Prepare("INSERT INTO Messages VALUES (?, ?, ?, ?)")
	if err != nil {
		return xerrors.Errorf("prepare insertMsgStmt: %w", err)
	}
	x.insertMsgStmt = stmt

	stmt, err = x.db.Prepare("DELETE FROM Messages WHERE tipset = ?")
	if err != nil {
		return xerrors.Errorf("prepare deleteTipSetStmt: %w", err)
	}
	x.deleteTipSetStmt = stmt

	return nil
}

// head change notifee
func (x *msgIndex) onHeadChange(rev, app []*types.TipSet) error {
	x.closeLk.RLock()
	defer x.closeLk.RUnlock()

	if x.closed {
		return nil
	}

	// do it in the background to avoid blocking head change processing
	x.mx.Lock()
	x.pend = append(x.pend, headChange{rev: rev, app: app})
	// TODO log loudly if this is building backlog (it shouldn't but better be safe on this)
	x.mx.Unlock()

	select {
	case x.sema <- struct{}{}:
	default:
	}

	return nil
}

func (x *msgIndex) background(ctx context.Context) {
	defer x.workers.Done()

	for {
		select {
		case <-x.sema:
			err := x.processHeadChanges(ctx)
			if err != nil {
				// TODO should we shut down the index altogether? we just log for now.
				log.Errorf("error processing head change notifications: %s", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (x *msgIndex) processHeadChanges(ctx context.Context) error {
	x.mx.Lock()
	pend := x.pend
	x.pend = nil
	x.mx.Unlock()

	tx, err := x.db.Begin()
	if err != nil {
		return xerrors.Errorf("error creating transaction: %w", err)
	}

	for _, hc := range pend {
		for _, ts := range hc.rev {
			if err := x.doRevert(ctx, tx, ts); err != nil {
				tx.Rollback()
				return xerrors.Errorf("error reverting %s: %w", ts, err)
			}
		}

		for _, ts := range hc.app {
			if err := x.doApply(ctx, tx, ts); err != nil {
				tx.Rollback()
				return xerrors.Errorf("error applying %s: %w", ts, err)
			}
		}
	}

	return tx.Commit()
}

func (x *msgIndex) doRevert(ctx context.Context, tx *sql.Tx, ts *types.TipSet) error {
	tskey, err := ts.Key().Cid()
	if err != nil {
		return xerrors.Errorf("error computing tipset cid: %w", err)
	}

	key := tskey.String()
	_, err = tx.Stmt(x.deleteTipSetStmt).Exec(key)
	return err
}

func (x *msgIndex) doApply(ctx context.Context, tx *sql.Tx, ts *types.TipSet) error {
	tscid, err := ts.Key().Cid()
	if err != nil {
		return xerrors.Errorf("error computing tipset cid: %w", err)
	}

	tskey := tscid.String()
	xepoch := int64(ts.Height())
	var xindex int64

	seen := make(map[string]struct{})
	insert := func(key string) error {
		if _, ok := seen[key]; ok {
			return nil
		}

		if _, err := tx.Stmt(x.insertMsgStmt).Exec(key, tskey, xepoch, xindex); err != nil {
			return err
		}
		seen[key] = struct{}{}
		xindex++

		return nil
	}

	for _, blk := range ts.Blocks() {
		bmsgs, smsgs, err := x.cs.MessagesForBlock(ctx, blk)
		if err != nil {
			return xerrors.Errorf("error retrieving messages for block %s in %s: %w", blk, ts, err)
		}

		for _, m := range bmsgs {
			key := m.Cid().String()
			if err := insert(key); err != nil {
				return err
			}
		}

		for _, m := range smsgs {
			key := m.Cid().String()
			if err := insert(key); err != nil {
				return err
			}
		}
	}

	return nil
}

// interface
func (x *msgIndex) GetMsgInfo(ctx context.Context, m cid.Cid) (MsgInfo, error) {
	x.closeLk.RLock()
	defer x.closeLk.RUnlock()

	if x.closed {
		return MsgInfo{}, ErrClosed
	}

	var (
		tipset string
		epoch  int64
		index  int64
	)

	key := m.String()
	row := x.selectMsgStmt.QueryRow(key)
	err := row.Scan(&tipset, &epoch, &index)
	switch {
	case err == sql.ErrNoRows:
		return MsgInfo{}, ErrNotFound

	case err != nil:
		return MsgInfo{}, xerrors.Errorf("error querying msgindex database: %w", err)
	}

	tipsetCid, err := cid.Decode(tipset)
	if err != nil {
		return MsgInfo{}, xerrors.Errorf("error decoding tipset cid: %w", err)
	}

	return MsgInfo{
		Message: m,
		TipSet:  tipsetCid,
		Epoch:   abi.ChainEpoch(epoch),
		Index:   int(index),
	}, nil
}

func (x *msgIndex) Close() error {
	x.closeLk.Lock()
	defer x.closeLk.Unlock()

	if x.closed {
		return nil
	}

	x.closed = true

	x.cancel()
	x.workers.Wait()

	return x.db.Close()
}

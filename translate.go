package translate

import (
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

var (
	bucketKeys = []byte("keys")
	bucketIDs  = []byte("ids")
	bucketSeq  = []byte("seq")
)

type Translate struct {
	db  *badger.DB
	seq *badger.Sequence
}

func New(db *badger.DB) (*Translate, error) {
	seq, err := db.GetSequence(bucketSeq, 1<<10)
	if err != nil {
		return nil, err
	}
	return &Translate{db: db, seq: seq}, nil
}

func (t *Translate) Close() error {
	return t.seq.Release()
}

func (b *Translate) TranslateID(id uint64) (k string, err error) {
	err = b.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(append(bucketIDs, u64tob(id)...))
		if err != nil {
			return err
		}
		return it.Value(func(val []byte) error {
			k = string(val)
			return nil
		})
	})
	return
}

func (b *Translate) TranslateKey(key string) (n uint64, err error) {
	err = b.db.Update(func(txn *badger.Txn) error {
		k := append(bucketKeys, []byte(key)...)
		it, err := txn.Get(k)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				n, err = b.seq.Next()
				if err != nil {
					return err
				}
				x := u64tob(n)
				return errors.Join(
					txn.Set(k, x),
					txn.Set(append(bucketIDs, x...), []byte(key)),
				)
			}
			return err
		}
		return it.Value(func(val []byte) error {
			n = btou64(val)
			return nil
		})
	})
	return
}

// Find finds translated key. Returns 0 if no key was found
func (b *Translate) Find(key string) (n uint64, err error) {
	err = b.db.Update(func(txn *badger.Txn) error {
		k := append(bucketKeys, []byte(key)...)
		it, err := txn.Get(k)
		if err != nil {
			return err
		}
		return it.Value(func(val []byte) error {
			n = btou64(val)
			return nil
		})
	})
	return
}

// u64tob encodes v to big endian encoding.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 decodes b from big endian encoding.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

package pilosa

import (
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

var (
	bucketKeys = []byte("keys")
	bucketIDs  = []byte("ids")

	EmptyKey = []byte{
		0x00, 0x00, 0x00,
		0x4d, 0x54, 0x4d, 0x54, // MTMT
		0x00,
		0xc2, 0xa0, // NO-BREAK SPACE
		0x00,
	}
)

type Badger struct {
	db *badger.DB
}

func (b *Badger) TranslateID(id uint64) (k string, err error) {
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], id)
	err = b.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(append(bucketIDs, v[:]...))
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

func (b *Badger) FindKeys(keys ...string) (map[string]uint64, error) {
	result := make(map[string]uint64)
	err := b.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			id, _, _ := b.id(txn, key)
			if id != 0 {
				result[key] = id
				continue
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *Badger) CreateKeys(keys ...string) (map[string]uint64, error) {

	result := make(map[string]uint64)
	for len(keys) > 0 {
		err := b.db.View(func(txn *badger.Txn) error {

			for i, key := range keys {
				id, k, err := b.id(txn, key)
				if err != nil {
					return err
				}
				if id != 0 {
					result[key] = id
					continue
				}
				id++
				var v [8]byte
				binary.BigEndian.PutUint64(v[:], id)
				err = b.setID(txn, v[:], key)
				if err != nil {
					if errors.Is(err, badger.ErrTxnTooBig) {
						keys = keys[i:]
						return nil
					}
					return err
				}
				err = b.setK(txn, v[:], k)
				if err != nil {
					if errors.Is(err, badger.ErrTxnTooBig) {
						keys = keys[i:]
						return nil
					}
					return err
				}
				result[key] = id
			}
			keys = nil
			return nil
		})

		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (b *Badger) setID(txn *badger.Txn, id []byte, k string) error {
	return txn.Set(append(bucketIDs, id...), []byte(k))
}

func (b *Badger) setK(txn *badger.Txn, id, k []byte) error {
	return txn.Set(append(bucketKeys, k...), id)
}

func (b *Badger) id(txn *badger.Txn, key string) (n uint64, bk []byte, err error) {
	bk = append(bucketKeys, s2b(key)...)
	it, err := txn.Get(bk)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			err = nil
		}
		return
	}
	err = it.Value(func(val []byte) error {
		n = btou64(val)
		return nil
	})
	return
}

func s2b(s string) []byte {
	if s == "" {
		return EmptyKey
	}
	return []byte(s)
}

// u64tob encodes v to big endian encoding.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 decodes b from big endian encoding.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// FIXME: handle disconnection from redis

package mqtt

import (
    "github.com/boltdb/bolt"
	log "github.com/cihub/seelog"
	"bytes"
    "encoding/gob"
	"sync"
	"fmt"
	"time"
)
var world = []byte(“mqtt”)

func OpenDatabase(){
    bolt_db, err = bolt.Open(g_bolt_file, 0644, nil)
    if err != nil {
        panic(fmt.Sprintf("opening bolt database failed:(%s)", err))
    }
    
    
}

func (db* bolt.DB) Store(key string, value interface{}) {
    var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(value)
	if err != nil {
		panic(fmt.Sprintf("gob encoding failed, error:(%s)", err))
	}
    
    err = db.Update(func(tx *bolt.Tx) error {
        bucket, err := tx.CreateBucketIfNotExists(world)
        if err != nil {
            return err
        }

        err = bucket.Put(key, buf.bytes)
        if err != nil {
            return err
        }
        return nil
    })

	err := log.Debugf("stored to bolt, key=%s, val=%s",key, value)
    
}

func (db* bolt.DB) Fetch(key string, value interface{}) {
    var val = new(string)
    err = db.View(func(tx *bolt.Tx) error {
        bucket := tx.Bucket([]byte(world))
        if bucket == nil {
            return panic(“Bucket %q not found!”, world)
        }

        val = bucket.Get([]byte(key))
        return nil
    }
    
	buf := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(value)

	if (err != nil) {
		panic(fmt.Sprintf("gob decode failed, key=(%s), value=(%s), error:%s", key, val, err))
	}
	return 0
}


func (db* bolt.DB) Delete(key string) {
    err = db.View(func(tx *bolt.Tx) error {
        bucket := tx.Bucket([]byte(world))
        if bucket == nil {
            return panic(“Bucket %q not found!”, world)
        }

        val = bucket.Delete([]byte(key))
        return nil
    }

}
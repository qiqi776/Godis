package database

import (
	"fmt"
	"godis/internal/aof"
	"godis/internal/core"
	"godis/pkg/protocol"
	"os"
	"strings"
	"testing"
	"time"
)

func makeExpirePayload(key string, seconds int) []byte {
	var sb strings.Builder
	sb.WriteString(protocol.MakeArrayHeader(3))
	sb.WriteString(protocol.MakeBulkString("EXPIRE"))
	sb.WriteString(protocol.MakeBulkString(key))
	sb.WriteString(protocol.MakeBulkString(fmt.Sprintf("%d", seconds)))
	return []byte(sb.String())
}

func makeSetPayload(key, val string) []byte {
	var sb strings.Builder
	sb.WriteString(protocol.MakeArrayHeader(3))
	sb.WriteString(protocol.MakeBulkString("SET"))
	sb.WriteString(protocol.MakeBulkString(key))
	sb.WriteString(protocol.MakeBulkString(val))
	return []byte(sb.String())
}

func TestDatabase_Persistence_WithTTL(t *testing.T) {
	tmpFile := "test_ttl_persistence.aof"
	defer os.Remove(tmpFile)
	{
		db := NewStandalone()
		aofHandler, err := aof.NewAof(tmpFile, aof.FsyncEverySec)
		if err != nil {
			t.Fatalf("AOF init failed: %v", err)
		}
		db.SetAof(aofHandler)
		db.Set(0, "key_ttl", &core.RedisObject{
			Type: core.ObjectTypeString,
			Ptr:  []byte("val_ttl"),
		})
		aofHandler.Write(makeSetPayload("key_ttl", "val_ttl"))
		deadline := time.Now().Add(100 * time.Second)
		db.SetExpiration(0, "key_ttl", deadline)
		aofHandler.Write(makeExpirePayload("key_ttl", 100))
		aofHandler.Close()
	}
	{
		dbRestart := NewStandalone()
		recoveryAof, err := aof.NewAof(tmpFile, aof.FsyncNo)
		if err != nil {
			t.Fatalf("Recovery init failed: %v", err)
		}
		err = recoveryAof.Read(func(cmd protocol.Value) {
			if cmd.Type != protocol.Array || len(cmd.Array) < 2 {
				return
			}
			cmdName := strings.ToUpper(string(cmd.Array[0].Bulk))
			key := string(cmd.Array[1].Bulk)
			if cmdName == "SET" {
				val := cmd.Array[2].Bulk
				dbRestart.Set(0, key, &core.RedisObject{
					Type: core.ObjectTypeString,
					Ptr:  val,
				})
			} else if cmdName == "EXPIRE" {
				secondsStr := string(cmd.Array[2].Bulk)
				var seconds int
				fmt.Sscanf(secondsStr, "%d", &seconds)
				dbRestart.SetExpiration(0, key, time.Now().Add(time.Duration(seconds)*time.Second))
			}
		})
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		obj, ok := dbRestart.Get(0, "key_ttl")
		if !ok {
			t.Errorf("Data lost after recovery")
		} else {
			valBytes, typeOk := obj.Ptr.([]byte)
			if !typeOk || string(valBytes) != "val_ttl" {
				t.Errorf("Data mismatch after recovery. Expected 'val_ttl', got %v", obj.Ptr)
			}
		}
		ttl, found, _ := dbRestart.GetTTL(0, "key_ttl")
		if !found {
			t.Errorf("TTL lost after recovery")
		}
		if ttl <= 0 {
			t.Errorf("Key should not be expired yet")
		}
		recoveryAof.Close()
	}
}
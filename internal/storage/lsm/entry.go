package lsm

import (
	"mini-kv/internal/storage/lsm/record"
	version "mini-kv/internal/storage/lsm/sstable"
)

type entry = record.Entry
type batch = record.Batch
type keyBounds = record.KeyBounds
type tableMeta = version.TableMeta
type versionEdit = version.Edit
type versionState = version.State

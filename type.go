package ddsmetadata

import (
	"github.com/sirupsen/logrus"
)

type MetaData interface {
	Init() error
	SetPosition(seq uint64, csn uint64) error
	GetPosition() (*uint64, *uint64, error)
	SetXid(xid []byte) error
	GetXid() ([]byte, error)
	SetLastUpdateTime(ltime uint64) error
	GetLastUpdateTime() (*uint64, error)
	SetCreateTime(c uint64) error
	GetCreateTime() (*uint64, error)
	SetDataBaseType(r string) error
	GetDataBaseType() (*string, error)
	SetProcessType(r string) error
	GetProcessType() (*string, error)
	SetFilePosition(seq uint64, rba uint64) error
	GetFilePosition() (*uint64, *uint64, error)
	Sync() error
	Close() error
	SetStartTime() error
	GetStartTime() (uint64, error)
	SetTransactionBeginTime(t uint64) error
	GetTransactionBeginTime() (uint64, error)
}

type MetaDatas interface {
	Registry(*MdHandle, *logrus.Logger) (MetaData, error)
}

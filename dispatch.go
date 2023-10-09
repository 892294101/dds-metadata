package dds_metadata

import (
	"encoding/binary"
	"github.com/892294101/dds-spfile"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

var CallFunctionErrorOracle = errors.Errorf("Oracle does not support this function")
var CallFunctionErrorMySQL = errors.Errorf("MySQL does not support this function")
var UnknownDT = errors.Errorf("Unknown database type")
var UnknownPT = errors.Errorf("Unknown process type")

const (
	ORACLE = iota
	MYSQL
)

const (
	CAPTURE = iota
	REPLICATION
)

const (
	NOW = iota + 1
	SPECIFY
)

const (
	SequenceInitialPlace = 0
	SequencePlace        = 8

	CommitScnInitialPlace = SequenceInitialPlace + SequencePlace
	CommitScnPlace        = 8

	XidInitialPlace = CommitScnInitialPlace + CommitScnPlace
	XidPlace        = 16

	LastTimeInitialPlace = XidInitialPlace + XidPlace
	LastTimePlace        = 8

	CreateTimeInitialPlace = LastTimeInitialPlace + LastTimePlace
	CreateTimePlace        = 8

	DataBaseTypeInitialPlace = CreateTimeInitialPlace + CreateTimePlace
	DataBaseTypePlace        = 8

	ProcessTypeInitialPlace = DataBaseTypeInitialPlace + DataBaseTypePlace
	ProcessTypePlace        = 8

	FileSequenceInitialPlace = ProcessTypeInitialPlace + ProcessTypePlace
	FileSequencePlace        = 8

	FileRbaInitialPlace = FileSequenceInitialPlace + FileSequencePlace
	FileRbaPlace        = 8

	StartTimeInitialPlace = FileRbaInitialPlace + FileRbaPlace
	StartTimePlace        = 8

	TransactionBeginInitialPlace = StartTimeInitialPlace + StartTimePlace
	TransactionBeginPlace        = 8

	LastTransactionInitialPlace = TransactionBeginInitialPlace + TransactionBeginPlace
	LastTransactionPlace        = 8
)

/*
=========================================================
MySQL的抓取进程元数据信息结构
*/
type MySQLMdExtConsole struct {
	metaData *MdHandle      // 元数据句柄
	log      *logrus.Logger // 日志记录器
	lock     sync.RWMutex
}

func (m *MySQLMdExtConsole) Init() error {
	return nil
}

func (m *MySQLMdExtConsole) Sync() error {
	// return m.metaData.handle.Sync()
	return nil
}

func (m *MySQLMdExtConsole) Close() error {
	/*if err := m.Sync(); err != nil {
		return err
	}
	if err := syscall.Flock(int(m.metaData.handle.Fd()), syscall.LOCK_UN); err != nil {
		m.log.Warnf("checkpoint file handle unlock failed: %v", err)
	}
	return m.metaData.handle.Close()*/
	return m.metaData.handle.Close()
}

// 纳秒，设置事务开始时间
func (m *MySQLMdExtConsole) SetLastTransactionTime(t uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, LastTransactionPlace)
	binary.LittleEndian.PutUint64(l, t)
	if _, err := m.metaData.handle.WriteAt(l, LastTransactionInitialPlace); err != nil {
		return err
	}
	return nil
}

// 秒，获取事务开始时间
func (m *MySQLMdExtConsole) GetLastTransactionTime() (uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, LastTransactionPlace)
	if _, err := m.metaData.handle.ReadAt(xid, LastTransactionInitialPlace); err != nil {
		return 0, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	return convInt, nil
}

// 纳秒，设置事务开始时间
func (m *MySQLMdExtConsole) SetTransactionBeginTime(t uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, TransactionBeginPlace)
	binary.LittleEndian.PutUint64(l, t)
	if _, err := m.metaData.handle.WriteAt(l, TransactionBeginInitialPlace); err != nil {
		return err
	}
	return nil
}

// 纳秒，获取事务开始时间
func (m *MySQLMdExtConsole) GetTransactionBeginTime() (uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, TransactionBeginPlace)
	if _, err := m.metaData.handle.ReadAt(xid, TransactionBeginInitialPlace); err != nil {
		return 0, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	return convInt, nil
}

// 纳秒,设置进程启动时间
func (m *MySQLMdExtConsole) SetStartTime() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, StartTimePlace)
	binary.LittleEndian.PutUint64(l, uint64(time.Now().UnixNano()))
	if _, err := m.metaData.handle.WriteAt(l, StartTimeInitialPlace); err != nil {
		return err
	}
	return nil
}

// 纳秒，获取进程启动时间
func (m *MySQLMdExtConsole) GetStartTime() (uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, StartTimePlace)
	if _, err := m.metaData.handle.ReadAt(xid, StartTimeInitialPlace); err != nil {
		return 0, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	return convInt, nil
}

func (m *MySQLMdExtConsole) SetFilePosition(seq uint64, rba uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	fn := make([]byte, FileSequencePlace)
	binary.LittleEndian.PutUint64(fn, seq)
	if _, err := m.metaData.handle.WriteAt(fn, FileSequenceInitialPlace); err != nil {
		return err
	}

	poss := make([]byte, FileRbaPlace)
	binary.LittleEndian.PutUint64(poss, rba)
	if _, err := m.metaData.handle.WriteAt(poss, FileRbaInitialPlace); err != nil {
		return err
	}
	return nil
}

func (m *MySQLMdExtConsole) GetFilePosition() (*uint64, *uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	fn := make([]byte, FileSequencePlace)
	if _, err := m.metaData.handle.ReadAt(fn, FileSequenceInitialPlace); err != nil {
		return nil, nil, err
	}
	convLog := binary.LittleEndian.Uint64(fn)

	poss := make([]byte, FileRbaPlace)
	if _, err := m.metaData.handle.ReadAt(poss, FileRbaInitialPlace); err != nil {
		return nil, nil, err
	}

	convPos := binary.LittleEndian.Uint64(poss)
	//m.log.Debugf("sequence %v rba %v", convLog, convPos)
	return &convLog, &convPos, nil
}

func (m *MySQLMdExtConsole) SetPosition(seq uint64, csn uint64) error {
	//m.log.Debugf("sequence %v position %v", seq, csn)
	m.lock.Lock()
	defer m.lock.Unlock()

	log := make([]byte, SequencePlace)
	binary.LittleEndian.PutUint64(log, seq)
	if _, err := m.metaData.handle.WriteAt(log, SequenceInitialPlace); err != nil {
		return err
	}

	poss := make([]byte, CommitScnPlace)
	binary.LittleEndian.PutUint64(poss, csn)
	if _, err := m.metaData.handle.WriteAt(poss, CommitScnInitialPlace); err != nil {
		return err
	}
	return nil
}

func (m *MySQLMdExtConsole) GetPosition() (*uint64, *uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	log := make([]byte, SequencePlace)
	if _, err := m.metaData.handle.ReadAt(log, SequenceInitialPlace); err != nil {
		return nil, nil, err
	}
	convLog := binary.LittleEndian.Uint64(log)

	poss := make([]byte, CommitScnPlace)
	if _, err := m.metaData.handle.ReadAt(poss, CommitScnInitialPlace); err != nil {
		return nil, nil, err
	}

	convPos := binary.LittleEndian.Uint64(poss)
	//m.log.Debugf("sequence %v position %v", convLog, convPos)
	return &convLog, &convPos, nil
}

func (m *MySQLMdExtConsole) SetXid(xid []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	x := make([]byte, XidPlace)
	copy(x, xid)
	if _, err := m.metaData.handle.WriteAt(x, XidInitialPlace); err != nil {
		return err
	}

	return nil
}

func (m *MySQLMdExtConsole) GetXid() ([]byte, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, XidPlace)
	if _, err := m.metaData.handle.ReadAt(xid, XidInitialPlace); err != nil {
		return nil, err
	}

	return xid, nil
}

// 最后事务的更新时间（单位秒）
func (m *MySQLMdExtConsole) SetLastUpdateTime(ltime uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, LastTimePlace)
	binary.LittleEndian.PutUint64(l, ltime)
	if _, err := m.metaData.handle.WriteAt(l, LastTimeInitialPlace); err != nil {
		return err
	}

	return nil
}

// 最后事务的更新时间（单位秒）
func (m *MySQLMdExtConsole) GetLastUpdateTime() (*uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, LastTimePlace)
	if _, err := m.metaData.handle.ReadAt(xid, LastTimeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)

	return &convInt, nil
}

// 创建进程时间（单位秒）
func (m *MySQLMdExtConsole) SetCreateTime(c uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, CreateTimePlace)
	binary.LittleEndian.PutUint64(l, c)
	if _, err := m.metaData.handle.WriteAt(l, CreateTimeInitialPlace); err != nil {
		return err
	}

	return nil
}

// 创建进程时间（单位秒）
func (m *MySQLMdExtConsole) GetCreateTime() (*uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, CreateTimePlace)
	if _, err := m.metaData.handle.ReadAt(xid, CreateTimeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)

	return &convInt, nil
}

func (m *MySQLMdExtConsole) SetDataBaseType(r string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	switch r {
	case dds_spfile.GetMySQLName():
		l := make([]byte, DataBaseTypePlace)
		binary.LittleEndian.PutUint64(l, MYSQL)
		if _, err := m.metaData.handle.WriteAt(l, DataBaseTypeInitialPlace); err != nil {
			return err
		}
	default:
		return errors.Errorf("MySQLMdExtConsole %v", UnknownDT)
	}
	dds_spfile.GetExtractName()
	return nil
}

func (m *MySQLMdExtConsole) GetDataBaseType() (*string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, DataBaseTypePlace)
	if _, err := m.metaData.handle.ReadAt(xid, DataBaseTypeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	switch convInt {
	case MYSQL:
		n := dds_spfile.GetMySQLName()
		return &n, nil
	default:
		return nil, UnknownDT
	}
}

func (m *MySQLMdExtConsole) SetProcessType(r string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	switch r {
	case dds_spfile.GetExtractName():
		l := make([]byte, ProcessTypePlace)
		binary.LittleEndian.PutUint64(l, CAPTURE)
		if _, err := m.metaData.handle.WriteAt(l, ProcessTypeInitialPlace); err != nil {
			return err
		}
	default:
		return UnknownPT
	}
	return nil
}

func (m *MySQLMdExtConsole) GetProcessType() (*string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, ProcessTypePlace)
	if _, err := m.metaData.handle.ReadAt(xid, ProcessTypeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	switch convInt {
	case CAPTURE:
		n := dds_spfile.GetExtractName()
		return &n, nil
	default:
		return nil, UnknownPT
	}
}

type MySQLMetaDataBus struct {
	md *MySQLMdExtConsole
}

func (m *MySQLMetaDataBus) Registry(md *MdHandle, log *logrus.Logger) (MetaData, error) {
	m.md = new(MySQLMdExtConsole)
	m.md.metaData = md
	m.md.log = log
	return m.md, nil
}

/*
==================================================================================================================
Oracle的元数据信息结构
*/
type threadPosition struct {
	lastUpdateTimeTS *uint64 // 最后一次更新时间
	lastXid          *string // 最后一次事务ID
	scn              *uint64 // mysql binlog 位置号
}

type captureDetail struct {
	lastUpdateTimeTS *uint64                 // 最后一次更新时间
	lastXid          *string                 // 最后一次事务ID
	lastCsn          *uint64                 // 最后一次提交SCN
	threadPos        map[int]*threadPosition // 抓取进程信息
}

type OracleMdExtConsole struct {
	metaData     *MdHandle      // 元数据句柄
	createTimeTS *time.Time     // 创建时间
	capPos       *captureDetail // 捕获信息位置
	log          *logrus.Logger // 日志记录器
	lock         sync.RWMutex
}

func (m *OracleMdExtConsole) Init() error {
	return nil
}

func (m *OracleMdExtConsole) Sync() error {
	// return m.metaData.handle.Sync()
	return nil
}

func (m *OracleMdExtConsole) Close() error {
	/*if err := m.Sync(); err != nil {
		return err
	}
	if err := syscall.Flock(int(m.metaData.handle.Fd()), syscall.LOCK_UN); err != nil {
		m.log.Warnf("checkpoint file handle unlock failed: %v", err)
	}
	return m.metaData.handle.Close()*/
	return m.metaData.handle.Close()
}

// 纳秒，设置事务开始时间
func (m *OracleMdExtConsole) SetLastTransactionTime(t uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, LastTransactionPlace)
	binary.LittleEndian.PutUint64(l, t)
	if _, err := m.metaData.handle.WriteAt(l, LastTransactionInitialPlace); err != nil {
		return err
	}
	return nil
}

// 秒，获取事务开始时间
func (m *OracleMdExtConsole) GetLastTransactionTime() (uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, LastTransactionPlace)
	if _, err := m.metaData.handle.ReadAt(xid, LastTransactionInitialPlace); err != nil {
		return 0, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	return convInt, nil
}

// 纳秒，设置事务开始时间
func (m *OracleMdExtConsole) SetTransactionBeginTime(t uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, TransactionBeginPlace)
	binary.LittleEndian.PutUint64(l, t)
	if _, err := m.metaData.handle.WriteAt(l, TransactionBeginInitialPlace); err != nil {
		return err
	}
	return nil
}

// 纳秒，获取事务开始时间
func (m *OracleMdExtConsole) GetTransactionBeginTime() (uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, TransactionBeginPlace)
	if _, err := m.metaData.handle.ReadAt(xid, TransactionBeginInitialPlace); err != nil {
		return 0, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	return convInt, nil
}

// 纳秒,设置进程启动时间
func (m *OracleMdExtConsole) SetStartTime() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, StartTimePlace)
	binary.LittleEndian.PutUint64(l, uint64(time.Now().UnixNano()))
	if _, err := m.metaData.handle.WriteAt(l, StartTimeInitialPlace); err != nil {
		return err
	}
	return nil
}

// 纳秒，获取进程启动时间
func (m *OracleMdExtConsole) GetStartTime() (uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, StartTimePlace)
	if _, err := m.metaData.handle.ReadAt(xid, StartTimeInitialPlace); err != nil {
		return 0, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	return convInt, nil
}

func (m *OracleMdExtConsole) SetFilePosition(seq uint64, rba uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	fn := make([]byte, FileSequencePlace)
	binary.LittleEndian.PutUint64(fn, seq)
	if _, err := m.metaData.handle.WriteAt(fn, FileSequenceInitialPlace); err != nil {
		return err
	}

	poss := make([]byte, FileRbaPlace)
	binary.LittleEndian.PutUint64(poss, rba)
	if _, err := m.metaData.handle.WriteAt(poss, FileRbaInitialPlace); err != nil {
		return err
	}
	return nil
}

func (m *OracleMdExtConsole) GetFilePosition() (*uint64, *uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	fn := make([]byte, FileSequencePlace)
	if _, err := m.metaData.handle.ReadAt(fn, FileSequenceInitialPlace); err != nil {
		return nil, nil, err
	}
	convLog := binary.LittleEndian.Uint64(fn)

	poss := make([]byte, FileRbaPlace)
	if _, err := m.metaData.handle.ReadAt(poss, FileRbaInitialPlace); err != nil {
		return nil, nil, err
	}

	convPos := binary.LittleEndian.Uint64(poss)
	//m.log.Debugf("sequence %v rba %v", convLog, convPos)
	return &convLog, &convPos, nil
}

func (m *OracleMdExtConsole) SetPosition(seq uint64, csn uint64) error {
	//m.log.Debugf("sequence %v position %v", seq, csn)
	m.lock.Lock()
	defer m.lock.Unlock()

	log := make([]byte, SequencePlace)
	binary.LittleEndian.PutUint64(log, seq)
	if _, err := m.metaData.handle.WriteAt(log, SequenceInitialPlace); err != nil {
		return err
	}

	poss := make([]byte, CommitScnPlace)
	binary.LittleEndian.PutUint64(poss, csn)
	if _, err := m.metaData.handle.WriteAt(poss, CommitScnInitialPlace); err != nil {
		return err
	}
	return nil
}

func (m *OracleMdExtConsole) GetPosition() (*uint64, *uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	log := make([]byte, SequencePlace)
	if _, err := m.metaData.handle.ReadAt(log, SequenceInitialPlace); err != nil {
		return nil, nil, err
	}
	convLog := binary.LittleEndian.Uint64(log)

	poss := make([]byte, CommitScnPlace)
	if _, err := m.metaData.handle.ReadAt(poss, CommitScnInitialPlace); err != nil {
		return nil, nil, err
	}

	convPos := binary.LittleEndian.Uint64(poss)
	//m.log.Debugf("sequence %v position %v", convLog, convPos)
	return &convLog, &convPos, nil
}

func (m *OracleMdExtConsole) SetXid(xid []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	x := make([]byte, XidPlace)
	copy(x, xid)
	if _, err := m.metaData.handle.WriteAt(x, XidInitialPlace); err != nil {
		return err
	}

	return nil
}

func (m *OracleMdExtConsole) GetXid() ([]byte, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, XidPlace)
	if _, err := m.metaData.handle.ReadAt(xid, XidInitialPlace); err != nil {
		return nil, err
	}

	return xid, nil
}

// 最后事务的更新时间（单位秒）
func (m *OracleMdExtConsole) SetLastUpdateTime(ltime uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, LastTimePlace)
	binary.LittleEndian.PutUint64(l, ltime)
	if _, err := m.metaData.handle.WriteAt(l, LastTimeInitialPlace); err != nil {
		return err
	}

	return nil
}

// 最后事务的更新时间（单位秒）
func (m *OracleMdExtConsole) GetLastUpdateTime() (*uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, LastTimePlace)
	if _, err := m.metaData.handle.ReadAt(xid, LastTimeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)

	return &convInt, nil
}

// 创建进程时间（单位秒）
func (m *OracleMdExtConsole) SetCreateTime(c uint64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	l := make([]byte, CreateTimePlace)
	binary.LittleEndian.PutUint64(l, c)
	if _, err := m.metaData.handle.WriteAt(l, CreateTimeInitialPlace); err != nil {
		return err
	}

	return nil
}

// 创建进程时间（单位秒）
func (m *OracleMdExtConsole) GetCreateTime() (*uint64, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, CreateTimePlace)
	if _, err := m.metaData.handle.ReadAt(xid, CreateTimeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)

	return &convInt, nil
}

func (m *OracleMdExtConsole) SetDataBaseType(r string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	switch r {
	case dds_spfile.GetOracleName():
		l := make([]byte, DataBaseTypePlace)
		binary.LittleEndian.PutUint64(l, MYSQL)
		if _, err := m.metaData.handle.WriteAt(l, DataBaseTypeInitialPlace); err != nil {
			return err
		}
	default:
		return errors.Errorf("OracleMdExtConsole %v", UnknownDT)
	}

	return nil
}

func (m *OracleMdExtConsole) GetDataBaseType() (*string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, DataBaseTypePlace)
	if _, err := m.metaData.handle.ReadAt(xid, DataBaseTypeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	switch convInt {
	case MYSQL:
		n := dds_spfile.GetMySQLName()
		return &n, nil
	default:
		return nil, UnknownDT
	}
}

func (m *OracleMdExtConsole) SetProcessType(r string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	switch r {
	case dds_spfile.GetExtractName():
		l := make([]byte, ProcessTypePlace)
		binary.LittleEndian.PutUint64(l, CAPTURE)
		if _, err := m.metaData.handle.WriteAt(l, ProcessTypeInitialPlace); err != nil {
			return err
		}
	default:
		return UnknownPT
	}
	return nil
}

func (m *OracleMdExtConsole) GetProcessType() (*string, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	xid := make([]byte, ProcessTypePlace)
	if _, err := m.metaData.handle.ReadAt(xid, ProcessTypeInitialPlace); err != nil {
		return nil, err
	}
	convInt := binary.LittleEndian.Uint64(xid)
	switch convInt {
	case CAPTURE:
		n := dds_spfile.GetExtractName()
		return &n, nil
	default:
		return nil, UnknownPT
	}
}

type OracleMetaDataBus struct {
	md *OracleMdExtConsole
}

func (m *OracleMetaDataBus) Registry(md *MdHandle, log *logrus.Logger) (MetaData, error) {
	m.md = new(OracleMdExtConsole)
	m.md.metaData = md
	m.md.log = log
	m.md.capPos = new(captureDetail)
	m.md.capPos.threadPos = make(map[int]*threadPosition)
	return m.md, nil
}

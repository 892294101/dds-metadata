package ddsmetadata

import (
	"fmt"
	"github.com/892294101/cache-mmap/mmap"
	"github.com/892294101/dds-spfile"
	"github.com/892294101/dds/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

const (
	_DefaultFilePerm     = 0755
	_DefaultMetadataSize = 512
)
const (
	CREATE = iota // 创建元数据
	LOAD          // 加载元数据
)

type MdHandle struct {
	path string // 元数据文件路径
	//handle       *os.File       // 元数据句柄
	handle       *mmap.File     // 元数据句柄
	processName  string         // 进程名称
	dataBaseType string         // 数据库类型
	processType  string         // 进程类型
	log          *logrus.Logger // 日志记录器
}

/*func (m *MdHandle) Unlock() error {
	 if err := syscall.Flock(int(m.handle.Fd()), syscall.LOCK_UN); err != nil {
		m.log.Warnf("checkpoint file handle unlock failed: %v", err)
	}
	return nil
}*/

// 初始化元数据句柄
func (m *MdHandle) LoadMetaDataFile(processName string, dataBaseType string, processType string, log *logrus.Logger) error {
	m.processName = processName
	m.dataBaseType = dataBaseType
	m.processType = processType
	m.log = log
	home, err := utils.GetHomeDirectory()
	if err != nil {
		return err
	}
	file := filepath.Join(*home, "chk", fmt.Sprintf("%s.ce", m.processName))
	m.path = file

	/*	fd, err := os.OpenFile(file, os.O_RDWR|os.O_SYNC, _DefaultFilePerm)
		if err != nil {
			return err
		}

		if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
			return errors.Errorf("add checkpoint exclusive lock failed: %v", err)
		}*/
	fd, err := mmap.NewMmap(file, os.O_RDWR, _DefaultMetadataSize)
	if err != nil {
		return err
	}
	m.handle = fd

	return nil
}

/*func (m *MdHandle) RemoveMetaDataFile() (err error) {
	if utils.IsFileExist(m.path) {
		err = os.Remove(m.path)
	}
	return err
}*/

// 初始化元数据句柄
func (m *MdHandle) CreateMetaDataFile(processName string, dataBaseType string, processType string, log *logrus.Logger) error {
	m.processName = processName
	m.dataBaseType = dataBaseType
	m.processType = processType
	m.log = log
	home, err := utils.GetHomeDirectory()
	if err != nil {
		return err
	}
	file := filepath.Join(*home, "chk", fmt.Sprintf("%s.ce", m.processName))
	m.path = file

	// 如果O_EXCL和O_CREATE一起使用则表示，文件必须不存在才能创建（如果文件存在则返回错误）
	/*fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, _DefaultFilePerm)
	if err != nil {
		return err
	}

	if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return errors.Errorf("add checkpoint exclusive lock failed: %v", err)
	}

	if err = fd.Truncate(int64(_DefaultMetadataSize)); err != nil {
		return errors.Errorf("error in extending checkpoint file: %v", err)
	}
	m.Unlock()
	*/
	fd, err := mmap.NewMmap(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, _DefaultMetadataSize)
	if err != nil {
		return err
	}

	m.handle = fd
	return nil
}

// 创建加载元数据文件
func InitMetaData(processName string, dataBaseType string, processType string, log *logrus.Logger, opsType int) (MetaData, error) {
	var md MetaDatas
	switch dataBaseType {
	case dds_spfile.GetMySQLName():
		switch processType {
		case dds_spfile.GetExtractName():
			md = &MySQLMetaDataBus{}
			// 初始化元数据句柄
			mdh := new(MdHandle)
			switch opsType {
			case CREATE:
				err := mdh.CreateMetaDataFile(processName, dataBaseType, processType, log)
				if err != nil {
					return nil, err
				}
			case LOAD:
				err := mdh.LoadMetaDataFile(processName, dataBaseType, processType, log)
				if err != nil {
					return nil, err
				}
			default:
				return nil, errors.Errorf("Metadata operation type is not supported")
			}
			mdBus, err := md.Registry(mdh, log)
			if err != nil {
				return nil, err
			}
			if err := mdBus.Init(); err != nil {
				return nil, err
			}
			return mdBus, nil

		case dds_spfile.GetReplicationName():
			return nil, errors.Errorf("The process type needs to be implemented")
		default:
			return nil, errors.Errorf("Metadata management does not support this process")
		}
	case dds_spfile.GetOracleName():
		switch processType {
		case dds_spfile.GetExtractName():
			md = &OracleMetaDataBus{}
			// 初始化元数据句柄
			mdh := new(MdHandle)
			switch opsType {
			case CREATE:
				err := mdh.CreateMetaDataFile(processName, dataBaseType, processType, log)
				if err != nil {
					return nil, err
				}
			case LOAD:
				err := mdh.LoadMetaDataFile(processName, dataBaseType, processType, log)
				if err != nil {
					return nil, err
				}
			default:
				return nil, errors.Errorf("Metadata operation type is not supported")
			}
			mdBus, err := md.Registry(mdh, log)
			if err != nil {
				return nil, err
			}
			if err := mdBus.Init(); err != nil {
				return nil, err
			}
			return mdBus, nil
		case dds_spfile.GetReplicationName():
			return nil, errors.Errorf("The process type needs to be implemented")
		}

	default:
		return nil, errors.Errorf("Metadata management does not support this database")
	}
	return nil, nil
}

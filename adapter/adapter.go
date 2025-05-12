package adapter

import (
	"database/sql"

	"gorm.io/gorm"
)

// Adapter 数据库适配器接口
type Adapter interface {
	// Connect 连接数据库
	Connect() (*gorm.DB, *sql.DB, error)

	// ForUpdate 生成锁定语句
	ForUpdate() string

	// ForShare 生成共享锁语句
	ForShare() string

	// Limit 生成分页语句
	Limit(offset, limit int) string

	// BatchInsert 批量插入
	BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error

	// MergeInto 合并插入（UPSERT）
	MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error

	QueryPage(out interface{}, page, pageSize int, filter interface{}, opts ...interface{}) (int64, error)
}

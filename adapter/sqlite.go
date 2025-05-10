package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// SQLite 适配器结构体
type SQLite struct {
	// 基础配置
	DSN         string        // 数据源名称（数据库文件路径）
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewSQLite 创建新的SQLite适配器
func NewSQLite(dsn string) *SQLite {
	return &SQLite{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (s *SQLite) WithMaxIdle(maxIdle int) *SQLite {
	s.MaxIdle = maxIdle
	return s
}

// WithMaxOpen 设置最大打开连接数
func (s *SQLite) WithMaxOpen(maxOpen int) *SQLite {
	s.MaxOpen = maxOpen
	return s
}

// WithMaxLifetime 设置连接最大生命周期
func (s *SQLite) WithMaxLifetime(maxLifetime time.Duration) *SQLite {
	s.MaxLifetime = maxLifetime
	return s
}

// WithDebug 设置调试模式
func (s *SQLite) WithDebug(debug bool) *SQLite {
	s.Debug = debug
	return s
}

// Connect 连接数据库
func (s *SQLite) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if s.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库
	db, err := gorm.Open(sqlite.Open(s.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(s.MaxIdle)
	sqlDB.SetMaxOpenConns(s.MaxOpen)
	sqlDB.SetConnMaxLifetime(s.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
// 对于SQLite，DSN就是数据库文件路径，可以添加一些参数
func (s *SQLite) BuildDSN(dbPath string, params map[string]string) string {
	// 基本DSN是文件路径
	dsn := dbPath

	// 添加参数
	if len(params) > 0 {
		var parameters []string
		for k, v := range params {
			parameters = append(parameters, fmt.Sprintf("%s=%s", k, v))
		}
		dsn = dsn + "?" + strings.Join(parameters, "&")
	}

	return dsn
}

// ForUpdate 生成FOR UPDATE锁定语句
// SQLite不支持标准的FOR UPDATE语法，但我们保留此方法以保持接口一致性
func (s *SQLite) ForUpdate() string {
	return ""
}

// ForShare 生成共享锁语句
// SQLite不支持标准的FOR SHARE语法，但我们保留此方法以保持接口一致性
func (s *SQLite) ForShare() string {
	return ""
}

// Limit 生成分页语句
func (s *SQLite) Limit(offset, limit int) string {
	return fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)
}

// BatchInsert 批量插入
func (s *SQLite) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// SQLite支持标准的多行INSERT语法
	var placeholders []string
	for range values {
		ph := "(" + strings.Repeat("?,", len(columns))
		ph = ph[:len(ph)-1] + ")" // 移除最后一个逗号并添加右括号
		placeholders = append(placeholders, ph)
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 构建完整SQL
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// 执行SQL
	return db.Exec(sql, flatValues...).Error
}

// BatchInsertOrReplace 批量插入或替换（SQLite的REPLACE INTO功能）
func (s *SQLite) BatchInsertOrReplace(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// SQLite支持REPLACE INTO语法
	var placeholders []string
	for range values {
		ph := "(" + strings.Repeat("?,", len(columns))
		ph = ph[:len(ph)-1] + ")" // 移除最后一个逗号并添加右括号
		placeholders = append(placeholders, ph)
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 构建完整SQL
	sql := fmt.Sprintf(
		"REPLACE INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// 执行SQL
	return db.Exec(sql, flatValues...).Error
}

// LockTable 锁定表
// SQLite不支持显式表锁，但我们保留此方法以保持接口一致性
func (s *SQLite) LockTable(db *gorm.DB, table string, lockType string) error {
	// SQLite使用隐式锁，不需要显式锁定表
	return nil
}

// UnlockTables 解锁所有表
// SQLite不支持显式表锁，但我们保留此方法以保持接口一致性
func (s *SQLite) UnlockTables(db *gorm.DB) error {
	// SQLite使用隐式锁，不需要显式解锁表
	return nil
}

// GetLastInsertID 获取最后插入的ID
func (s *SQLite) GetLastInsertID(db *gorm.DB) (int64, error) {
	var id int64
	err := db.Raw("SELECT last_insert_rowid()").Scan(&id).Error
	return id, err
}

// Pragma 执行SQLite的PRAGMA语句
func (s *SQLite) Pragma(db *gorm.DB, pragma string, value interface{}) error {
	return db.Exec(fmt.Sprintf("PRAGMA %s = %v", pragma, value)).Error
}

// EnableForeignKeys 启用外键约束
func (s *SQLite) EnableForeignKeys(db *gorm.DB) error {
	return s.Pragma(db, "foreign_keys", "ON")
}

// DisableForeignKeys 禁用外键约束
func (s *SQLite) DisableForeignKeys(db *gorm.DB) error {
	return s.Pragma(db, "foreign_keys", "OFF")
}

// EnableWAL 启用WAL模式（Write-Ahead Logging）
func (s *SQLite) EnableWAL(db *gorm.DB) error {
	return s.Pragma(db, "journal_mode", "WAL")
}

// SetSynchronous 设置同步模式
func (s *SQLite) SetSynchronous(db *gorm.DB, mode string) error {
	return s.Pragma(db, "synchronous", mode)
}

// Vacuum 整理数据库文件
func (s *SQLite) Vacuum(db *gorm.DB) error {
	return db.Exec("VACUUM").Error
}

// 适用于SQLite 3.24.0+的更简单实现
func (s *SQLite) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 || len(keyColumns) == 0 {
		return nil
	}

	// 构建占位符
	var placeholders []string
	for range values {
		ph := "(" + strings.Repeat("?,", len(columns))
		ph = ph[:len(ph)-1] + ")" // 移除最后一个逗号并添加右括号
		placeholders = append(placeholders, ph)
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 构建更新部分
	var updates []string
	for _, col := range updateColumns {
		updates = append(updates, fmt.Sprintf("%s = excluded.%s", col, col))
	}

	// 构建完整SQL
	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON CONFLICT(%s) DO UPDATE SET %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(keyColumns, ", "),
		strings.Join(updates, ", "),
	)

	// 执行SQL
	return db.Exec(sql, flatValues...).Error
}

//// MergeInto 合并插入（UPSERT）- SQLite实现
//func (s *SQLite) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
//	if len(values) == 0 || len(keyColumns) == 0 {
//		return nil
//	}
//
//	// SQLite 3.24.0及以上版本支持标准的UPSERT语法
//	// 但为了兼容性，我们使用事务和临时表来实现
//
//	// 开始事务
//	tx := db.Begin()
//	if tx.Error != nil {
//		return tx.Error
//	}
//
//	// 创建临时表
//	tempTable := fmt.Sprintf("%s_temp_%d", table, time.Now().UnixNano())
//	createSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM %s WHERE 0", tempTable, table)
//	if err := tx.Exec(createSQL).Error; err != nil {
//		tx.Rollback()
//		return err
//	}
//
//	// 向临时表插入数据
//	if err := s.BatchInsert(tx, tempTable, columns, values); err != nil {
//		tx.Rollback()
//		return err
//	}
//
//	// 构建UPDATE语句
//	var updates []string
//	for _, col := range updateColumns {
//		updates = append(updates, fmt.Sprintf("%s = excluded.%s", col, col))
//	}
//
//	// 构建WHERE条件
//	var whereConditions []string
//	for _, key := range keyColumns {
//		whereConditions = append(whereConditions, fmt.Sprintf("%s.%s = excluded.%s", table, key, key))
//	}
//
//	// 执行UPSERT操作
//	upsertSQL := fmt.Sprintf(`
//        INSERT INTO %s (%s)
//        SELECT %s FROM %s
//        ON CONFLICT(%s) DO UPDATE SET %s
//    `,
//		table,
//		strings.Join(columns, ", "),
//		strings.Join(columns, ", "),
//		tempTable,
//		strings.Join(keyColumns, ", "),
//		strings.Join(updates, ", "))
//
//	if err := tx.Exec(upsertSQL).Error; err != nil {
//		tx.Rollback()
//		return err
//	}
//
//	// 删除临时表
//	if err := tx.Exec(fmt.Sprintf("DROP TABLE %s", tempTable)).Error; err != nil {
//		tx.Rollback()
//		return err
//	}
//
//	// 提交事务
//	return tx.Commit().Error
//}

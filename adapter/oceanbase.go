package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// OceanBase 适配器结构体
type OceanBase struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewOceanBase 创建新的OceanBase适配器
func NewOceanBase(dsn string) *OceanBase {
	return &OceanBase{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (o *OceanBase) WithMaxIdle(maxIdle int) *OceanBase {
	o.MaxIdle = maxIdle
	return o
}

// WithMaxOpen 设置最大打开连接数
func (o *OceanBase) WithMaxOpen(maxOpen int) *OceanBase {
	o.MaxOpen = maxOpen
	return o
}

// WithMaxLifetime 设置连接最大生命周期
func (o *OceanBase) WithMaxLifetime(maxLifetime time.Duration) *OceanBase {
	o.MaxLifetime = maxLifetime
	return o
}

// WithDebug 设置调试模式
func (o *OceanBase) WithDebug(debug bool) *OceanBase {
	o.Debug = debug
	return o
}

// Connect 连接数据库
func (o *OceanBase) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if o.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库 - 使用MySQL驱动，因为OceanBase兼容MySQL协议
	db, err := gorm.Open(mysql.Open(o.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(o.MaxIdle)
	sqlDB.SetMaxOpenConns(o.MaxOpen)
	sqlDB.SetConnMaxLifetime(o.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (o *OceanBase) BuildDSN(username, password, host string, port int, database string, params map[string]string) string {
	// 基本DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)

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
func (o *OceanBase) ForUpdate() string {
	return "FOR UPDATE"
}

// ForShare 生成共享锁语句
func (o *OceanBase) ForShare() string {
	return "LOCK IN SHARE MODE"
}

// Limit 生成分页语句
func (o *OceanBase) Limit(offset, limit int) string {
	return fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)
}

// BatchInsert 批量插入
func (o *OceanBase) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// 构建INSERT语句
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

	// 构建占位符
	placeholder := "(" + strings.Repeat("?,", len(columns))
	placeholder = placeholder[:len(placeholder)-1] + ")"

	// 添加多行值
	for i := range values {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString(placeholder)
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 执行SQL
	return db.Exec(sqlBuilder.String(), flatValues...).Error
}

// MergeInto 实现OceanBase的UPSERT功能（ON DUPLICATE KEY UPDATE）
func (o *OceanBase) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 {
		return nil
	}

	// 构建INSERT语句
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

	// 构建占位符
	placeholder := "(" + strings.Repeat("?,", len(columns))
	placeholder = placeholder[:len(placeholder)-1] + ")"

	// 添加多行值
	for i := range values {
		if i > 0 {
			sqlBuilder.WriteString(", ")
		}
		sqlBuilder.WriteString(placeholder)
	}

	// 如果有更新列，添加ON DUPLICATE KEY UPDATE子句
	if len(updateColumns) > 0 {
		sqlBuilder.WriteString(" ON DUPLICATE KEY UPDATE ")
		for i, col := range updateColumns {
			if i > 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString(fmt.Sprintf("%s = VALUES(%s)", col, col))
		}
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 执行SQL
	return db.Exec(sqlBuilder.String(), flatValues...).Error
}

// QueryPage 实现分页查询
func (o *OceanBase) QueryPage(out interface{}, page, pageSize int, tableName string, filter interface{}, opts ...interface{}) (int64, error) {
	// 计算偏移量
	offset := (page - 1) * pageSize

	// 假设第一个opts是db *gorm.DB
	if len(opts) == 0 {
		return 0, fmt.Errorf("需要提供gorm.DB实例")
	}

	db, ok := opts[0].(*gorm.DB)
	if !ok {
		return 0, fmt.Errorf("第一个参数必须是*gorm.DB类型")
	}

	// 克隆DB以避免修改原始DB
	countDB := db.Session(&gorm.Session{})
	queryDB := db.Session(&gorm.Session{})

	// 应用过滤条件
	if filter != nil {
		countDB = countDB.Where(filter)
		queryDB = queryDB.Where(filter)
	}

	// 计算总记录数
	var total int64
	if err := countDB.Table(tableName).Count(&total).Error; err != nil {
		return 0, err
	}

	// 如果没有记录，直接返回
	if total == 0 {
		return 0, nil
	}

	// 查询分页数据
	err := queryDB.Table(tableName).Offset(offset).Limit(pageSize).Find(out).Error
	if err != nil {
		return 0, err
	}

	return total, nil
}

// GetVersionSQL 返回获取OceanBase版本的SQL
func (o *OceanBase) GetVersionSQL() string {
	return "SELECT VERSION()"
}

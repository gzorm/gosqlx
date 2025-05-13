package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// ClickHouse 适配器结构体
type ClickHouse struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewClickHouse 创建新的ClickHouse适配器
func NewClickHouse(dsn string) *ClickHouse {
	return &ClickHouse{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (c *ClickHouse) WithMaxIdle(maxIdle int) *ClickHouse {
	c.MaxIdle = maxIdle
	return c
}

// WithMaxOpen 设置最大打开连接数
func (c *ClickHouse) WithMaxOpen(maxOpen int) *ClickHouse {
	c.MaxOpen = maxOpen
	return c
}

// WithMaxLifetime 设置连接最大生命周期
func (c *ClickHouse) WithMaxLifetime(maxLifetime time.Duration) *ClickHouse {
	c.MaxLifetime = maxLifetime
	return c
}

// WithDebug 设置调试模式
func (c *ClickHouse) WithDebug(debug bool) *ClickHouse {
	c.Debug = debug
	return c
}

// Connect 连接数据库
func (c *ClickHouse) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if c.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库
	db, err := gorm.Open(clickhouse.Open(c.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(c.MaxIdle)
	sqlDB.SetMaxOpenConns(c.MaxOpen)
	sqlDB.SetConnMaxLifetime(c.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (c *ClickHouse) BuildDSN(username, password, host string, port int, database string, params map[string]string) string {
	// 基本DSN
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s", username, password, host, port, database)

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
// 注意：ClickHouse不支持标准的FOR UPDATE语法
func (c *ClickHouse) ForUpdate() string {
	return ""
}

// ForShare 生成共享锁语句
// 注意：ClickHouse不支持标准的共享锁语法
func (c *ClickHouse) ForShare() string {
	return ""
}

// Limit 生成分页语句
func (c *ClickHouse) Limit(offset, limit int) string {
	return fmt.Sprintf("LIMIT %d, %d", offset, limit)
}

// BatchInsert 批量插入
func (c *ClickHouse) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
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

// MergeInto 实现ClickHouse的UPSERT功能
// ClickHouse支持使用ALTER TABLE ... UPDATE语法或REPLACE INTO语法
func (c *ClickHouse) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 {
		return nil
	}

	// 使用REPLACE INTO语法（如果表引擎支持）
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

// QueryPage 实现分页查询
func (c *ClickHouse) QueryPage(out interface{}, page, pageSize int, filter interface{}, opts ...interface{}) (int64, error) {
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

	// 假设filter是查询条件
	query := db
	if filter != nil {
		query = query.Where(filter)
	}

	// 获取总记录数
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return 0, err
	}

	// 如果没有记录，直接返回
	if total == 0 {
		return 0, nil
	}

	// 执行分页查询
	if err := query.Limit(pageSize).Offset(offset).Find(out).Error; err != nil {
		return 0, err
	}

	return total, nil
}

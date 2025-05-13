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

// TiDB 适配器结构体
type TiDB struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewTiDB 创建新的TiDB适配器
func NewTiDB(dsn string) *TiDB {
	return &TiDB{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (t *TiDB) WithMaxIdle(maxIdle int) *TiDB {
	t.MaxIdle = maxIdle
	return t
}

// WithMaxOpen 设置最大打开连接数
func (t *TiDB) WithMaxOpen(maxOpen int) *TiDB {
	t.MaxOpen = maxOpen
	return t
}

// WithMaxLifetime 设置连接最大生命周期
func (t *TiDB) WithMaxLifetime(maxLifetime time.Duration) *TiDB {
	t.MaxLifetime = maxLifetime
	return t
}

// WithDebug 设置调试模式
func (t *TiDB) WithDebug(debug bool) *TiDB {
	t.Debug = debug
	return t
}

// Connect 连接数据库
func (t *TiDB) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if t.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库
	db, err := gorm.Open(mysql.Open(t.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(t.MaxIdle)
	sqlDB.SetMaxOpenConns(t.MaxOpen)
	sqlDB.SetConnMaxLifetime(t.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (t *TiDB) BuildDSN(username, password, host string, port int, database string, params map[string]string) string {
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
func (t *TiDB) ForUpdate() string {
	return "FOR UPDATE"
}

// ForShare 生成共享锁语句
func (t *TiDB) ForShare() string {
	return "LOCK IN SHARE MODE"
}

// Limit 生成分页语句
func (t *TiDB) Limit(offset, limit int) string {
	return fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)
}

// BatchInsert 批量插入
func (t *TiDB) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
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

// MergeInto 实现TiDB的UPSERT功能（ON DUPLICATE KEY UPDATE）
func (t *TiDB) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
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

// QueryPage 分页查询
func (t *TiDB) QueryPage(out interface{}, page, pageSize int, filter interface{}, opts ...interface{}) (int64, error) {
	// 参数验证
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}

	// 从 opts 中提取 db 和其他参数
	if len(opts) == 0 {
		return 0, fmt.Errorf("缺少必要参数：数据库连接")
	}

	db, ok := opts[0].(*gorm.DB)
	if !ok {
		return 0, fmt.Errorf("第一个可选参数必须是 *gorm.DB 类型")
	}

	// 处理 filter 参数
	var sqlStr string
	var values []interface{}

	switch f := filter.(type) {
	case string:
		// 如果 filter 是 SQL 字符串
		sqlStr = f
		// 提取剩余的参数作为 values
		if len(opts) > 1 {
			for _, v := range opts[1:] {
				values = append(values, v)
			}
		}
	case map[string]interface{}:
		// 如果 filter 是条件映射，构建 WHERE 子句
		// 这里简单实现，实际应用中可能需要更复杂的处理
		var conditions []string
		for k, v := range f {
			conditions = append(conditions, fmt.Sprintf("%s = ?", k))
			values = append(values, v)
		}

		// 假设 opts[1] 是基础 SQL（如果提供）
		baseSQL := "SELECT * FROM unknown_table"
		if len(opts) > 1 {
			if s, ok := opts[1].(string); ok {
				baseSQL = s
			}
		}

		if len(conditions) > 0 {
			if strings.Contains(strings.ToUpper(baseSQL), " WHERE ") {
				sqlStr = fmt.Sprintf("%s AND %s", baseSQL, strings.Join(conditions, " AND "))
			} else {
				sqlStr = fmt.Sprintf("%s WHERE %s", baseSQL, strings.Join(conditions, " AND "))
			}
		} else {
			sqlStr = baseSQL
		}
	default:
		return 0, fmt.Errorf("不支持的过滤条件类型")
	}

	// 计算偏移量
	offset := (page - 1) * pageSize

	// 查询总记录数
	var total int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_table", sqlStr)
	err := db.Raw(countSQL, values...).Count(&total).Error
	if err != nil {
		return 0, fmt.Errorf("查询总记录数失败: %w", err)
	}

	// 如果没有记录，直接返回
	if total == 0 {
		return 0, nil
	}

	// 查询分页数据
	pageSQL := fmt.Sprintf("%s LIMIT %d OFFSET %d", sqlStr, pageSize, offset)
	err = db.Raw(pageSQL, values...).Scan(out).Error
	if err != nil {
		return 0, fmt.Errorf("查询分页数据失败: %w", err)
	}

	return total, nil
}

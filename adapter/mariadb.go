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

// MariaDB 适配器结构体
type MariaDB struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewMariaDB 创建新的MariaDB适配器
func NewMariaDB(dsn string) *MariaDB {
	return &MariaDB{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (m *MariaDB) WithMaxIdle(maxIdle int) *MariaDB {
	m.MaxIdle = maxIdle
	return m
}

// WithMaxOpen 设置最大打开连接数
func (m *MariaDB) WithMaxOpen(maxOpen int) *MariaDB {
	m.MaxOpen = maxOpen
	return m
}

// WithMaxLifetime 设置连接最大生命周期
func (m *MariaDB) WithMaxLifetime(maxLifetime time.Duration) *MariaDB {
	m.MaxLifetime = maxLifetime
	return m
}

// WithDebug 设置调试模式
func (m *MariaDB) WithDebug(debug bool) *MariaDB {
	m.Debug = debug
	return m
}

// Connect 连接数据库
func (m *MariaDB) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if m.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库 - 使用MySQL驱动，因为MariaDB兼容MySQL协议
	db, err := gorm.Open(mysql.Open(m.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(m.MaxIdle)
	sqlDB.SetMaxOpenConns(m.MaxOpen)
	sqlDB.SetConnMaxLifetime(m.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (m *MariaDB) BuildDSN(username, password, host string, port int, database string, params map[string]string) string {
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
func (m *MariaDB) ForUpdate() string {
	return "FOR UPDATE"
}

// ForShare 生成共享锁语句
func (m *MariaDB) ForShare() string {
	return "LOCK IN SHARE MODE"
}

// Limit 生成分页语句
func (m *MariaDB) Limit(offset, limit int) string {
	return fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)
}

// BatchInsert 批量插入
func (m *MariaDB) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
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

// MergeInto 实现MariaDB的UPSERT功能（ON DUPLICATE KEY UPDATE）
func (m *MariaDB) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
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

func (m *MariaDB) QueryPage(out interface{}, page, pageSize int, tableName string, filter interface{}, opts ...interface{}) (int64, error) {
	// 参数验证
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	if tableName == "" {
		return 0, fmt.Errorf("表名不能为空")
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
		baseSQL := fmt.Sprintf("SELECT * FROM %s", tableName)
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

	// 检查 SQL 语句是否包含 SELECT 和 FROM 关键字
	hasSelect := strings.Contains(strings.ToUpper(sqlStr), "SELECT ")
	hasFrom := strings.Contains(strings.ToUpper(sqlStr), " FROM ")

	// 如果不是完整的 SQL 查询语句，则将其视为条件表达式
	if !hasSelect || !hasFrom {
		// 构建完整的 SQL 查询语句
		sqlStr = fmt.Sprintf("SELECT * FROM %s WHERE %s", tableName, sqlStr)
	} else {
		// 检查 SQL 语句是否包含 WHERE 子句
		hasWhere := strings.Contains(strings.ToUpper(sqlStr), " WHERE ")

		// 如果没有 WHERE 子句，但有条件需要添加
		if !hasWhere {
			// 查找 FROM 子句后面的位置
			fromIndex := strings.Index(strings.ToUpper(sqlStr), " FROM ")
			if fromIndex >= 0 {
				// 查找可能的子句位置
				clauseKeywords := []string{" ORDER BY ", " GROUP BY ", " HAVING ", " LIMIT "}
				insertPos := len(sqlStr)

				for _, keyword := range clauseKeywords {
					pos := strings.Index(strings.ToUpper(sqlStr), keyword)
					if pos >= 0 && pos < insertPos {
						insertPos = pos
					}
				}

				// 插入 WHERE 子句
				sqlStr = sqlStr[:insertPos] + " WHERE 1=1" + sqlStr[insertPos:]
			}
		}
	}

	// 计算偏移量
	offset := (page - 1) * pageSize

	// 查询总记录数
	var total int64
	var countSQL string

	// 检查是否是简单查询（只有 SELECT ... FROM ... WHERE ...）
	isSimpleQuery := true
	complexKeywords := []string{" GROUP BY ", " HAVING ", " DISTINCT ", " UNION "}
	for _, keyword := range complexKeywords {
		if strings.Contains(strings.ToUpper(sqlStr), keyword) {
			isSimpleQuery = false
			break
		}
	}

	if isSimpleQuery {
		// 对于简单查询，直接从表中计数
		// 提取 FROM 和 WHERE 部分
		fromIndex := strings.Index(strings.ToUpper(sqlStr), " FROM ")
		if fromIndex >= 0 {
			// 获取 FROM 之后的部分
			fromPart := sqlStr[fromIndex:]
			countSQL = "SELECT COUNT(*)" + fromPart
		} else {
			// 如果无法解析，回退到子查询方式
			countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_table", sqlStr)
		}
	} else {
		// 对于复杂查询，使用子查询
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_table", sqlStr)
	}

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

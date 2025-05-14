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
func (t *TiDB) QueryPage(dbOption interface{}, out interface{}, page, pageSize int, tableName string, orderBy []interface{}, filter ...interface{}) (int64, error) {
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

	// 从参数中提取 db
	if dbOption == nil {
		return 0, fmt.Errorf("缺少必要参数：数据库连接")
	}
	db, ok := dbOption.(*gorm.DB)
	if !ok {
		return 0, fmt.Errorf("数据库连接参数必须是 *gorm.DB 类型")
	}

	// 处理查询条件和参数
	var sqlStr string
	var values []interface{}
	var orderClause string

	// 处理排序
	if len(orderBy) > 0 {
		var orders []string
		for _, order := range orderBy {
			if orderStr, ok := order.(string); ok {
				orders = append(orders, orderStr)
			}
		}
		if len(orders) > 0 {
			orderClause = " ORDER BY " + strings.Join(orders, ", ")
		}
	}

	// 处理查询条件
	if len(filter) == 0 {
		// 没有查询条件，构建基本查询
		sqlStr = fmt.Sprintf("SELECT * FROM %s", tableName)
	} else if len(filter) == 1 {
		// 单个查询条件
		switch f := filter[0].(type) {
		case nil:
			sqlStr = fmt.Sprintf("SELECT * FROM %s", tableName)
		case string:
			// 如果是SQL字符串
			sqlStr = f
			// 提取剩余的参数
			if len(filter) > 1 {
				values = append(values, filter[1:]...)
			}
		case []interface{}:
			// 如果是切片，处理第一个元素
			if len(f) > 0 {
				if sqlCond, ok := f[0].(string); ok {
					sqlStr = sqlCond
					// 提取剩余的参数
					if len(f) > 1 {
						values = append(values, f[1:]...)
					}
				} else {
					return 0, fmt.Errorf("切片的第一个元素必须是SQL字符串")
				}
			} else {
				sqlStr = fmt.Sprintf("SELECT * FROM %s", tableName)
			}
		case map[string]interface{}:
			// 如果是条件映射
			var conditions []string
			for k, v := range f {
				conditions = append(conditions, fmt.Sprintf("%s = ?", k))
				values = append(values, v)
			}

			baseSQL := fmt.Sprintf("SELECT * FROM %s", tableName)
			if len(conditions) > 0 {
				sqlStr = fmt.Sprintf("%s WHERE %s", baseSQL, strings.Join(conditions, " AND "))
			} else {
				sqlStr = baseSQL
			}
		default:
			return 0, fmt.Errorf("不支持的查询条件类型")
		}
	} else {
		// 多个查询条件，第一个是SQL字符串，后面的是参数
		if sqlCond, ok := filter[0].(string); ok {
			sqlStr = sqlCond
			values = append(values, filter[1:]...)
		} else {
			return 0, fmt.Errorf("多参数查询时，第一个参数必须是SQL字符串")
		}
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
		hasOrder := strings.Contains(strings.ToUpper(sqlStr), " ORDER BY ")

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

		// 如果没有 ORDER BY 子句，但有排序需要添加
		if !hasOrder && orderClause != "" {
			// 查找可能的子句位置
			clauseKeywords := []string{" LIMIT ", " OFFSET "}
			insertPos := len(sqlStr)

			for _, keyword := range clauseKeywords {
				pos := strings.Index(strings.ToUpper(sqlStr), keyword)
				if pos >= 0 && pos < insertPos {
					insertPos = pos
				}
			}

			// 插入 ORDER BY 子句
			sqlStr = sqlStr[:insertPos] + orderClause + sqlStr[insertPos:]
		}
	}

	// 计算偏移量
	offset := (page - 1) * pageSize

	// 查询总记录数
	var total int64
	var countSQL string

	// 检查是否是复杂查询
	isComplexQuery := false

	// 检查是否包含子查询 - 通过查找括号内包含 SELECT 关键字
	if strings.Contains(strings.ToUpper(sqlStr), "(SELECT ") ||
		strings.Contains(strings.ToUpper(sqlStr), "( SELECT ") {
		isComplexQuery = true
	}

	// 检查其他复杂查询关键字
	complexKeywords := []string{
		" JOIN ", " LEFT JOIN ", " RIGHT JOIN ", " INNER JOIN ", " OUTER JOIN ",
		" GROUP BY ", " HAVING ", " DISTINCT ", " UNION ", " INTERSECT ", " EXCEPT ",
	}

	if !isComplexQuery {
		for _, keyword := range complexKeywords {
			if strings.Contains(strings.ToUpper(sqlStr), keyword) {
				isComplexQuery = true
				break
			}
		}
	}

	// 使用固定别名以避免冲突
	const countTableAlias = "count_table"

	if !isComplexQuery {
		// 对于简单查询，直接从表中计数
		// 提取 FROM 和 WHERE 部分
		fromIndex := strings.Index(strings.ToUpper(sqlStr), " FROM ")
		if fromIndex >= 0 {
			// 获取 FROM 之后的部分
			fromPart := sqlStr[fromIndex:]

			// 检查 FROM 部分是否包含子查询
			if strings.Contains(strings.ToUpper(fromPart), "(SELECT ") ||
				strings.Contains(strings.ToUpper(fromPart), "( SELECT ") {
				// 如果 FROM 部分包含子查询，使用子查询方式
				countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS %s", sqlStr, countTableAlias)
			} else {
				// 否则直接使用 FROM 部分
				countSQL = "SELECT COUNT(*)" + fromPart
			}
		} else {
			// 如果无法解析，回退到子查询方式
			countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS %s", sqlStr, countTableAlias)
		}
	} else {
		// 对于复杂查询，使用子查询
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS %s", sqlStr, countTableAlias)
	}

	// 移除 ORDER BY 子句以提高计数查询性能
	orderByIndex := strings.Index(strings.ToUpper(countSQL), " ORDER BY ")
	if orderByIndex > 0 {
		// 查找 ORDER BY 后面可能的 LIMIT 或结束位置
		limitIndex := strings.Index(strings.ToUpper(countSQL[orderByIndex:]), " LIMIT ")
		if limitIndex > 0 {
			// 有 LIMIT 子句，移除 ORDER BY 到 LIMIT 之间的部分
			countSQL = countSQL[:orderByIndex] + countSQL[orderByIndex+limitIndex:]
		} else {
			// 没有 LIMIT 子句，直接移除 ORDER BY 到结束
			countSQL = countSQL[:orderByIndex]
		}
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

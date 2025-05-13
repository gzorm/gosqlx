package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// Postgres 适配器结构体
type Postgres struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewPostgres 创建新的Postgres适配器
func NewPostgres(dsn string) *Postgres {
	return &Postgres{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (p *Postgres) WithMaxIdle(maxIdle int) *Postgres {
	p.MaxIdle = maxIdle
	return p
}

// WithMaxOpen 设置最大打开连接数
func (p *Postgres) WithMaxOpen(maxOpen int) *Postgres {
	p.MaxOpen = maxOpen
	return p
}

// WithMaxLifetime 设置连接最大生命周期
func (p *Postgres) WithMaxLifetime(maxLifetime time.Duration) *Postgres {
	p.MaxLifetime = maxLifetime
	return p
}

// WithDebug 设置调试模式
func (p *Postgres) WithDebug(debug bool) *Postgres {
	p.Debug = debug
	return p
}

// Connect 连接数据库
func (p *Postgres) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if p.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库
	db, err := gorm.Open(postgres.Open(p.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(p.MaxIdle)
	sqlDB.SetMaxOpenConns(p.MaxOpen)
	sqlDB.SetConnMaxLifetime(p.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (p *Postgres) BuildDSN(host string, port int, username, password, database string, params map[string]string) string {
	// 基本DSN
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s", host, port, username, password, database)

	// 添加参数
	if len(params) > 0 {
		for k, v := range params {
			dsn = dsn + " " + k + "=" + v
		}
	} else {
		// 默认参数
		dsn = dsn + " sslmode=disable TimeZone=Asia/Shanghai"
	}

	return dsn
}

// ForUpdate 生成FOR UPDATE锁定语句
func (p *Postgres) ForUpdate() string {
	return "FOR UPDATE"
}

// ForShare 生成共享锁语句
func (p *Postgres) ForShare() string {
	return "FOR SHARE"
}

// Limit 生成分页语句
func (p *Postgres) Limit(offset, limit int) string {
	return fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)
}

// OnConflict 生成ON CONFLICT语句（相当于MySQL的ON DUPLICATE KEY UPDATE）
func (p *Postgres) OnConflict(db *gorm.DB, table string, values interface{}, conflictColumns []string, updateColumns []string) *gorm.DB {
	// 构建冲突列
	var columns []clause.Column
	for _, col := range conflictColumns {
		columns = append(columns, clause.Column{Name: col})
	}

	return db.Clauses(clause.OnConflict{
		Columns:   columns,
		DoUpdates: clause.AssignmentColumns(updateColumns),
	}).Table(table).Create(values)
}

// BatchInsert 批量插入
func (p *Postgres) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// 构建SQL语句
	var placeholders []string
	for i, row := range values {
		var rowPlaceholders []string
		for j := range row {
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", i*len(columns)+j+1))
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 构建完整SQL
	sqlStr := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	// 执行SQL
	return db.Exec(sqlStr, flatValues...).Error
}

// BatchInsertOrUpdate 批量插入或更新
func (p *Postgres) BatchInsertOrUpdate(db *gorm.DB, table string, columns []string, values [][]interface{}, conflictColumns []string, updateColumns []string) error {
	if len(values) == 0 {
		return nil
	}

	// 构建SQL语句
	var placeholders []string
	for i, row := range values {
		var rowPlaceholders []string
		for j := range row {
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", i*len(columns)+j+1))
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 构建ON CONFLICT子句
	var conflictClause string
	if len(conflictColumns) > 0 {
		conflictClause = fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET ", strings.Join(conflictColumns, ", "))

		var updates []string
		for _, col := range updateColumns {
			updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}

		conflictClause += strings.Join(updates, ", ")
	}

	// 构建完整SQL
	sqlStr := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s%s",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
		conflictClause,
	)

	// 执行SQL
	return db.Exec(sqlStr, flatValues...).Error
}

// CreateSequence 创建序列
func (p *Postgres) CreateSequence(db *gorm.DB, name string, startWith int, incrementBy int) error {
	sqlStr := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s START WITH %d INCREMENT BY %d", name, startWith, incrementBy)
	return db.Exec(sqlStr).Error
}

// DropSequence 删除序列
func (p *Postgres) DropSequence(db *gorm.DB, name string) error {
	sqlStr := fmt.Sprintf("DROP SEQUENCE IF EXISTS %s", name)
	return db.Exec(sqlStr).Error
}

// NextVal 获取序列的下一个值
func (p *Postgres) NextVal(db *gorm.DB, name string) (int64, error) {
	var result int64
	err := db.Raw(fmt.Sprintf("SELECT nextval('%s')", name)).Scan(&result).Error
	return result, err
}

// CurrVal 获取序列的当前值
func (p *Postgres) CurrVal(db *gorm.DB, name string) (int64, error) {
	var result int64
	err := db.Raw(fmt.Sprintf("SELECT currval('%s')", name)).Scan(&result).Error
	return result, err
}

// CreateDatabase 创建数据库
func (p *Postgres) CreateDatabase(db *gorm.DB, name string) error {
	return db.Exec(fmt.Sprintf("CREATE DATABASE %s", name)).Error
}

// DropDatabase 删除数据库
func (p *Postgres) DropDatabase(db *gorm.DB, name string) error {
	return db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)).Error
}

// ShowDatabases 获取所有数据库
func (p *Postgres) ShowDatabases(db *gorm.DB) ([]string, error) {
	var databases []string
	err := db.Raw("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname").Scan(&databases).Error
	return databases, err
}

// ShowTables 获取所有表
func (p *Postgres) ShowTables(db *gorm.DB) ([]string, error) {
	var tables []string
	err := db.Raw(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		ORDER BY table_name
	`).Scan(&tables).Error
	return tables, err
}

// ShowCreateTable 获取创建表的DDL
func (p *Postgres) ShowCreateTable(db *gorm.DB, table string) (string, error) {
	var result string
	err := db.Raw(`
		SELECT 
			'CREATE TABLE ' || relname || E'\n(\n' ||
			array_to_string(
				array_agg(
					'    ' || column_name || ' ' || type || ' ' || not_null
				)
				, E',\n'
			) || E'\n);\n' as ddl
		FROM (
			SELECT 
				c.relname, 
				a.attname AS column_name,
				pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
				CASE WHEN a.attnotnull THEN 'NOT NULL' ELSE 'NULL' END as not_null
			FROM pg_class c,
				pg_attribute a,
				pg_type t
			WHERE c.relname = ?
				AND a.attnum > 0
				AND a.attrelid = c.oid
				AND a.atttypid = t.oid
			ORDER BY a.attnum
		) as tabledefinition
		GROUP BY relname;
	`, table).Scan(&result).Error
	return result, err
}

// TruncateTable 清空表
func (p *Postgres) TruncateTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
}

// GetTableStatus 获取表状态
func (p *Postgres) GetTableStatus(db *gorm.DB, table string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := db.Raw(`
		SELECT
			c.relname AS table_name,
			c.reltuples AS row_count,
			pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
			pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
			pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS index_size,
			obj_description(c.oid, 'pg_class') AS comment
		FROM
			pg_class c
		LEFT JOIN
			pg_namespace n ON n.oid = c.relnamespace
		WHERE
			c.relkind = 'r'
			AND n.nspname = 'public'
			AND c.relname = ?
	`, table).Scan(&result).Error
	return result, err
}

// GetVersion 获取PostgreSQL版本
func (p *Postgres) GetVersion(db *gorm.DB) (string, error) {
	var version string
	err := db.Raw("SELECT version()").Scan(&version).Error
	return version, err
}

// GetTableColumns 获取表的列信息
func (p *Postgres) GetTableColumns(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			column_name, 
			data_type, 
			character_maximum_length, 
			numeric_precision, 
			numeric_scale, 
			is_nullable, 
			column_default,
			ordinal_position
		FROM 
			information_schema.columns 
		WHERE 
			table_schema = 'public' 
			AND table_name = ? 
		ORDER BY 
			ordinal_position
	`, table).Scan(&results).Error
	return results, err
}

// GetTableIndexes 获取表的索引信息
func (p *Postgres) GetTableIndexes(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT
			i.relname AS index_name,
			am.amname AS index_type,
			array_to_string(array_agg(a.attname), ', ') AS column_names,
			ix.indisunique AS is_unique,
			ix.indisprimary AS is_primary
		FROM
			pg_class t,
			pg_class i,
			pg_index ix,
			pg_attribute a,
			pg_am am
		WHERE
			t.oid = ix.indrelid
			AND i.oid = ix.indexrelid
			AND a.attrelid = t.oid
			AND a.attnum = ANY(ix.indkey)
			AND t.relkind = 'r'
			AND t.relname = ?
			AND i.relam = am.oid
		GROUP BY
			i.relname,
			am.amname,
			ix.indisunique,
			ix.indisprimary
		ORDER BY
			i.relname
	`, table).Scan(&results).Error
	return results, err
}

// GetTableConstraints 获取表的约束信息
func (p *Postgres) GetTableConstraints(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT
			c.conname AS constraint_name,
			CASE c.contype
				WHEN 'c' THEN 'CHECK'
				WHEN 'f' THEN 'FOREIGN KEY'
				WHEN 'p' THEN 'PRIMARY KEY'
				WHEN 'u' THEN 'UNIQUE'
				WHEN 't' THEN 'TRIGGER'
				WHEN 'x' THEN 'EXCLUSION'
			END AS constraint_type,
			array_to_string(array_agg(col.attname), ', ') AS column_names,
			CASE c.contype
				WHEN 'f' THEN (SELECT r.relname FROM pg_class r WHERE r.oid = c.confrelid)
				ELSE NULL
			END AS referenced_table
		FROM
			pg_constraint c
		JOIN
			pg_class t ON c.conrelid = t.oid
		JOIN
			pg_attribute col ON col.attrelid = t.oid AND col.attnum = ANY(c.conkey)
		WHERE
			t.relname = ?
		GROUP BY
			c.conname,
			c.contype,
			c.confrelid
		ORDER BY
			c.conname
	`, table).Scan(&results).Error
	return results, err
}

// GetProcessList 获取会话列表
func (p *Postgres) GetProcessList(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT
			pid,
			usename,
			datname,
			client_addr,
			client_port,
			backend_start,
			query_start,
			state,
			wait_event_type,
			wait_event,
			query
		FROM
			pg_stat_activity
		WHERE
			pid <> pg_backend_pid()
		ORDER BY
			backend_start
	`).Scan(&results).Error
	return results, err
}

// KillProcess 终止会话
func (p *Postgres) KillProcess(db *gorm.DB, pid int) error {
	return db.Exec("SELECT pg_terminate_backend(?)", pid).Error
}

// GetTablespace 获取表空间信息
func (p *Postgres) GetTablespace(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT
			spcname AS tablespace_name,
			pg_size_pretty(pg_tablespace_size(spcname)) AS size,
			pg_tablespace_location(oid) AS location
		FROM
			pg_tablespace
		ORDER BY
			spcname
	`).Scan(&results).Error
	return results, err
}

// GetUsers 获取用户列表
func (p *Postgres) GetUsers(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT
			usename AS username,
			usesysid AS user_id,
			usesuper AS is_superuser,
			usecreatedb AS can_create_db,
			useconfig AS config,
			valuntil AS valid_until
		FROM
			pg_user
		ORDER BY
			usename
	`).Scan(&results).Error
	return results, err
}

// CreateUser 创建用户
func (p *Postgres) CreateUser(db *gorm.DB, username, password string, superuser, createdb bool) error {
	sqlStr := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", username, password)
	if superuser {
		sqlStr += " SUPERUSER"
	} else {
		sqlStr += " NOSUPERUSER"
	}
	if createdb {
		sqlStr += " CREATEDB"
	} else {
		sqlStr += " NOCREATEDB"
	}
	return db.Exec(sqlStr).Error
}

// DropUser 删除用户
func (p *Postgres) DropUser(db *gorm.DB, username string) error {
	return db.Exec(fmt.Sprintf("DROP USER IF EXISTS %s", username)).Error
}

// GrantPrivileges 授予权限
func (p *Postgres) GrantPrivileges(db *gorm.DB, privileges string, objects string, username string) error {
	sqlStr := fmt.Sprintf("GRANT %s ON %s TO %s", privileges, objects, username)
	return db.Exec(sqlStr).Error
}

// RevokePrivileges 撤销权限
func (p *Postgres) RevokePrivileges(db *gorm.DB, privileges string, objects string, username string) error {
	sqlStr := fmt.Sprintf("REVOKE %s ON %s FROM %s", privileges, objects, username)
	return db.Exec(sqlStr).Error
}

// CreateExtension 创建扩展
func (p *Postgres) CreateExtension(db *gorm.DB, name string) error {
	return db.Exec(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", name)).Error
}

// DropExtension 删除扩展
func (p *Postgres) DropExtension(db *gorm.DB, name string) error {
	return db.Exec(fmt.Sprintf("DROP EXTENSION IF EXISTS %s", name)).Error
}

// ListExtensions 列出所有扩展
func (p *Postgres) ListExtensions(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT
			extname AS name,
			extversion AS version,
			nspname AS schema,
			extconfig AS config
		FROM
			pg_extension e
		JOIN
			pg_namespace n ON n.oid = e.extnamespace
		ORDER BY
			extname
	`).Scan(&results).Error
	return results, err
}

// CreateSchema 创建模式
func (p *Postgres) CreateSchema(db *gorm.DB, name string) error {
	return db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", name)).Error
}

// DropSchema 删除模式
func (p *Postgres) DropSchema(db *gorm.DB, name string, cascade bool) error {
	sqlStr := fmt.Sprintf("DROP SCHEMA IF EXISTS %s", name)
	if cascade {
		sqlStr += " CASCADE"
	}
	return db.Exec(sqlStr).Error
}

// ListSchemas 列出所有模式
func (p *Postgres) ListSchemas(db *gorm.DB) ([]string, error) {
	var results []string
	err := db.Raw(`
		SELECT
			nspname
		FROM
			pg_namespace
		WHERE
			nspname NOT LIKE 'pg_%'
			AND nspname != 'information_schema'
		ORDER BY
			nspname
	`).Scan(&results).Error
	return results, err
}

// MergeInto 合并插入（UPSERT）- PostgreSQL实现
func (p *Postgres) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 || len(keyColumns) == 0 {
		return nil
	}

	// 已经有一个类似的方法 BatchInsertOrUpdate，可以复用其逻辑
	return p.BatchInsertOrUpdate(db, table, columns, values, keyColumns, updateColumns)
}

// QueryPage 分页查询
func (p *Postgres) QueryPage(out interface{}, page, pageSize int, tableName string, filter interface{}, opts ...interface{}) (int64, error) {
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
			conditions = append(conditions, fmt.Sprintf("%s = $%d", k, len(values)+1))
			values = append(values, v)
		}

		// 使用提供的表名
		baseSQL := fmt.Sprintf("SELECT * FROM %s", tableName)

		if len(conditions) > 0 {
			sqlStr = fmt.Sprintf("%s WHERE %s", baseSQL, strings.Join(conditions, " AND "))
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
	// PostgreSQL 使用 $1, $2, ... 作为参数占位符，但 GORM 会自动处理
	pageSQL := fmt.Sprintf("%s LIMIT %d OFFSET %d", sqlStr, pageSize, offset)
	err = db.Raw(pageSQL, values...).Scan(out).Error
	if err != nil {
		return 0, fmt.Errorf("查询分页数据失败: %w", err)
	}

	return total, nil
}

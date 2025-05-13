package dialect

import (
	"fmt"
	"strings"
)

// Dialect 数据库方言接口
type Dialect interface {
	// 获取方言名称
	GetName() string

	// 引号处理
	Quote(str string) string

	// 表名引号处理
	QuoteTable(table string) string

	// 列名引号处理
	QuoteColumn(column string) string

	// 值引号处理
	QuoteValue(value string) string

	// 分页查询
	BuildLimit(query string, offset, limit int) string

	// 获取序列值
	GetSequenceSQL(sequence string) string

	// 是否支持事务隔离级别
	SupportsSavepoints() bool

	// 创建保存点
	CreateSavepointSQL(name string) string

	// 回滚到保存点
	RollbackToSavepointSQL(name string) string

	// 释放保存点
	ReleaseSavepointSQL(name string) string

	// 获取表列表
	GetTablesSQL() string

	// 获取表结构
	GetTableSchemaSQL(table string) string

	// 获取索引列表
	GetIndexesSQL(table string) string

	// 获取外键列表
	GetForeignKeysSQL(table string) string

	// 获取数据库版本
	GetVersionSQL() string

	// 获取当前数据库名
	GetCurrentDatabaseSQL() string

	// 创建数据库
	CreateDatabaseSQL(name string, options map[string]string) string

	// 删除数据库
	DropDatabaseSQL(name string) string

	// 创建表
	CreateTableSQL(table string, columns []string, options map[string]string) string

	// 删除表
	DropTableSQL(table string) string

	// 清空表
	TruncateTableSQL(table string) string

	// 添加列
	AddColumnSQL(table, column, columnType string, options map[string]string) string

	// 修改列
	ModifyColumnSQL(table, column, columnType string, options map[string]string) string

	// 删除列
	DropColumnSQL(table, column string) string

	// 添加索引
	AddIndexSQL(table, indexName string, columns []string, unique bool) string

	// 删除索引
	DropIndexSQL(table, indexName string) string

	// 添加外键
	AddForeignKeySQL(table, foreignKey, refTable string, columns, refColumns []string, onDelete, onUpdate string) string

	// 删除外键
	DropForeignKeySQL(table, foreignKey string) string

	// 锁定表
	LockTableSQL(table string, lockType string) string

	// 解锁表
	UnlockTableSQL() string

	// 行锁
	ForUpdateSQL() string

	// 共享锁
	ForShareSQL() string

	// 批量插入
	BatchInsertSQL(table string, columns []string, rowCount int) string

	// 是否支持UPSERT
	SupportsUpsert() bool

	// UPSERT语句
	UpsertSQL(table string, columns, uniqueColumns, updateColumns []string) string
}

// 基础方言实现
type BaseDialect struct {
	name string
}

// 创建基础方言
func NewBaseDialect(name string) *BaseDialect {
	return &BaseDialect{name: name}
}

// 获取方言名称
func (d *BaseDialect) GetName() string {
	return d.name
}

// 引号处理
func (d *BaseDialect) Quote(str string) string {
	return fmt.Sprintf("\"%s\"", str)
}

// 表名引号处理
func (d *BaseDialect) QuoteTable(table string) string {
	return d.Quote(table)
}

// 列名引号处理
func (d *BaseDialect) QuoteColumn(column string) string {
	return d.Quote(column)
}

// 值引号处理
func (d *BaseDialect) QuoteValue(value string) string {
	return fmt.Sprintf("'%s'", strings.Replace(value, "'", "''", -1))
}

// 分页查询
func (d *BaseDialect) BuildLimit(query string, offset, limit int) string {
	if limit <= 0 {
		return query
	}

	if offset <= 0 {
		return fmt.Sprintf("%s LIMIT %d", query, limit)
	}

	return fmt.Sprintf("%s LIMIT %d OFFSET %d", query, limit, offset)
}

// 获取序列值
func (d *BaseDialect) GetSequenceSQL(sequence string) string {
	return fmt.Sprintf("SELECT nextval('%s')", sequence)
}

// 是否支持事务隔离级别
func (d *BaseDialect) SupportsSavepoints() bool {
	return true
}

// 创建保存点
func (d *BaseDialect) CreateSavepointSQL(name string) string {
	return fmt.Sprintf("SAVEPOINT %s", name)
}

// 回滚到保存点
func (d *BaseDialect) RollbackToSavepointSQL(name string) string {
	return fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)
}

// 释放保存点
func (d *BaseDialect) ReleaseSavepointSQL(name string) string {
	return fmt.Sprintf("RELEASE SAVEPOINT %s", name)
}

// 获取表列表
func (d *BaseDialect) GetTablesSQL() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
}

// 获取表结构
func (d *BaseDialect) GetTableSchemaSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			column_name, 
			data_type, 
			character_maximum_length, 
			is_nullable, 
			column_default 
		FROM 
			information_schema.columns 
		WHERE 
			table_name = '%s' 
		ORDER BY 
			ordinal_position
	`, table)
}

// 获取索引列表
func (d *BaseDialect) GetIndexesSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			i.relname as index_name, 
			a.attname as column_name, 
			ix.indisunique as is_unique 
		FROM 
			pg_class t, 
			pg_class i, 
			pg_index ix, 
			pg_attribute a 
		WHERE 
			t.oid = ix.indrelid 
			AND i.oid = ix.indexrelid 
			AND a.attrelid = t.oid 
			AND a.attnum = ANY(ix.indkey) 
			AND t.relkind = 'r' 
			AND t.relname = '%s' 
		ORDER BY 
			i.relname, a.attnum
	`, table)
}

// 获取外键列表
func (d *BaseDialect) GetForeignKeysSQL(table string) string {
	return fmt.Sprintf(`
		SELECT
			tc.constraint_name,
			kcu.column_name,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column
		FROM
			information_schema.table_constraints AS tc
		JOIN
			information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
		JOIN
			information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
		WHERE
			tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = '%s'
	`, table)
}

// 获取数据库版本
func (d *BaseDialect) GetVersionSQL() string {
	return "SELECT version()"
}

// 获取当前数据库名
func (d *BaseDialect) GetCurrentDatabaseSQL() string {
	return "SELECT current_database()"
}

// 创建数据库
func (d *BaseDialect) CreateDatabaseSQL(name string, options map[string]string) string {
	sql := fmt.Sprintf("CREATE DATABASE %s", d.Quote(name))

	if options != nil {
		if charset, ok := options["charset"]; ok {
			sql += fmt.Sprintf(" CHARACTER SET %s", charset)
		}

		if collate, ok := options["collate"]; ok {
			sql += fmt.Sprintf(" COLLATE %s", collate)
		}
	}

	return sql
}

// 删除数据库
func (d *BaseDialect) DropDatabaseSQL(name string) string {
	return fmt.Sprintf("DROP DATABASE IF EXISTS %s", d.Quote(name))
}

// 创建表
func (d *BaseDialect) CreateTableSQL(table string, columns []string, options map[string]string) string {
	sql := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)", d.QuoteTable(table), strings.Join(columns, ",\n  "))

	if options != nil {
		if engine, ok := options["engine"]; ok {
			sql += fmt.Sprintf(" ENGINE=%s", engine)
		}

		if charset, ok := options["charset"]; ok {
			sql += fmt.Sprintf(" DEFAULT CHARSET=%s", charset)
		}

		if collate, ok := options["collate"]; ok {
			sql += fmt.Sprintf(" COLLATE=%s", collate)
		}
	}

	return sql
}

// 删除表
func (d *BaseDialect) DropTableSQL(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", d.QuoteTable(table))
}

// 清空表
func (d *BaseDialect) TruncateTableSQL(table string) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", d.QuoteTable(table))
}

// 添加列
func (d *BaseDialect) AddColumnSQL(table, column, columnType string, options map[string]string) string {
	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		d.QuoteTable(table),
		d.QuoteColumn(column),
		columnType)

	if options != nil {
		if nullable, ok := options["nullable"]; ok && nullable == "false" {
			sql += " NOT NULL"
		}

		if defaultValue, ok := options["default"]; ok {
			sql += fmt.Sprintf(" DEFAULT %s", defaultValue)
		}
	}

	return sql
}

// 修改列
func (d *BaseDialect) ModifyColumnSQL(table, column, columnType string, options map[string]string) string {
	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s",
		d.QuoteTable(table),
		d.QuoteColumn(column),
		columnType)

	if options != nil {
		if nullable, ok := options["nullable"]; ok {
			if nullable == "true" {
				sql += fmt.Sprintf(", ALTER COLUMN %s DROP NOT NULL", d.QuoteColumn(column))
			} else {
				sql += fmt.Sprintf(", ALTER COLUMN %s SET NOT NULL", d.QuoteColumn(column))
			}
		}

		if defaultValue, ok := options["default"]; ok {
			if defaultValue == "" {
				sql += fmt.Sprintf(", ALTER COLUMN %s DROP DEFAULT", d.QuoteColumn(column))
			} else {
				sql += fmt.Sprintf(", ALTER COLUMN %s SET DEFAULT %s", d.QuoteColumn(column), defaultValue)
			}
		}
	}

	return sql
}

// 删除列
func (d *BaseDialect) DropColumnSQL(table, column string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s",
		d.QuoteTable(table),
		d.QuoteColumn(column))
}

// 添加索引
func (d *BaseDialect) AddIndexSQL(table, indexName string, columns []string, unique bool) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	uniqueStr := ""
	if unique {
		uniqueStr = "UNIQUE "
	}

	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniqueStr,
		d.Quote(indexName),
		d.QuoteTable(table),
		strings.Join(quotedColumns, ", "))
}

// 删除索引
func (d *BaseDialect) DropIndexSQL(table, indexName string) string {
	return fmt.Sprintf("DROP INDEX %s", d.Quote(indexName))
}

// 添加外键
func (d *BaseDialect) AddForeignKeySQL(table, foreignKey, refTable string, columns, refColumns []string, onDelete, onUpdate string) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	var quotedRefColumns []string
	for _, column := range refColumns {
		quotedRefColumns = append(quotedRefColumns, d.QuoteColumn(column))
	}

	sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
		d.QuoteTable(table),
		d.Quote(foreignKey),
		strings.Join(quotedColumns, ", "),
		d.QuoteTable(refTable),
		strings.Join(quotedRefColumns, ", "))

	if onDelete != "" {
		sql += fmt.Sprintf(" ON DELETE %s", onDelete)
	}

	if onUpdate != "" {
		sql += fmt.Sprintf(" ON UPDATE %s", onUpdate)
	}

	return sql
}

// 删除外键
func (d *BaseDialect) DropForeignKeySQL(table, foreignKey string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s",
		d.QuoteTable(table),
		d.Quote(foreignKey))
}

// 锁定表
func (d *BaseDialect) LockTableSQL(table string, lockType string) string {
	return fmt.Sprintf("LOCK TABLE %s IN %s MODE", d.QuoteTable(table), lockType)
}

// 解锁表
func (d *BaseDialect) UnlockTableSQL() string {
	return "COMMIT"
}

// 行锁
func (d *BaseDialect) ForUpdateSQL() string {
	return "FOR UPDATE"
}

// 共享锁
func (d *BaseDialect) ForShareSQL() string {
	return "FOR SHARE"
}

// 批量插入
func (d *BaseDialect) BatchInsertSQL(table string, columns []string, rowCount int) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		d.QuoteTable(table),
		strings.Join(quotedColumns, ", "))

	var placeholders []string
	for i := 0; i < rowCount; i++ {
		var rowPlaceholders []string
		for j := 0; j < len(columns); j++ {
			rowPlaceholders = append(rowPlaceholders, "?")
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	return sql + strings.Join(placeholders, ", ")
}

// 是否支持UPSERT
func (d *BaseDialect) SupportsUpsert() bool {
	return false
}

// UPSERT语句
func (d *BaseDialect) UpsertSQL(table string, columns, uniqueColumns, updateColumns []string) string {
	return ""
}

// 方言工厂映射
var dialectMap = map[string]func() Dialect{
	"mysql":      func() Dialect { return NewMySQLDialect() },
	"postgres":   func() Dialect { return NewPostgresDialect() },
	"postgresql": func() Dialect { return NewPostgresDialect() },
	"sqlite":     func() Dialect { return NewSQLiteDialect() },
	"sqlserver":  func() Dialect { return NewSQLServerDialect() },
	"mssql":      func() Dialect { return NewSQLServerDialect() },
	"oracle":     func() Dialect { return NewOracleDialect() },
	"clickhouse": func() Dialect { return NewClickHouseDialect() },
	"mariadb":    func() Dialect { return NewMariaDBDialect() },
}

// 注册自定义方言
func RegisterDialect(name string, factory func() Dialect) {
	dialectMap[strings.ToLower(name)] = factory
}

// 获取方言实例
func GetDialect(name string) Dialect {
	if factory, ok := dialectMap[strings.ToLower(name)]; ok {
		return factory()
	}
	return NewBaseDialect(name)
}

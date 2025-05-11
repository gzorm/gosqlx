package dialect

import "fmt"

// SQLite方言
type SQLiteDialect struct {
	*BaseDialect
}

// 创建SQLite方言
func NewSQLiteDialect() *SQLiteDialect {
	return &SQLiteDialect{NewBaseDialect("sqlite")}
}

// 获取表列表
func (d *SQLiteDialect) GetTablesSQL() string {
	return "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
}

// 获取表结构
func (d *SQLiteDialect) GetTableSchemaSQL(table string) string {
	return fmt.Sprintf("PRAGMA table_info(%s)", d.QuoteTable(table))
}

// 获取索引列表
func (d *SQLiteDialect) GetIndexesSQL(table string) string {
	return fmt.Sprintf("PRAGMA index_list(%s)", d.QuoteTable(table))
}

// 获取外键列表
func (d *SQLiteDialect) GetForeignKeysSQL(table string) string {
	return fmt.Sprintf("PRAGMA foreign_key_list(%s)", d.QuoteTable(table))
}

// 获取数据库版本
func (d *SQLiteDialect) GetVersionSQL() string {
	return "SELECT sqlite_version()"
}

// 修改列
func (d *SQLiteDialect) ModifyColumnSQL(table, column, columnType string, options map[string]string) string {
	// SQLite不支持直接修改列，需要创建新表并复制数据
	return fmt.Sprintf("-- SQLite不支持直接修改列，需要创建新表并复制数据\n-- ALTER TABLE %s MODIFY COLUMN %s %s",
		d.QuoteTable(table),
		d.QuoteColumn(column),
		columnType)
}

// 初始化方言
func init() {
	RegisterDialect("sqlite", func() Dialect {
		return NewSQLiteDialect()
	})
}

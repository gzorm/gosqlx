package dialect

import (
	"fmt"
	"strings"
)

// MySQL方言
type MySQLDialect struct {
	*BaseDialect
}

// 创建MySQL方言
func NewMySQLDialect() *MySQLDialect {
	return &MySQLDialect{NewBaseDialect("mysql")}
}

// 引号处理
func (d *MySQLDialect) Quote(str string) string {
	return fmt.Sprintf("`%s`", str)
}

// 获取表列表
func (d *MySQLDialect) GetTablesSQL() string {
	return "SHOW TABLES"
}

// 获取表结构
func (d *MySQLDialect) GetTableSchemaSQL(table string) string {
	return fmt.Sprintf("SHOW FULL COLUMNS FROM %s", d.QuoteTable(table))
}

// 获取索引列表
func (d *MySQLDialect) GetIndexesSQL(table string) string {
	return fmt.Sprintf("SHOW INDEX FROM %s", d.QuoteTable(table))
}

// 获取外键列表
func (d *MySQLDialect) GetForeignKeysSQL(table string) string {
	return fmt.Sprintf(`
		SELECT
			constraint_name,
			column_name,
			referenced_table_name,
			referenced_column_name
		FROM
			information_schema.key_column_usage
		WHERE
			table_schema = DATABASE()
			AND table_name = '%s'
			AND referenced_table_name IS NOT NULL
	`, table)
}

// 获取数据库版本
func (d *MySQLDialect) GetVersionSQL() string {
	return "SELECT VERSION()"
}

// 获取当前数据库名
func (d *MySQLDialect) GetCurrentDatabaseSQL() string {
	return "SELECT DATABASE()"
}

// 修改列
func (d *MySQLDialect) ModifyColumnSQL(table, column, columnType string, options map[string]string) string {
	sql := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s",
		d.QuoteTable(table),
		d.QuoteColumn(column),
		columnType)

	if options != nil {
		if nullable, ok := options["nullable"]; ok && nullable == "false" {
			sql += " NOT NULL"
		}

		if defaultValue, ok := options["default"]; ok {
			if defaultValue == "" {
				sql += " DEFAULT NULL"
			} else {
				sql += fmt.Sprintf(" DEFAULT %s", defaultValue)
			}
		}
	}

	return sql
}

// 是否支持UPSERT
func (d *MySQLDialect) SupportsUpsert() bool {
	return true
}

// UPSERT语句
func (d *MySQLDialect) UpsertSQL(table string, columns, uniqueColumns, updateColumns []string) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	var placeholders []string
	for i := 0; i < len(columns); i++ {
		placeholders = append(placeholders, "?")
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.QuoteTable(table),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))

	if len(updateColumns) > 0 {
		var updates []string
		for _, column := range updateColumns {
			updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", d.QuoteColumn(column), d.QuoteColumn(column)))
		}
		sql += fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", strings.Join(updates, ", "))
	}

	return sql
}

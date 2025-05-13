package dialect

import (
	"fmt"
	"strings"
)

// ClickHouse方言
type ClickHouseDialect struct {
	*BaseDialect
}

// 创建ClickHouse方言
func NewClickHouseDialect() *ClickHouseDialect {
	return &ClickHouseDialect{NewBaseDialect("clickhouse")}
}

// 引号处理
func (d *ClickHouseDialect) Quote(str string) string {
	return fmt.Sprintf("`%s`", str)
}

// 分页查询
func (d *ClickHouseDialect) BuildLimit(query string, offset, limit int) string {
	if limit <= 0 {
		return query
	}

	if offset <= 0 {
		return fmt.Sprintf("%s LIMIT %d", query, limit)
	}

	return fmt.Sprintf("%s LIMIT %d, %d", query, offset, limit)
}

// 获取表列表
func (d *ClickHouseDialect) GetTablesSQL() string {
	return "SHOW TABLES"
}

// 获取表结构
func (d *ClickHouseDialect) GetTableSchemaSQL(table string) string {
	return fmt.Sprintf("DESCRIBE TABLE %s", d.QuoteTable(table))
}

// 获取索引列表
func (d *ClickHouseDialect) GetIndexesSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			name as index_name,
			column as column_name,
			type as index_type,
			'0' as is_unique
		FROM 
			system.data_skipping_indices
		WHERE 
			table = '%s'
		UNION ALL
		SELECT
			name as index_name,
			expr as column_name,
			type as index_type,
			'0' as is_unique
		FROM
			system.projection_dependencies
		WHERE
			table = '%s'
	`, table, table)
}

// 获取外键列表 - ClickHouse不支持外键
func (d *ClickHouseDialect) GetForeignKeysSQL(table string) string {
	return "SELECT 1 WHERE 0" // 返回空结果集
}

// 获取数据库版本
func (d *ClickHouseDialect) GetVersionSQL() string {
	return "SELECT version()"
}

// 获取当前数据库名
func (d *ClickHouseDialect) GetCurrentDatabaseSQL() string {
	return "SELECT currentDatabase()"
}

// 修改列 - ClickHouse支持ALTER TABLE修改列
func (d *ClickHouseDialect) ModifyColumnSQL(table, column, columnType string, options map[string]string) string {
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

// 行锁 - ClickHouse不支持标准的FOR UPDATE语法
func (d *ClickHouseDialect) ForUpdateSQL() string {
	return ""
}

// 共享锁 - ClickHouse不支持标准的共享锁语法
func (d *ClickHouseDialect) ForShareSQL() string {
	return ""
}

// 是否支持UPSERT - ClickHouse支持使用ALTER TABLE ... UPDATE语法或REPLACE INTO语法
func (d *ClickHouseDialect) SupportsUpsert() bool {
	return true
}

// UPSERT语句 - 使用ClickHouse的特定语法
func (d *ClickHouseDialect) UpsertSQL(table string, columns, uniqueColumns, updateColumns []string) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	var placeholders []string
	for i := 0; i < len(columns); i++ {
		placeholders = append(placeholders, "?")
	}

	// ClickHouse使用INSERT语法，不支持标准的UPSERT
	// 对于ReplacingMergeTree引擎，可以通过插入实现更新
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.QuoteTable(table),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))
}

// 批量插入
func (d *ClickHouseDialect) BatchInsertSQL(table string, columns []string, rowCount int) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	var placeholders []string
	for i := 0; i < rowCount; i++ {
		var rowPlaceholders []string
		for j := 0; j < len(columns); j++ {
			rowPlaceholders = append(rowPlaceholders, "?")
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		d.QuoteTable(table),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))
}

// 初始化方言
func init() {
	RegisterDialect("clickhouse", func() Dialect {
		return NewClickHouseDialect()
	})
}

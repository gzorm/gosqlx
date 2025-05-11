package dialect

import (
	"fmt"
	"strings"
)

// SQL Server方言
type SQLServerDialect struct {
	*BaseDialect
}

// 创建SQL Server方言
func NewSQLServerDialect() *SQLServerDialect {
	return &SQLServerDialect{NewBaseDialect("sqlserver")}
}

// 引号处理
func (d *SQLServerDialect) Quote(str string) string {
	return fmt.Sprintf("[%s]", str)
}

// 分页查询
func (d *SQLServerDialect) BuildLimit(query string, offset, limit int) string {
	if limit <= 0 {
		return query
	}

	// SQL Server 2012+使用OFFSET-FETCH
	if offset <= 0 {
		return fmt.Sprintf("%s OFFSET 0 ROWS FETCH NEXT %d ROWS ONLY", query, limit)
	}

	return fmt.Sprintf("%s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", query, offset, limit)
}

// 获取表列表
func (d *SQLServerDialect) GetTablesSQL() string {
	return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
}

// 获取表结构
func (d *SQLServerDialect) GetTableSchemaSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			COLUMN_NAME, 
			DATA_TYPE, 
			CHARACTER_MAXIMUM_LENGTH, 
			IS_NULLABLE, 
			COLUMN_DEFAULT 
		FROM 
			INFORMATION_SCHEMA.COLUMNS 
		WHERE 
			TABLE_NAME = '%s' 
		ORDER BY 
			ORDINAL_POSITION
	`, table)
}

// 获取索引列表
func (d *SQLServerDialect) GetIndexesSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			i.name AS index_name,
			COL_NAME(ic.object_id, ic.column_id) AS column_name,
			i.is_unique
		FROM 
			sys.indexes i
		INNER JOIN 
			sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN 
			sys.tables t ON i.object_id = t.object_id
		WHERE 
			t.name = '%s'
		ORDER BY 
			i.name, ic.key_ordinal
	`, table)
}

// 获取外键列表
func (d *SQLServerDialect) GetForeignKeysSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			f.name AS constraint_name,
			COL_NAME(fc.parent_object_id, fc.parent_column_id) AS column_name,
			OBJECT_NAME(f.referenced_object_id) AS referenced_table,
			COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS referenced_column
		FROM 
			sys.foreign_keys f
		INNER JOIN 
			sys.foreign_key_columns fc ON f.object_id = fc.constraint_object_id
		INNER JOIN 
			sys.tables t ON f.parent_object_id = t.object_id
		WHERE 
			t.name = '%s'
	`, table)
}

// 获取数据库版本
func (d *SQLServerDialect) GetVersionSQL() string {
	return "SELECT @@VERSION"
}

// 获取当前数据库名
func (d *SQLServerDialect) GetCurrentDatabaseSQL() string {
	return "SELECT DB_NAME()"
}

// 修改列
func (d *SQLServerDialect) ModifyColumnSQL(table, column, columnType string, options map[string]string) string {
	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s",
		d.QuoteTable(table),
		d.QuoteColumn(column),
		columnType)

	if options != nil {
		if nullable, ok := options["nullable"]; ok && nullable == "false" {
			sql += " NOT NULL"
		} else {
			sql += " NULL"
		}
	}

	return sql
}

// 行锁
func (d *SQLServerDialect) ForUpdateSQL() string {
	return "WITH (UPDLOCK, ROWLOCK)"
}

// 共享锁
func (d *SQLServerDialect) ForShareSQL() string {
	return "WITH (HOLDLOCK, ROWLOCK)"
}

// 是否支持UPSERT
func (d *SQLServerDialect) SupportsUpsert() bool {
	return true
}

// UPSERT语句
func (d *SQLServerDialect) UpsertSQL(table string, columns, uniqueColumns, updateColumns []string) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	var placeholders []string
	for i := 0; i < len(columns); i++ {
		placeholders = append(placeholders, "?")
	}

	// SQL Server使用MERGE语法实现UPSERT
	var quotedUniqueColumns []string
	for _, column := range uniqueColumns {
		quotedUniqueColumns = append(quotedUniqueColumns, d.QuoteColumn(column))
	}

	var targetJoin []string
	var sourceJoin []string
	for _, column := range uniqueColumns {
		targetJoin = append(targetJoin, fmt.Sprintf("target.%s = source.%s", d.QuoteColumn(column), d.QuoteColumn(column)))
		sourceJoin = append(sourceJoin, fmt.Sprintf("source.%s", d.QuoteColumn(column)))
	}

	var updateSet []string
	for _, column := range updateColumns {
		updateSet = append(updateSet, fmt.Sprintf("target.%s = source.%s", d.QuoteColumn(column), d.QuoteColumn(column)))
	}

	var insertColumns []string
	var insertValues []string
	for _, column := range columns {
		insertColumns = append(insertColumns, d.QuoteColumn(column))
		insertValues = append(insertValues, fmt.Sprintf("source.%s", d.QuoteColumn(column)))
	}

	sql := fmt.Sprintf(`
				MERGE INTO %s AS target
				USING (VALUES (%s)) AS source (%s)
				ON %s
				`, d.QuoteTable(table), strings.Join(placeholders, ", "), strings.Join(quotedColumns, ", "), strings.Join(targetJoin, " AND "))

	if len(updateColumns) > 0 {
		sql += fmt.Sprintf("WHEN MATCHED THEN UPDATE SET %s\n", strings.Join(updateSet, ", "))
	}

	sql += fmt.Sprintf("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(insertColumns, ", "),
		strings.Join(insertValues, ", "))

	return sql
}

// 初始化方言
func init() {
	RegisterDialect("sqlserver", func() Dialect {
		return NewSQLServerDialect()
	})
}

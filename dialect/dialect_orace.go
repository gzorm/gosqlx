package dialect

import (
	"fmt"
	"strconv"
	"strings"
)

// Oracle方言
type OracleDialect struct {
	*BaseDialect
}

// 创建Oracle方言
func NewOracleDialect() *OracleDialect {
	return &OracleDialect{NewBaseDialect("oracle")}
}

// 引号处理
func (d *OracleDialect) Quote(str string) string {
	return fmt.Sprintf("\"%s\"", str)
}

// 分页查询
func (d *OracleDialect) BuildLimit(query string, offset, limit int) string {
	if limit <= 0 {
		return query
	}

	// Oracle使用ROWNUM或ROW_NUMBER()实现分页
	if offset <= 0 {
		return fmt.Sprintf(`
			SELECT * FROM (
				%s
			) WHERE ROWNUM <= %d
		`, query, limit)
	}

	return fmt.Sprintf(`
		SELECT * FROM (
			SELECT a.*, ROWNUM rnum FROM (
				%s
			) a WHERE ROWNUM <= %d
		) WHERE rnum > %d
	`, query, offset+limit, offset)
}

// 获取序列值
func (d *OracleDialect) GetSequenceSQL(sequence string) string {
	return fmt.Sprintf("SELECT %s.NEXTVAL FROM DUAL", sequence)
}

// 获取表列表
func (d *OracleDialect) GetTablesSQL() string {
	return "SELECT table_name FROM user_tables ORDER BY table_name"
}

// 获取表结构
func (d *OracleDialect) GetTableSchemaSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			column_name, 
			data_type, 
			data_length, 
			nullable, 
			data_default 
		FROM 
			user_tab_columns 
		WHERE 
			table_name = '%s' 
		ORDER BY 
			column_id
	`, strings.ToUpper(table))
}

// 获取索引列表
func (d *OracleDialect) GetIndexesSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			i.index_name, 
			c.column_name, 
			i.uniqueness 
		FROM 
			user_indexes i 
		JOIN 
			user_ind_columns c ON i.index_name = c.index_name 
		WHERE 
			i.table_name = '%s' 
		ORDER BY 
			i.index_name, c.column_position
	`, strings.ToUpper(table))
}

// 获取外键列表
func (d *OracleDialect) GetForeignKeysSQL(table string) string {
	return fmt.Sprintf(`
		SELECT 
			c.constraint_name, 
			cc.column_name, 
			r.table_name as referenced_table, 
			rc.column_name as referenced_column 
		FROM 
			user_constraints c 
		JOIN 
			user_cons_columns cc ON c.constraint_name = cc.constraint_name 
		JOIN 
			user_constraints r ON c.r_constraint_name = r.constraint_name 
		JOIN 
			user_cons_columns rc ON r.constraint_name = rc.constraint_name 
		WHERE 
			c.constraint_type = 'R' AND c.table_name = '%s'
	`, strings.ToUpper(table))
}

// 获取数据库版本
func (d *OracleDialect) GetVersionSQL() string {
	return "SELECT banner FROM v$version WHERE banner LIKE 'Oracle%'"
}

// 获取当前数据库名
func (d *OracleDialect) GetCurrentDatabaseSQL() string {
	return "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL"
}

// 修改列
func (d *OracleDialect) ModifyColumnSQL(table, column, columnType string, options map[string]string) string {
	sql := fmt.Sprintf("ALTER TABLE %s MODIFY %s %s",
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
func (d *OracleDialect) SupportsUpsert() bool {
	return true
}

// UPSERT语句
func (d *OracleDialect) UpsertSQL(table string, columns, uniqueColumns, updateColumns []string) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	var placeholders []string
	for i := 0; i < len(columns); i++ {
		placeholders = append(placeholders, ":"+strconv.Itoa(i+1))
	}

	// Oracle使用MERGE语法实现UPSERT
	var quotedUniqueColumns []string
	for _, column := range uniqueColumns {
		quotedUniqueColumns = append(quotedUniqueColumns, d.QuoteColumn(column))
	}

	var targetJoin []string
	for _, column := range uniqueColumns {
		targetJoin = append(targetJoin, fmt.Sprintf("target.%s = source.%s", d.QuoteColumn(column), d.QuoteColumn(column)))
	}

	var updateSet []string
	for _, column := range updateColumns {
		updateSet = append(updateSet, fmt.Sprintf("target.%s = source.%s", d.QuoteColumn(column), d.QuoteColumn(column)))
	}

	sql := fmt.Sprintf(`
MERGE INTO %s target
USING (SELECT %s FROM DUAL) source (%s)
ON (%s)
`, d.QuoteTable(table), strings.Join(placeholders, ", "), strings.Join(quotedColumns, ", "), strings.Join(targetJoin, " AND "))

	if len(updateColumns) > 0 {
		sql += fmt.Sprintf("WHEN MATCHED THEN UPDATE SET %s\n", strings.Join(updateSet, ", "))
	}

	sql += fmt.Sprintf("WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))

	return sql
}

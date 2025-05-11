package dialect

import (
	"fmt"
	"strings"
)

// PostgreSQL方言
type PostgresDialect struct {
	*BaseDialect
}

// 创建PostgreSQL方言
func NewPostgresDialect() *PostgresDialect {
	return &PostgresDialect{NewBaseDialect("postgres")}
}

// 是否支持UPSERT
func (d *PostgresDialect) SupportsUpsert() bool {
	return true
}

// UPSERT语句
func (d *PostgresDialect) UpsertSQL(table string, columns, uniqueColumns, updateColumns []string) string {
	var quotedColumns []string
	for _, column := range columns {
		quotedColumns = append(quotedColumns, d.QuoteColumn(column))
	}

	var placeholders []string
	for i := 0; i < len(columns); i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.QuoteTable(table),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "))

	if len(uniqueColumns) > 0 {
		var quotedUniqueColumns []string
		for _, column := range uniqueColumns {
			quotedUniqueColumns = append(quotedUniqueColumns, d.QuoteColumn(column))
		}

		sql += fmt.Sprintf(" ON CONFLICT (%s)", strings.Join(quotedUniqueColumns, ", "))

		if len(updateColumns) > 0 {
			var updates []string
			for _, column := range updateColumns {
				updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", d.QuoteColumn(column), d.QuoteColumn(column)))
			}
			sql += fmt.Sprintf(" DO UPDATE SET %s", strings.Join(updates, ", "))
		} else {
			sql += " DO NOTHING"
		}
	}

	return sql
}

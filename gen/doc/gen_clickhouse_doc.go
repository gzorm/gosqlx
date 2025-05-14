package doc

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/gzorm/gosqlx"
)

// createClickHouseDBConnection 创建ClickHouse数据库连接
func createClickHouseDBConnection(config *Config) (*sql.DB, error) {
	dbConfig := &gosqlx.Config{
		Type:        config.DBType,
		Source:      config.Source,
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
	}
	ctx := &gosqlx.Context{
		Context: nil,
		Nick:    "clickhouse_doc_generator",
		Mode:    "ro",
		DBType:  config.DBType,
		Timeout: time.Second * 30,
	}
	database, err := gosqlx.NewDatabase(ctx, dbConfig)
	if err != nil {
		return nil, err
	}
	return database.SqlDB(), nil
}

// getAllClickHouseTables 获取所有ClickHouse表信息
func getAllClickHouseTables(db *sql.DB, dbName string) ([]TableDoc, error) {
	rows, err := db.Query(`SELECT name FROM system.tables WHERE database = ?`, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}

	var tables []TableDoc
	for _, tableName := range tableNames {
		table, err := getClickHouseTableInfo(db, dbName, tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

// getClickHouseTableInfo 获取ClickHouse表详细信息
func getClickHouseTableInfo(db *sql.DB, dbName, tableName string) (TableDoc, error) {
	// 获取表注释（ClickHouse 通常没有表注释，这里留空或自定义）
	tableComment := ""

	// 获取列信息
	columns, err := getClickHouseColumnInfo(db, dbName, tableName)
	if err != nil {
		return TableDoc{}, err
	}

	// ClickHouse 没有主键和索引的概念，这里主键和索引信息留空
	return TableDoc{
		TableName:    tableName,
		TableComment: tableComment,
		Columns:      columns,
		PrimaryKeys:  nil,
		Indexes:      nil,
	}, nil
}

// getClickHouseColumnInfo 获取ClickHouse列信息
func getClickHouseColumnInfo(db *sql.DB, dbName, tableName string) ([]ColumnDoc, error) {
	query := `
		SELECT 
			name, type, is_in_partition_key, is_in_primary_key, default_kind, default_expression
		FROM system.columns
		WHERE database = ? AND table = ?
		ORDER BY position
	`
	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnDoc
	for rows.Next() {
		var col ColumnDoc
		var isInPartitionKey, isInPrimaryKey uint8
		var defaultKind, defaultExpr sql.NullString
		if err := rows.Scan(&col.ColumnName, &col.DataType, &isInPartitionKey, &isInPrimaryKey, &defaultKind, &defaultExpr); err != nil {
			return nil, err
		}
		col.IsNullable = "" // ClickHouse 类型里有 Nullable，可自行解析
		col.ColumnDefault = defaultExpr.String
		col.ColumnKey = ""
		col.Extra = ""
		col.ColumnComment = ""
		columns = append(columns, col)
	}
	return columns, nil
}

// GenerateClickHouseDoc 生成ClickHouse数据库文档
func GenerateClickHouseDoc(config *Config) error {
	db, err := createClickHouseDBConnection(config)
	if err != nil {
		return fmt.Errorf("连接ClickHouse数据库失败: %v", err)
	}
	defer db.Close()

	tables, err := getAllClickHouseTables(db, config.DBName)
	if err != nil {
		return fmt.Errorf("获取ClickHouse表信息失败: %v", err)
	}

	// 生成Word文档
	err = generateWordDoc(tables, config)
	if err != nil {
		return fmt.Errorf("生成Word文档失败: %v", err)
	}
	return nil
}

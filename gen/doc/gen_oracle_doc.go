package doc

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/gzorm/gosqlx"
)

// createOracleDBConnection 创建Oracle数据库连接
func createOracleDBConnection(config *Config) (*sql.DB, error) {
	dbConfig := &gosqlx.Config{
		Type:        config.DBType,
		Source:      config.Source,
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
	}
	ctx := &gosqlx.Context{
		Context: nil,
		Nick:    "oracle_doc_generator",
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

// getAllOracleTables 获取所有Oracle表信息
func getAllOracleTables(db *sql.DB) ([]TableDoc, error) {
	rows, err := db.Query(`SELECT TABLE_NAME FROM USER_TABLES`)
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
		table, err := getOracleTableInfo(db, tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

// getOracleTableInfo 获取Oracle表详细信息
func getOracleTableInfo(db *sql.DB, tableName string) (TableDoc, error) {
	// 获取表注释
	var tableComment string
	err := db.QueryRow(`SELECT COMMENTS FROM USER_TAB_COMMENTS WHERE TABLE_NAME = :1`, tableName).Scan(&tableComment)
	if err == sql.ErrNoRows {
		tableComment = ""
	} else if err != nil {
		return TableDoc{}, err
	}

	// 获取列信息
	columns, err := getOracleColumnInfo(db, tableName)
	if err != nil {
		return TableDoc{}, err
	}

	// 获取主键
	primaryKeys, err := getOraclePrimaryKeys(db, tableName)
	if err != nil {
		return TableDoc{}, err
	}

	// 获取索引
	indexes, err := getOracleIndexes(db, tableName)
	if err != nil {
		return TableDoc{}, err
	}

	return TableDoc{
		TableName:    tableName,
		TableComment: tableComment,
		Columns:      columns,
		PrimaryKeys:  primaryKeys,
		Indexes:      indexes,
	}, nil
}

// getOracleColumnInfo 获取Oracle列信息
func getOracleColumnInfo(db *sql.DB, tableName string) ([]ColumnDoc, error) {
	query := `
		SELECT 
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.NULLABLE,
			c.DATA_DEFAULT,
			cc.COMMENTS
		FROM USER_TAB_COLUMNS c
		LEFT JOIN USER_COL_COMMENTS cc ON c.TABLE_NAME = cc.TABLE_NAME AND c.COLUMN_NAME = cc.COLUMN_NAME
		WHERE c.TABLE_NAME = :1
		ORDER BY c.COLUMN_ID
	`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnDoc
	for rows.Next() {
		var col ColumnDoc
		var nullable, dataDefault sql.NullString
		if err := rows.Scan(&col.ColumnName, &col.DataType, &nullable, &dataDefault, &col.ColumnComment); err != nil {
			return nil, err
		}
		col.IsNullable = nullable.String
		col.ColumnDefault = dataDefault.String
		columns = append(columns, col)
	}
	return columns, nil
}

// getOraclePrimaryKeys 获取Oracle主键
func getOraclePrimaryKeys(db *sql.DB, tableName string) ([]string, error) {
	query := `
		SELECT cols.COLUMN_NAME
		FROM USER_CONSTRAINTS cons, USER_CONS_COLUMNS cols
		WHERE cons.CONSTRAINT_TYPE = 'P'
		  AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
		  AND cons.TABLE_NAME = :1
	`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// getOracleIndexes 获取Oracle索引
func getOracleIndexes(db *sql.DB, tableName string) ([]IndexDoc, error) {
	query := `
		SELECT INDEX_NAME, UNIQUENESS
		FROM USER_INDEXES
		WHERE TABLE_NAME = :1
	`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []IndexDoc
	for rows.Next() {
		var idx IndexDoc
		var uniqueness string
		if err := rows.Scan(&idx.IndexName, &uniqueness); err != nil {
			return nil, err
		}
		idx.IsUnique = (uniqueness == "UNIQUE")
		// 获取索引列
		colRows, err := db.Query(`SELECT COLUMN_NAME FROM USER_IND_COLUMNS WHERE TABLE_NAME = :1 AND INDEX_NAME = :2`, tableName, idx.IndexName)
		if err == nil {
			for colRows.Next() {
				var col string
				_ = colRows.Scan(&col)
				idx.Columns = append(idx.Columns, col)
			}
			colRows.Close()
		}
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

// GenerateOracleDBDoc 生成Oracle数据库文档
func GenerateOracleDBDoc(config *Config) error {
	db, err := createOracleDBConnection(config)
	if err != nil {
		return fmt.Errorf("连接Oracle数据库失败: %v", err)
	}
	defer db.Close()

	tables, err := getAllOracleTables(db)
	if err != nil {
		return fmt.Errorf("获取Oracle表信息失败: %v", err)
	}

	// 生成Word文档
	err = generateWordDoc(tables, config)
	if err != nil {
		return fmt.Errorf("生成Word文档失败: %v", err)
	}
	return nil
}

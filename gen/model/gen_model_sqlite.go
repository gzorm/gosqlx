package model

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteGenerator SQLite表结构生成器
type SQLiteGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewSQLiteGenerator 创建SQLite表结构生成器
func NewSQLiteGenerator(config *Config) (*SQLiteGenerator, error) {
	if config.DBType != "sqlite" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	// SQLite连接字符串
	dsn := config.DatabaseName

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return &SQLiteGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *SQLiteGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *SQLiteGenerator) Generate() error {
	// 获取所有表名
	tables, err := g.GetAllTables()
	if err != nil {
		return err
	}

	// 确保输出目录存在
	outputDir := filepath.Join(g.Config.OutputDir, "poes")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 收集所有表信息
	var tableInfos []*TableInfo
	for _, tableName := range tables {
		tableInfo, err := g.GetTableInfo(tableName)
		if err != nil {
			return err
		}
		tableInfos = append(tableInfos, tableInfo)
	}

	// 生成单个模型文件
	if err := g.GenerateModelFile(tableInfos, outputDir); err != nil {
		return err
	}

	return nil
}

// GetAllTables 获取所有表名
func (g *SQLiteGenerator) GetAllTables() ([]string, error) {
	// 查询所有表（排除系统表和视图）
	query := `
		SELECT name 
		FROM sqlite_master 
		WHERE type = 'table' 
		AND name NOT LIKE 'sqlite_%'
		ORDER BY name
	`
	rows, err := g.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询表失败: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %v", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// GetTableInfo 获取表信息
func (g *SQLiteGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
	// SQLite没有表注释，使用空字符串
	tableComment := ""

	// 获取表的创建语句，用于提取主键信息
	var createSQL string
	query := `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`
	err := g.DB.QueryRow(query, tableName).Scan(&createSQL)
	if err != nil {
		return nil, fmt.Errorf("获取表创建语句失败: %v", err)
	}

	// 获取列信息
	columns, err := g.GetColumnInfo(tableName)
	if err != nil {
		return nil, err
	}

	// 获取主键
	primaryKeys, err := g.GetPrimaryKeys(tableName, createSQL)
	if err != nil {
		return nil, err
	}

	// 获取索引
	indexes, err := g.GetIndexes(tableName)
	if err != nil {
		return nil, err
	}

	// 生成模型名称（表名转为驼峰命名）
	modelName := g.ToCamelCase(tableName)

	return &TableInfo{
		TableName:    tableName,
		TableComment: tableComment,
		Columns:      columns,
		PrimaryKeys:  primaryKeys,
		Indexes:      indexes,
		ModelName:    modelName,
	}, nil
}

// GetColumnInfo 获取列信息
func (g *SQLiteGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
	// 获取表的PRAGMA信息
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := g.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询列信息失败: %v", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var dfltValue interface{}

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		// SQLite没有列注释，使用空字符串
		columnComment := ""

		// 设置是否可为空
		isNullable := "YES"
		if notNull == 1 {
			isNullable = "NO"
		}

		// 设置键类型
		columnKey := ""
		if pk == 1 {
			columnKey = "PRI"
		}

		// 设置额外信息
		extra := ""
		if dfltValue != nil {
			extra = fmt.Sprintf("DEFAULT %v", dfltValue)
		}

		// 创建列信息
		col := ColumnInfo{
			ColumnName:    name,
			DataType:      dataType,
			ColumnType:    dataType,
			IsNullable:    isNullable,
			ColumnKey:     columnKey,
			ColumnComment: columnComment,
			Extra:         extra,
			FieldName:     g.ToCamelCase(name),
			GoType:        g.MapSQLiteTypeToGo(dataType, isNullable == "YES"),
			JsonTag:       name,
		}

		// 生成GORM标签
		gormTag := fmt.Sprintf("column:%s;", name)

		// 添加类型信息
		gormTag += fmt.Sprintf("type:%s;", dataType)

		// 添加是否为空
		if isNullable == "NO" {
			gormTag += "not null;"
		}

		// 添加主键信息
		if columnKey == "PRI" {
			gormTag += "primaryKey;"
		}

		// 检查是否为自增列（SQLite中通常是INTEGER PRIMARY KEY）
		if columnKey == "PRI" && strings.ToUpper(dataType) == "INTEGER" {
			gormTag += "autoIncrement;"
		}

		// 添加默认值
		if dfltValue != nil {
			defaultValue := fmt.Sprintf("%v", dfltValue)
			// 如果默认值是字符串，需要处理引号
			if strings.HasPrefix(defaultValue, "'") && strings.HasSuffix(defaultValue, "'") {
				defaultValue = strings.Trim(defaultValue, "'")
				gormTag += fmt.Sprintf("default:'%s';", strings.Replace(defaultValue, "'", "\\'", -1))
			} else {
				gormTag += fmt.Sprintf("default:%s;", defaultValue)
			}
		}

		// 添加注释（SQLite不支持列注释，使用列名）
		gormTag += fmt.Sprintf("comment:'%s字段';", name)

		col.GormTag = gormTag

		columns = append(columns, col)
	}

	return columns, nil
}

// GetPrimaryKeys 获取主键
func (g *SQLiteGenerator) GetPrimaryKeys(tableName string, createSQL string) ([]string, error) {
	// 从PRAGMA获取主键信息
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := g.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询主键失败: %v", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var dfltValue interface{}

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("扫描主键失败: %v", err)
		}

		if pk > 0 {
			primaryKeys = append(primaryKeys, name)
		}
	}

	// 如果没有找到主键，尝试从创建语句中解析
	if len(primaryKeys) == 0 && createSQL != "" {
		// 查找PRIMARY KEY (column1, column2, ...)格式
		pkStart := strings.Index(strings.ToUpper(createSQL), "PRIMARY KEY (")
		if pkStart > 0 {
			pkStart += 13 // "PRIMARY KEY ("的长度
			pkEnd := strings.Index(createSQL[pkStart:], ")")
			if pkEnd > 0 {
				pkColumns := createSQL[pkStart : pkStart+pkEnd]
				for _, col := range strings.Split(pkColumns, ",") {
					col = strings.TrimSpace(col)
					// 移除可能的引号
					col = strings.Trim(col, "`\"'[]")
					primaryKeys = append(primaryKeys, col)
				}
			}
		}
	}

	return primaryKeys, nil
}

// GetIndexes 获取索引
func (g *SQLiteGenerator) GetIndexes(tableName string) ([]IndexInfo, error) {
	// 查询索引
	query := `
		SELECT 
			name, 
			CASE WHEN sql LIKE '%UNIQUE%' THEN 1 ELSE 0 END as is_unique
		FROM 
			sqlite_master 
		WHERE 
			type = 'index' 
			AND tbl_name = ? 
			AND name NOT LIKE 'sqlite_autoindex_%'
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询索引失败: %v", err)
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var indexName string
		var isUnique int

		if err := rows.Scan(&indexName, &isUnique); err != nil {
			return nil, fmt.Errorf("扫描索引失败: %v", err)
		}

		// 获取索引列
		var indexInfo IndexInfo
		indexInfo.IndexName = indexName
		indexInfo.IsUnique = isUnique == 1
		indexInfo.IndexType = "BTREE" // SQLite默认使用B-tree索引

		// 查询索引列
		colQuery := fmt.Sprintf("PRAGMA index_info(%s)", indexName)
		colRows, err := g.DB.Query(colQuery)
		if err != nil {
			return nil, fmt.Errorf("查询索引列失败: %v", err)
		}

		for colRows.Next() {
			var seqno, cid int
			var colName string

			if err := colRows.Scan(&seqno, &cid, &colName); err != nil {
				colRows.Close()
				return nil, fmt.Errorf("扫描索引列失败: %v", err)
			}

			indexInfo.ColumnNames = append(indexInfo.ColumnNames, colName)
		}
		colRows.Close()

		indexes = append(indexes, indexInfo)
	}

	return indexes, nil
}

// MapSQLiteTypeToGo 将SQLite类型映射到Go类型
func (g *SQLiteGenerator) MapSQLiteTypeToGo(sqliteType string, isNullable bool) string {
	// SQLite类型不区分大小写
	sqliteType = strings.ToUpper(sqliteType)

	// 提取基本类型（去除长度等信息）
	baseType := sqliteType
	if idx := strings.Index(baseType, "("); idx > 0 {
		baseType = baseType[:idx]
	}

	switch baseType {
	case "INTEGER", "INT", "SMALLINT", "MEDIUMINT", "BIGINT":
		if strings.Contains(sqliteType, "UNSIGNED") {
			if isNullable {
				return "*uint64"
			}
			return "uint64"
		}
		if strings.Contains(sqliteType, "BIGINT") {
			if isNullable {
				return "*int64"
			}
			return "int64"
		}
		if isNullable {
			return "*int"
		}
		return "int"
	case "REAL", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC":
		if isNullable {
			return "*float64"
		}
		return "float64"
	case "TEXT", "CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "CLOB":
		if isNullable {
			return "*string"
		}
		return "string"
	case "BLOB":
		return "[]byte"
	case "BOOLEAN", "BOOL":
		if isNullable {
			return "*bool"
		}
		return "bool"
	case "DATE", "DATETIME", "TIMESTAMP":
		if isNullable {
			return "*time.Time"
		}
		return "time.Time"
	case "JSON":
		return "json.RawMessage"
	default:
		// 对于未知类型，默认使用字符串
		if isNullable {
			return "*string"
		}
		return "string"
	}
}

// ToCamelCase 转换为驼峰命名
func (g *SQLiteGenerator) ToCamelCase(s string) string {
	// 处理下划线分隔的命名
	parts := strings.Split(s, "_")
	for i := range parts {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

// GenerateModelFile 生成模型文件
func (g *SQLiteGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
	// 模板定义
	tmpl := `// 代码由 gosqlx 自动生成，请勿手动修改
// 生成时间: {{.GenerateTime}}
package {{.PackageName}}

import (
	"time"	
)

{{range .TableInfos}}
// {{.ModelName}} {{.TableComment}}
type {{.ModelName}} struct {
{{- range .Columns}}
	{{.FieldName}} {{.GoType}} ` + "`json:\"{{.JsonTag}}\" gorm:\"{{.GormTag}}\"`" + ` // {{.ColumnComment}}
{{- end}}
}

// TableName 表名
func (m *{{.ModelName}}) TableName() string {
	return "{{.TableName}}"
}

{{end}}
`

	// 准备模板数据
	data := struct {
		PackageName  string
		TableInfos   []*TableInfo
		GenerateTime string
	}{
		PackageName:  g.Config.PackageName,
		TableInfos:   tableInfos,
		GenerateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	// 解析模板
	t, err := template.New("model").Parse(tmpl)
	if err != nil {
		return fmt.Errorf("解析模板失败: %v", err)
	}

	// 生成文件名
	filePath := filepath.Join(outputDir, "poes.go")

	// 创建文件
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("创建文件失败: %v", err)
	}
	defer file.Close()

	// 执行模板
	if err := t.Execute(file, data); err != nil {
		return fmt.Errorf("执行模板失败: %v", err)
	}

	fmt.Printf("生成模型文件: %s\n", filePath)
	return nil
}

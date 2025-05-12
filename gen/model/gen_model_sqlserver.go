package model

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	_ "gorm.io/driver/sqlserver"
)

// SQLServerGenerator SQL Server表结构生成器
type SQLServerGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewSQLServerGenerator 创建SQL Server表结构生成器
func NewSQLServerGenerator(config *Config) (*SQLServerGenerator, error) {
	if config.DBType != "sqlserver" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	// 构建连接字符串
	dsn := fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s",
		config.Host, config.Port, config.Username, config.Password, config.DatabaseName)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return &SQLServerGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *SQLServerGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *SQLServerGenerator) Generate() error {
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
func (g *SQLServerGenerator) GetAllTables() ([]string, error) {
	// 查询用户表（排除系统表）
	query := `
		SELECT TABLE_NAME 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_TYPE = 'BASE TABLE' 
		AND TABLE_SCHEMA = 'dbo'
		ORDER BY TABLE_NAME
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
func (g *SQLServerGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
	// 获取表注释
	var tableComment string
	query := `
		SELECT ISNULL(ep.value, '') AS TableComment
		FROM sys.tables t
		LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
		WHERE t.name = @p1
	`
	err := g.DB.QueryRow(query, tableName).Scan(&tableComment)
	if err != nil {
		// 如果没有注释，不返回错误，而是使用空字符串
		if err == sql.ErrNoRows {
			tableComment = ""
		} else {
			return nil, fmt.Errorf("获取表注释失败: %v", err)
		}
	}

	// 获取列信息
	columns, err := g.GetColumnInfo(tableName)
	if err != nil {
		return nil, err
	}

	// 获取主键
	primaryKeys, err := g.GetPrimaryKeys(tableName)
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
func (g *SQLServerGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
	// 查询列信息
	query := `
		SELECT 
			c.COLUMN_NAME,
			c.DATA_TYPE,
			CONCAT(c.DATA_TYPE, 
				CASE 
					WHEN c.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar') THEN '(' + ISNULL(CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR), 'MAX') + ')'
					WHEN c.DATA_TYPE IN ('decimal', 'numeric') THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
					ELSE ''
				END) AS COLUMN_TYPE,
			c.IS_NULLABLE,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS COLUMN_KEY,
			ISNULL(ep.value, '') AS COLUMN_COMMENT,
			CASE 
				WHEN ic.COLUMN_NAME IS NOT NULL THEN 'auto_increment'
				WHEN c.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT ' + c.COLUMN_DEFAULT 
				ELSE '' 
			END AS EXTRA
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN (
			SELECT ku.COLUMN_NAME
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
			JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
				ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
				AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
			WHERE ku.TABLE_NAME = @p1
		) AS pk ON c.COLUMN_NAME = pk.COLUMN_NAME
		LEFT JOIN (
			SELECT 
				col.name AS COLUMN_NAME,
				ep.value
			FROM sys.tables t
			INNER JOIN sys.columns col ON col.object_id = t.object_id
			LEFT JOIN sys.extended_properties ep ON ep.major_id = col.object_id AND ep.minor_id = col.column_id AND ep.name = 'MS_Description'
			WHERE t.name = @p1
		) AS ep ON c.COLUMN_NAME = ep.COLUMN_NAME
		LEFT JOIN (
			SELECT 
				COL_NAME(ic.object_id, ic.column_id) AS COLUMN_NAME
			FROM sys.identity_columns ic
			JOIN sys.tables t ON ic.object_id = t.object_id
			WHERE t.name = @p1
		) AS ic ON c.COLUMN_NAME = ic.COLUMN_NAME
		WHERE c.TABLE_NAME = @p1
		ORDER BY c.ORDINAL_POSITION
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询列信息失败: %v", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo

		if err := rows.Scan(
			&col.ColumnName,
			&col.DataType,
			&col.ColumnType,
			&col.IsNullable,
			&col.ColumnKey,
			&col.ColumnComment,
			&col.Extra,
		); err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		// 设置Go相关字段
		col.FieldName = g.ToCamelCase(col.ColumnName)
		col.GoType = g.MapSQLServerTypeToGo(col.DataType, col.IsNullable == "YES")
		col.JsonTag = col.ColumnName

		// 生成GORM标签
		gormTag := fmt.Sprintf("column:%s;", col.ColumnName)

		// 添加类型信息
		gormTag += fmt.Sprintf("type:%s;", col.ColumnType)

		// 添加是否为空
		if col.IsNullable == "NO" {
			gormTag += "not null;"
		}

		// 添加主键信息
		if col.ColumnKey == "PRI" {
			gormTag += "primaryKey;"
		}

		// 添加自增信息
		if strings.Contains(strings.ToLower(col.Extra), "auto_increment") {
			gormTag += "autoIncrement;"
		}

		// 添加默认值
		if strings.Contains(strings.ToLower(col.Extra), "default") {
			defaultValue := g.ExtractDefaultValue(col.Extra)
			if defaultValue != "" {
				// 如果默认值是字符串，需要处理引号
				if strings.HasPrefix(defaultValue, "'") && strings.HasSuffix(defaultValue, "'") {
					defaultValue = strings.Trim(defaultValue, "'")
					gormTag += fmt.Sprintf("default:'%s';", strings.Replace(defaultValue, "'", "\\'", -1))
				} else {
					gormTag += fmt.Sprintf("default:%s;", defaultValue)
				}
			}
		}

		// 添加注释
		if col.ColumnComment != "" {
			gormTag += fmt.Sprintf("comment:'%s';", strings.Replace(col.ColumnComment, "'", "\\'", -1))
		}

		col.GormTag = gormTag

		columns = append(columns, col)
	}

	return columns, nil
}

// ExtractDefaultValue 从Extra字段中提取默认值
func (g *SQLServerGenerator) ExtractDefaultValue(extra string) string {
	if !strings.Contains(strings.ToLower(extra), "default") {
		return ""
	}

	parts := strings.SplitN(extra, "DEFAULT ", 2)
	if len(parts) < 2 {
		return ""
	}

	defaultValue := strings.TrimSpace(parts[1])
	// 移除可能的括号
	defaultValue = strings.TrimPrefix(defaultValue, "(")
	defaultValue = strings.TrimSuffix(defaultValue, ")")

	return defaultValue
}

// GetPrimaryKeys 获取主键
func (g *SQLServerGenerator) GetPrimaryKeys(tableName string) ([]string, error) {
	query := `
		SELECT ku.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
			ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
			AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
		WHERE ku.TABLE_NAME = @p1
		ORDER BY ku.ORDINAL_POSITION
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询主键失败: %v", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("扫描主键失败: %v", err)
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	return primaryKeys, nil
}

// GetIndexes 获取索引
func (g *SQLServerGenerator) GetIndexes(tableName string) ([]IndexInfo, error) {
	query := `
		SELECT 
			i.name AS index_name,
			i.is_unique AS is_unique,
			i.type_desc AS index_type,
			c.name AS column_name
		FROM 
			sys.indexes i
		INNER JOIN 
			sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN 
			sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		INNER JOIN 
			sys.tables t ON i.object_id = t.object_id
		WHERE 
			t.name = @p1
			AND i.is_primary_key = 0 -- 排除主键索引
		ORDER BY 
			i.name, ic.key_ordinal
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询索引失败: %v", err)
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var indexName, indexType, columnName string
		var isUnique bool

		if err := rows.Scan(&indexName, &isUnique, &indexType, &columnName); err != nil {
			return nil, fmt.Errorf("扫描索引失败: %v", err)
		}

		// 如果索引不存在，创建新索引
		if _, ok := indexMap[indexName]; !ok {
			indexMap[indexName] = &IndexInfo{
				IndexName: indexName,
				IndexType: indexType,
				IsUnique:  isUnique,
			}
		}

		// 添加索引列
		indexMap[indexName].ColumnNames = append(indexMap[indexName].ColumnNames, columnName)
	}

	// 转换为切片
	var indexes []IndexInfo
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// MapSQLServerTypeToGo 将SQL Server类型映射到Go类型
func (g *SQLServerGenerator) MapSQLServerTypeToGo(sqlType string, isNullable bool) string {
	switch strings.ToLower(sqlType) {
	case "tinyint":
		if isNullable {
			return "*uint8"
		}
		return "uint8"
	case "smallint":
		if isNullable {
			return "*int16"
		}
		return "int16"
	case "int":
		if isNullable {
			return "*int"
		}
		return "int"
	case "bigint":
		if isNullable {
			return "*int64"
		}
		return "int64"
	case "bit":
		if isNullable {
			return "*bool"
		}
		return "bool"
	case "decimal", "numeric", "money", "smallmoney":
		if isNullable {
			return "*float64"
		}
		return "float64"
	case "float", "real":
		if isNullable {
			return "*float32"
		}
		return "float32"
	case "char", "varchar", "text", "nchar", "nvarchar", "ntext":
		if isNullable {
			return "*string"
		}
		return "string"
	case "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time":
		if isNullable {
			return "*time.Time"
		}
		return "time.Time"
	case "binary", "varbinary", "image":
		return "[]byte"
	case "uniqueidentifier":
		if isNullable {
			return "*string"
		}
		return "string"
	case "xml":
		return "string"
	default:
		if isNullable {
			return "interface{}"
		}
		return "string"
	}
}

// ToCamelCase 转换为驼峰命名
func (g *SQLServerGenerator) ToCamelCase(s string) string {
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
func (g *SQLServerGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
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

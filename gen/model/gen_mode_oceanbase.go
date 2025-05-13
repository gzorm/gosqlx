package model

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// OceanBaseGenerator OceanBase表结构生成器
type OceanBaseGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewOceanBaseGenerator 创建OceanBase表结构生成器
func NewOceanBaseGenerator(config *Config) (*OceanBaseGenerator, error) {
	if config.DBType != "oceanbase" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.Username, config.Password, config.Host, config.Port, config.DatabaseName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return &OceanBaseGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *OceanBaseGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *OceanBaseGenerator) Generate() error {
	// 获取所有表名
	tables, err := g.GetAllTables()
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		return fmt.Errorf("数据库 %s 中没有找到表", g.Config.DatabaseName)
	}

	// 确保输出目录存在
	outputDir := filepath.Join(g.Config.OutputDir, "models")
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
func (g *OceanBaseGenerator) GetAllTables() ([]string, error) {
	query := "SHOW TABLES"
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历表名时发生错误: %v", err)
	}

	return tables, nil
}

// GetTableInfo 获取表信息
func (g *OceanBaseGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
	// 获取表注释
	var tableComment string
	query := `
		SELECT table_comment 
		FROM information_schema.tables 
		WHERE table_schema = ? AND table_name = ?
	`
	err := g.DB.QueryRow(query, g.Config.DatabaseName, tableName).Scan(&tableComment)
	if err != nil {
		return nil, fmt.Errorf("获取表 %s 注释失败: %v", tableName, err)
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

	var indexInfos []IndexInfo
	for indexName, columns := range indexes {
		indexInfos = append(indexInfos, IndexInfo{
			IndexName:   indexName,
			ColumnNames: columns,
		})
	}

	// 生成模型名称（表名转为驼峰命名）
	modelName := g.ToCamelCase(tableName)

	return &TableInfo{
		TableName:    tableName,
		TableComment: tableComment,
		Columns:      columns,
		PrimaryKeys:  primaryKeys,
		Indexes:      indexInfos,
		ModelName:    modelName,
	}, nil
}

// GetColumnInfo 获取列信息
func (g *OceanBaseGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT 
			column_name, data_type, column_type, 
			is_nullable, column_key, column_comment, extra
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`

	rows, err := g.DB.Query(query, g.Config.DatabaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询表 %s 列信息失败: %v", tableName, err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		if err := rows.Scan(
			&col.ColumnName, &col.DataType, &col.ColumnType,
			&col.IsNullable, &col.ColumnKey, &col.ColumnComment, &col.Extra,
		); err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		// 设置Go相关字段
		col.FieldName = g.ToCamelCase(col.ColumnName)
		col.GoType = g.MapOceanBaseTypeToGo(col.DataType, col.IsNullable == "YES")
		col.JsonTag = col.ColumnName

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列信息时发生错误: %v", err)
	}

	return columns, nil
}

// GetPrimaryKeys 获取主键
func (g *OceanBaseGenerator) GetPrimaryKeys(tableName string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position
	`

	rows, err := g.DB.Query(query, g.Config.DatabaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询表 %s 主键失败: %v", tableName, err)
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历主键时发生错误: %v", err)
	}

	return primaryKeys, nil
}

// GetIndexes 获取索引
func (g *OceanBaseGenerator) GetIndexes(tableName string) (map[string][]string, error) {
	query := `
		SELECT index_name, column_name
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name != 'PRIMARY'
		ORDER BY index_name, seq_in_index
	`

	rows, err := g.DB.Query(query, g.Config.DatabaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询表 %s 索引失败: %v", tableName, err)
	}
	defer rows.Close()

	indexes := make(map[string][]string)
	for rows.Next() {
		var indexName, columnName string
		if err := rows.Scan(&indexName, &columnName); err != nil {
			return nil, fmt.Errorf("扫描索引失败: %v", err)
		}
		indexes[indexName] = append(indexes[indexName], columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历索引时发生错误: %v", err)
	}

	return indexes, nil
}

// GenerateModelFile 生成模型文件
func (g *OceanBaseGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
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

// ToCamelCase 转换为驼峰命名
func (g *OceanBaseGenerator) ToCamelCase(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}

	var result string
	parts := strings.Split(s, "_")
	for _, part := range parts {
		if part == "" {
			continue
		}
		result += strings.ToUpper(part[:1]) + part[1:]
	}

	return result
}

// ToSnakeCase 转换为蛇形命名
func (g *OceanBaseGenerator) ToSnakeCase(s string) string {
	var result string
	for i, r := range s {
		if i > 0 && 'A' <= r && r <= 'Z' {
			result += "_"
		}
		result += string(r)
	}
	return strings.ToLower(result)
}

// MapOceanBaseTypeToGo 将OceanBase数据类型映射到Go类型
func (g *OceanBaseGenerator) MapOceanBaseTypeToGo(dataType string, isNullable bool) string {
	// OceanBase兼容MySQL，所以类型映射与MySQL相同
	switch strings.ToLower(dataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer":
		if isNullable {
			return "*int"
		}
		return "int"
	case "bigint":
		if isNullable {
			return "*int64"
		}
		return "int64"
	case "float", "double", "decimal", "numeric":
		if isNullable {
			return "*float64"
		}
		return "float64"
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		if isNullable {
			return "*string"
		}
		return "string"
	case "date", "datetime", "timestamp", "time":
		if isNullable {
			return "*time.Time"
		}
		return "time.Time"
	case "tinyblob", "blob", "mediumblob", "longblob", "binary", "varbinary":
		return "[]byte"
	case "bit", "bool", "boolean":
		if isNullable {
			return "*bool"
		}
		return "bool"
	case "json":
		return "json.RawMessage"
	default:
		if isNullable {
			return "*string"
		}
		return "string"
	}
}

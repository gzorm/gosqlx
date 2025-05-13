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

// MariaDBGenerator MariaDB表结构生成器
type MariaDBGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewMariaDBGenerator 创建MariaDB表结构生成器
func NewMariaDBGenerator(config *Config) (*MariaDBGenerator, error) {
	if config.DBType != "mariadb" {
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

	return &MariaDBGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *MariaDBGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *MariaDBGenerator) Generate() error {
	// 获取所有表名
	tables, err := g.GetAllTables()
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		return fmt.Errorf("数据库 %s 中没有找到表", g.Config.DatabaseName)
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

	// 根据配置选择生成单个文件还是多个文件
	if g.Config.SingleFile {
		// 生成单个模型文件
		if err := g.GenerateModelFile(tableInfos, outputDir); err != nil {
			return err
		}
	} else {
		// 生成多个模型文件
		if err := g.GenerateModelFiles(tableInfos, outputDir); err != nil {
			return err
		}
	}

	return nil
}

// GetAllTables 获取所有表名
func (g *MariaDBGenerator) GetAllTables() ([]string, error) {
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
func (g *MariaDBGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
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
func (g *MariaDBGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
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
		col.GoType = g.MapMariaDBTypeToGo(col.DataType, col.IsNullable == "YES")
		col.JsonTag = col.ColumnName

		// 生成GORM标签
		gormTag := fmt.Sprintf("column:%s;", col.ColumnName)

		// 添加类型信息
		gormTag += fmt.Sprintf("type:%s;", g.GetGormDataType(col.DataType, col.ColumnType))

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
				gormTag += fmt.Sprintf("default:%s;", defaultValue)
			}
		}

		// 添加注释
		if col.ColumnComment != "" {
			gormTag += fmt.Sprintf("comment:'%s';", strings.Replace(col.ColumnComment, "'", "\\'", -1))
		}

		col.GormTag = gormTag

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历列信息时发生错误: %v", err)
	}

	return columns, nil
}

// GetPrimaryKeys 获取主键
func (g *MariaDBGenerator) GetPrimaryKeys(tableName string) ([]string, error) {
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
func (g *MariaDBGenerator) GetIndexes(tableName string) ([]IndexInfo, error) {
	query := `
		SELECT 
			index_name, non_unique, index_type, column_name
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ?
		ORDER BY index_name, seq_in_index
	`

	rows, err := g.DB.Query(query, g.Config.DatabaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询表 %s 索引失败: %v", tableName, err)
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var indexName string
		var nonUnique int
		var indexType, columnName string

		if err := rows.Scan(&indexName, &nonUnique, &indexType, &columnName); err != nil {
			return nil, fmt.Errorf("扫描索引失败: %v", err)
		}

		// 跳过主键索引，因为已经单独处理
		if indexName == "PRIMARY" {
			continue
		}

		// 如果索引不存在，创建新索引
		if _, ok := indexMap[indexName]; !ok {
			indexMap[indexName] = &IndexInfo{
				IndexName: indexName,
				IndexType: indexType,
				IsUnique:  nonUnique == 0,
			}
		}

		// 添加索引列
		indexMap[indexName].ColumnNames = append(indexMap[indexName].ColumnNames, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历索引时发生错误: %v", err)
	}

	// 转换为切片
	var indexes []IndexInfo
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// GenerateModelFile 生成模型文件
func (g *MariaDBGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
	// 模板定义
	tmpl := `// 代码由 gosqlx 自动生成，请勿手动修改
// 生成时间: {{.GenerateTime}}
package {{.PackageName}}

import (
    "time"    
    {{if .NeedJsonImport}}"encoding/json"{{end}}
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

	// 检查是否需要导入json包
	needJsonImport := false
	for _, tableInfo := range tableInfos {
		for _, col := range tableInfo.Columns {
			if col.GoType == "json.RawMessage" {
				needJsonImport = true
				break
			}
		}
		if needJsonImport {
			break
		}
	}

	// 准备模板数据
	data := struct {
		PackageName    string
		TableInfos     []*TableInfo
		GenerateTime   string
		NeedJsonImport bool
	}{
		PackageName:    g.Config.PackageName,
		TableInfos:     tableInfos,
		GenerateTime:   time.Now().Format("2006-01-02 15:04:05"),
		NeedJsonImport: needJsonImport,
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

// MapMariaDBTypeToGo 将MariaDB类型映射到Go类型
func (g *MariaDBGenerator) MapMariaDBTypeToGo(mariadbType string, isNullable bool) string {
	switch strings.ToLower(mariadbType) {
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
	case "tinyint(1)", "bit(1)":
		if isNullable {
			return "*bool"
		}
		return "bool"
	case "json":
		return "json.RawMessage"
	case "blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary":
		return "[]byte"
	default:
		if isNullable {
			return "interface{}"
		}
		return "string"
	}
}

// GetGormDataType 获取GORM数据类型
func (g *MariaDBGenerator) GetGormDataType(dataType string, columnType string) string {
	// 对于某些类型，需要保留完整的列类型信息
	switch strings.ToLower(dataType) {
	case "int", "tinyint", "smallint", "mediumint", "bigint":
		return columnType
	case "decimal", "numeric", "float", "double":
		return columnType
	case "varchar", "char":
		return columnType
	default:
		return dataType
	}
}

// ExtractDefaultValue 提取默认值
func (g *MariaDBGenerator) ExtractDefaultValue(extra string) string {
	// 查找默认值
	if strings.Contains(strings.ToLower(extra), "default") {
		parts := strings.Split(extra, "DEFAULT")
		if len(parts) > 1 {
			defaultValue := strings.TrimSpace(parts[1])
			return defaultValue
		}
	}
	return ""
}

// ToCamelCase 转换为驼峰命名
func (g *MariaDBGenerator) ToCamelCase(s string) string {
	// 处理下划线分隔的命名
	parts := strings.Split(s, "_")
	for i := range parts {
		// 检查Config中是否有FirstLetterUpper字段
		// 如果没有，默认首字母大写
		firstLetterUpper := true
		if i == 0 && g.Config != nil {
			firstLetterUpper = g.Config.FirstLetterUpper

		}

		if i > 0 || firstLetterUpper {
			if len(parts[i]) > 0 {
				parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
			}
		}
	}
	return strings.Join(parts, "")
}

// GenerateModelFiles 生成单个模型文件
func (g *MariaDBGenerator) GenerateModelFiles(tableInfos []*TableInfo, outputDir string) error {
	// 模板定义
	tmpl := `// 代码由 gosqlx 自动生成，请勿手动修改
// 生成时间: {{.GenerateTime}}
package {{.PackageName}}

import (
    "time"
    {{if .NeedJsonImport}}"encoding/json"{{end}}
)

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
`

	// 解析模板
	t, err := template.New("model").Parse(tmpl)
	if err != nil {
		return fmt.Errorf("解析模板失败: %v", err)
	}

	// 为每个表生成单独的模型文件
	for _, tableInfo := range tableInfos {
		// 检查是否需要导入json包
		needJsonImport := false
		for _, col := range tableInfo.Columns {
			if col.GoType == "json.RawMessage" {
				needJsonImport = true
				break
			}
		}

		// 准备模板数据
		data := struct {
			PackageName    string
			ModelName      string
			TableName      string
			TableComment   string
			Columns        []ColumnInfo
			GenerateTime   string
			NeedJsonImport bool
		}{
			PackageName:    g.Config.PackageName,
			ModelName:      tableInfo.ModelName,
			TableName:      tableInfo.TableName,
			TableComment:   tableInfo.TableComment,
			Columns:        tableInfo.Columns,
			GenerateTime:   time.Now().Format("2006-01-02 15:04:05"),
			NeedJsonImport: needJsonImport,
		}

		// 生成文件名
		fileName := fmt.Sprintf("%s.go", strings.ToLower(tableInfo.ModelName))
		filePath := filepath.Join(outputDir, fileName)

		// 创建文件
		file, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("创建文件失败: %v", err)
		}

		// 执行模板
		if err := t.Execute(file, data); err != nil {
			_ = file.Close()
			return fmt.Errorf("执行模板失败: %v", err)
		}

		_ = file.Close()
		fmt.Printf("生成模型文件: %s\n", filePath)
	}

	return nil
}

package model

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouseGenerator ClickHouse表结构生成器
type ClickHouseGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewClickHouseGenerator 创建ClickHouse表结构生成器
func NewClickHouseGenerator(config *Config) (*ClickHouseGenerator, error) {
	if config.DBType != "clickhouse" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	// ClickHouse连接字符串
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=10s&max_execution_time=60",
		config.Username, config.Password, config.Host, config.Port, config.DatabaseName)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		err := db.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return &ClickHouseGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *ClickHouseGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *ClickHouseGenerator) Generate() error {
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
func (g *ClickHouseGenerator) GetAllTables() ([]string, error) {
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

	return tables, nil
}

// GetTableInfo 获取表信息
func (g *ClickHouseGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
	// 获取表引擎和注释
	var engine, tableComment string
	query := `
		SELECT 
			engine,
			comment
		FROM system.tables
		WHERE database = currentDatabase() AND name = ?
	`
	err := g.DB.QueryRow(query, tableName).Scan(&engine, &tableComment)
	if err != nil {
		return nil, fmt.Errorf("获取表信息失败: %v", err)
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
func (g *ClickHouseGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT 
			name, type, default_expression, 
			comment, is_in_primary_key
		FROM system.columns
		WHERE database = currentDatabase() AND table = ?
		ORDER BY position
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询列信息失败: %v", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultExpr, comment sql.NullString
		var isPrimaryKey bool

		if err := rows.Scan(
			&col.ColumnName, &col.DataType, &defaultExpr,
			&comment, &isPrimaryKey,
		); err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		// 设置列类型和是否可为空
		col.ColumnType = col.DataType
		col.IsNullable = "YES" // ClickHouse中Nullable类型会在类型中显示
		if !strings.Contains(col.DataType, "Nullable") {
			col.IsNullable = "NO"
		}

		// 设置列注释
		if comment.Valid {
			col.ColumnComment = comment.String
		}

		// 设置列键
		if isPrimaryKey {
			col.ColumnKey = "PRI"
		}

		// 设置默认值
		if defaultExpr.Valid {
			col.Extra = fmt.Sprintf("DEFAULT %s", defaultExpr.String)
		}

		// 设置Go相关字段
		col.FieldName = g.ToCamelCase(col.ColumnName)
		col.GoType = g.MapClickHouseTypeToGo(col.DataType, col.IsNullable == "YES")
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

		// 添加默认值
		if defaultExpr.Valid {
			gormTag += fmt.Sprintf("default:%s;", defaultExpr.String)
		}

		// 添加注释
		if comment.Valid && comment.String != "" {
			gormTag += fmt.Sprintf("comment:'%s';", strings.Replace(comment.String, "'", "\\'", -1))
		}

		col.GormTag = gormTag

		columns = append(columns, col)
	}

	return columns, nil
}

// GetPrimaryKeys 获取主键
func (g *ClickHouseGenerator) GetPrimaryKeys(tableName string) ([]string, error) {
	query := `
		SELECT name
		FROM system.columns
		WHERE database = currentDatabase() AND table = ? AND is_in_primary_key = 1
		ORDER BY position
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
func (g *ClickHouseGenerator) GetIndexes(tableName string) ([]IndexInfo, error) {
	// ClickHouse的索引查询
	query := `
		SELECT 
			name as index_name,
			column as column_name,
			type as index_type,
			0 as is_unique
		FROM 
			system.data_skipping_indices
		WHERE 
			database = currentDatabase() AND table = ?
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询索引失败: %v", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)

	indexMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var indexName, columnName, indexType string
		var isUnique int

		if err := rows.Scan(&indexName, &columnName, &indexType, &isUnique); err != nil {
			return nil, fmt.Errorf("扫描索引失败: %v", err)
		}

		// 如果索引不存在，创建新索引
		if _, ok := indexMap[indexName]; !ok {
			indexMap[indexName] = &IndexInfo{
				IndexName: indexName,
				IndexType: indexType,
				IsUnique:  isUnique == 1,
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

// GenerateModelFile 生成模型文件
func (g *ClickHouseGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
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
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	// 执行模板
	if err := t.Execute(file, data); err != nil {
		return fmt.Errorf("执行模板失败: %v", err)
	}

	fmt.Printf("生成模型文件: %s\n", filePath)
	return nil
}

// MapClickHouseTypeToGo 将ClickHouse类型映射到Go类型
func (g *ClickHouseGenerator) MapClickHouseTypeToGo(clickhouseType string, isNullable bool) string {
	// 处理Nullable类型
	if strings.HasPrefix(clickhouseType, "Nullable(") {
		// 提取内部类型
		innerType := strings.TrimPrefix(clickhouseType, "Nullable(")
		innerType = strings.TrimSuffix(innerType, ")")
		// 递归调用，但强制为可空类型
		return g.MapClickHouseTypeToGo(innerType, true)
	}

	// 处理Array类型
	if strings.HasPrefix(clickhouseType, "Array(") {
		// 提取内部类型
		innerType := strings.TrimPrefix(clickhouseType, "Array(")
		innerType = strings.TrimSuffix(innerType, ")")
		// 递归调用获取元素类型
		elementType := g.MapClickHouseTypeToGo(innerType, false)
		return "[]" + elementType
	}

	// 处理基本类型
	lowerType := strings.ToLower(clickhouseType)
	switch {
	case strings.Contains(lowerType, "int8"), strings.Contains(lowerType, "int16"), strings.Contains(lowerType, "int32"):
		if isNullable {
			return "*int"
		}
		return "int"
	case strings.Contains(lowerType, "int64"), strings.Contains(lowerType, "uint64"):
		if isNullable {
			return "*int64"
		}
		return "int64"
	case strings.Contains(lowerType, "float32"), strings.Contains(lowerType, "float64"), strings.Contains(lowerType, "decimal"):
		if isNullable {
			return "*float64"
		}
		return "float64"
	case strings.Contains(lowerType, "string"), strings.Contains(lowerType, "fixedstring"):
		if isNullable {
			return "*string"
		}
		return "string"
	case strings.Contains(lowerType, "date"), strings.Contains(lowerType, "datetime"):
		if isNullable {
			return "*time.Time"
		}
		return "time.Time"
	case strings.Contains(lowerType, "bool"), strings.Contains(lowerType, "boolean"):
		if isNullable {
			return "*bool"
		}
		return "bool"
	case strings.Contains(lowerType, "uuid"):
		if isNullable {
			return "*string"
		}
		return "string"
	case strings.Contains(lowerType, "json"):
		return "[]byte"
	default:
		if isNullable {
			return "interface{}"
		}
		return "string"
	}
}

// GetGormDataType 获取GORM数据类型
func (g *ClickHouseGenerator) GetGormDataType(dataType string, columnType string) string {
	// 对于ClickHouse，我们使用原始类型
	return columnType
}

// ExtractDefaultValue 提取默认值
func (g *ClickHouseGenerator) ExtractDefaultValue(extra string) string {
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
func (g *ClickHouseGenerator) ToCamelCase(s string) string {
	// 处理下划线分隔的命名
	parts := strings.Split(s, "_")
	for i := range parts {
		// 这里我们检查Config中是否有FirstLetterUpper字段
		// 如果没有，我们默认首字母大写
		firstLetterUpper := true
		if i == 0 && g.Config != nil {
			// 假设Config结构体中有一个FirstLetterUpper字段
			// 如果没有这个字段，我们需要修改Config结构体
			// 这里我们假设它存在，如果不存在，编译会失败
			firstLetterUpper = g.Config.FirstLetterUpper // 默认为true
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
func (g *ClickHouseGenerator) GenerateModelFiles(tableInfos []*TableInfo, outputDir string) error {
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
			if strings.Contains(col.GoType, "json.RawMessage") {
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

		err = file.Close()
		if err != nil {
			return err
		}
		fmt.Printf("生成模型文件: %s\n", filePath)
	}

	return nil
}

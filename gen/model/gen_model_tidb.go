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

// TiDBGenerator TiDB表结构生成器
type TiDBGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewTiDBGenerator 创建TiDB表结构生成器
func NewTiDBGenerator(config *Config) (*TiDBGenerator, error) {
	if config.DBType != "tidb" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	// TiDB 使用 MySQL 协议，因此连接字符串与 MySQL 相同
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.Username, config.Password, config.Host, config.Port, config.DatabaseName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return &TiDBGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *TiDBGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *TiDBGenerator) Generate() error {
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
func (g *TiDBGenerator) GetAllTables() ([]string, error) {
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
func (g *TiDBGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
	// 获取表注释
	var tableComment string
	query := `
		SELECT table_comment 
		FROM information_schema.tables 
		WHERE table_schema = ? AND table_name = ?
	`
	err := g.DB.QueryRow(query, g.Config.DatabaseName, tableName).Scan(&tableComment)
	if err != nil {
		return nil, fmt.Errorf("获取表注释失败: %v", err)
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
func (g *TiDBGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
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
		return nil, fmt.Errorf("查询列信息失败: %v", err)
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
		col.GoType = g.MapTiDBTypeToGo(col.DataType, col.IsNullable == "YES")
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
		if strings.Contains(strings.ToLower(col.Extra), "default") ||
			strings.Contains(col.Extra, "DEFAULT") {
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
func (g *TiDBGenerator) ExtractDefaultValue(extra string) string {
	if !strings.Contains(strings.ToLower(extra), "default") {
		return ""
	}

	// 尝试匹配 DEFAULT xxx 格式
	parts := strings.SplitN(extra, "DEFAULT ", 2)
	if len(parts) < 2 {
		// 尝试匹配小写的 default xxx 格式
		parts = strings.SplitN(extra, "default ", 2)
		if len(parts) < 2 {
			return ""
		}
	}

	defaultValue := strings.TrimSpace(parts[1])
	// 如果有其他额外信息，只取第一部分
	if idx := strings.Index(defaultValue, " "); idx > 0 {
		defaultValue = defaultValue[:idx]
	}

	return defaultValue
}

// GetPrimaryKeys 获取主键
func (g *TiDBGenerator) GetPrimaryKeys(tableName string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position
	`

	rows, err := g.DB.Query(query, g.Config.DatabaseName, tableName)
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
func (g *TiDBGenerator) GetIndexes(tableName string) ([]IndexInfo, error) {
	// TiDB 支持 SHOW INDEX 命令
	query := `SHOW INDEX FROM ` + tableName

	rows, err := g.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询索引失败: %v", err)
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var tableName, indexName, columnName, indexType string
		var nonUnique int
		var seqInIndex, cardinality, subPart, packed, nullable, indexComment interface{}

		// SHOW INDEX 返回的列顺序：
		// Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality,
		// Sub_part, Packed, Null, Index_type, Comment, Index_comment, Visible, Expression
		if err := rows.Scan(
			&tableName, &nonUnique, &indexName, &seqInIndex, &columnName,
			&nullable, &indexType, &indexComment, &cardinality, &subPart, &packed,
		); err != nil {
			// 如果列数不匹配，尝试更简单的扫描方式
			rows.Close()
			return g.GetIndexesAlternative(tableName)
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

	// 转换为切片
	var indexes []IndexInfo
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// GetIndexesAlternative 获取索引的替代方法
func (g *TiDBGenerator) GetIndexesAlternative(tableName string) ([]IndexInfo, error) {
	// 使用 information_schema 查询索引
	query := `
		SELECT 
			index_name, 
			NOT non_unique AS is_unique, 
			index_type,
			column_name
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND index_name != 'PRIMARY'
		ORDER BY index_name, seq_in_index
	`

	rows, err := g.DB.Query(query, g.Config.DatabaseName, tableName)
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

// MapTiDBTypeToGo 将TiDB类型映射到Go类型
func (g *TiDBGenerator) MapTiDBTypeToGo(tidbType string, isNullable bool) string {
	// TiDB 与 MySQL 兼容，可以使用相同的类型映射
	switch strings.ToLower(tidbType) {
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

// ToCamelCase 转换为驼峰命名
func (g *TiDBGenerator) ToCamelCase(s string) string {
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
func (g *TiDBGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
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

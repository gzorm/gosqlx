package model

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	_ "gorm.io/driver/postgres"
)

// PostgresGenerator PostgreSQL表结构生成器
type PostgresGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewPostgresGenerator 创建PostgreSQL表结构生成器
func NewPostgresGenerator(config *Config) (*PostgresGenerator, error) {
	if config.DBType != "postgres" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.Username, config.Password, config.DatabaseName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return &PostgresGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *PostgresGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *PostgresGenerator) Generate() error {
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
func (g *PostgresGenerator) GetAllTables() ([]string, error) {
	// 查询当前schema下的所有表（排除系统表）
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
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
func (g *PostgresGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
	// 获取表注释
	var tableComment sql.NullString
	query := `
		SELECT obj_description(c.oid) 
		FROM pg_class c 
		JOIN pg_namespace n ON n.oid = c.relnamespace 
		WHERE c.relname = $1 
		AND n.nspname = 'public'
	`
	err := g.DB.QueryRow(query, tableName).Scan(&tableComment)
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
		TableComment: tableComment.String,
		Columns:      columns,
		PrimaryKeys:  primaryKeys,
		Indexes:      indexes,
		ModelName:    modelName,
	}, nil
}

// GetColumnInfo 获取列信息
func (g *PostgresGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
	// 查询列信息
	query := `
		SELECT 
			c.column_name, 
			c.data_type, 
			c.udt_name, 
			c.is_nullable, 
			c.column_default,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			pg_catalog.col_description(format('%s.%s', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position) as column_comment
		FROM 
			information_schema.columns c
		WHERE 
			c.table_schema = 'public' 
			AND c.table_name = $1
		ORDER BY 
			c.ordinal_position
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询列信息失败: %v", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var dataType, udtName string
		var charMaxLength, numPrecision, numScale sql.NullInt64
		var columnDefault, columnComment sql.NullString

		if err := rows.Scan(
			&col.ColumnName,
			&dataType,
			&udtName,
			&col.IsNullable,
			&columnDefault,
			&charMaxLength,
			&numPrecision,
			&numScale,
			&columnComment,
		); err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		// 设置数据类型
		col.DataType = dataType
		col.ColumnType = g.FormatColumnType(dataType, udtName, charMaxLength, numPrecision, numScale)
		col.ColumnComment = columnComment.String

		// 设置键类型（在PostgreSQL中需要单独查询）
		col.ColumnKey = ""

		// 设置额外信息
		if columnDefault.Valid && strings.Contains(columnDefault.String, "nextval") {
			col.Extra = "auto_increment"
		} else if columnDefault.Valid {
			col.Extra = fmt.Sprintf("DEFAULT %s", columnDefault.String)
		} else {
			col.Extra = ""
		}

		// 设置Go相关字段
		col.FieldName = g.ToCamelCase(col.ColumnName)
		col.GoType = g.MapPostgresTypeToGo(dataType, udtName, col.IsNullable == "YES")
		col.JsonTag = col.ColumnName

		// 生成GORM标签
		gormTag := fmt.Sprintf("column:%s;", col.ColumnName)

		// 添加类型信息
		gormTag += fmt.Sprintf("type:%s;", col.ColumnType)

		// 添加是否为空
		if col.IsNullable == "NO" {
			gormTag += "not null;"
		}

		// 添加默认值
		if columnDefault.Valid {
			// 处理序列默认值
			if strings.Contains(columnDefault.String, "nextval") {
				gormTag += "autoIncrement;"
			} else {
				defaultValue := columnDefault.String
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
		if columnComment.Valid && columnComment.String != "" {
			gormTag += fmt.Sprintf("comment:'%s';", strings.Replace(columnComment.String, "'", "\\'", -1))
		}

		col.GormTag = gormTag

		columns = append(columns, col)
	}

	// 设置主键信息
	primaryKeys, err := g.GetPrimaryKeys(tableName)
	if err != nil {
		return nil, err
	}

	// 标记主键列
	for i := range columns {
		for _, pk := range primaryKeys {
			if columns[i].ColumnName == pk {
				columns[i].ColumnKey = "PRI"
				// 添加主键标记到GORM标签
				columns[i].GormTag += "primaryKey;"
				break
			}
		}
	}

	return columns, nil
}

// FormatColumnType 格式化列类型
func (g *PostgresGenerator) FormatColumnType(dataType, udtName string, charMaxLength, numPrecision, numScale sql.NullInt64) string {
	switch dataType {
	case "character varying", "varchar":
		if charMaxLength.Valid {
			return fmt.Sprintf("%s(%d)", dataType, charMaxLength.Int64)
		}
		return dataType
	case "numeric", "decimal":
		if numPrecision.Valid && numScale.Valid {
			return fmt.Sprintf("%s(%d,%d)", dataType, numPrecision.Int64, numScale.Int64)
		}
		return dataType
	case "USER-DEFINED":
		return udtName
	default:
		return dataType
	}
}

// GetPrimaryKeys 获取主键
func (g *PostgresGenerator) GetPrimaryKeys(tableName string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		AND i.indisprimary
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
func (g *PostgresGenerator) GetIndexes(tableName string) ([]IndexInfo, error) {
	query := `
		SELECT
			i.relname as index_name,
			ix.indisunique as is_unique,
			a.amname as index_type,
			array_to_string(array_agg(attr.attname order by array_position(ix.indkey, attr.attnum)), ',') as column_names
		FROM
			pg_class t,
			pg_class i,
			pg_index ix,
			pg_attribute attr,
			pg_am a
		WHERE
			t.oid = ix.indrelid
			AND i.oid = ix.indexrelid
			AND a.oid = i.relam
			AND attr.attrelid = t.oid
			AND attr.attnum = ANY(ix.indkey)
			AND t.relkind = 'r'
			AND t.relname = $1
		GROUP BY
			i.relname,
			ix.indisunique,
			a.amname
		ORDER BY
			i.relname
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询索引失败: %v", err)
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var idx IndexInfo
		var columnNamesStr string

		if err := rows.Scan(&idx.IndexName, &idx.IsUnique, &idx.IndexType, &columnNamesStr); err != nil {
			return nil, fmt.Errorf("扫描索引失败: %v", err)
		}

		// 跳过主键索引，因为已经单独处理
		if strings.HasSuffix(idx.IndexName, "_pkey") {
			continue
		}

		// 解析列名
		idx.ColumnNames = strings.Split(columnNamesStr, ",")
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

// MapPostgresTypeToGo 将PostgreSQL类型映射到Go类型
func (g *PostgresGenerator) MapPostgresTypeToGo(dataType, udtName string, isNullable bool) string {
	switch strings.ToLower(dataType) {
	case "smallint", "integer", "int", "int4":
		if isNullable {
			return "*int"
		}
		return "int"
	case "bigint", "int8":
		if isNullable {
			return "*int64"
		}
		return "int64"
	case "real", "float4":
		if isNullable {
			return "*float32"
		}
		return "float32"
	case "double precision", "float8":
		if isNullable {
			return "*float64"
		}
		return "float64"
	case "numeric", "decimal":
		if isNullable {
			return "*float64"
		}
		return "float64"
	case "character", "character varying", "varchar", "text":
		if isNullable {
			return "*string"
		}
		return "string"
	case "boolean", "bool":
		if isNullable {
			return "*bool"
		}
		return "bool"
	case "date", "timestamp", "timestamp without time zone", "timestamp with time zone":
		if isNullable {
			return "*time.Time"
		}
		return "time.Time"
	case "bytea":
		return "[]byte"
	case "json", "jsonb":
		return "json.RawMessage"
	case "uuid":
		return "string"
	case "user-defined":
		// 处理自定义类型，通常是枚举
		if strings.HasPrefix(udtName, "_") {
			// 数组类型
			return "[]string"
		}
		return "string"
	default:
		if isNullable {
			return "interface{}"
		}
		return "string"
	}
}

// ToCamelCase 转换为驼峰命名
func (g *PostgresGenerator) ToCamelCase(s string) string {
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
func (g *PostgresGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
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

package model

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	_ "github.com/seelly/gorm-oracle"
)

// OracleGenerator Oracle表结构生成器
type OracleGenerator struct {
	Config *Config
	DB     *sql.DB
}

// NewOracleGenerator 创建Oracle表结构生成器
func NewOracleGenerator(config *Config) (*OracleGenerator, error) {
	if config.DBType != "oracle" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	// 构建连接字符串
	dsn := fmt.Sprintf("%s/%s@%s:%d/%s",
		config.Username, config.Password, config.Host, config.Port, config.DatabaseName)

	db, err := sql.Open("godror", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("测试数据库连接失败: %v", err)
	}

	return &OracleGenerator{
		Config: config,
		DB:     db,
	}, nil
}

// Close 关闭数据库连接
func (g *OracleGenerator) Close() error {
	if g.DB != nil {
		return g.DB.Close()
	}
	return nil
}

// Generate 生成所有表的模型
func (g *OracleGenerator) Generate() error {
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
func (g *OracleGenerator) GetAllTables() ([]string, error) {
	// 查询用户表（排除系统表）
	query := `
		SELECT TABLE_NAME 
		FROM USER_TABLES 
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
func (g *OracleGenerator) GetTableInfo(tableName string) (*TableInfo, error) {
	// 获取表注释
	var tableComment string
	query := `
		SELECT COMMENTS 
		FROM USER_TAB_COMMENTS 
		WHERE TABLE_NAME = :1
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
	modelName := g.ToCamelCase(strings.ToLower(tableName))

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
func (g *OracleGenerator) GetColumnInfo(tableName string) ([]ColumnInfo, error) {
	// 查询列信息
	query := `
		SELECT 
			c.COLUMN_NAME,
			c.DATA_TYPE,
			CASE 
				WHEN c.DATA_TYPE LIKE '%CHAR%' THEN c.DATA_TYPE || '(' || c.CHAR_LENGTH || ')'
				WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NOT NULL AND c.DATA_SCALE IS NOT NULL THEN 
					c.DATA_TYPE || '(' || c.DATA_PRECISION || ',' || c.DATA_SCALE || ')'
				WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NOT NULL THEN 
					c.DATA_TYPE || '(' || c.DATA_PRECISION || ')'
				ELSE c.DATA_TYPE
			END AS COLUMN_TYPE,
			c.NULLABLE,
			CASE WHEN p.COLUMN_NAME IS NOT NULL THEN 'PRI' ELSE '' END AS COLUMN_KEY,
			NVL(cc.COMMENTS, '') AS COLUMN_COMMENT,
			CASE WHEN c.DATA_DEFAULT IS NOT NULL THEN 'DEFAULT ' || c.DATA_DEFAULT ELSE '' END AS EXTRA
		FROM 
			USER_TAB_COLUMNS c
		LEFT JOIN (
			SELECT 
				cols.COLUMN_NAME
			FROM 
				USER_CONSTRAINTS cons, 
				USER_CONS_COLUMNS cols
			WHERE 
				cons.CONSTRAINT_TYPE = 'P'
				AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
				AND cons.TABLE_NAME = :1
		) p ON c.COLUMN_NAME = p.COLUMN_NAME
		LEFT JOIN 
			USER_COL_COMMENTS cc ON c.TABLE_NAME = cc.TABLE_NAME AND c.COLUMN_NAME = cc.COLUMN_NAME
		WHERE 
			c.TABLE_NAME = :1
		ORDER BY 
			c.COLUMN_ID
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询列信息失败: %v", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string

		if err := rows.Scan(
			&col.ColumnName,
			&col.DataType,
			&col.ColumnType,
			&nullable,
			&col.ColumnKey,
			&col.ColumnComment,
			&col.Extra,
		); err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %v", err)
		}

		// 设置是否可为空
		col.IsNullable = "NO"
		if nullable == "Y" {
			col.IsNullable = "YES"
		}

		// 设置Go相关字段
		col.FieldName = g.ToCamelCase(strings.ToLower(col.ColumnName))
		col.GoType = g.MapOracleTypeToGo(col.DataType, col.IsNullable == "YES")
		col.JsonTag = strings.ToLower(col.ColumnName)

		// 生成GORM标签
		gormTag := fmt.Sprintf("column:%s;", strings.ToLower(col.ColumnName))

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

		// 添加自增信息（Oracle使用序列实现自增）
		if strings.Contains(strings.ToUpper(col.Extra), "NEXTVAL") {
			gormTag += "autoIncrement;"
		}

		// 添加默认值
		if strings.Contains(col.Extra, "DEFAULT") {
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
func (g *OracleGenerator) ExtractDefaultValue(extra string) string {
	if !strings.Contains(extra, "DEFAULT") {
		return ""
	}

	parts := strings.SplitN(extra, "DEFAULT ", 2)
	if len(parts) < 2 {
		return ""
	}

	defaultValue := strings.TrimSpace(parts[1])
	return defaultValue
}

// GetPrimaryKeys 获取主键
func (g *OracleGenerator) GetPrimaryKeys(tableName string) ([]string, error) {
	query := `
		SELECT 
			cols.COLUMN_NAME
		FROM 
			USER_CONSTRAINTS cons, 
			USER_CONS_COLUMNS cols
		WHERE 
			cons.CONSTRAINT_TYPE = 'P'
			AND cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
			AND cons.TABLE_NAME = :1
		ORDER BY 
			cols.POSITION
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
func (g *OracleGenerator) GetIndexes(tableName string) ([]IndexInfo, error) {
	query := `
		SELECT 
			i.INDEX_NAME,
			i.UNIQUENESS,
			i.INDEX_TYPE,
			c.COLUMN_NAME
		FROM 
			USER_INDEXES i
		JOIN 
			USER_IND_COLUMNS c ON i.INDEX_NAME = c.INDEX_NAME
		WHERE 
			i.TABLE_NAME = :1
			AND i.INDEX_NAME NOT IN (
				SELECT CONSTRAINT_NAME 
				FROM USER_CONSTRAINTS 
				WHERE CONSTRAINT_TYPE = 'P' AND TABLE_NAME = :1
			)
		ORDER BY 
			i.INDEX_NAME, c.COLUMN_POSITION
	`

	rows, err := g.DB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("查询索引失败: %v", err)
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var indexName, uniqueness, indexType, columnName string

		if err := rows.Scan(&indexName, &uniqueness, &indexType, &columnName); err != nil {
			return nil, fmt.Errorf("扫描索引失败: %v", err)
		}

		// 如果索引不存在，创建新索引
		if _, ok := indexMap[indexName]; !ok {
			indexMap[indexName] = &IndexInfo{
				IndexName: indexName,
				IndexType: indexType,
				IsUnique:  uniqueness == "UNIQUE",
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

// MapOracleTypeToGo 将Oracle类型映射到Go类型
func (g *OracleGenerator) MapOracleTypeToGo(oracleType string, isNullable bool) string {
	switch strings.ToUpper(oracleType) {
	case "NUMBER":
		if isNullable {
			return "*float64"
		}
		return "float64"
	case "INTEGER", "INT":
		if isNullable {
			return "*int"
		}
		return "int"
	case "SMALLINT":
		if isNullable {
			return "*int16"
		}
		return "int16"
	case "FLOAT", "BINARY_FLOAT":
		if isNullable {
			return "*float32"
		}
		return "float32"
	case "DOUBLE PRECISION", "BINARY_DOUBLE":
		if isNullable {
			return "*float64"
		}
		return "float64"
	case "VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG":
		if isNullable {
			return "*string"
		}
		return "string"
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		if isNullable {
			return "*time.Time"
		}
		return "time.Time"
	case "BLOB", "RAW", "LONG RAW":
		return "[]byte"
	case "BOOLEAN":
		if isNullable {
			return "*bool"
		}
		return "bool"
	default:
		if isNullable {
			return "interface{}"
		}
		return "string"
	}
}

// ToCamelCase 转换为驼峰命名
func (g *OracleGenerator) ToCamelCase(s string) string {
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
func (g *OracleGenerator) GenerateModelFile(tableInfos []*TableInfo, outputDir string) error {
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

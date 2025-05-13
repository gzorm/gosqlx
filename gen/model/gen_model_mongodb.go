package model

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoDBGenerator MongoDB集合结构生成器
type MongoDBGenerator struct {
	Config *Config
	Client *mongo.Client
}

// MongoDBField MongoDB字段信息
type MongoDBField struct {
	FieldName string // 字段名
	GoType    string // Go类型
	BsonTag   string // BSON标签
	JsonTag   string // JSON标签
	GormTag   string // GORM标签
	OmitEmpty bool   // 是否忽略空值
}

// MongoDBCollectionInfo MongoDB集合信息
type MongoDBCollectionInfo struct {
	CollectionName string         // 集合名
	ModelName      string         // 模型名称（驼峰命名）
	Fields         []MongoDBField // 字段信息
	Indexes        []MongoDBIndex // 索引信息
}

// MongoDBIndex MongoDB索引信息
type MongoDBIndex struct {
	Name       string         // 索引名称
	Keys       map[string]int // 索引键
	Unique     bool           // 是否唯一索引
	Background bool           // 是否后台创建
	Sparse     bool           // 是否稀疏索引
}

// NewMongoDBGenerator 创建MongoDB集合结构生成器
func NewMongoDBGenerator(config *Config) (*MongoDBGenerator, error) {
	if config.DBType != "mongodb" {
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	// 构建连接URI
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d/%s",
		config.Username, config.Password, config.Host, config.Port, config.DatabaseName)

	// 如果没有用户名和密码，则使用简单URI
	if config.Username == "" || config.Password == "" {
		uri = fmt.Sprintf("mongodb://%s:%d/%s", config.Host, config.Port, config.DatabaseName)
	}

	// 创建并连接到MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("连接MongoDB失败: %v", err)
	}

	// 测试连接
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, fmt.Errorf("测试MongoDB连接失败: %v", err)
	}

	return &MongoDBGenerator{
		Config: config,
		Client: client,
	}, nil
}

// Close 关闭数据库连接
func (g *MongoDBGenerator) Close() error {
	if g.Client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return g.Client.Disconnect(ctx)
	}
	return nil
}

// Generate 生成所有集合的模型
func (g *MongoDBGenerator) Generate() error {
	// 获取所有集合名
	collections, err := g.GetAllCollections()
	if err != nil {
		return err
	}

	// 确保输出目录存在
	outputDir := filepath.Join(g.Config.OutputDir, "poes")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("创建输出目录失败: %v", err)
	}

	// 收集所有集合信息
	var collectionInfos []*MongoDBCollectionInfo
	for _, collectionName := range collections {
		collectionInfo, err := g.GetCollectionInfo(collectionName)
		if err != nil {
			return err
		}
		collectionInfos = append(collectionInfos, collectionInfo)
	}

	// 生成单个模型文件
	if err := g.GenerateModelFile(collectionInfos, outputDir); err != nil {
		return err
	}

	return nil
}

// GetAllCollections 获取所有集合名
func (g *MongoDBGenerator) GetAllCollections() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := g.Client.Database(g.Config.DatabaseName)
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("获取集合列表失败: %v", err)
	}

	return collections, nil
}

// GetCollectionInfo 获取集合信息
func (g *MongoDBGenerator) GetCollectionInfo(collectionName string) (*MongoDBCollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := g.Client.Database(g.Config.DatabaseName)
	collection := db.Collection(collectionName)

	// 获取集合中的文档样本
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetLimit(100))
	if err != nil {
		return nil, fmt.Errorf("查询集合文档失败: %v", err)
	}
	defer cursor.Close(ctx)

	// 分析文档结构
	fieldTypes := make(map[string]string)
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		// 分析文档字段
		g.analyzeDocument(doc, "", fieldTypes)
	}

	// 获取索引信息
	indexes, err := g.GetCollectionIndexes(collectionName)
	if err != nil {
		return nil, err
	}

	// 生成字段列表
	var fields []MongoDBField
	for fieldPath, fieldType := range fieldTypes {
		// 生成GORM标签
		gormTag := fmt.Sprintf("column:%s;", fieldPath)

		// 添加类型信息
		gormType := g.MapMongoTypeToGormType(fieldType)
		gormTag += fmt.Sprintf("type:%s;", gormType)

		// 添加是否为空
		gormTag += "omitempty;"

		// 检查是否为主键 (_id 字段)
		if fieldPath == "_id" {
			gormTag += "primaryKey;"
		}

		// 添加注释
		gormTag += fmt.Sprintf("comment:'%s字段';", fieldPath)

		field := MongoDBField{
			FieldName: g.fieldPathToFieldName(fieldPath),
			GoType:    fieldType,
			BsonTag:   fieldPath,
			JsonTag:   fieldPath,
			GormTag:   gormTag,
			OmitEmpty: true, // 默认忽略空值
		}
		fields = append(fields, field)
	}

	// 生成模型名称（集合名转为驼峰命名）
	modelName := g.ToCamelCase(collectionName)

	return &MongoDBCollectionInfo{
		CollectionName: collectionName,
		ModelName:      modelName,
		Fields:         fields,
		Indexes:        indexes,
	}, nil
}

// MapMongoTypeToGormType 将MongoDB类型映射到GORM类型
func (g *MongoDBGenerator) MapMongoTypeToGormType(goType string) string {
	switch goType {
	case "int", "int32":
		return "int"
	case "int64":
		return "bigint"
	case "float32":
		return "float"
	case "float64":
		return "double"
	case "bool":
		return "boolean"
	case "string":
		return "varchar(255)"
	case "time.Time":
		return "datetime"
	case "[]byte":
		return "binary"
	case "primitive.ObjectID":
		return "varchar(24)"
	default:
		if strings.HasPrefix(goType, "[]") {
			return "json"
		}
		return "json"
	}
}

// analyzeDocument 分析文档结构
func (g *MongoDBGenerator) analyzeDocument(doc bson.M, prefix string, fieldTypes map[string]string) {
	for key, value := range doc {
		fieldPath := key
		if prefix != "" {
			fieldPath = prefix + "." + key
		}

		switch v := value.(type) {
		case nil:
			fieldTypes[fieldPath] = "interface{}"
		case bool:
			fieldTypes[fieldPath] = "bool"
		case int, int32:
			fieldTypes[fieldPath] = "int"
		case int64:
			fieldTypes[fieldPath] = "int64"
		case float64:
			fieldTypes[fieldPath] = "float64"
		case string:
			fieldTypes[fieldPath] = "string"
		case time.Time:
			fieldTypes[fieldPath] = "time.Time"
		case bson.M:
			// 嵌套文档
			subType := g.ToCamelCase(key)
			fieldTypes[fieldPath] = subType
			g.analyzeDocument(v, fieldPath, fieldTypes)
		case []interface{}:
			// 数组
			if len(v) > 0 {
				switch elem := v[0].(type) {
				case bson.M:
					// 对象数组
					subType := g.ToCamelCase(key)
					fieldTypes[fieldPath] = "[]" + subType
					g.analyzeDocument(elem, fieldPath+".0", fieldTypes)
				case bool:
					fieldTypes[fieldPath] = "[]bool"
				case int, int32:
					fieldTypes[fieldPath] = "[]int"
				case int64:
					fieldTypes[fieldPath] = "[]int64"
				case float64:
					fieldTypes[fieldPath] = "[]float64"
				case string:
					fieldTypes[fieldPath] = "[]string"
				case time.Time:
					fieldTypes[fieldPath] = "[]time.Time"
				default:
					fieldTypes[fieldPath] = "[]interface{}"
				}
			} else {
				fieldTypes[fieldPath] = "[]interface{}"
			}
		default:
			fieldTypes[fieldPath] = "interface{}"
		}
	}
}

// fieldPathToFieldName 将字段路径转换为字段名
func (g *MongoDBGenerator) fieldPathToFieldName(fieldPath string) string {
	parts := strings.Split(fieldPath, ".")
	for i, part := range parts {
		parts[i] = g.ToCamelCase(part)
	}
	return strings.Join(parts, "")
}

// GetCollectionIndexes 获取集合索引
func (g *MongoDBGenerator) GetCollectionIndexes(collectionName string) ([]MongoDBIndex, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db := g.Client.Database(g.Config.DatabaseName)
	collection := db.Collection(collectionName)

	// 获取索引信息
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("获取索引信息失败: %v", err)
	}
	defer cursor.Close(ctx)

	var indexes []MongoDBIndex
	for cursor.Next(ctx) {
		var idx bson.M
		if err := cursor.Decode(&idx); err != nil {
			continue
		}

		// 解析索引信息
		name, _ := idx["name"].(string)
		keys, _ := idx["key"].(bson.M)
		unique, _ := idx["unique"].(bool)
		background, _ := idx["background"].(bool)
		sparse, _ := idx["sparse"].(bool)

		// 转换索引键
		indexKeys := make(map[string]int)
		for k, v := range keys {
			if val, ok := v.(int32); ok {
				indexKeys[k] = int(val)
			} else if val, ok := v.(float64); ok {
				indexKeys[k] = int(val)
			}
		}

		// 跳过默认的_id索引
		if name == "_id_" {
			continue
		}

		indexes = append(indexes, MongoDBIndex{
			Name:       name,
			Keys:       indexKeys,
			Unique:     unique,
			Background: background,
			Sparse:     sparse,
		})
	}

	return indexes, nil
}

// ToCamelCase 转换为驼峰命名
func (g *MongoDBGenerator) ToCamelCase(s string) string {
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
func (g *MongoDBGenerator) GenerateModelFile(collectionInfos []*MongoDBCollectionInfo, outputDir string) error {
	// 模板定义
	tmpl := `// 代码由 gosqlx 自动生成，请勿手动修改
// 生成时间: {{.GenerateTime}}
package {{.PackageName}}

import (
	"time"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

{{range .CollectionInfos}}
// {{.ModelName}} {{.CollectionName}}集合模型
type {{.ModelName}} struct {
{{- range .Fields}}
	{{.FieldName}} {{.GoType}} ` + "`bson:\"{{.BsonTag}},omitempty\" json:\"{{.JsonTag}},omitempty\" gorm:\"{{.GormTag}}\"`" + `
{{- end}}
}

// CollectionName 集合名
func (m *{{.ModelName}}) CollectionName() string {
	return "{{.CollectionName}}"
}

{{end}}
`

	// 准备模板数据
	data := struct {
		PackageName     string
		CollectionInfos []*MongoDBCollectionInfo
		GenerateTime    string
	}{
		PackageName:     g.Config.PackageName,
		CollectionInfos: collectionInfos,
		GenerateTime:    time.Now().Format("2006-01-02 15:04:05"),
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

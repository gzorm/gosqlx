package adapter

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gorm.io/gorm"
)

// MongoDB 适配器结构体
type MongoDB struct {
	// 基础配置
	URI         string        // 连接URI
	Database    string        // 数据库名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
	client      *mongo.Client // MongoDB客户端
}

// NewMongoDB 创建新的MongoDB适配器
func NewMongoDB(uri string, database string) *MongoDB {
	return &MongoDB{
		URI:         uri,
		Database:    database,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (m *MongoDB) WithMaxIdle(maxIdle int) *MongoDB {
	m.MaxIdle = maxIdle
	return m
}

// WithMaxOpen 设置最大打开连接数
func (m *MongoDB) WithMaxOpen(maxOpen int) *MongoDB {
	m.MaxOpen = maxOpen
	return m
}

// WithMaxLifetime 设置连接最大生命周期
func (m *MongoDB) WithMaxLifetime(maxLifetime time.Duration) *MongoDB {
	m.MaxLifetime = maxLifetime
	return m
}

// WithDebug 设置调试模式
func (m *MongoDB) WithDebug(debug bool) *MongoDB {
	m.Debug = debug
	return m
}

// Connect 连接数据库
// 注意：MongoDB适配器的Connect方法返回的gorm.DB和sql.DB为nil，因为MongoDB不使用这些接口
// 实际应用中应该使用GetClient方法获取MongoDB客户端
func (m *MongoDB) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建MongoDB客户端选项
	clientOptions := options.Client().ApplyURI(m.URI)

	// 设置连接池参数
	clientOptions.SetMaxPoolSize(uint64(m.MaxOpen))
	clientOptions.SetMinPoolSize(uint64(m.MaxIdle))
	clientOptions.SetMaxConnIdleTime(m.MaxLifetime)

	// 连接MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, nil, err
	}

	// 验证连接
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	m.client = client

	// MongoDB适配器不使用GORM，返回nil
	return nil, nil, nil
}

// GetClient 获取MongoDB客户端
func (m *MongoDB) GetClient() *mongo.Client {
	return m.client
}

// GetDatabase 获取MongoDB数据库
func (m *MongoDB) GetDatabase() *mongo.Database {
	if m.client == nil {
		return nil
	}
	return m.client.Database(m.Database)
}

// Close 关闭连接
func (m *MongoDB) Close() error {
	if m.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return m.client.Disconnect(ctx)
}

// ForUpdate 生成锁定语句（MongoDB不支持，返回空字符串）
func (m *MongoDB) ForUpdate() string {
	return ""
}

// ForShare 生成共享锁语句（MongoDB不支持，返回空字符串）
func (m *MongoDB) ForShare() string {
	return ""
}

// Limit 生成分页语句（MongoDB不使用SQL语法，返回空字符串）
func (m *MongoDB) Limit(offset, limit int) string {
	return ""
}

// BatchInsert 批量插入
// 注意：此方法为适配器接口实现，实际应用中应使用InsertMany方法
func (m *MongoDB) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	// MongoDB适配器不使用GORM，此方法仅为接口实现
	return fmt.Errorf("MongoDB适配器不支持通过GORM进行批量插入，请使用InsertMany方法")
}

// MergeInto 合并插入（UPSERT）
// 注意：此方法为适配器接口实现，实际应用中应使用UpdateMany方法
func (m *MongoDB) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	// MongoDB适配器不使用GORM，此方法仅为接口实现
	return fmt.Errorf("MongoDB适配器不支持通过GORM进行合并插入，请使用UpdateMany方法")
}

// InsertMany 批量插入文档
func (m *MongoDB) InsertMany(collection string, documents []interface{}) (*mongo.InsertManyResult, error) {
	if m.client == nil {
		return nil, fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	return coll.InsertMany(ctx, documents)
}

// UpdateMany 批量更新文档
func (m *MongoDB) UpdateMany(collection string, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	if m.client == nil {
		return nil, fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	return coll.UpdateMany(ctx, filter, update, opts...)
}

// FindOne 查询单个文档
func (m *MongoDB) FindOne(collection string, filter interface{}, result interface{}, opts ...*options.FindOneOptions) error {
	if m.client == nil {
		return fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	return coll.FindOne(ctx, filter, opts...).Decode(result)
}

// Find 查询多个文档
func (m *MongoDB) Find(collection string, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	if m.client == nil {
		return nil, fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	return coll.Find(ctx, filter, opts...)
}

// Aggregate 聚合查询
func (m *MongoDB) Aggregate(collection string, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	if m.client == nil {
		return nil, fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	return coll.Aggregate(ctx, pipeline, opts...)
}

// DeleteMany 批量删除文档
func (m *MongoDB) DeleteMany(collection string, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	if m.client == nil {
		return nil, fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	return coll.DeleteMany(ctx, filter, opts...)
}

// CreateCollection 创建集合
func (m *MongoDB) CreateCollection(name string, opts ...*options.CreateCollectionOptions) error {
	if m.client == nil {
		return fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return m.client.Database(m.Database).CreateCollection(ctx, name, opts...)
}

// ListCollections 列出所有集合
func (m *MongoDB) ListCollections(filter interface{}, opts ...*options.ListCollectionsOptions) ([]string, error) {
	if m.client == nil {
		return nil, fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if filter == nil {
		filter = bson.D{}
	}

	cursor, err := m.client.Database(m.Database).ListCollections(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var collections []string
	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		if name, ok := result["name"].(string); ok {
			collections = append(collections, name)
		}
	}

	return collections, nil
}

// DropCollection 删除集合
func (m *MongoDB) DropCollection(name string) error {
	if m.client == nil {
		return fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return m.client.Database(m.Database).Collection(name).Drop(ctx)
}

// CreateIndex 创建索引
func (m *MongoDB) CreateIndex(collection string, keys interface{}, opts ...*options.IndexOptions) (string, error) {
	if m.client == nil {
		return "", fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	indexModel := mongo.IndexModel{
		Keys:    keys,
		Options: opts[0],
	}

	return coll.Indexes().CreateOne(ctx, indexModel)
}

// ListIndexes 列出集合的所有索引
func (m *MongoDB) ListIndexes(collection string) ([]bson.M, error) {
	if m.client == nil {
		return nil, fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	if err = cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}

	return indexes, nil
}

// DropIndex 删除索引
func (m *MongoDB) DropIndex(collection string, indexName string) error {
	if m.client == nil {
		return fmt.Errorf("MongoDB客户端未初始化")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	coll := m.client.Database(m.Database).Collection(collection)
	_, err := coll.Indexes().DropOne(ctx, indexName)
	return err
}

// BuildURI 构建MongoDB连接URI
func (m *MongoDB) BuildURI(host string, port int, username, password, database string, params map[string]string) string {
	// 基本URI
	var uri string
	if username != "" && password != "" {
		uri = fmt.Sprintf("mongodb://%s:%s@%s:%d", username, password, host, port)
	} else {
		uri = fmt.Sprintf("mongodb://%s:%d", host, port)
	}

	// 添加数据库名称
	if database != "" {
		uri = uri + "/" + database
	}

	// 添加参数
	if len(params) > 0 {
		var parameters []string
		for k, v := range params {
			parameters = append(parameters, fmt.Sprintf("%s=%s", k, v))
		}
		uri = uri + "?" + strings.Join(parameters, "&")
	}

	return uri
}

// QueryPage 分页查询
func (m *MongoDB) QueryPage(out interface{}, page, pageSize int, filter interface{}, opts ...interface{}) (int64, error) {
	if m.client == nil {
		return 0, fmt.Errorf("MongoDB客户端未初始化")
	}

	// 参数验证
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}

	// 计算偏移量
	skip := (page - 1) * pageSize

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 解析集合名称
	// 这里假设第一个参数是集合名称，可能需要根据实际情况调整
	collection := ""
	if len(opts) > 0 {
		if collName, ok := opts[0].(string); ok {
			collection = collName
		}
	}

	if collection == "" {
		// 尝试从 out 参数推断集合名称
		outType := reflect.TypeOf(out)
		if outType.Kind() == reflect.Ptr {
			outType = outType.Elem()
		}
		collection = strings.ToLower(outType.Name())
	}

	coll := m.client.Database(m.Database).Collection(collection)

	// 查询总记录数
	total, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("查询总记录数失败: %w", err)
	}

	// 如果没有记录，直接返回
	if total == 0 {
		return 0, nil
	}

	// 查询分页数据
	findOptions := options.Find().SetSkip(int64(skip)).SetLimit(int64(pageSize))

	// 添加排序条件（如果有）
	if len(opts) > 1 {
		if sort, ok := opts[1].(bson.D); ok {
			findOptions.SetSort(sort)
		}
	}

	cursor, err := coll.Find(ctx, filter, findOptions)
	if err != nil {
		return 0, fmt.Errorf("查询分页数据失败: %w", err)
	}
	defer cursor.Close(ctx)

	// 解码结果到输出参数
	err = cursor.All(ctx, out)
	if err != nil {
		return 0, fmt.Errorf("解码查询结果失败: %w", err)
	}

	return total, nil
}

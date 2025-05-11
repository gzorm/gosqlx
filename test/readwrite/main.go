package main

import (
	"fmt"
	"time"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/query"

	"log"
)

func initMySQLConfig() gosqlx.ConfigMap {
	// 创建配置映射
	configs := gosqlx.ConfigMap{
		"production": {
			"main": &gosqlx.Config{
				Type:        gosqlx.MySQL,
				Driver:      "mysql",
				Source:      "user:password@tcp(master.mysql.example.com:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local",
				MaxIdle:     10,
				MaxOpen:     100,
				MaxLifetime: 3600,
				Debug:       false,
			},
			"main_readonly": &gosqlx.Config{
				Type:        gosqlx.MySQL,
				Driver:      "mysql",
				Source:      "readonly_user:password@tcp(slave.mysql.example.com:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local",
				MaxIdle:     20,
				MaxOpen:     200,
				MaxLifetime: 3600,
				Debug:       false,
			},
		},
	}

	return configs
}
func initPostgresConfig() gosqlx.ConfigMap {
	configs := gosqlx.ConfigMap{
		"production": {
			"main": &gosqlx.Config{
				Type:        gosqlx.PostgreSQL,
				Driver:      "postgres",
				Source:      "host=master.postgres.example.com port=5432 user=postgres password=password dbname=mydb sslmode=disable",
				MaxIdle:     10,
				MaxOpen:     100,
				MaxLifetime: 3600,
				Debug:       false,
			},
			"main_readonly": &gosqlx.Config{
				Type:        gosqlx.PostgreSQL,
				Driver:      "postgres",
				Source:      "host=slave.postgres.example.com port=5432 user=postgres_readonly password=password dbname=mydb sslmode=disable",
				MaxIdle:     20,
				MaxOpen:     200,
				MaxLifetime: 3600,
				Debug:       false,
			},
		},
	}

	return configs
}
func initOracleConfig() gosqlx.ConfigMap {
	configs := gosqlx.ConfigMap{
		"production": {
			"main": &gosqlx.Config{
				Type:        gosqlx.Oracle,
				Driver:      "oracle",
				Source:      "oracle://user:password@master.oracle.example.com:1521/service_name",
				MaxIdle:     10,
				MaxOpen:     100,
				MaxLifetime: 3600,
				Debug:       false,
			},
			"main_readonly": &gosqlx.Config{
				Type:        gosqlx.Oracle,
				Driver:      "oracle",
				Source:      "oracle://readonly_user:password@slave.oracle.example.com:1521/service_name",
				MaxIdle:     20,
				MaxOpen:     200,
				MaxLifetime: 3600,
				Debug:       false,
			},
		},
	}

	return configs
}
func initSQLServerConfig() gosqlx.ConfigMap {
	configs := gosqlx.ConfigMap{
		"production": {
			"main": &gosqlx.Config{
				Type:        gosqlx.SQLServer,
				Driver:      "sqlserver",
				Source:      "sqlserver://user:password@master.sqlserver.example.com:1433?database=mydb",
				MaxIdle:     10,
				MaxOpen:     100,
				MaxLifetime: 3600,
				Debug:       false,
			},
			"main_readonly": &gosqlx.Config{
				Type:        gosqlx.SQLServer,
				Driver:      "sqlserver",
				Source:      "sqlserver://readonly_user:password@slave.sqlserver.example.com:1433?database=mydb",
				MaxIdle:     20,
				MaxOpen:     200,
				MaxLifetime: 3600,
				Debug:       false,
			},
		},
	}

	return configs
}
func initSQLiteConfig() gosqlx.ConfigMap {
	configs := gosqlx.ConfigMap{
		"development": {
			"main": &gosqlx.Config{
				Type:        "sqlite",
				Driver:      "sqlite3",
				Source:      "file:mydb.sqlite?cache=shared&mode=rwc",
				MaxIdle:     5,
				MaxOpen:     10,
				MaxLifetime: 3600,
				Debug:       true,
			},
		},
	}

	return configs
}
func initMongoDBConfig() gosqlx.ConfigMap {
	configs := gosqlx.ConfigMap{
		"production": {
			"main": &gosqlx.Config{
				Type:        gosqlx.MongoDB,
				Driver:      "mongodb",
				Source:      "mongodb://user:password@master.mongodb.example.com:27017/dbname",
				MaxIdle:     10,
				MaxOpen:     100,
				MaxLifetime: 3600,
				Debug:       false,
			},
			"main_readonly": &gosqlx.Config{
				Type:        gosqlx.MongoDB,
				Driver:      "mongodb",
				Source:      "mongodb://readonly_user:password@slave.mongodb.example.com:27017/dbname",
				MaxIdle:     20,
				MaxOpen:     200,
				MaxLifetime: 3600,
				Debug:       false,
			},
		},
	}

	return configs
}
func main() {
	// 创建配置提供者
	configs := initMySQLConfig() // 使用上面定义的配置函数
	provider := gosqlx.NewConfigProvider(configs)

	// 创建数据库管理器
	configManager := gosqlx.NewConfigManager(provider)

	//
	manager := gosqlx.NewDatabaseManager(configManager)

	//if err != nil {
	//	log.Fatalf("初始化数据库管理器失败: %v", err)
	//}

	// 使用读写分离功能
	useReadWriteSeparation(manager)
}
func useReadWriteSeparation(manager *gosqlx.DatabaseManager) {

	// 创建数据库上下文
	dbCtx := &gosqlx.Context{
		Nick: "main",
		Mode: gosqlx.ModeReadWrite,
	}

	// 获取读写数据库连接
	db, err := manager.GetDatabase(dbCtx)
	if err != nil {
		log.Fatalf("获取读写数据库连接失败: %v", err)
	}

	// 执行写操作
	err = db.Exec("INSERT INTO users (username, email) VALUES (?, ?)", "newuser", "newuser@example.com")
	if err != nil {
		log.Fatalf("执行写操作失败: %v", err)
	}

	log.Println("写操作成功")

	// 使用只读模式查询
	useReadOnlyMode(manager)
}
func useReadOnlyMode(manager *gosqlx.DatabaseManager) {
	// 创建数据库上下文
	dbCtx := &gosqlx.Context{
		Nick: "main_readonly",
		Mode: gosqlx.ModeReadOnly,
	}

	// 获取只读数据库连接
	db, err := manager.GetDatabase(dbCtx)
	if err != nil {
		log.Fatalf("获取只读数据库连接失败: %v", err)
	}

	// 执行读操作
	var count int
	err = db.ScanRaw(&count, "SELECT COUNT(*) FROM users ")
	if err != nil {
		log.Fatalf("执行读操作失败: %v", err)
	}

	log.Printf("读操作成功，用户总数: %d", count)
}
func useQueryBuilderWithReadWriteSeparation(manager *gosqlx.DatabaseManager) {
	// 创建读写数据库上下文
	rwCtx := &gosqlx.Context{
		Nick: "main",
		Mode: gosqlx.ModeReadWrite,
	}

	// 获取读写数据库连接
	rwDB, err := manager.GetDatabase(rwCtx)
	if err != nil {
		log.Fatalf("获取读写数据库失败: %v", err)
	}

	// 创建只读数据库上下文
	roCtx := &gosqlx.Context{
		Nick: "main_readonly",
		Mode: gosqlx.ModeReadOnly,
	}

	// 获取只读数据库连接
	roDB, err := manager.GetDatabase(roCtx)
	if err != nil {
		log.Fatalf("获取只读数据库失败: %v", err)
	}

	// 使用读写数据库进行写操作
	err = rwDB.Exec("INSERT INTO users (username, email) VALUES (?, ?)", "queryuser", "query@example.com")
	if err != nil {
		log.Fatalf("Query构建器写操作失败: %v", err)
	}

	// 使用只读数据库进行读操作
	roQuery := query.NewQuery(roDB.DB())
	var users []User
	err = roQuery.Table("users").
		Select("id", "username", "email").
		Where("username LIKE ?", "query%").
		Get(&users)
	if err != nil {
		log.Fatalf("Query构建器读操作失败: %v", err)
	}

	log.Printf("Query构建器读操作成功，查询到 %d 条记录", len(users))
}
func useTransactionWithReadWriteSeparation(manager *gosqlx.DatabaseManager) {
	// 创建读写数据库上下文
	rwCtx := &gosqlx.Context{
		Nick: "main",
		Mode: gosqlx.ModeReadWrite,
	}

	// 获取读写数据库（事务必须在读写库上执行）
	db, err := manager.GetDatabase(rwCtx)
	if err != nil {
		log.Fatalf("获取读写数据库失败: %v", err)
	}

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("开始事务失败: %v", err)
	}

	// 执行事务操作
	err = tx.Exec("INSERT INTO users (username, email) VALUES (?, ?)", "txuser", "tx@example.com")
	if err != nil {
		tx.Rollback()
		log.Fatalf("事务操作失败: %v", err)
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		log.Fatalf("提交事务失败: %v", err)
	}

	log.Println("事务操作成功")
}
func loadConfigFromFile() {
	// 创建文件配置加载器
	loader := gosqlx.NewFileConfigLoader("config/database.json")

	// 加载配置
	configs, err := loader.Load()
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 创建配置提供者
	provider := gosqlx.NewConfigProvider(configs)
	// 创建数据库管理器
	configManager := gosqlx.NewConfigManager(provider)

	// 创建数据库管理器
	manager := gosqlx.NewDatabaseManager(configManager)
	//if err != nil {
	//	log.Fatalf("初始化数据库管理器失败: %v", err)
	//}
	fmt.Println(manager)
	// 使用数据库管理器...
}

type User struct {
	Username string `json:"username"`
	Email    string `json:"email"`
}

func useMongoDBQueryBuilder(manager *gosqlx.DatabaseManager) {
	// 创建 MongoDB 上下文
	dbCtx := &gosqlx.Context{
		Nick: "main",
		Mode: gosqlx.ModeReadWrite,
	}

	// 获取 MongoDB 连接
	db, err := manager.GetDatabase(dbCtx)
	if err != nil {
		log.Fatalf("获取 MongoDB 连接失败: %v", err)
	}

	// 使用查询构建器构建 MongoDB 聚合管道查询
	q := query.NewQuery(db.DB())

	// 示例1：简单查询
	var users []User
	err = q.Table("users").
		Select("_id", "username", "email").
		Where("active", true).
		Limit(10).
		Get(&users)

	if err != nil {
		log.Fatalf("MongoDB 查询失败: %v", err)
	}

	log.Printf("查询到 %d 个用户", len(users))

	// 示例2：复杂聚合管道查询
	var userArticles []UserArticle
	q = query.NewQuery(db.DB())
	err = q.Table("users").
		Select("_id", "username", "email", "articles").
		Lookup("articles", "_id", "user_id", "articles").
		Unwind("articles").
		Match("articles.status", "published").
		AddField("fullName", map[string]interface{}{
			"$concat": []string{"$firstName", " ", "$lastName"},
		}).
		Sort(map[string]int{
			"articles.created_at": -1,
		}).
		Limit(20).
		Get(&userArticles)

	if err != nil {
		log.Fatalf("MongoDB 聚合查询失败: %v", err)
	}

	log.Printf("查询到 %d 个用户文章", len(userArticles))

	// 示例3：分组统计查询
	var stats []ArticleStats
	q = query.NewQuery(db.DB())
	err = q.Table("articles").
		GroupBy("category").
		GroupCount("count").
		GroupSum("views", "totalViews").
		GroupAvg("rating", "avgRating").
		Sort(map[string]int{
			"count": -1,
		}).
		Get(&stats)

	if err != nil {
		log.Fatalf("MongoDB 分组统计查询失败: %v", err)
	}

	log.Printf("查询到 %d 个分类统计", len(stats))
}

// UserArticle 用户文章结构
type UserArticle struct {
	ID       string `json:"_id" bson:"_id"`
	Username string `json:"username" bson:"username"`
	Email    string `json:"email" bson:"email"`
	Article  struct {
		ID        string    `json:"_id" bson:"_id"`
		Title     string    `json:"title" bson:"title"`
		Content   string    `json:"content" bson:"content"`
		Status    string    `json:"status" bson:"status"`
		CreatedAt time.Time `json:"created_at" bson:"created_at"`
	} `json:"article" bson:"article"`
}

// ArticleStats 文章统计结构
type ArticleStats struct {
	Category   string  `json:"_id" bson:"_id"`
	Count      int     `json:"count" bson:"count"`
	TotalViews int     `json:"totalViews" bson:"totalViews"`
	AvgRating  float64 `json:"avgRating" bson:"avgRating"`
}

package main

import (
	"fmt"
	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/gen/doc"
	"log"

	"github.com/gzorm/gosqlx/gen/model"
)

func main() {
	//gen_MySql_POES()
	gen_myql_doc()
}
func gen_myql_doc() {
	// 创建文档生成配置
	config := &doc.Config{
		DBType:     gosqlx.MySQL,
		Source:     "root:root@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local",
		DBName:     "mydb",
		OutputPath: "database_doc.docx",
		Title:      "数据库设计文档",
		Author:     "系统管理员",
		Company:    "我的公司",
	}

	// 生成Word文档
	err := doc.GenerateDBDoc(config)
	if err != nil {
		log.Fatalf("生成数据库文档失败: %v", err)
	}

	fmt.Println("数据库文档生成成功!")

	// 如果需要生成Excel格式的文档
	excelConfig := &doc.Config{
		DBType:     gosqlx.MySQL,
		Source:     "root:password@tcp(localhost:3306)/mydb?charset=utf8mb4&parseTime=True&loc=Local",
		DBName:     "mydb",
		OutputPath: "database_doc.xlsx",
		Title:      "数据库设计文档",
		Author:     "系统管理员",
		Company:    "我的公司",
	}

	err = doc.GenerateExcelDoc(excelConfig)
	if err != nil {
		log.Fatalf("生成Excel数据库文档失败: %v", err)
	}

	fmt.Println("Excel数据库文档生成成功!")
}
func gen_MySql_POES() {
	config := &model.Config{
		DBType:       "mysql",
		Host:         "localhost",
		Port:         3306,
		Username:     "root",
		Password:     "root",
		DatabaseName: "test",
		OutputDir:    "./gen/model",
		PackageName:  "models",
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}

	fmt.Println("模型生成完成！")
}
func gen_Postgres_POES() {
	config := &model.Config{
		DBType:       "postgres",
		Host:         "localhost",
		Port:         5432,
		Username:     "postgres",
		Password:     "password",
		DatabaseName: "test_db",
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}
func gen_Sqlserver_POES() {
	config := &model.Config{
		DBType:       "sqlserver",
		Host:         "localhost",
		Port:         1433,
		Username:     "sa",
		Password:     "YourPassword",
		DatabaseName: "test_db",
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}
func gen_Oracle_POES() {
	config := &model.Config{
		DBType:       "oracle",
		Host:         "localhost",
		Port:         1521,
		Username:     "system",
		Password:     "password",
		DatabaseName: "XE",          // 服务名或SID
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}
func gen_TIDB_POES() {
	config := &model.Config{
		DBType:       "tidb",
		Host:         "localhost",
		Port:         4000, // TiDB 默认端口
		Username:     "root",
		Password:     "password",
		DatabaseName: "test_db",
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}
func gen_SQLITE_POES() {
	config := &model.Config{
		DBType:       "sqlite",
		DatabaseName: "./test.db",   // SQLite 数据库文件路径
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}
func gen_MongoDb_POES() {
	config := &model.Config{
		DBType:       "mongodb",
		Host:         "localhost",
		Port:         27017,
		Username:     "admin",    // 可选
		Password:     "password", // 可选
		DatabaseName: "test_db",
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}
func gen_MariaDB_POES() {
	config := &model.Config{
		DBType:       "mariadb",
		Host:         "localhost",
		Port:         3306,
		Username:     "root",
		Password:     "password",
		DatabaseName: "test_db",
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}

	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}
func gen_ClickHouse_POES() {
	config := &model.Config{
		DBType:       "clickhouse",
		Host:         "localhost",
		Port:         9000, // ClickHouse 默认端口
		Username:     "default",
		Password:     "",
		DatabaseName: "test_db",
		OutputDir:    "./gen/model", // 会自动创建 model/poes 目录
		PackageName:  "poes",        // 生成的包名
	}
	if err := model.GenerateModels(config); err != nil {
		log.Fatalf("生成模型失败: %v", err)
	}
}

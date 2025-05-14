# GoSQLX - 高性能多数据库适配器与查询构建框架

## 💖 支持项目

🌟 **如果你觉得本项目有用，请先点个 Star 支持我们！**  

[![给项目加星](https://img.shields.io/github/stars/gzorm/gosqlx.svg?style=social)](https://github.com/gzorm/gosqlx)


## 简介
GoSQLX 是一个功能强大的 Go 语言数据库操作框架，提供了统一的接口来操作多种关系型数据库，包括 MySQL、PostgreSQL、Oracle、SQL Server、Tidb、Mongodb  和 SQLite。它基于 GORM 和标准库构建，同时提供了更高级的抽象和功能扩展。

## 特性
- 多数据库适配 ：无缝支持 MySQL、PostgreSQL、Oracle、SQL Server、Tidb、Mongodb、ClickHouse、MariaDb、OceanBase 和 SQLite 等主流数据库
- 读写分离 ：内置读写分离支持，轻松实现数据库负载均衡
- 灵活配置管理 ：支持多环境、多数据库配置，适应复杂的部署场景
- 强大的查询构建器 ：链式 API 设计，简化 SQL 构建过程
- 事务支持 ：完善的事务处理机制，包括只读事务
- 连接池管理 ：智能连接池配置，优化数据库连接资源
- 上下文感知 ：支持 context.Context，便于超时控制和请求追踪
- 调试模式 ：内置 SQL 日志记录，方便开发调试

## 安装
<pre class="command-line"><code>go get github.com/gzorm/gosqlx</code></pre>

## 快速开始
### 基本配置
<pre class="command-line"><code>

package main

import (
    "github.com/gzorm/gosqlx"
)

func main() {
    // 创建配置映射
    configs := gosqlx.ConfigMap{
        "development": {
            "main": &gosqlx.Config{
                Type:        gosqlx.MySQL,
                Driver:      "mysql",
                Source:      "user:password@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local",
                MaxIdle:     10,
                MaxOpen:     100,
                MaxLifetime: 3600,
                Debug:       true,
            },
        },
    }
    
    // 创建配置提供者
    provider := gosqlx.NewConfigProvider(configs)

    // 创建配置管理器
    configManager := gosqlx.NewConfigManager(provider)

    // 创建数据库管理器
     manager := gosqlx.NewDatabaseManager(configManager)   

    // 创建数据库上下文
	dbCtx := &gosqlx.Context{
		Nick: "main",
		Mode: gosqlx.ModeReadWrite,
	}
    
    // 获取数据库连接
    db, err := manager.GetDatabase(dbCtx)
    if err != nil {
        panic(err)
    }
    
    // 使用数据库连接
    // ...
}


</code></pre>
### 读写分离配置
<pre class="command-line"><code>

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
</code></pre>
### 基本查询操作
<pre class="command-line"><code>

// 执行查询
rows, err := db.Query("SELECT id, name FROM users WHERE age > ?", 18)
if err != nil {
    // 处理错误
}
defer rows.Close()

// 遍历结果
for rows.Next() {
    var id int
    var name string
    if err := rows.Scan(&id, &name); err != nil {
        // 处理错误
    }
    fmt.Printf("ID: %d, Name: %s\n", id, name)
}</code></pre>
### 使用查询构建器
<pre class="command-line"><code>
import "github.com/gzorm/gosqlx/query"

// 创建查询构建器
q := query.NewQuery(db.DB())

// 查询单条记录
var user User
err := q.Table("users").
    Select("id", "username", "email").
    Where("id = ?", 1).
    First(&user)

// 查询多条记录
var users []User
err := q.Table("users").
    Select("id", "username", "email").
    Where("age > ?", 18).
    OrderBy("id DESC").
    Limit(10).
    Offset(0).
    Get(&users)


</code></pre>
### 事务处理
<pre class="command-line"><code>
// 开始事务
tx, err := db.Begin()
if err != nil {
    // 处理错误
}

// 执行事务操作
err = tx.Exec("INSERT INTO users (username, email) VALUES (?, ?)", "newuser", "newuser@example.com")
if err != nil {
    tx.Rollback()
    // 处理错误
    return
}

// 提交事务
if err := tx.Commit(); err != nil {
    // 处理错误
}
</code></pre>
### 读写分离使用
<pre class="command-line"><code>
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

// 执行读操作
var count int
err = roDB.ScanRaw(&count, "SELECT COUNT(*) FROM users ")
if err != nil {
  log.Fatalf("执行读操作失败: %v", err)
}

</code></pre>
## 高级用法
### 从文件加载配置
<pre class="command-line"><code>

// 创建文件配置加载器
loader := gosqlx.NewFileConfigLoader("config/database.json")

// 加载配置
configs, err := loader.Load()
if err != nil {
    panic(err)
}

 
// 创建配置提供者
provider := gosqlx.NewConfigProvider(configs)
// 创建数据库管理器
configManager := gosqlx.NewConfigManager(provider)

// 创建数据库管理器
manager := gosqlx.NewDatabaseManager(configManager)

</code></pre>

### 自定义适配器
<pre class="command-line"><code>

// 实现自定义适配器
type MyCustomAdapter struct {
    // ...
}

// 实现适配器接口方法
func (a *MyCustomAdapter) Connect() (*gorm.DB, *sql.DB, error) {
    // ...
}

// 注册自定义适配器
gosqlx.RegisterAdapter("mycustom", func(config *gosqlx.Config) gosqlx.Adapter {
    return &MyCustomAdapter{
        // ...
    }
})

</code></pre>
## 支持的数据库
- MySQL
- PostgreSQL
- Oracle
- SQL Server
- TIDB
- Mongodb
- SQLite
- ClickHouse
- MariaDB
- OceanBase
## 贡献指南
欢迎贡献代码、报告问题或提出改进建议。


## 许可证
本项目采用 Apache 2.0 许可证 - 详情请参阅 LICENSE 文件。

## 联系方式
- 项目维护者：gzorm
- GitHub： https://github.com/gzorm


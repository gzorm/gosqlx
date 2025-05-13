# GoSQLX - High-Performance Multi-Database Adapter and Query Builder Framework

[English](README.md) | [中文](README_zh.md)

## 💖 Support This Project

Your support means a lot to us! If GoSQLX has helped you solve problems, please consider:

- ⭐ Giving the project a star
- 🔄 Sharing it with your colleagues and friends
- 🐛 Reporting issues you find
- 🔧 Submitting PRs to help improve the project
- 📝 Sharing your experience using GoSQLX

[![Star this project](https://img.shields.io/github/stars/gzorm/gosqlx.svg?style=social)](https://github.com/gzorm/gosqlx)

## Introduction
GoSQLX is a powerful Go language database operation framework that provides a unified interface for operating multiple relational databases, including MySQL, PostgresSQL, Oracle, SQL Server, TiDB, MongoDB, and SQLite. It is built on GORM and the standard library, while providing higher-level abstractions and functional extensions.

## Features
- Multi-database support: Seamlessly supports mainstream databases like MySQL, PostgresSQL, Oracle, SQL Server, TiDB, MongoDB, and SQLite
- Read-write separation: Built-in read-write separation support for easy database load balancing
- Flexible configuration management: Supports multi-environment, multi-database configurations for complex deployment scenarios
- Powerful query builder: Chain API design simplifies the SQL building process
- Transaction support: Complete transaction processing mechanism, including read-only transactions
- Connection pool management: Smart connection pool configuration optimizes database connection resources
- Context awareness: Supports context.Context for timeout control and request tracking
- Debug mode: Built-in SQL logging for easy development debugging

## Installation
``` bash
     
     go get github.com/gzorm/gosqlx

```
## Quick Start
### Basic Configuration
```go
package main

import (
    "github.com/gzorm/gosqlx"
)

func main() {
    // Create configuration mapping
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
    
    // Create configuration provider
    provider := gosqlx.NewConfigProvider(configs)

    // Create configuration manager
    configManager := gosqlx.NewConfigManager(provider)

    // Create database manager
    manager := gosqlx.NewDatabaseManager(configManager)   

    // Create database context
    dbCtx := &gosqlx.Context{
        Nick: "main",
        Mode: gosqlx.ModeReadWrite,
    }
    
    // Get database connection
    db, err := manager.GetDatabase(dbCtx)
    if err != nil {
        panic(err)
    }
    
    // Use database connection
    // ...
}
```
## Read-Write Separation Configuration
```go
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
```
## Basic Query Operations
```go
// Execute query
rows, err := db.Query("SELECT id, name FROM users WHERE age > ?", 18)
if err != nil {
    // Handle error
}
defer rows.Close()

// Iterate through results
for rows.Next() {
    var id int
    var name string
    if err := rows.Scan(&id, &name); err != nil {
        // Handle error
    }
    fmt.Printf("ID: %d, Name: %s\n", id, name)
}
```
## Using Query Builder
```go
import "github.com/gzorm/gosqlx/query"

// Create query builder
q := query.NewQuery(db.DB())

// Query single record
var user User
err := q.Table("users").
    Select("id", "username", "email").
    Where("id = ?", 1).
    First(&user)

// Query multiple records
var users []User
err := q.Table("users").
    Select("id", "username", "email").
    Where("age > ?", 18).
    OrderBy("id DESC").
    Limit(10).
    Offset(0).
    Get(&users)
```
## Transaction Handling
```go
// Start transaction
tx, err := db.Begin()
if err != nil {
    // Handle error
}

// Execute transaction operations
err = tx.Exec("INSERT INTO users (username, email) VALUES (?, ?)", "newuser", "newuser@example.com")
if err != nil {
    tx.Rollback()
    // Handle error
    return
}

// Commit transaction
if err := tx.Commit(); err != nil {
    // Handle error
}
```
## Read-Write Separation Usage
```go
// Create read-write database context
rwCtx := &gosqlx.Context{
  Nick: "main",
  Mode: gosqlx.ModeReadWrite,
}

// Get read-write database connection
rwDB, err := manager.GetDatabase(rwCtx)
if err != nil {
  log.Fatalf("Failed to get read-write database: %v", err)
}

// Create read-only database context
roCtx := &gosqlx.Context{
  Nick: "main_readonly",
  Mode: gosqlx.ModeReadOnly,
}

// Get read-only database connection
roDB, err := manager.GetDatabase(roCtx)
if err != nil {
  log.Fatalf("Failed to get read-only database: %v", err)
}

// Use read-write database for write operations
err = rwDB.Exec("INSERT INTO users (username, email) VALUES (?, ?)", "queryuser", "query@example.com")
if err != nil {
   log.Fatalf("Query builder write operation failed: %v", err)
}

// Execute read operations
var count int
err = roDB.ScanRaw(&count, "SELECT COUNT(*) FROM users ")
if err != nil {
  log.Fatalf("Read operation failed: %v", err)
}
```
## Advanced Usage
### Loading Configuration from File
```go
// Create file configuration loader
loader := gosqlx.NewFileConfigLoader("config/database.json")

// Load configuration
configs, err := loader.Load()
if err != nil {
    panic(err)
}

// Create configuration provider
provider := gosqlx.NewConfigProvider(configs)
// Create configuration manager
configManager := gosqlx.NewConfigManager(provider)

// Create database manager
manager := gosqlx.NewDatabaseManager(configManager)
```
## Custom Adapter
```go
// Implement custom adapter
type MyCustomAdapter struct {
    // ...
}

// Implement adapter interface methods
func (a *MyCustomAdapter) Connect() (*gorm.DB, *sql.DB, error) {
    // ...
}

// Register custom adapter
gosqlx.RegisterAdapter("mycustom", func(config *gosqlx.Config) gosqlx.Adapter {
    return &MyCustomAdapter{
        // ...
    }
})
```
## Supported Databases
- MySQL
- PostgresSQL
- Oracle
- SQL Server
- TiDB
- MongoDB
- SQLite
## Contribution Guidelines
We welcome contributions from the community! If you would like to contribute to GoSQLX, please follow these guidelines:

- Fork the repository and create a new branch for your changes
- Make your changes and ensure that they are well-tested
- Submit a pull request with a clear description of your changes
 
## License
This project is licensed under the Apache 2.0 License - see the LICENSE file for details.
## Contact
- Project Maintainer: gzorm
- GitHub: https://github.com/gzorm
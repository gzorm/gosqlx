package test

import (
	"fmt"
	"log"

	"github.com/gzorm/gosqlx"
)

func main() {
	// 1. 创建数据库上下文
	ctx := &gosqlx.Context{
		Nick:    "test_db",
		Mode:    "rw",
		DBType:  gosqlx.MySQL,
		Timeout: 10, // 超时时间（秒）
	}

	// 2. 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.MySQL,
		Driver:      "mysql",
		Source:      "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local",
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: 3600,
		Debug:       true,
	}

	// 3. 初始化数据库连接
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}
	defer db.Close()

	// 4. 测试数据库连接
	if err := db.Ping(); err != nil {
		log.Fatalf("测试数据库连接失败: %v", err)
	}

	// 6. 使用方言相关功能
	// 例如：构建分页查询
	baseQuery := "SELECT * FROM users WHERE age > 18"

	// 根据数据库类型自动使用正确的方言
	switch config.Type {
	case gosqlx.MySQL:
		// MySQL 分页
		limitQuery := baseQuery + " LIMIT 10 OFFSET 20"
		fmt.Println("MySQL分页查询:", limitQuery)

	case gosqlx.PostgresSQL:
		// PostgreSQL 分页
		limitQuery := baseQuery + " LIMIT 10 OFFSET 20"
		fmt.Println("PostgreSQL分页查询:", limitQuery)

	case gosqlx.SQLServer:
		// SQL Server 分页
		limitQuery := "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS row_num FROM users WHERE age > 18) AS t WHERE row_num BETWEEN 21 AND 30"
		fmt.Println("SQL Server分页查询:", limitQuery)
	}

	// 7. 使用查询构建器（内部会使用方言）
	type Product struct {
		ID    int     `gorm:"column:id"`
		Name  string  `gorm:"column:name"`
		Price float64 `gorm:"column:price"`
	}

	// 使用Find方法查询
	var products []Product
	// 创建查询构建器
	db.Table("products").
		Select("id", "name", "price").
		Where("price > ?", 100).
		Order("price DESC").
		Limit(10).
		Find(&products)

	// 执行查询
	var users []struct {
		ID       int    `db:"id"`
		Username string `db:"username"`
		Email    string `db:"email"`
	}

	if err := db.Find(&users); err != nil {
		log.Fatalf("查询失败: %v", err)
	}

	fmt.Printf("查询到 %d 条记录\n", len(users))
	for _, user := range users {
		fmt.Printf("用户: %d, %s, %s\n", user.ID, user.Username, user.Email)
	}

	// 8. 执行原生SQL（方言会处理SQL语法差异）
	result2, err := db.ExecWithResult("INSERT INTO users (username, email, age) VALUES (?, ?, ?)", "newuser", "new@example.com", 25)
	if err != nil {
		log.Fatalf("插入失败: %v", err)
	}

	id, _ := result2.LastInsertId()
	fmt.Printf("插入成功，ID: %d\n", id)
}

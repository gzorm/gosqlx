package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gzorm/gosqlx"
)

// 初始化数据库连接
func initMariaDB(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.MariaDB,
		Driver:      "mysql", // MariaDB 使用 MySQL 驱动
		Source:      "root:root@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local",
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "test_mariadb",
		Mode:    "rw",
		DBType:  gosqlx.MariaDB,
		Timeout: time.Second * 10,
	}

	// 创建数据库实例
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}

	return db
}

// 准备测试表
func prepareMariaDBTestTables(t *testing.T, db *gosqlx.Database) {
	// 创建用户表
	err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			username VARCHAR(50) NOT NULL,
			email VARCHAR(100) NOT NULL,
			age INT NOT NULL DEFAULT 0,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("创建用户表失败: %v", err)
	}

	// 创建文章表
	err = db.Exec(`
		CREATE TABLE IF NOT EXISTS articles (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			title VARCHAR(200) NOT NULL,
			content TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)
	`)
	if err != nil {
		t.Fatalf("创建文章表失败: %v", err)
	}

	// 清空测试数据
	err = db.Exec("TRUNCATE TABLE articles")
	if err != nil {
		t.Fatalf("清空文章表失败: %v", err)
	}

	err = db.Exec("TRUNCATE TABLE users")
	if err != nil {
		t.Fatalf("清空用户表失败: %v", err)
	}
}

// 测试插入操作
func TestMariaDBInsert(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 创建用户
	user := &User{
		Username:  "testuser",
		Email:     "test@example.com",
		Age:       25,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 执行插入
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}

	// 获取插入ID
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	t.Logf("插入用户成功，ID: %d", lastID)

	// 验证插入结果
	var count int
	err = db.ScanRaw(&count, "SELECT COUNT(*) FROM users WHERE id = ?", lastID)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("插入用户验证失败，期望记录数: 1, 实际记录数: %d", count)
	}
}

// 测试批量插入
func TestMariaDBBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 准备批量插入数据
	users := []User{
		{Username: "user1", Email: "user1@example.com", Age: 21, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user2", Email: "user2@example.com", Age: 22, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user3", Email: "user3@example.com", Age: 23, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user4", Email: "user4@example.com", Age: 24, Active: false, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user5", Email: "user5@example.com", Age: 25, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	// 构建批量插入SQL
	sql := "INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES "
	values := []interface{}{}

	for i, user := range users {
		if i > 0 {
			sql += ", "
		}
		sql += "(?, ?, ?, ?, ?, ?)"
		values = append(values, user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt)
	}

	// 执行批量插入
	result, err := db.ExecWithResult(sql, values...)
	if err != nil {
		t.Fatalf("批量插入用户失败: %v", err)
	}

	// 获取影响行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("获取影响行数失败: %v", err)
	}

	t.Logf("批量插入用户成功，影响行数: %d", rowsAffected)

	// 验证插入结果
	var count int
	err = db.ScanRaw(&count, "SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("查询用户总数失败: %v", err)
	}

	if count != len(users) {
		t.Fatalf("批量插入用户验证失败，期望记录数: %d, 实际记录数: %d", len(users), count)
	}
}

// 测试查询操作
func TestMariaDBQuery(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 插入测试数据
	usernames := []string{"user1", "user2", "user3", "user4", "user5"}
	for _, username := range usernames {
		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, username+"@example.com", 20+len(username), true,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 查询多条记录
	rows, err := db.Query("SELECT id, username, email, age, active, created_at, updated_at FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("查询多条记录失败: %v", err)
	}
	defer rows.Close()

	// 遍历结果集
	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active, &user.CreatedAt, &user.UpdatedAt)
		if err != nil {
			t.Fatalf("扫描结果集失败: %v", err)
		}
		users = append(users, user)
	}

	// 检查遍历错误
	if err = rows.Err(); err != nil {
		t.Fatalf("遍历结果集错误: %v", err)
	}

	// 验证查询结果
	if len(users) != len(usernames) {
		t.Fatalf("查询结果验证失败，期望记录数: %d, 实际记录数: %d", len(usernames), len(users))
	}

	t.Logf("查询多条记录成功，记录数: %d", len(users))
}

// 测试单行查询
func TestMariaDBQueryRow(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 插入测试数据
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"queryuser", "query@example.com", 30, true,
	)
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	// 查询单条记录
	var user User
	err = db.ScanRaw(&user, "SELECT id, username, email, age, active, created_at, updated_at FROM users WHERE id = ?", userID)

	if err != nil {
		t.Fatalf("查询单条记录失败: %v", err)
	}

	// 验证查询结果
	if user.ID != userID || user.Username != "queryuser" || user.Email != "query@example.com" || user.Age != 30 || !user.Active {
		t.Fatalf("查询结果验证失败，期望用户ID: %d, 实际用户ID: %d", userID, user.ID)
	}

	t.Logf("查询单条记录成功: %+v", user)
}

// 测试更新操作
func TestMariaDBUpdate(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 插入测试数据
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"updateuser", "update@example.com", 30, true,
	)
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	// 执行更新
	result, err = db.ExecWithResult(
		"UPDATE users SET username = ?, email = ?, age = ?, active = ? WHERE id = ?",
		"updateduser", "updated@example.com", 35, false, userID,
	)
	if err != nil {
		t.Fatalf("更新用户失败: %v", err)
	}

	// 获取影响行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("获取影响行数失败: %v", err)
	}

	if rowsAffected != 1 {
		t.Fatalf("更新用户验证失败，期望影响行数: 1, 实际影响行数: %d", rowsAffected)
	}

	// 验证更新结果
	var user User
	err = db.QueryRow("SELECT id, username, email, age, active FROM users WHERE id = ?", userID).
		Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active)
	if err != nil {
		t.Fatalf("查询更新后的用户失败: %v", err)
	}

	if user.Username != "updateduser" || user.Email != "updated@example.com" || user.Age != 35 || user.Active {
		t.Fatalf("更新结果验证失败: %+v", user)
	}

	t.Logf("更新用户成功: %+v", user)
}

// 测试分页查询
func TestMariaDBPagination(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("pageuser%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 测试分页参数
	page := 2
	pageSize := 5
	offset := (page - 1) * pageSize

	// 查询分页数据
	rows, err := db.Query(
		"SELECT id, username, email, age, active FROM users ORDER BY id LIMIT ? OFFSET ?",
		pageSize, offset,
	)
	if err != nil {
		t.Fatalf("分页查询失败: %v", err)
	}
	defer rows.Close()

	// 遍历结果集
	var users []User
	for rows.Next() {
		var user User
		err := rows.Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active)
		if err != nil {
			t.Fatalf("扫描结果集失败: %v", err)
		}
		users = append(users, user)
	}

	// 检查遍历错误
	if err = rows.Err(); err != nil {
		t.Fatalf("遍历结果集错误: %v", err)
	}

	// 验证分页结果
	if len(users) != pageSize {
		t.Fatalf("分页查询验证失败，期望记录数: %d, 实际记录数: %d", pageSize, len(users))
	}

	// 查询总记录数
	var total int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&total)
	if err != nil {
		t.Fatalf("查询总记录数失败: %v", err)
	}

	t.Logf("分页查询成功，当前页: %d, 每页大小: %d, 总记录数: %d", page, pageSize, total)
}

// 测试事务
func TestMariaDBTransaction(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 使用 Transaction 方法执行事务
	err := db.Transaction(func(tx *gosqlx.Database) error {
		// 在事务中插入用户
		result, err := tx.ExecWithResult(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			"txuser", "tx@example.com", 35, true,
		)
		if err != nil {
			return err
		}

		userID, err := result.LastInsertId()
		if err != nil {
			return err
		}

		// 在事务中插入文章
		err = tx.Exec(
			"INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
			userID, "事务文章", "这是一篇在事务中创建的文章",
		)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		t.Fatalf("事务操作失败: %v", err)
	}

	// 验证事务结果
	var userCount int
	err = db.ScanRaw(&userCount, "SELECT COUNT(*) FROM users WHERE username = ?", "txuser")
	if err != nil {
		t.Fatalf("查询用户记录失败: %v", err)
	}

	var articleCount int
	err = db.ScanRaw(&articleCount, "SELECT COUNT(*) FROM articles WHERE title = ?", "事务文章")
	if err != nil {
		t.Fatalf("查询文章记录失败: %v", err)
	}

	if userCount != 1 || articleCount != 1 {
		t.Fatalf("事务验证失败，用户记录数: %d, 文章记录数: %d", userCount, articleCount)
	}

	t.Logf("事务操作成功")
}

// 测试 ON DUPLICATE KEY UPDATE 功能
func TestMariaDBOnDuplicateKeyUpdate(t *testing.T) {
	// 初始化数据库
	db := initMariaDB(t)
	defer db.Close()

	// 准备测试表
	prepareMariaDBTestTables(t, db)

	// 插入初始数据
	err := db.Exec(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"dupuser", "dup@example.com", 30, true,
	)
	if err != nil {
		t.Fatalf("插入初始数据失败: %v", err)
	}

	// 使用 ON DUPLICATE KEY UPDATE 更新数据
	err = db.Exec(`
		INSERT INTO users (username, email, age, active) 
		VALUES (?, ?, ?, ?) 
		ON DUPLICATE KEY UPDATE age = ?, active = ?
	`, "dupuser", "dup@example.com", 35, false, 35, false)
	if err != nil {
		t.Fatalf("执行 ON DUPLICATE KEY UPDATE 失败: %v", err)
	}

	// 验证结果
	var user User
	err = db.ScanRaw(&user, "SELECT id, username, email, age, active FROM users WHERE username = ?", "dupuser")
	if err != nil {
		t.Fatalf("查询更新后的用户失败: %v", err)
	}

	if user.Age != 35 || user.Active != false {
		t.Fatalf("ON DUPLICATE KEY UPDATE 验证失败: %+v", user)
	}

	t.Logf("ON DUPLICATE KEY UPDATE 测试成功: %+v", user)
}

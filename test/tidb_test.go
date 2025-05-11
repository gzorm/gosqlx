package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/query"
)

// 初始化TiDB数据库连接
func initTiDBDB(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.TiDB,
		Driver:      "mysql",                                                                         // TiDB 使用 MySQL 驱动
		Source:      "root:root@tcp(localhost:4000)/testdb?charset=utf8mb4&parseTime=True&loc=Local", // TiDB 默认端口为4000
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "test_tidb",
		Mode:    "rw",
		DBType:  gosqlx.TiDB,
		Timeout: time.Second * 10,
	}

	// 创建数据库实例
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("初始化TiDB数据库失败: %v", err)
	}

	return db
}

// 准备TiDB测试表
func prepareTiDBTestTables(t *testing.T, db *gosqlx.Database) {
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

// 测试TiDB插入操作
func TestTiDBInsert(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

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

// 测试TiDB查询操作
func TestTiDBQuery(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

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

// 测试TiDB事务
func TestTiDBTransaction(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

	// 测试提交事务
	t.Run("Commit", func(t *testing.T) {
		// 开始事务
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("开始事务失败: %v", err)
		}

		// 插入用户
		result, err := tx.ExecWithResult(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			"txuser1", "tx1@example.com", 25, true,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		userID, err := result.LastInsertId()
		if err != nil {
			tx.Rollback()
			t.Fatalf("获取用户ID失败: %v", err)
		}

		// 插入文章
		err = tx.Exec(
			"INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
			userID, "事务文章", "这是一篇事务测试文章",
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入文章失败: %v", err)
		}

		// 提交事务
		err = tx.Commit()
		if err != nil {
			t.Fatalf("提交事务失败: %v", err)
		}

		// 验证事务提交结果
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM articles WHERE user_id = ?", userID).Scan(&count)
		if err != nil {
			t.Fatalf("验证事务提交失败: %v", err)
		}

		if count != 1 {
			t.Fatalf("事务提交验证失败，期望文章数: 1, 实际文章数: %d", count)
		}

		t.Log("事务提交成功")
	})

	// 测试回滚事务
	t.Run("Rollback", func(t *testing.T) {
		// 开始事务
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("开始事务失败: %v", err)
		}

		// 插入用户
		result, err := tx.ExecWithResult(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			"txuser2", "tx2@example.com", 30, true,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		userID, err := result.LastInsertId()
		if err != nil {
			tx.Rollback()
			t.Fatalf("获取用户ID失败: %v", err)
		}

		// 回滚事务
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("回滚事务失败: %v", err)
		}

		// 验证事务回滚结果
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
		if err != nil {
			t.Fatalf("验证事务回滚失败: %v", err)
		}

		if count != 0 {
			t.Fatalf("事务回滚验证失败，期望用户数: 0, 实际用户数: %d", count)
		}

		t.Log("事务回滚成功")
	})
}

// 测试TiDB事务函数
func TestTiDBTransactionFunc(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

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
}

// 测试TiDB分页查询
func TestTiDBPagination(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

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

// 测试TiDB关联查询
func TestTiDBJoinQuery(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

	// 插入用户数据
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"joinuser", "join@example.com", 30, true,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for _, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
			userID, title, "这是"+title+"的内容",
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 执行关联查询
	rows, err := db.Query(`
		SELECT u.id AS user_id, u.username, a.id AS article_id, a.title AS article_title
		FROM users u
		JOIN articles a ON u.id = a.user_id
		WHERE u.id = ?
		ORDER BY a.id
	`, userID)
	if err != nil {
		t.Fatalf("关联查询失败: %v", err)
	}
	defer rows.Close()

	// 遍历结果集
	var userArticles []UserArticle
	for rows.Next() {
		var ua UserArticle
		err := rows.Scan(&ua.UserID, &ua.Username, &ua.ArticleID, &ua.ArticleTitle)
		if err != nil {
			t.Fatalf("扫描结果集失败: %v", err)
		}
		userArticles = append(userArticles, ua)
	}

	// 检查遍历错误
	if err = rows.Err(); err != nil {
		t.Fatalf("遍历结果集错误: %v", err)
	}

	// 验证关联查询结果
	if len(userArticles) != len(articleTitles) {
		t.Fatalf("关联查询验证失败，期望记录数: %d, 实际记录数: %d", len(articleTitles), len(userArticles))
	}

	t.Logf("关联查询成功，记录数: %d", len(userArticles))
}

// 测试TiDB查询构建器
func TestTiDBQueryBuilder(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("builder%d", i)
		email := fmt.Sprintf("builder%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用Query构建器查询
	q := query.NewQuery(db.DB())

	// 构建查询
	q.Table("users").
		Select("id", "username", "email", "age").
		Where("age > ?", 25).
		Where("active = ?", true).
		OrderByDesc("id").
		Limit(3)

	// 执行查询
	var users []User
	err := q.Get(&users)
	if err != nil {
		t.Fatalf("Query构建器查询失败: %v", err)
	}

	// 验证查询结果
	if len(users) == 0 {
		t.Fatalf("Query构建器查询验证失败，未返回任何记录")
	}

	t.Logf("Query构建器查询成功，记录数: %d", len(users))

	// 测试Count
	count, err := q.CountNum()
	if err != nil {
		t.Fatalf("Query构建器Count失败: %v", err)
	}

	t.Logf("Query构建器Count成功，总记录数: %d", count)
}

// 测试TiDB特有的Hint功能
func TestTiDBHint(t *testing.T) {
	// 初始化数据库
	db := initTiDBDB(t)
	defer db.Close()

	// 准备测试表
	prepareTiDBTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 5; i++ {
		username := fmt.Sprintf("hintuser%d", i)
		email := fmt.Sprintf("hint%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, true,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用TiDB特有的Hint功能
	q := query.NewQuery(db.DB())
	q.Table("users").
		Select("id", "username", "email").
		Hint("/*+ TIDB_SMJ(users) */"). // 使用Sort Merge Join算法的提示
		Where("age > ?", 21).
		OrderByAsc("id")

	// 执行查询
	var users []User
	err := q.Get(&users)
	if err != nil {
		t.Fatalf("TiDB Hint查询失败: %v", err)
	}

	// 验证查询结果
	if len(users) == 0 {
		t.Fatalf("TiDB Hint查询验证失败，未返回任何记录")
	}

	t.Logf("TiDB Hint查询成功，记录数: %d", len(users))
}

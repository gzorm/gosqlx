package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/query"
)

// 用户结构体
type PgUser struct {
	ID        int64     `db:"id"`
	Username  string    `db:"username"`
	Email     string    `db:"email"`
	Age       int       `db:"age"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
}

// 文章结构体
type PgArticle struct {
	ID        int64     `db:"id"`
	UserID    int64     `db:"user_id"`
	Title     string    `db:"title"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
}

// 用户文章关联结构体
type PgUserArticle struct {
	UserID       int64  `db:"user_id"`
	Username     string `db:"username"`
	ArticleID    int64  `db:"article_id"`
	ArticleTitle string `db:"article_title"`
}

func initPostgresDB(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.PostgreSQL,
		Driver:      "postgres",
		Source:      "host=localhost port=5432 user=postgres password=postgres dbname=testdb sslmode=disable",
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "postgres_test",
		Mode:    "rw",
		DBType:  gosqlx.PostgreSQL,
		Timeout: time.Second * 10,
	}

	// 创建数据库实例
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("连接PostgreSQL数据库失败: %v", err)
	}

	return db
}

// 准备测试表
func preparePgTestTables(t *testing.T, db *gosqlx.Database) {
	// 删除已存在的表
	err := db.Exec("DROP TABLE IF EXISTS articles")
	if err != nil {
		t.Fatalf("删除articles表失败: %v", err)
	}

	err = db.Exec("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Fatalf("删除users表失败: %v", err)
	}

	// 创建用户表
	err = db.Exec(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(100) NOT NULL,
			email VARCHAR(100) NOT NULL,
			age INT DEFAULT 0,
			active BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("创建users表失败: %v", err)
	}

	// 创建文章表
	err = db.Exec(`
		CREATE TABLE articles (
			id SERIAL PRIMARY KEY,
			user_id INT NOT NULL,
			title VARCHAR(200) NOT NULL,
			content TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT fk_articles_user FOREIGN KEY (user_id) REFERENCES users(id)
		)
	`)
	if err != nil {
		t.Fatalf("创建articles表失败: %v", err)
	}

	t.Log("测试表准备完成")
}

// 测试基本的CRUD操作
func TestPostgresCRUD(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	preparePgTestTables(t, db)

	// 测试插入
	var userID int64
	err := db.QueryRow(
		"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4) RETURNING id",
		"testuser", "test@example.com", 25, true,
	).Scan(&userID)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}

	t.Logf("插入用户成功，ID: %d", userID)

	// 测试查询
	var user PgUser
	err = db.QueryRow(
		"SELECT id, username, email, age, active, created_at FROM users WHERE id = $1",
		userID,
	).Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active, &user.CreatedAt)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}

	t.Logf("查询用户成功: %+v", user)

	// 测试更新
	err = db.Exec(
		"UPDATE users SET age = $1, email = $2 WHERE id = $3",
		30, "updated@example.com", userID,
	)
	if err != nil {
		t.Fatalf("更新用户失败: %v", err)
	}

	// 验证更新
	var age int
	var email string
	err = db.QueryRow(
		"SELECT age, email FROM users WHERE id = $1",
		userID,
	).Scan(&age, &email)
	if err != nil {
		t.Fatalf("验证更新失败: %v", err)
	}

	if age != 30 || email != "updated@example.com" {
		t.Fatalf("更新验证失败，期望: (30, updated@example.com), 实际: (%d, %s)", age, email)
	}

	t.Log("更新用户成功")

	// 测试删除
	err = db.Exec("DELETE FROM users WHERE id = $1", userID)
	if err != nil {
		t.Fatalf("删除用户失败: %v", err)
	}

	// 验证删除
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = $1", userID).Scan(&count)
	if err != nil {
		t.Fatalf("验证删除失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("删除验证失败，期望记录数: 0, 实际记录数: %d", count)
	}

	t.Log("删除用户成功")
}

// 测试批量插入
func TestPostgresBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	//preparePgTestTables(t, db)

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	// 批量插入用户
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("batch%d", i)
		email := fmt.Sprintf("batch%d@example.com", i)

		err := tx.Exec(
			"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4)",
			username, email, 20+i, i%2 == 0,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("批量插入用户失败: %v", err)
		}
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}

	// 验证批量插入
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("验证批量插入失败: %v", err)
	}

	if count != 10 {
		t.Fatalf("批量插入验证失败，期望记录数: 10, 实际记录数: %d", count)
	}

	t.Logf("批量插入成功，记录数: %d", count)
}

// 测试分页查询
func TestPostgresPagination(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	//preparePgTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4)",
			username, email, 20+i, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 分页查询
	page := 2
	pageSize := 5
	offset := (page - 1) * pageSize

	rows, err := db.Query(`
		SELECT id, username, email, age, active, created_at
		FROM users
		ORDER BY id
		LIMIT $1 OFFSET $2
	`, pageSize, offset)
	if err != nil {
		t.Fatalf("分页查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var users []PgUser
	for rows.Next() {
		var user PgUser
		err := rows.Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active, &user.CreatedAt)
		if err != nil {
			t.Fatalf("扫描行数据失败: %v", err)
		}
		users = append(users, user)
	}

	// 验证分页结果
	if len(users) != pageSize {
		t.Fatalf("分页查询验证失败，期望记录数: %d, 实际记录数: %d", pageSize, len(users))
	}

	t.Logf("分页查询成功，当前页: %d, 每页大小: %d, 返回记录数: %d", page, pageSize, len(users))
}

// 测试关联查询
func TestPostgresJoin(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	//preparePgTestTables(t, db)

	// 插入用户数据
	var userID int64
	err := db.QueryRow(
		"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4) RETURNING id",
		"joinuser", "join@example.com", 30, true,
	).Scan(&userID)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for _, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO articles (user_id, title, content) VALUES ($1, $2, $3)",
			userID, title, "这是"+title+"的内容",
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 关联查询
	rows, err := db.Query(`
		SELECT u.id AS user_id, u.username, a.id AS article_id, a.title AS article_title
		FROM users u
		LEFT JOIN articles a ON u.id = a.user_id
		WHERE u.id = $1
		ORDER BY a.id
	`, userID)
	if err != nil {
		t.Fatalf("关联查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var userArticles []PgUserArticle
	for rows.Next() {
		var ua PgUserArticle
		err := rows.Scan(&ua.UserID, &ua.Username, &ua.ArticleID, &ua.ArticleTitle)
		if err != nil {
			t.Fatalf("扫描行数据失败: %v", err)
		}
		userArticles = append(userArticles, ua)
	}

	// 验证关联查询结果
	if len(userArticles) != len(articleTitles) {
		t.Fatalf("关联查询验证失败，期望记录数: %d, 实际记录数: %d", len(articleTitles), len(userArticles))
	}

	t.Logf("关联查询成功，记录数: %d", len(userArticles))
}

// 测试事务
func TestPostgresTransaction(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	preparePgTestTables(t, db)

	// 测试提交事务
	t.Run("Commit", func(t *testing.T) {
		// 开始事务
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("开始事务失败: %v", err)
		}

		// 插入用户
		var userID int64
		err = tx.QueryRow(
			"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4) RETURNING id",
			"txuser1", "tx1@example.com", 25, true,
		).Scan(&userID)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 插入文章
		err = tx.Exec(
			"INSERT INTO articles (user_id, title, content) VALUES ($1, $2, $3)",
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
		err = db.QueryRow("SELECT COUNT(*) FROM articles WHERE user_id = $1", userID).Scan(&count)
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
		var userID int64
		err = tx.QueryRow(
			"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4) RETURNING id",
			"txuser2", "tx2@example.com", 30, true,
		).Scan(&userID)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 回滚事务
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("回滚事务失败: %v", err)
		}

		// 验证事务回滚结果
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM users WHERE username = $1", "txuser2").Scan(&count)
		if err != nil {
			t.Fatalf("验证事务回滚失败: %v", err)
		}

		if count != 0 {
			t.Fatalf("事务回滚验证失败，期望用户数: 0, 实际用户数: %d", count)
		}

		t.Log("事务回滚成功")
	})
}

// 测试使用Query构建器进行分页查询
func TestPostgresQueryBuilderPagination(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	preparePgTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4)",
			username, email, 20+i, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用Query构建器进行分页查询
	page := 2
	pageSize := 5

	q := query.NewQuery(db.DB())
	q.Table("users").
		Select("id", "username", "email", "age", "active").
		OrderByAsc("id").
		Page(page, pageSize)

	// 执行查询
	var users []PgUser
	err := q.Get(&users)
	if err != nil {
		t.Fatalf("Query构建器分页查询失败: %v", err)
	}

	// 验证查询结果
	if len(users) != pageSize {
		t.Fatalf("Query构建器分页查询验证失败，期望记录数: %d, 实际记录数: %d", pageSize, len(users))
	}

	// 获取总记录数
	count, err := q.CountNum()
	if err != nil {
		t.Fatalf("Query构建器获取总记录数失败: %v", err)
	}

	t.Logf("Query构建器分页查询成功，当前页: %d, 每页大小: %d, 总记录数: %d", page, pageSize, count)
}

// 测试使用Query构建器进行关联查询
func TestPostgresQueryBuilderJoin(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	preparePgTestTables(t, db)

	// 插入用户数据
	var userID int64
	err := db.QueryRow(
		"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4) RETURNING id",
		"joinuser", "join@example.com", 30, true,
	).Scan(&userID)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for _, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO articles (user_id, title, content) VALUES ($1, $2, $3)",
			userID, title, "这是"+title+"的内容",
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 使用Query构建器进行关联查询
	q := query.NewQuery(db.DB())
	q.Table("users u").
		Select("u.id AS user_id", "u.username", "a.id AS article_id", "a.title AS article_title").
		LeftJoin("articles a", "u.id = a.user_id").
		Where("u.id = ?", userID).
		OrderByAsc("a.id")

	// 执行查询
	var userArticles []PgUserArticle
	err = q.Get(&userArticles)
	if err != nil {
		t.Fatalf("Query构建器关联查询失败: %v", err)
	}

	// 验证查询结果
	if len(userArticles) != len(articleTitles) {
		t.Fatalf("Query构建器关联查询验证失败，期望记录数: %d, 实际记录数: %d", len(articleTitles), len(userArticles))
	}

	t.Logf("Query构建器关联查询成功，记录数: %d", len(userArticles))
}

// 测试使用Query构建器进行聚合查询
func TestPostgresQueryBuilderAggregate(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	preparePgTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4)",
			username, email, 20+i, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 测试AVG聚合函数
	q := query.NewQuery(db.DB())
	avgAge, err := q.Table("users").AvgNum("age")
	if err != nil {
		t.Fatalf("Query构建器AVG聚合查询失败: %v", err)
	}

	t.Logf("Query构建器AVG聚合查询成功，平均年龄: %.2f", avgAge)

	// 测试SUM聚合函数
	q = query.NewQuery(db.DB())
	sumAge, err := q.Table("users").SumNum("age")
	if err != nil {
		t.Fatalf("Query构建器SUM聚合查询失败: %v", err)
	}

	t.Logf("Query构建器SUM聚合查询成功，年龄总和: %.2f", sumAge)

	// 测试MAX聚合函数
	q = query.NewQuery(db.DB())
	maxAge, err := q.Table("users").MaxNum("age")
	if err != nil {
		t.Fatalf("Query构建器MAX聚合查询失败: %v", err)
	}

	t.Logf("Query构建器MAX聚合查询成功，最大年龄: %v", maxAge)

	// 测试MIN聚合函数
	q = query.NewQuery(db.DB())
	minAge, err := q.Table("users").MinNum("age")
	if err != nil {
		t.Fatalf("Query构建器MIN聚合查询失败: %v", err)
	}

	t.Logf("Query构建器MIN聚合查询成功，最小年龄: %v", minAge)
}

// 测试使用Query构建器进行分组查询
func TestPostgresQueryBuilderGroup(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	preparePgTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("group%d", i)
		email := fmt.Sprintf("group%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4)",
			username, email, 20+i%5, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用Query构建器进行分组查询
	q := query.NewQuery(db.DB())
	q.Table("users").
		SelectRaw("active, COUNT(*) as count, AVG(age) as avg_age").
		Group("active").
		OrderByAsc("active")

	// 执行查询
	type GroupResult struct {
		Active bool    `db:"active"`
		Count  int     `db:"count"`
		AvgAge float64 `db:"avg_age"`
	}

	var results []GroupResult
	err := q.Get(&results)
	if err != nil {
		t.Fatalf("Query构建器分组查询失败: %v", err)
	}

	// 验证查询结果
	if len(results) != 2 { // 应该有两组：active=true和active=false
		t.Fatalf("Query构建器分组查询验证失败，期望记录数: 2, 实际记录数: %d", len(results))
	}

	for _, result := range results {
		t.Logf("Query构建器分组查询成功，active=%v, 数量=%d, 平均年龄=%.2f", result.Active, result.Count, result.AvgAge)
	}
}

// 测试使用Query构建器进行事务操作
func TestPostgresQueryBuilderTransaction(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 准备测试表
	preparePgTestTables(t, db)

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	// 在事务中使用Query构建器
	q := query.NewQuery(tx)

	// 插入用户
	var userID int64
	err = tx.QueryRow(
		"INSERT INTO users (username, email, age, active) VALUES ($1, $2, $3, $4) RETURNING id",
		"txbuilder", "txbuilder@example.com", 30, true,
	).Scan(&userID)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中插入用户失败: %v", err)
	}

	// 插入文章
	err = tx.Exec(
		"INSERT INTO articles (user_id, title, content) VALUES ($1, $2, $3)",
		userID, "事务构建器文章", "这是一篇使用事务构建器创建的文章",
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

	// 验证事务结果
	q = query.NewQuery(db.DB())
	count, err := q.Table("articles").
		Where("user_id = ?", userID).
		CountNum()
	if err != nil {
		t.Fatalf("查询文章数量失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("事务验证失败，期望文章数: 1, 实际文章数: %d", count)
	}

	t.Logf("Query构建器事务操作成功，用户ID: %d", userID)
}

// 测试PostgreSQL特有功能：JSON操作
func TestPostgresJSON(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 创建带JSON字段的表
	err := db.Exec(`
		DROP TABLE IF EXISTS json_test;
		CREATE TABLE json_test (
			id SERIAL PRIMARY KEY,
			data JSONB NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("创建JSON测试表失败: %v", err)
	}

	// 插入JSON数据
	err = db.Exec(
		"INSERT INTO json_test (data) VALUES ($1)",
		`{"name": "测试用户", "age": 30, "tags": ["json", "postgres"]}`,
	)
	if err != nil {
		t.Fatalf("插入JSON数据失败: %v", err)
	}

	// 查询JSON数据
	var jsonData string
	err = db.QueryRow("SELECT data FROM json_test WHERE id = 1").Scan(&jsonData)
	if err != nil {
		t.Fatalf("查询JSON数据失败: %v", err)
	}

	t.Logf("查询JSON数据成功: %s", jsonData)

	// 使用JSON操作符查询
	var name string
	err = db.QueryRow("SELECT data->>'name' FROM json_test WHERE id = 1").Scan(&name)
	if err != nil {
		t.Fatalf("查询JSON字段失败: %v", err)
	}

	if name != "测试用户" {
		t.Fatalf("JSON字段查询验证失败，期望: 测试用户, 实际: %s", name)
	}

	t.Logf("查询JSON字段成功: %s", name)

	// 使用JSON包含操作符查询
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM json_test WHERE data @> $1", `{"tags": ["json"]}`).Scan(&count)
	if err != nil {
		t.Fatalf("JSON包含查询失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("JSON包含查询验证失败，期望记录数: 1, 实际记录数: %d", count)
	}

	t.Log("JSON包含查询成功")

	// 更新JSON字段
	err = db.Exec("UPDATE json_test SET data = data || $1 WHERE id = 1", `{"email": "test@example.com"}`)
	if err != nil {
		t.Fatalf("更新JSON字段失败: %v", err)
	}

	// 验证更新
	var email string
	err = db.QueryRow("SELECT data->>'email' FROM json_test WHERE id = 1").Scan(&email)
	if err != nil {
		t.Fatalf("验证JSON字段更新失败: %v", err)
	}

	if email != "test@example.com" {
		t.Fatalf("JSON字段更新验证失败，期望: test@example.com, 实际: %s", email)
	}

	t.Log("更新JSON字段成功")
}

// 测试PostgreSQL特有功能：数组操作
func TestPostgresArray(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 创建带数组字段的表
	err := db.Exec(`
		DROP TABLE IF EXISTS array_test;
		CREATE TABLE array_test (
			id SERIAL PRIMARY KEY,
			int_array INTEGER[] NOT NULL,
			text_array TEXT[] NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("创建数组测试表失败: %v", err)
	}

	// 插入数组数据
	err = db.Exec(
		"INSERT INTO array_test (int_array, text_array) VALUES ($1, $2)",
		"{1,2,3,4,5}", "{\"a\",\"b\",\"c\"}",
	)
	if err != nil {
		t.Fatalf("插入数组数据失败: %v", err)
	}

	// 查询数组数据
	var intArray, textArray string
	err = db.QueryRow("SELECT int_array::text, text_array::text FROM array_test WHERE id = 1").Scan(&intArray, &textArray)
	if err != nil {
		t.Fatalf("查询数组数据失败: %v", err)
	}

	t.Logf("查询数组数据成功: int_array=%s, text_array=%s", intArray, textArray)

	// 使用数组包含操作符查询
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM array_test WHERE int_array @> $1", "{3,4}").Scan(&count)
	if err != nil {
		t.Fatalf("数组包含查询失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("数组包含查询验证失败，期望记录数: 1, 实际记录数: %d", count)
	}

	t.Log("数组包含查询成功")

	// 更新数组字段
	err = db.Exec("UPDATE array_test SET int_array = array_append(int_array, 6), text_array = array_append(text_array, 'd') WHERE id = 1")
	if err != nil {
		t.Fatalf("更新数组字段失败: %v", err)
	}

	// 验证更新
	err = db.QueryRow("SELECT int_array::text, text_array::text FROM array_test WHERE id = 1").Scan(&intArray, &textArray)
	if err != nil {
		t.Fatalf("验证数组字段更新失败: %v", err)
	}

	t.Logf("更新数组字段成功: int_array=%s, text_array=%s", intArray, textArray)
}

// 测试PostgreSQL特有功能：全文搜索
func TestPostgresFullTextSearch(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 创建全文搜索测试表
	err := db.Exec(`
		DROP TABLE IF EXISTS fts_test;
		CREATE TABLE fts_test (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			content TEXT NOT NULL,
			ts_vector TSVECTOR
		)
	`)
	if err != nil {
		t.Fatalf("创建全文搜索测试表失败: %v", err)
	}

	// 创建全文搜索索引
	err = db.Exec(`
		CREATE INDEX idx_fts_test_ts_vector ON fts_test USING GIN(ts_vector);
	`)
	if err != nil {
		t.Fatalf("创建全文搜索索引失败: %v", err)
	}

	// 创建更新触发器
	err = db.Exec(`
		CREATE OR REPLACE FUNCTION fts_test_trigger() RETURNS trigger AS $$
		BEGIN
			NEW.ts_vector = to_tsvector('chinese', NEW.title || ' ' || NEW.content);
			RETURN NEW;
		END
		$$ LANGUAGE plpgsql;

		DROP TRIGGER IF EXISTS trig_fts_test_update ON fts_test;
		CREATE TRIGGER trig_fts_test_update BEFORE INSERT OR UPDATE ON fts_test
		FOR EACH ROW EXECUTE FUNCTION fts_test_trigger();
	`)
	if err != nil {
		t.Fatalf("创建全文搜索触发器失败: %v", err)
	}

	// 插入测试数据
	testData := []struct {
		title   string
		content string
	}{
		{"PostgreSQL数据库", "PostgreSQL是一个功能强大的开源对象关系数据库系统"},
		{"全文搜索功能", "PostgreSQL提供了强大的全文搜索功能，支持多种语言"},
		{"JSON数据类型", "PostgreSQL支持JSON和JSONB数据类型，方便存储和查询结构化数据"},
		{"数组数据类型", "PostgreSQL原生支持数组数据类型，可以存储多个值"},
	}

	for _, data := range testData {
		err := db.Exec(
			"INSERT INTO fts_test (title, content) VALUES ($1, $2)",
			data.title, data.content,
		)
		if err != nil {
			t.Fatalf("插入全文搜索测试数据失败: %v", err)
		}
	}

	// 执行全文搜索查询
	rows, err := db.Query(`
		SELECT id, title, content
		FROM fts_test
		WHERE ts_vector @@ to_tsquery('chinese', '数据库')
		ORDER BY id
	`)
	if err != nil {
		t.Fatalf("执行全文搜索查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type FtsResult struct {
		ID      int64
		Title   string
		Content string
	}

	var results []FtsResult
	for rows.Next() {
		var result FtsResult
		err := rows.Scan(&result.ID, &result.Title, &result.Content)
		if err != nil {
			t.Fatalf("扫描全文搜索结果失败: %v", err)
		}
		results = append(results, result)
	}

	// 验证查询结果
	if len(results) == 0 {
		t.Fatalf("全文搜索查询验证失败，未返回任何记录")
	}

	for _, result := range results {
		t.Logf("全文搜索结果: ID=%d, 标题=%s", result.ID, result.Title)
	}

	t.Logf("全文搜索查询成功，记录数: %d", len(results))
}

// 测试PostgreSQL特有功能：递归查询
func TestPostgresRecursiveQuery(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 创建树形结构测试表
	err := db.Exec(`
		DROP TABLE IF EXISTS tree_test;
		CREATE TABLE tree_test (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			parent_id INTEGER,
			CONSTRAINT fk_tree_parent FOREIGN KEY (parent_id) REFERENCES tree_test(id)
		)
	`)
	if err != nil {
		t.Fatalf("创建树形结构测试表失败: %v", err)
	}

	// 插入测试数据
	testData := []struct {
		name     string
		parentID interface{}
	}{
		{"根节点", nil},
		{"一级节点1", 1},
		{"一级节点2", 1},
		{"二级节点1", 2},
		{"二级节点2", 2},
		{"二级节点3", 3},
		{"三级节点1", 4},
	}

	for _, data := range testData {
		err := db.Exec(
			"INSERT INTO tree_test (name, parent_id) VALUES ($1, $2)",
			data.name, data.parentID,
		)
		if err != nil {
			t.Fatalf("插入树形结构测试数据失败: %v", err)
		}
	}

	// 执行递归查询
	rows, err := db.Query(`
		WITH RECURSIVE tree AS (
			SELECT id, name, parent_id, 1 AS level, ARRAY[id] AS path
			FROM tree_test
			WHERE parent_id IS NULL
			UNION ALL
			SELECT t.id, t.name, t.parent_id, tr.level + 1, tr.path || t.id
			FROM tree_test t
			JOIN tree tr ON t.parent_id = tr.id
		)
		SELECT id, name, parent_id, level, path::text
		FROM tree
		ORDER BY path
	`)
	if err != nil {
		t.Fatalf("执行递归查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type TreeNode struct {
		ID       int64
		Name     string
		ParentID interface{}
		Level    int
		Path     string
	}

	var nodes []TreeNode
	for rows.Next() {
		var node TreeNode
		var parentID sql.NullInt64
		err := rows.Scan(&node.ID, &node.Name, &parentID, &node.Level, &node.Path)
		if err != nil {
			t.Fatalf("扫描递归查询结果失败: %v", err)
		}
		if parentID.Valid {
			node.ParentID = parentID.Int64
		} else {
			node.ParentID = nil
		}
		nodes = append(nodes, node)
	}

	// 验证查询结果
	if len(nodes) != len(testData) {
		t.Fatalf("递归查询验证失败，期望记录数: %d, 实际记录数: %d", len(testData), len(nodes))
	}

	for _, node := range nodes {
		t.Logf("递归查询结果: ID=%d, 名称=%s, 级别=%d, 路径=%s", node.ID, node.Name, node.Level, node.Path)
	}

	t.Logf("递归查询成功，记录数: %d", len(nodes))
}

// 测试PostgreSQL特有功能：窗口函数
func TestPostgresWindowFunction(t *testing.T) {
	// 初始化数据库
	db := initPostgresDB(t)
	defer db.Close()

	// 创建测试表
	err := db.Exec(`
		DROP TABLE IF EXISTS sales;
		CREATE TABLE sales (
			id SERIAL PRIMARY KEY,
			product_id INTEGER NOT NULL,
			category_id INTEGER NOT NULL,
			amount DECIMAL(10,2) NOT NULL,
			sale_date DATE NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("创建销售测试表失败: %v", err)
	}

	// 插入测试数据
	for i := 1; i <= 3; i++ { // 3个分类
		for j := 1; j <= 5; j++ { // 每个分类5个产品
			for k := 1; k <= 10; k++ { // 每个产品10条销售记录
				amount := 100.0 + float64(i*10) + float64(j*5) + float64(k)
				date := time.Now().AddDate(0, 0, -k)
				err := db.Exec(
					"INSERT INTO sales (product_id, category_id, amount, sale_date) VALUES ($1, $2, $3, $4)",
					j, i, amount, date.Format("2006-01-02"),
				)
				if err != nil {
					t.Fatalf("插入销售测试数据失败: %v", err)
				}
			}
		}
	}

	// 使用窗口函数查询
	rows, err := db.Query(`
		SELECT 
			category_id,
			product_id,
			SUM(amount) AS total_amount,
			RANK() OVER (PARTITION BY category_id ORDER BY SUM(amount) DESC) AS rank,
			SUM(amount) / SUM(SUM(amount)) OVER (PARTITION BY category_id) * 100 AS percentage
		FROM sales
		GROUP BY category_id, product_id
		ORDER BY category_id, rank
	`)
	if err != nil {
		t.Fatalf("执行窗口函数查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type SalesRank struct {
		CategoryID  int
		ProductID   int
		TotalAmount float64
		Rank        int
		Percentage  float64
	}

	var results []SalesRank
	for rows.Next() {
		var result SalesRank
		err := rows.Scan(&result.CategoryID, &result.ProductID, &result.TotalAmount, &result.Rank, &result.Percentage)
		if err != nil {
			t.Fatalf("扫描窗口函数查询结果失败: %v", err)
		}
		results = append(results, result)
	}

	// 验证查询结果
	if len(results) == 0 {
		t.Fatalf("窗口函数查询验证失败，未返回任何记录")
	}

	for _, result := range results {
		t.Logf("窗口函数查询结果: 分类=%d, 产品=%d, 总金额=%.2f, 排名=%d, 百分比=%.2f%%",
			result.CategoryID, result.ProductID, result.TotalAmount, result.Rank, result.Percentage)
	}

	t.Logf("窗口函数查询成功，记录数: %d", len(results))
}

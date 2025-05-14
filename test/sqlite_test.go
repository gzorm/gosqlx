package test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/adapter"
	"github.com/gzorm/gosqlx/query"
)

// 用户结构体
type SQLiteUser struct {
	ID        int64     `db:"id"`
	Username  string    `db:"username"`
	Email     string    `db:"email"`
	Age       int       `db:"age"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
}

// 文章结构体
type SQLiteArticle struct {
	ID        int64     `db:"id"`
	UserID    int64     `db:"user_id"`
	Title     string    `db:"title"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
}

// 用户文章关联结构体
type SQLiteUserArticle struct {
	UserID       int64  `db:"user_id"`
	Username     string `db:"username"`
	ArticleID    int64  `db:"article_id"`
	ArticleTitle string `db:"article_title"`
}

// 初始化SQLite数据库
func initSQLiteDB(t *testing.T) *gosqlx.Database {
	// 创建临时数据库文件
	dbFile := fmt.Sprintf("./sqlite_test_%d.db", time.Now().UnixNano())

	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.SQLite,
		Driver:      "sqlite3",
		Source:      dbFile,
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "sqlite_test",
		Mode:    "rw",
		DBType:  gosqlx.SQLite,
		Timeout: time.Second * 10,
	}

	// 创建数据库实例
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("连接SQLite数据库失败: %v", err)
	}

	// 注册清理函数，测试结束后删除数据库文件
	t.Cleanup(func() {
		db.Close()
		os.Remove(dbFile)
	})

	return db
}

// 准备测试表
func prepareSQLiteTestTables(t *testing.T, db *gosqlx.Database) {
	// 删除已存在的表
	err := db.Exec("DROP TABLE IF EXISTS articles")
	if err != nil {
		t.Logf("删除articles表失败: %v", err)
	}

	err = db.Exec("DROP TABLE IF EXISTS users")
	if err != nil {
		t.Logf("删除users表失败: %v", err)
	}

	// 创建用户表
	err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER DEFAULT 0,
			active INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("创建users表失败: %v", err)
	}

	// 创建文章表
	err = db.Exec(`
		CREATE TABLE articles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			content TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)
	`)
	if err != nil {
		t.Fatalf("创建articles表失败: %v", err)
	}

	t.Log("测试表准备完成")
}

// 测试基本的CRUD操作
func TestSQLiteCRUD(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 测试插入
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"testuser", "test@example.com", 25, 1,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}

	// 获取自增ID
	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	t.Logf("插入用户成功，ID: %d", userID)

	// 测试查询
	var user SQLiteUser
	err = db.QueryRow(
		"SELECT id, username, email, age, active, created_at FROM users WHERE id = ?",
		userID,
	).Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active, &user.CreatedAt)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}

	t.Logf("查询用户成功: %+v", user)

	// 测试更新
	err = db.Exec(
		"UPDATE users SET age = ?, email = ? WHERE id = ?",
		30, "updated@example.com", userID,
	)
	if err != nil {
		t.Fatalf("更新用户失败: %v", err)
	}

	// 验证更新
	var age int
	var email string
	err = db.QueryRow(
		"SELECT age, email FROM users WHERE id = ?",
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
	err = db.Exec("DELETE FROM users WHERE id = ?", userID)
	if err != nil {
		t.Fatalf("删除用户失败: %v", err)
	}

	// 验证删除
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
	if err != nil {
		t.Fatalf("验证删除失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("删除验证失败，期望记录数: 0, 实际记录数: %d", count)
	}

	t.Log("删除用户成功")
}

// 测试批量插入
func TestSQLiteBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 开始事务
	tx := db.Begin()

	// 批量插入用户
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("batch%d", i)
		email := fmt.Sprintf("batch%d@example.com", i)

		err := tx.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, i%2,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("批量插入用户失败: %v", err)
		}
	}

	// 提交事务
	err := tx.Commit()
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
func TestSQLitePagination(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, i%2,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 分页查询
	page := 2
	pageSize := 5
	offset := (page - 1) * pageSize

	// SQLite分页查询
	rows, err := db.Query(`
		SELECT id, username, email, age, active, created_at
		FROM users
		ORDER BY id
		LIMIT ? OFFSET ?
	`, pageSize, offset)
	if err != nil {
		t.Fatalf("分页查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var users []SQLiteUser
	for rows.Next() {
		var user SQLiteUser
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
func TestSQLiteJoin(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入用户数据
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"joinuser", "join@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 获取用户ID
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

	// 关联查询
	rows, err := db.Query(`
		SELECT u.id AS user_id, u.username, a.id AS article_id, a.title AS article_title
		FROM users u
		LEFT JOIN articles a ON u.id = a.user_id
		WHERE u.id = ?
		ORDER BY a.id
	`, userID)
	if err != nil {
		t.Fatalf("关联查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var userArticles []SQLiteUserArticle
	for rows.Next() {
		var ua SQLiteUserArticle
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
func TestSQLiteTransaction(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 测试提交事务
	t.Run("Commit", func(t *testing.T) {
		// 开始事务
		tx := db.Begin()

		// 插入用户
		result, err := tx.ExecWithResult(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			"txuser1", "tx1@example.com", 25, 1,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 获取用户ID
		userID, err := result.LastInsertId()
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中获取用户ID失败: %v", err)
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
		tx := db.Begin()

		// 插入用户
		result, err := tx.ExecWithResult(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			"txuser2", "tx2@example.com", 30, 1,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 获取用户ID
		userID, err := result.LastInsertId()
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中获取用户ID失败: %v", err)
		}

		// 回滚事务
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("回滚事务失败: %v", err)
		}
		fmt.Println(userID)
		// 验证事务回滚结果
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM users WHERE username = ?", "txuser2").Scan(&count)
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
func TestSQLiteQueryBuilderPagination(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, i%2,
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
	var users []SQLiteUser
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
func TestSQLiteQueryBuilderJoin(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入用户数据
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"joinuser", "join@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 获取用户ID
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

	// 使用Query构建器进行关联查询
	q := query.NewQuery(db.DB())
	q.Table("users u").
		Select("u.id AS user_id", "u.username", "a.id AS article_id", "a.title AS article_title").
		LeftJoin("articles a", "u.id = a.user_id").
		Where("u.id = ?", userID).
		OrderByAsc("a.id")

	// 执行查询
	var userArticles []SQLiteUserArticle
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
func TestSQLiteQueryBuilderAggregate(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, i%2,
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
func TestSQLiteQueryBuilderGroup(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("group%d", i)
		email := fmt.Sprintf("group%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i%5, i%2,
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
		Active int     `db:"active"`
		Count  int     `db:"count"`
		AvgAge float64 `db:"avg_age"`
	}

	var results []GroupResult
	err := q.Get(&results)
	if err != nil {
		t.Fatalf("Query构建器分组查询失败: %v", err)
	}

	// 验证查询结果
	if len(results) != 2 { // 应该有两组：active=0和active=1
		t.Fatalf("Query构建器分组查询验证失败，期望记录数: 2, 实际记录数: %d", len(results))
	}

	for _, result := range results {
		t.Logf("Query构建器分组查询成功，active=%v, 数量=%d, 平均年龄=%.2f", result.Active, result.Count, result.AvgAge)
	}
}

// 测试SQLite特有功能：PRAGMA语句
func TestSQLitePragma(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 获取SQLite适配器
	sqliteAdapter, ok := db.Adapter().(*adapter.SQLite)
	if !ok {
		t.Fatalf("无法获取SQLite适配器")
	}

	// 获取GORM数据库实例
	gormDB, _, err := sqliteAdapter.Connect()
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	// 测试外键约束
	err = sqliteAdapter.EnableForeignKeys(gormDB)
	if err != nil {
		t.Fatalf("启用外键约束失败: %v", err)
	}

	// 验证外键约束
	var foreignKeys int
	err = db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys)
	if err != nil {
		t.Fatalf("查询外键约束状态失败: %v", err)
	}

	if foreignKeys != 1 {
		t.Fatalf("外键约束验证失败，期望: 1, 实际: %d", foreignKeys)
	}

	t.Log("外键约束启用成功")

	// 测试WAL模式
	err = sqliteAdapter.EnableWAL(gormDB)
	if err != nil {
		t.Fatalf("启用WAL模式失败: %v", err)
	}

	// 验证WAL模式
	var journalMode string
	err = db.QueryRow("PRAGMA journal_mode").Scan(&journalMode)
	if err != nil {
		t.Fatalf("查询日志模式失败: %v", err)
	}

	if journalMode != "wal" {
		t.Fatalf("WAL模式验证失败，期望: wal, 实际: %s", journalMode)
	}

	t.Log("WAL模式启用成功")

	// 测试同步模式
	err = sqliteAdapter.SetSynchronous(gormDB, "NORMAL")
	if err != nil {
		t.Fatalf("设置同步模式失败: %v", err)
	}

	// 验证同步模式
	var synchronous int
	err = db.QueryRow("PRAGMA synchronous").Scan(&synchronous)
	if err != nil {
		t.Fatalf("查询同步模式失败: %v", err)
	}

	t.Logf("同步模式设置成功，当前值: %d", synchronous)
}

// 测试SQLite特有功能：VACUUM
func TestSQLiteVacuum(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入和删除一些数据以产生碎片
	for i := 1; i <= 100; i++ {
		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			fmt.Sprintf("vacuum%d", i), fmt.Sprintf("vacuum%d@example.com", i), 20+i, i%2,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 删除一半的数据
	err := db.Exec("DELETE FROM users WHERE id % 2 = 0")
	if err != nil {
		t.Fatalf("删除测试数据失败: %v", err)
	}

	// 获取数据库文件大小
	dbFile := db.DSN()
	info, err := os.Stat(dbFile)
	if err != nil {
		t.Fatalf("获取数据库文件信息失败: %v", err)
	}
	sizeBeforeVacuum := info.Size()

	// 获取SQLite适配器
	sqliteAdapter, ok := db.Adapter().(*adapter.SQLite)
	if !ok {
		t.Fatalf("无法获取SQLite适配器")
	}

	// 获取GORM数据库实例
	gormDB, _, err := sqliteAdapter.Connect()
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	// 执行VACUUM
	err = sqliteAdapter.Vacuum(gormDB)
	if err != nil {
		t.Fatalf("执行VACUUM失败: %v", err)
	}

	// 获取VACUUM后的数据库文件大小
	info, err = os.Stat(dbFile)
	if err != nil {
		t.Fatalf("获取VACUUM后数据库文件信息失败: %v", err)
	}
	sizeAfterVacuum := info.Size()

	t.Logf("VACUUM执行成功，VACUUM前大小: %d 字节, VACUUM后大小: %d 字节", sizeBeforeVacuum, sizeAfterVacuum)
}

// 测试使用Query构建器进行事务操作
func TestSQLiteQueryBuilderTransaction(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 开始事务
	tx := db.Begin()

	// 在事务中使用Query构建器
	q := query.NewQuery(tx)

	// 插入用户
	err := tx.Exec(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"txbuilder", "txbuilder@example.com", 30, 1,
	)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中插入用户失败: %v", err)
	}

	// 查询插入的用户
	var user SQLiteUser
	err = q.Table("users").
		Select("id", "username", "email").
		Where("username = ?", "txbuilder").
		First(&user)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中查询用户失败: %v", err)
	}

	// 插入文章
	err = tx.Exec(
		"INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
		user.ID, "事务构建器文章", "这是一篇使用事务构建器创建的文章",
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
		Where("user_id = ?", user.ID).
		CountNum()
	if err != nil {
		t.Fatalf("查询文章数量失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("事务验证失败，期望文章数: 1, 实际文章数: %d", count)
	}

	t.Logf("Query构建器事务操作成功，用户ID: %d", user.ID)
}

// 测试SQLite特有功能：批量插入或替换
func TestSQLiteBatchInsertOrReplace(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 获取SQLite适配器
	sqliteAdapter, ok := db.Adapter().(*adapter.SQLite)
	if !ok {
		t.Fatalf("无法获取SQLite适配器")
	}

	// 获取GORM数据库实例
	gormDB, _, err := sqliteAdapter.Connect()
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	// 准备批量插入数据
	columns := []string{"username", "email", "age", "active"}
	values := [][]interface{}{
		{"batch1", "batch1@example.com", 21, 1},
		{"batch2", "batch2@example.com", 22, 0},
		{"batch3", "batch3@example.com", 23, 1},
	}

	// 执行批量插入
	err = sqliteAdapter.BatchInsert(gormDB, "users", columns, values)
	if err != nil {
		t.Fatalf("批量插入失败: %v", err)
	}

	// 验证批量插入结果
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE username LIKE 'batch%'").Scan(&count)
	if err != nil {
		t.Fatalf("验证批量插入失败: %v", err)
	}

	if count != 3 {
		t.Fatalf("批量插入验证失败，期望记录数: 3, 实际记录数: %d", count)
	}

	t.Logf("批量插入成功，记录数: %d", count)

	// 准备批量插入或替换数据（包含已存在的记录）
	values = [][]interface{}{
		{"batch1", "batch1_updated@example.com", 31, 0}, // 已存在，将被替换
		{"batch4", "batch4@example.com", 24, 1},         // 新记录，将被插入
		{"batch5", "batch5@example.com", 25, 0},         // 新记录，将被插入
	}

	// 执行批量插入或替换
	err = sqliteAdapter.BatchInsertOrReplace(gormDB, "users", columns, values)
	if err != nil {
		t.Fatalf("批量插入或替换失败: %v", err)
	}

	// 验证批量插入或替换结果
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE username LIKE 'batch%'").Scan(&count)
	if err != nil {
		t.Fatalf("验证批量插入或替换失败: %v", err)
	}

	if count != 5 {
		t.Fatalf("批量插入或替换验证失败，期望记录数: 5, 实际记录数: %d", count)
	}

	// 验证替换是否成功
	var email string
	err = db.QueryRow("SELECT email FROM users WHERE username = 'batch1'").Scan(&email)
	if err != nil {
		t.Fatalf("验证记录替换失败: %v", err)
	}

	if email != "batch1_updated@example.com" {
		t.Fatalf("记录替换验证失败，期望email: batch1_updated@example.com, 实际email: %s", email)
	}

	t.Logf("批量插入或替换成功，总记录数: %d", count)
}

// 测试SQLite特有功能：获取最后插入ID
func TestSQLiteGetLastInsertID(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 插入用户
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"lastid", "lastid@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}

	// 获取标准方式的最后插入ID
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取最后插入ID失败: %v", err)
	}

	// 获取SQLite适配器
	sqliteAdapter, ok := db.Adapter().(*adapter.SQLite)
	if !ok {
		t.Fatalf("无法获取SQLite适配器")
	}

	// 获取GORM数据库实例
	gormDB, _, err := sqliteAdapter.Connect()
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	// 使用适配器方法获取最后插入ID
	adapterLastID, err := sqliteAdapter.GetLastInsertID(gormDB)
	if err != nil {
		t.Fatalf("使用适配器获取最后插入ID失败: %v", err)
	}

	// 验证两种方式获取的ID是否一致
	if lastID != adapterLastID {
		t.Fatalf("最后插入ID验证失败，标准方式: %d, 适配器方式: %d", lastID, adapterLastID)
	}

	t.Logf("获取最后插入ID成功: %d", lastID)
}

// 测试SQLite特有功能：外键约束
func TestSQLiteForeignKeys(t *testing.T) {
	// 初始化数据库
	db := initSQLiteDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLiteTestTables(t, db)

	// 获取SQLite适配器
	sqliteAdapter, ok := db.Adapter().(*adapter.SQLite)
	if !ok {
		t.Fatalf("无法获取SQLite适配器")
	}

	// 获取GORM数据库实例
	gormDB, _, err := sqliteAdapter.Connect()
	if err != nil {
		t.Fatalf("连接数据库失败: %v", err)
	}

	// 启用外键约束
	err = sqliteAdapter.EnableForeignKeys(gormDB)
	if err != nil {
		t.Fatalf("启用外键约束失败: %v", err)
	}

	// 插入用户
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"fkuser", "fk@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}

	// 获取用户ID
	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	// 插入文章
	err = db.Exec(
		"INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
		userID, "外键测试文章", "这是一篇测试外键约束的文章",
	)
	if err != nil {
		t.Fatalf("插入文章失败: %v", err)
	}

	// 尝试删除用户（应该失败，因为有关联的文章）
	err = db.Exec("DELETE FROM users WHERE id = ?", userID)
	if err == nil {
		t.Fatalf("删除用户应该失败，但实际成功了")
	}

	t.Logf("外键约束测试成功，删除用户失败（预期行为）: %v", err)

	// 禁用外键约束
	err = sqliteAdapter.DisableForeignKeys(gormDB)
	if err != nil {
		t.Fatalf("禁用外键约束失败: %v", err)
	}

	// 再次尝试删除用户（应该成功，因为外键约束已禁用）
	err = db.Exec("DELETE FROM users WHERE id = ?", userID)
	if err != nil {
		t.Fatalf("禁用外键约束后删除用户失败: %v", err)
	}

	t.Log("禁用外键约束后删除用户成功")
}

// 测试SQLite适配器的DSN构建
func TestSQLiteBuildDSN(t *testing.T) {
	// 创建SQLite适配器
	sqliteAdapter := adapter.NewSQLite("test.db")

	// 测试基本DSN
	dsn := sqliteAdapter.BuildDSN("test.db", nil)
	if dsn != "test.db" {
		t.Fatalf("基本DSN构建失败，期望: test.db, 实际: %s", dsn)
	}

	// 测试带参数的DSN
	params := map[string]string{
		"mode":          "ro",
		"cache":         "shared",
		"_foreign_keys": "1",
	}
	dsn = sqliteAdapter.BuildDSN("test.db", params)

	// 验证DSN包含所有参数
	if !strings.Contains(dsn, "test.db?") {
		t.Fatalf("带参数的DSN构建失败，不包含基本路径和问号: %s", dsn)
	}

	for k, v := range params {
		if !strings.Contains(dsn, fmt.Sprintf("%s=%s", k, v)) {
			t.Fatalf("带参数的DSN构建失败，不包含参数 %s=%s: %s", k, v, dsn)
		}
	}

	t.Logf("DSN构建成功: %s", dsn)
}

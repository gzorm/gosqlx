package test

import (
	"context"
	"fmt"

	"testing"
	"time"

	"github.com/gzorm/gosqlx/adapter"
	"github.com/gzorm/gosqlx/dialect"
	"github.com/stretchr/testify/assert"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/builder"
	"github.com/gzorm/gosqlx/query"
)

// 测试用的用户模型
type User struct {
	ID        int64     `db:"id"`
	Username  string    `db:"username"`
	Email     string    `db:"email"`
	Age       int       `db:"age"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// 测试用的文章模型
type Article struct {
	ID        int64     `db:"id"`
	UserID    int64     `db:"user_id"`
	Title     string    `db:"title"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// 用户文章关联查询结果
type UserArticle struct {
	UserID       int64  `db:"user_id"`
	Username     string `db:"username"`
	ArticleID    int64  `db:"article_id"`
	ArticleTitle string `db:"article_title"`
}

// 初始化数据库连接
func initMySQLDB(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.MySQL,
		Driver:      "mysql",
		Source:      "root:root@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local",
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "test_mysql",
		Mode:    "rw",
		DBType:  gosqlx.MySQL,
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
func prepareTestTables(t *testing.T, db *gosqlx.Database) {
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
func TestMySQLInsert(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	prepareTestTables(t, db)

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
func TestMySQLBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

// 测试查询单条记录
func TestMySQLQueryRow(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

// 测试查询多条记录
func TestMySQLQuery(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

// 测试更新操作
func TestMySQLUpdate(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

// 测试删除操作
func TestMySQLDelete(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

	// 插入测试数据
	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"deleteuser", "delete@example.com", 40, true,
	)
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	// 执行删除
	result, err = db.ExecWithResult("DELETE FROM users WHERE id = ?", userID)
	if err != nil {
		t.Fatalf("删除用户失败: %v", err)
	}

	// 获取影响行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("获取影响行数失败: %v", err)
	}

	if rowsAffected != 1 {
		t.Fatalf("删除用户验证失败，期望影响行数: 1, 实际影响行数: %d", rowsAffected)
	}

	// 验证删除结果
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
	if err != nil {
		t.Fatalf("查询删除后的用户失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("删除结果验证失败，期望记录数: 0, 实际记录数: %d", count)
	}

	t.Logf("删除用户成功，ID: %d", userID)
}

// 测试分页查询
func TestMySQLPagination(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

// 测试关联查询
func TestMySQLJoinQuery(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

// 测试事务操作
func TestMySQLTransaction(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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
	//// 开始事务
	//tx, err := db.Begin()
	//if err != nil {
	//	t.Fatalf("开始事务失败: %v", err)
	//}
	//
	//// 在事务中插入用户
	//result, err := tx.ExecWithResult(
	//	"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
	//	"txuser", "tx@example.com", 35, true,
	//)
	//if err != nil {
	//	tx.Rollback()
	//	t.Fatalf("事务中插入用户失败: %v", err)
	//}
	//
	//userID, err := result.LastInsertId()
	//if err != nil {
	//	tx.Rollback()
	//	t.Fatalf("获取用户ID失败: %v", err)
	//}
	//
	//// 在事务中插入文章
	//err = tx.Exec(
	//	"INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
	//	userID, "事务文章", "这是一篇在事务中创建的文章",
	//)
	//if err != nil {
	//	tx.Rollback()
	//	t.Fatalf("事务中插入文章失败: %v", err)
	//}
	//
	//// 提交事务
	//err = tx.Commit()
	//if err != nil {
	//	t.Fatalf("提交事务失败: %v", err)
	//}

	//// 验证事务结果
	//var userCount, articleCount int
	//err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&userCount)
	//if err != nil {
	//	t.Fatalf("查询用户记录失败: %v", err)
	//}
	//
	//err = db.QueryRow("SELECT COUNT(*) FROM articles WHERE user_id = ?", userID).Scan(&articleCount)
	//if err != nil {
	//	t.Fatalf("查询文章记录失败: %v", err)
	//}
	//
	//if userCount != 1 || articleCount != 1 {
	//	t.Fatalf("事务验证失败，用户记录数: %d, 文章记录数: %d", userCount, articleCount)
	//}
	//
	//t.Logf("事务操作成功，用户ID: %d", userID)
}

// 测试事务回滚
func TestMySQLTransactionRollback(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

	// 开始事务
	tx := db.Begin()

	// 在事务中插入用户
	result, err := tx.ExecWithResult(
		"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"rollbackuser", "rollback@example.com", 40, true,
	)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("事务中插入用户失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("获取用户ID失败: %v", err)
	}

	// 回滚事务
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("回滚事务失败: %v", err)
	}

	// 验证回滚结果
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE id = ?", userID).Scan(&count)
	if err != nil {
		t.Fatalf("查询用户记录失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("事务回滚验证失败，期望记录数: 0, 实际记录数: %d", count)
	}

	t.Logf("事务回滚成功")
}

// 测试使用Query构建器进行查询
func TestMySQLQueryBuilder(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

// 测试使用Query构建器进行分页查询
func TestMySQLQueryBuilderPagination(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	prepareTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
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
	var users []User
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
func TestMySQLQueryBuilderJoin(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	prepareTestTables(t, db)

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

	// 使用Query构建器进行关联查询
	q := query.NewQuery(db.DB())
	q.Table("users u").
		Select("u.id AS user_id", "u.username", "a.id AS article_id", "a.title AS article_title").
		LeftJoin("articles a", "u.id = a.user_id").
		Where("u.id = ?", userID).
		OrderByAsc("a.id")

	// 执行查询
	var userArticles []UserArticle
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
func TestMySQLQueryBuilderAggregate(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	prepareTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
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
func TestMySQLQueryBuilderGroup(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	prepareTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("group%d", i)
		email := fmt.Sprintf("group%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
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

// 测试使用Query构建器进行复杂条件查询
func TestMySQLQueryBuilderComplexWhere(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	prepareTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("complex%d", i)
		email := fmt.Sprintf("complex%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, i%3 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用Query构建器进行复杂条件查询
	q := query.NewQuery(db.DB())
	q.Table("users").
		Select("id", "username", "email", "age", "active").
		Where("age > ?", 25).
		WhereIn("age", []int{26, 28, 30, 32, 34}).
		WhereIf(true, "active = ?", true).
		WhereIf(false, "username LIKE ?", "not%").
		OrderByDesc("id").
		Limit(5)

	// 执行查询
	var users []User
	err := q.Get(&users)
	if err != nil {
		t.Fatalf("Query构建器复杂条件查询失败: %v", err)
	}

	// 验证查询结果
	if len(users) == 0 {
		t.Fatalf("Query构建器复杂条件查询验证失败，未返回任何记录")
	}

	t.Logf("Query构建器复杂条件查询成功，记录数: %d", len(users))

	// 测试条件组
	q = query.NewQuery(db.DB())
	q.Table("users").
		Select("id", "username", "email", "age", "active").
		Where("age > ?", 25).
		WhereGroup(func(w *builder.Where) {
			w.Where("active = ?", true).
				Or("age > ?", 35)
		}).
		OrderByDesc("id").
		Limit(5)

	// 执行查询
	users = nil
	err = q.Get(&users)
	if err != nil {
		t.Fatalf("Query构建器条件组查询失败: %v", err)
	}

	t.Logf("Query构建器条件组查询成功，记录数: %d", len(users))
}

// 测试使用Query构建器进行子查询
func TestMySQLQueryBuilderSubquery(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	prepareTestTables(t, db)

	// 插入用户数据
	for i := 1; i <= 5; i++ {
		username := fmt.Sprintf("subquery%d", i)
		email := fmt.Sprintf("subquery%d@example.com", i)

		result, err := db.ExecWithResult(
			"INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
			username, email, 20+i, true,
		)
		if err != nil {
			t.Fatalf("插入用户数据失败: %v", err)
		}

		userID, _ := result.LastInsertId()

		// 为每个用户插入文章
		for j := 1; j <= i; j++ {
			err := db.Exec(
				"INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
				userID, fmt.Sprintf("文章%d-%d", i, j), fmt.Sprintf("这是用户%d的第%d篇文章", i, j),
			)
			if err != nil {
				t.Fatalf("插入文章数据失败: %v", err)
			}
		}
	}

	// 使用子查询查找有2篇以上文章的用户
	q := query.NewQuery(db.DB())
	q.Table("users u").
		Select("u.id", "u.username", "u.email", "u.age").
		WhereRaw("u.id IN (SELECT user_id FROM articles GROUP BY user_id HAVING COUNT(*) > 2)").
		OrderByAsc("u.id")

	// 执行查询
	var users []User
	err := q.Get(&users)
	if err != nil {
		t.Fatalf("Query构建器子查询失败: %v", err)
	}

	// 验证查询结果
	if len(users) == 0 {
		t.Fatalf("Query构建器子查询验证失败，未返回任何记录")
	}

	t.Logf("Query构建器子查询成功，记录数: %d", len(users))

	// 查询每个用户的文章数量
	q = query.NewQuery(db.DB())
	q.Table("users u").
		SelectRaw("u.id, u.username, (SELECT COUNT(*) FROM articles a WHERE a.user_id = u.id) as article_count").
		OrderByAsc("u.id")

	// 执行查询
	type UserArticleCount struct {
		ID           int64  `db:"id"`
		Username     string `db:"username"`
		ArticleCount int    `db:"article_count"`
	}

	var userCounts []UserArticleCount
	err = q.Get(&userCounts)
	if err != nil {
		t.Fatalf("Query构建器子查询计数失败: %v", err)
	}

	for _, uc := range userCounts {
		t.Logf("用户: %s, 文章数: %d", uc.Username, uc.ArticleCount)
	}
}

// 测试使用Query构建器进行事务操作
func TestMySQLQueryBuilderTransaction(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

	// 开始事务
	tx := db.Begin()

	// 在事务中使用Query构建器
	q := query.NewQuery(tx)

	// 插入用户
	err := db.Exec("INSERT INTO users (username, email, age, active) VALUES (?, ?, ?, ?)",
		"txbuilder", "txbuilder@example.com", 30, true)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("事务中插入用户失败: %v", err)
	}

	// 查询插入的用户
	var user User
	err = q.Table("users").
		Select("id", "username", "email").
		Where("username = ?", "txbuilder").First(&user)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("事务中查询用户失败: %v", err)
	}

	// 插入文章
	err = db.
		Exec("INSERT INTO articles (user_id, title, content) VALUES (?, ?, ?)",
			user.ID, "事务构建器文章", "这是一篇使用事务构建器创建的文章")
	if err != nil {
		_ = tx.Rollback()
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

// TestMySQLAdapterQueryPage 测试 MySQL 适配器的 QueryPage 方法
func TestMySQLAdapterQueryPage(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表
	//prepareTestTables(t, db)

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

	// 获取 MySQL 适配器
	mysqlAdapter, ok := db.Adapter().(*adapter.MySQL)
	if !ok {
		t.Fatalf("无法获取 MySQL 适配器")
	}
	// 测试用例
	testCases := []struct {
		name       string
		page       int
		pageSize   int
		tableName  string
		orderBy    []interface{}
		filter     []interface{}
		expectLen  int
		expectErr  bool
		expectDesc string
	}{
		{
			name:       "基本分页查询",
			page:       1,
			pageSize:   5,
			tableName:  "users",
			orderBy:    []interface{}{"id ASC"},
			filter:     nil,
			expectLen:  5,
			expectErr:  false,
			expectDesc: "应返回前5条记录",
		},
		{
			name:       "字符串条件查询",
			page:       1,
			pageSize:   10,
			tableName:  "users",
			orderBy:    []interface{}{"id DESC"},
			filter:     []interface{}{"age > 30"},
			expectLen:  10,
			expectErr:  false,
			expectDesc: "应返回年龄大于30的记录",
		},
		{
			name:       "Map条件查询",
			page:       1,
			pageSize:   10,
			tableName:  "users",
			orderBy:    []interface{}{"username ASC"},
			filter:     []interface{}{map[string]interface{}{"active": true}},
			expectLen:  10,
			expectErr:  false,
			expectDesc: "应返回active为true的记录",
		},
		{
			name:       "参数化查询",
			page:       1,
			pageSize:   10,
			tableName:  "users",
			orderBy:    []interface{}{"age ASC"},
			filter:     []interface{}{"age > ?", 35},
			expectLen:  5,
			expectErr:  false,
			expectDesc: "应返回年龄大于35的记录",
		},
		{
			name:       "完整SQL查询",
			page:       1,
			pageSize:   10,
			tableName:  "users",
			orderBy:    []interface{}{"id ASC"},
			filter:     []interface{}{"SELECT * FROM users WHERE email LIKE ?", "%page1%"},
			expectLen:  2,
			expectErr:  false,
			expectDesc: "应返回email包含page1的记录",
		},
		{
			name:       "切片条件查询",
			page:       1,
			pageSize:   10,
			tableName:  "users",
			orderBy:    []interface{}{"id ASC"},
			filter:     []interface{}{[]interface{}{"age > ?", 30}},
			expectLen:  10,
			expectErr:  false,
			expectDesc: "应正确处理切片类型的条件",
		},
		{
			name:       "无效表名",
			page:       1,
			pageSize:   10,
			tableName:  "",
			orderBy:    nil,
			filter:     nil,
			expectLen:  0,
			expectErr:  true,
			expectDesc: "表名为空应返回错误",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var results []User
			total, err := mysqlAdapter.QueryPage(db.DB(), &results, tc.page, tc.pageSize, tc.tableName, tc.orderBy, tc.filter...)

			if tc.expectErr {
				assert.Error(t, err, tc.expectDesc)
			} else {
				assert.NoError(t, err, tc.expectDesc)
				assert.Equal(t, tc.expectLen, len(results), tc.expectDesc)
				assert.Greater(t, total, int64(0), "总记录数应大于0")
			}
		})
	}
}

func TestQuery(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	mysqlDialect := dialect.GetDialect("mysql")
	assert.NotNil(t, mysqlDialect, "MySQL 方言不应为空")

	// 2. 调用 GetTableSchemaSQL 方法
	tableName := "users"
	schemaSQL := mysqlDialect.GetTableSchemaSQL(tableName)

	//rows, err := db.Query(schemaSQL)
	//if err != nil {
	//	t.Fatal(err)
	//	return
	//}
	// 定义结构体来接收结果
	var columns []struct {
		Field      string      `db:"Field"`
		Type       string      `db:"Type"`
		Collation  string      `db:"Collation"`
		Null       string      `db:"Null"`
		Key        string      `db:"Key"`
		Default    interface{} `db:"Default"`
		Extra      string      `db:"Extra"`
		Privileges string      `db:"Privileges"`
		Comment    string      `db:"Comment"`
	}

	// 使用 ScanRaw 方法执行查询
	err := db.ScanRaw(&columns, schemaSQL)
	if err != nil {
		t.Fatalf("执行表结构 SQL 失败: %v", err)
	}

	// 打印结果
	for _, col := range columns {
		t.Logf("字段: %s, 类型: %s, 可空: %s, 键: %s, 默认值: %v, 额外: %s, 注释: %s",
			col.Field, col.Type, col.Null, col.Key, col.Default, col.Extra, col.Comment)
	}

}

// 复杂查询结果结构体
type UserWithArticleCount struct {
	ID           int64  `db:"id"`
	Username     string `db:"username"`
	Email        string `db:"email"`
	Age          int    `db:"age"`
	ArticleCount int    `db:"article_count"`
}

// 测试 QueryPage 函数处理复杂子查询
func TestMySQLQueryPageWithSubquery(t *testing.T) {
	// 初始化数据库
	db := initMySQLDB(t)
	defer db.Close()

	// 准备测试表和数据
	//prepareTestTables(t, db)
	//prepareTestData(t, db)

	// 获取 MySQL 适配器
	mysqlAdapter, ok := db.Adapter().(*adapter.MySQL)
	if !ok {
		t.Fatalf("无法获取 MySQL 适配器")
	}

	subquery := `
		SELECT 
			u.id, u.username, u.email, u.age, COUNT(a.id) AS article_count
		FROM 
			users u
		LEFT JOIN 
			articles a ON u.id = a.user_id
		WHERE 
			u.age > ?
		GROUP BY 
			u.id
		HAVING 
			COUNT(a.id) >= ?
	`

	var results []UserWithArticleCount
	page := 1
	pageSize := 10

	total, err := mysqlAdapter.QueryPage(db.DB(),
		&results,
		page,
		pageSize,
		"("+subquery+") AS subq", // 作为表名传入
		nil,                      // filter 传 nil
		20, 1,                    // SQL参数
	)
	if err != nil {
		t.Fatalf("执行 QueryPage 失败: %v", err)
	}

	// 验证结果
	t.Logf("总记录数: %d", total)
	t.Logf("查询结果: %+v", results)

	// 断言结果不为空
	assert.Greater(t, len(results), 0, "应该至少有一条记录")
	assert.Equal(t, total, int64(len(results)), "总记录数应该与结果数量一致")

	// 验证每个结果的文章数量至少为1
	for _, user := range results {
		assert.GreaterOrEqual(t, user.ArticleCount, 1, "每个用户的文章数量应该至少为1")
		assert.Greater(t, user.Age, 20, "每个用户的年龄应该大于20")
	}
}

// 准备测试数据
func prepareTestData(t *testing.T, db *gosqlx.Database) {
	// 插入测试用户
	users := []User{
		{Username: "user1", Email: "user1@example.com", Age: 21, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user2", Email: "user2@example.com", Age: 22, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user3", Email: "user3@example.com", Age: 23, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user4", Email: "user4@example.com", Age: 24, Active: false, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user5", Email: "user5@example.com", Age: 25, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user6", Email: "user6@example.com", Age: 19, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	// 批量插入用户
	for _, user := range users {
		result, err := db.ExecWithResult(
			"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
			user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
		)
		if err != nil {
			t.Fatalf("插入用户失败: %v", err)
		}

		userID, _ := result.LastInsertId()

		// 为每个用户插入不同数量的文章
		articleCount := int(user.Age - 20)
		if articleCount < 0 {
			articleCount = 0
		}

		for i := 0; i < articleCount; i++ {
			_, err := db.ExecWithResult(
				"INSERT INTO articles (user_id, title, content, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
				userID,
				fmt.Sprintf("%s的文章 #%d", user.Username, i+1),
				fmt.Sprintf("这是%s的第%d篇文章内容", user.Username, i+1),
				time.Now(),
				time.Now(),
			)
			if err != nil {
				t.Fatalf("插入文章失败: %v", err)
			}
		}
	}
}

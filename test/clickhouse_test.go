package test

import (
	"context"
	"fmt"
	"github.com/gzorm/gosqlx/builder"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/query"
)

// 初始化ClickHouse数据库连接
func initClickHouseDB(t *testing.T) *gosqlx.Database {
	// 配置ClickHouse连接
	config := &gosqlx.Config{
		Type:        gosqlx.ClickHouse,
		Driver:      "clickhouse",
		Source:      "clickhouse://default:@localhost:9000/test?dial_timeout=10s&max_execution_time=60",
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: 3600,
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

	// 准备测试表
	prepareClickHouseTestTables(t, db)

	return db
}

// 准备ClickHouse测试表
func prepareClickHouseTestTables(t *testing.T, db *gosqlx.Database) {
	// 删除已存在的表
	dropTables := []string{
		"DROP TABLE IF EXISTS articles",
		"DROP TABLE IF EXISTS users",
	}

	for _, sql := range dropTables {
		if err := db.Exec(sql); err != nil {
			t.Fatalf("删除表失败: %v", err)
		}
	}

	// 创建用户表 - 使用ReplacingMergeTree引擎
	createUserTable := `
	CREATE TABLE users (
		id UInt64,
		username String,
		email String,
		age UInt8,
		active UInt8,
		created_at DateTime DEFAULT now(),
		updated_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (id)
	`

	if err := db.Exec(createUserTable); err != nil {
		t.Fatalf("创建用户表失败: %v", err)
	}

	// 创建文章表 - 使用ReplacingMergeTree引擎
	createArticleTable := `
	CREATE TABLE articles (
		id UInt64,
		user_id UInt64,
		title String,
		content String,
		created_at DateTime DEFAULT now(),
		updated_at DateTime DEFAULT now()
	) ENGINE = ReplacingMergeTree()
	ORDER BY (id)
	`

	if err := db.Exec(createArticleTable); err != nil {
		t.Fatalf("创建文章表失败: %v", err)
	}
}

// 测试ClickHouse基本查询
func TestClickHouseQuery(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入测试数据
	usernames := []string{"user1", "user2", "user3", "user4", "user5"}
	for i, username := range usernames {
		err := db.Exec(
			"INSERT INTO users (id, username, email, age, active) VALUES (?, ?, ?, ?, ?)",
			i+1, username, username+"@example.com", 20+len(username), 1,
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

// 测试ClickHouse查询构建器
func TestClickHouseQueryBuilder(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("builder%d", i)
		email := fmt.Sprintf("builder%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (id, username, email, age, active) VALUES (?, ?, ?, ?, ?)",
			i, username, email, 20+i, i%2,
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
		Where("active = ?", 1).
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

// 测试ClickHouse关联查询
func TestClickHouseJoinQuery(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入用户数据
	userID := uint64(1)
	err := db.Exec(
		"INSERT INTO users (id, username, email, age, active) VALUES (?, ?, ?, ?, ?)",
		userID, "joinuser", "join@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for i, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO articles (id, user_id, title, content) VALUES (?, ?, ?, ?)",
			i+1, userID, title, "这是"+title+"的内容",
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

// 测试ClickHouse查询构建器关联查询
func TestClickHouseQueryBuilderJoin(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入用户数据
	userID := uint64(1)
	err := db.Exec(
		"INSERT INTO users (id, username, email, age, active) VALUES (?, ?, ?, ?, ?)",
		userID, "joinuser", "join@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for i, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO articles (id, user_id, title, content) VALUES (?, ?, ?, ?)",
			i+1, userID, title, "这是"+title+"的内容",
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

// 测试ClickHouse查询构建器聚合函数
func TestClickHouseQueryBuilderAggregate(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (id, username, email, age, active) VALUES (?, ?, ?, ?, ?)",
			i, username, email, 20+i, i%2,
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

// 测试ClickHouse查询构建器分组查询
func TestClickHouseQueryBuilderGroup(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("group%d", i)
		email := fmt.Sprintf("group%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (id, username, email, age, active) VALUES (?, ?, ?, ?, ?)",
			i, username, email, 20+i%5, i%2,
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
		Active uint8   `db:"active"`
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

// 测试ClickHouse查询构建器复杂条件
func TestClickHouseQueryBuilderComplexWhere(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("complex%d", i)
		email := fmt.Sprintf("complex%d@example.com", i)

		err := db.Exec(
			"INSERT INTO users (id, username, email, age, active) VALUES (?, ?, ?, ?, ?)",
			i, username, email, 20+i, i%3,
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
		WhereIf(true, "active = ?", 1).
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
			w.Where("active = ?", 1).
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

// 测试ClickHouse批量插入
func TestClickHouseBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 准备批量插入数据
	users := []map[string]interface{}{
		{"id": 101, "username": "batch1", "email": "batch1@example.com", "age": 21, "active": 1},
		{"id": 102, "username": "batch2", "email": "batch2@example.com", "age": 22, "active": 0},
		{"id": 103, "username": "batch3", "email": "batch3@example.com", "age": 23, "active": 1},
		{"id": 104, "username": "batch4", "email": "batch4@example.com", "age": 24, "active": 0},
		{"id": 105, "username": "batch5", "email": "batch5@example.com", "age": 25, "active": 1},
	}

	// 执行批量插入
	columns := []string{"id", "username", "email", "age", "active"}
	var values [][]interface{}
	for _, user := range users {
		row := make([]interface{}, len(columns))
		for i, col := range columns {
			row[i] = user[col]
		}
		values = append(values, row)
	}

	// 使用ClickHouse适配器的批量插入方法
	err := db.BatchInsert("users", columns, values)
	if err != nil {
		t.Fatalf("批量插入失败: %v", err)
	}

	// 验证批量插入结果
	q := query.NewQuery(db.DB())
	count, err := q.Table("users").Where("id >= ?", 101).Where("id <= ?", 105).CountNum()
	if err != nil {
		t.Fatalf("查询批量插入结果失败: %v", err)
	}

	if int(count) != len(users) {
		t.Fatalf("批量插入验证失败，期望记录数: %d, 实际记录数: %d", len(users), count)
	}

	t.Logf("批量插入成功，记录数: %d", count)
}

// 测试ClickHouse数据类型转换
func TestClickHouseDataTypeConversion(t *testing.T) {
	// 初始化数据库
	db := initClickHouseDB(t)
	defer db.Close()

	// 插入一条包含各种数据类型的记录
	now := time.Now()
	err := db.Exec(
		"INSERT INTO users (id, username, email, age, active, created_at) VALUES (?, ?, ?, ?, ?, ?)",
		1000, "typetest", "type@example.com", 30, 1, now,
	)
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	// 查询并验证数据类型
	var user User
	err = db.QueryRow(
		"SELECT id, username, email, age, active, created_at FROM users WHERE id = ?",
		1000,
	).Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active, &user.CreatedAt)
	if err != nil {
		t.Fatalf("查询测试数据失败: %v", err)
	}

	// 验证数据类型转换
	assert.Equal(t, uint64(1000), user.ID, "ID类型转换错误")
	assert.Equal(t, "typetest", user.Username, "字符串类型转换错误")
	assert.Equal(t, "type@example.com", user.Email, "字符串类型转换错误")
	assert.Equal(t, uint8(30), user.Age, "整数类型转换错误")
	assert.Equal(t, true, user.Active, "布尔类型转换错误")
	assert.WithinDuration(t, now, user.CreatedAt, time.Second, "时间类型转换错误")

	t.Logf("数据类型转换测试成功")
}

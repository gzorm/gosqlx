package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/builder"
	"github.com/gzorm/gosqlx/query"
)

// 测试用的用户模型
type MongoUser struct {
	ID        int64     `db:"_id"`
	Username  string    `db:"username"`
	Email     string    `db:"email"`
	Age       int       `db:"age"`
	Active    bool      `db:"active"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// 测试用的文章模型
type MongoArticle struct {
	ID        string    `db:"_id"`
	UserID    int64     `db:"user_id"`
	Title     string    `db:"title"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// 用户文章关联查询结果
type MongoUserArticle struct {
	UserID       string `db:"user_id"`
	Username     string `db:"username"`
	ArticleID    string `db:"article_id"`
	ArticleTitle string `db:"article_title"`
}

// 初始化数据库连接
func initMongoDB(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.MongoDB,
		Driver:      "mongodb",
		Source:      "mongodb://localhost:27017/testdb",
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "test_mongodb",
		Mode:    "rw",
		DBType:  gosqlx.MongoDB,
		Timeout: time.Second * 10,
	}

	// 创建数据库实例
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("初始化数据库失败: %v", err)
	}

	return db
}

// 准备测试集合
func prepareMongoTestCollections(t *testing.T, db *gosqlx.Database) {
	// 清空用户集合
	err := db.Exec("db.users.drop()")
	if err != nil {
		t.Logf("删除用户集合失败: %v", err)
	}

	// 清空文章集合
	err = db.Exec("db.articles.drop()")
	if err != nil {
		t.Logf("删除文章集合失败: %v", err)
	}

	// 创建用户集合索引
	err = db.Exec("db.users.createIndex({username: 1}, {unique: true})")
	if err != nil {
		t.Fatalf("创建用户集合索引失败: %v", err)
	}

	// 创建文章集合索引
	err = db.Exec("db.articles.createIndex({user_id: 1})")
	if err != nil {
		t.Fatalf("创建文章集合索引失败: %v", err)
	}
}

// 测试插入操作
func TestMongoInsert(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 创建用户
	user := &MongoUser{
		Username:  "testuser",
		Email:     "test@example.com",
		Age:       25,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// 执行插入
	result, err := db.ExecWithResult(
		"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
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

	t.Logf("插入用户成功，ID: %s", lastID)

	// 验证插入结果
	var count int
	err = db.ScanRaw(&count, "db.users.count({_id: ObjectId(?)})", lastID)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("插入用户验证失败，期望记录数: 1, 实际记录数: %d", count)
	}
}

// 测试批量插入
func TestMongoBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 准备批量插入数据
	users := []MongoUser{
		{Username: "user1", Email: "user1@example.com", Age: 21, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user2", Email: "user2@example.com", Age: 22, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user3", Email: "user3@example.com", Age: 23, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user4", Email: "user4@example.com", Age: 24, Active: false, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user5", Email: "user5@example.com", Age: 25, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	// 构建批量插入数据
	documents := []interface{}{}
	for _, user := range users {
		documents = append(documents, map[string]interface{}{
			"username":   user.Username,
			"email":      user.Email,
			"age":        user.Age,
			"active":     user.Active,
			"created_at": user.CreatedAt,
			"updated_at": user.UpdatedAt,
		})
	}

	// 执行批量插入
	result, err := db.ExecWithResult("db.users.insertMany(?)", documents)
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
	err = db.ScanRaw(&count, "db.users.count({})")
	if err != nil {
		t.Fatalf("查询用户总数失败: %v", err)
	}

	if count != len(users) {
		t.Fatalf("批量插入用户验证失败，期望记录数: %d, 实际记录数: %d", len(users), count)
	}
}

// 测试查询单条记录
func TestMongoQueryRow(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	result, err := db.ExecWithResult(
		"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
		"queryuser", "query@example.com", 30, true, time.Now(), time.Now(),
	)
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	// 查询单条记录
	var user MongoUser
	err = db.ScanRaw(&user, "db.users.findOne({_id: ObjectId(?)})", userID)
	if err != nil {
		t.Fatalf("查询单条记录失败: %v", err)
	}

	// 验证查询结果
	if user.ID != userID || user.Username != "queryuser" || user.Email != "query@example.com" || user.Age != 30 || !user.Active {
		t.Fatalf("查询结果验证失败，期望用户ID: %s, 实际用户ID: %s", userID, user.ID)
	}

	t.Logf("查询单条记录成功: %+v", user)
}

// 测试查询多条记录
func TestMongoQuery(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	usernames := []string{"user1", "user2", "user3", "user4", "user5"}
	for _, username := range usernames {
		err := db.Exec(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			username, username+"@example.com", 20+len(username), true, time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 查询多条记录
	rows, err := db.Query("db.users.find({}).sort({_id: 1})")
	if err != nil {
		t.Fatalf("查询多条记录失败: %v", err)
	}
	defer rows.Close()

	// 遍历结果集
	var users []MongoUser
	for rows.Next() {
		var user MongoUser
		err := rows.Scan(&user)
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

// 测试关联查询
func TestMongoJoinQuery(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入用户数据
	result, err := db.ExecWithResult(
		"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
		"joinuser", "join@example.com", 30, true, time.Now(), time.Now(),
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
			"db.articles.insertOne({user_id: ?, title: ?, content: ?, created_at: ?, updated_at: ?})",
			userID, title, "这是"+title+"的内容", time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 执行关联查询 (MongoDB 使用聚合管道实现关联)
	rows, err := db.Query(`
		db.users.aggregate([
			{ $match: { _id: ObjectId(?) } },
			{ $lookup: {
				from: "articles",
				localField: "_id",
				foreignField: "user_id",
				as: "articles"
			}},
			{ $unwind: "$articles" },
			{ $project: {
				user_id: "$_id",
				username: 1,
				article_id: "$articles._id",
				article_title: "$articles.title"
			}}
		])
	`, userID)
	if err != nil {
		t.Fatalf("关联查询失败: %v", err)
	}
	defer rows.Close()

	// 遍历结果集
	var userArticles []MongoUserArticle
	for rows.Next() {
		var ua MongoUserArticle
		err := rows.Scan(&ua)
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

// 测试事务
func TestMongoTransaction(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 使用 Transaction 方法执行事务
	err := db.Transaction(func(tx *gosqlx.Database) error {
		// 在事务中插入用户
		result, err := tx.ExecWithResult(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			"txuser", "tx@example.com", 35, true, time.Now(), time.Now(),
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
			"db.articles.insertOne({user_id: ?, title: ?, content: ?, created_at: ?, updated_at: ?})",
			userID, "事务文章", "这是一篇在事务中创建的文章", time.Now(), time.Now(),
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

// 测试Query构建器
func TestMongoQueryBuilder(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("builder%d", i)
		email := fmt.Sprintf("builder%d@example.com", i)

		err := db.Exec(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			username, email, 20+i, i%2 == 0, time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用Query构建器查询
	q := query.NewQuery(db.DB())

	// 构建查询
	q.Table("users").
		Select("_id", "username", "email", "age").
		Where("age > ?", 25).
		Where("active = ?", true).
		OrderByDesc("_id").
		Limit(3)

	// 执行查询
	var users []MongoUser
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

// 测试Query构建器关联查询
func TestMongoQueryBuilderJoin(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入用户数据
	result, err := db.ExecWithResult(
		"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
		"joinuser", "join@example.com", 30, true, time.Now(), time.Now(),
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
			"db.articles.insertOne({user_id: ?, title: ?, content: ?, created_at: ?, updated_at: ?})",
			userID, title, "这是"+title+"的内容", time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 使用Query构建器进行关联查询
	q := query.NewQuery(db.DB())
	q.Table("users").
		SelectRaw("_id as user_id, username, articles._id as article_id, articles.title as article_title").
		Lookup("articles", "_id", "user_id", "articles").
		Unwind("articles").
		Match("_id", userID).
		OrderByAsc("articles._id")

	// 执行查询
	var userArticles []MongoUserArticle
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

// 测试Query构建器聚合函数
func TestMongoQueryBuilderAggregate(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)

		err := db.Exec(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			username, email, 20+i, i%2 == 0, time.Now(), time.Now(),
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

// 测试Query构建器分组
func TestMongoQueryBuilderGroup(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("group%d", i)
		email := fmt.Sprintf("group%d@example.com", i)

		err := db.Exec(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			username, email, 20+i%5, i%2 == 0, time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用Query构建器进行分组查询
	q := query.NewQuery(db.DB())
	q.Table("users").
		SelectRaw("_id.active as active, count as count, avg_age as avg_age").
		GroupBy("active").
		GroupCount("count").
		GroupAvg("age", "avg_age").
		OrderByAsc("_id.active")

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

// 测试Query构建器复杂条件
func TestMongoQueryBuilderComplexWhere(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("complex%d", i)
		email := fmt.Sprintf("complex%d@example.com", i)

		err := db.Exec(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			username, email, 20+i, i%3 == 0, time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用Query构建器进行复杂条件查询
	q := query.NewQuery(db.DB())
	q.Table("users").
		Select("_id", "username", "email", "age", "active").
		Where("age > ?", 25).
		WhereIn("age", []int{26, 28, 30, 32, 34}).
		WhereIf(true, "active = ?", true).
		WhereIf(false, "username LIKE ?", "not%").
		OrderByDesc("_id").
		Limit(5)

	// 执行查询
	var users []MongoUser
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
		Select("_id", "username", "email", "age", "active").
		Where("age > ?", 25).
		WhereGroup(func(w *builder.Where) {
			w.Where("active = ?", true).
				Or("age > ?", 35)
		}).
		OrderByDesc("_id").
		Limit(5)

	// 执行查询
	users = nil
	err = q.Get(&users)
	if err != nil {
		t.Fatalf("Query构建器条件组查询失败: %v", err)
	}

	t.Logf("Query构建器条件组查询成功，记录数: %d", len(users))
}

// 测试Query构建器子查询
func TestMongoQueryBuilderSubquery(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入用户数据
	for i := 1; i <= 5; i++ {
		username := fmt.Sprintf("subquery%d", i)
		email := fmt.Sprintf("subquery%d@example.com", i)

		result, err := db.ExecWithResult(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			username, email, 20+i, true, time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入用户数据失败: %v", err)
		}

		userID, _ := result.LastInsertId()

		// 为每个用户插入文章
		for j := 1; j <= i; j++ {
			err := db.Exec(
				"db.articles.insertOne({user_id: ?, title: ?, content: ?, created_at: ?, updated_at: ?})",
				userID, fmt.Sprintf("文章 %d-%d", i, j), fmt.Sprintf("用户 %d 的第 %d 篇文章", i, j), time.Now(), time.Now(),
			)
			if err != nil {
				t.Fatalf("插入文章数据失败: %v", err)
			}
		}
	}

	// 使用Query构建器进行子查询
	// 查询拥有至少3篇文章的用户
	q := query.NewQuery(db.DB())
	q.Table("users").
		Select("_id", "username", "email").
		Lookup("articles", "_id", "user_id", "articles").
		Match("articles", []interface{}{map[string]interface{}{"$exists": true}}).
		AddField("article_count", map[string]interface{}{"$size": "$articles"}).
		Match("article_count", []interface{}{map[string]interface{}{"$gte": 3}}).
		OrderByDesc("article_count")

	// 执行查询
	type UserWithArticleCount struct {
		ID           string `db:"_id"`
		Username     string `db:"username"`
		Email        string `db:"email"`
		ArticleCount int    `db:"article_count"`
	}

	var users []UserWithArticleCount
	err := q.Get(&users)
	if err != nil {
		t.Fatalf("Query构建器子查询失败: %v", err)
	}

	// 验证查询结果
	if len(users) == 0 {
		t.Fatalf("Query构建器子查询验证失败，未返回任何记录")
	}

	t.Logf("Query构建器子查询成功，记录数: %d", len(users))
	for _, user := range users {
		t.Logf("用户: %s, 文章数: %d", user.Username, user.ArticleCount)
	}
}

// 测试更新操作
func TestMongoUpdate(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	result, err := db.ExecWithResult(
		"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
		"updateuser", "update@example.com", 30, true, time.Now(), time.Now(),
	)
	if err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	// 执行更新操作
	updateResult, err := db.ExecWithResult(
		"db.users.updateOne({_id: ObjectId(?)}, {$set: {email: ?, age: ?, updated_at: ?}})",
		userID, "updated@example.com", 35, time.Now(),
	)
	if err != nil {
		t.Fatalf("更新操作失败: %v", err)
	}

	// 获取影响行数
	rowsAffected, err := updateResult.RowsAffected()
	if err != nil {
		t.Fatalf("获取影响行数失败: %v", err)
	}

	if rowsAffected != 1 {
		t.Fatalf("更新操作验证失败，期望影响行数: 1, 实际影响行数: %d", rowsAffected)
	}

	// 验证更新结果
	var user MongoUser
	err = db.ScanRaw(&user, "db.users.findOne({_id: ObjectId(?)})", userID)
	if err != nil {
		t.Fatalf("查询更新结果失败: %v", err)
	}

	if user.Email != "updated@example.com" || user.Age != 35 {
		t.Fatalf("更新结果验证失败，期望邮箱: updated@example.com, 实际邮箱: %s, 期望年龄: 35, 实际年龄: %d",
			user.Email, user.Age)
	}

	t.Logf("更新操作成功: %+v", user)
}

// 测试删除操作
func TestMongoDelete(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	for i := 1; i <= 5; i++ {
		username := fmt.Sprintf("delete%d", i)
		email := fmt.Sprintf("delete%d@example.com", i)

		err := db.Exec(
			"db.users.insertOne({username: ?, email: ?, age: ?, active: ?, created_at: ?, updated_at: ?})",
			username, email, 20+i, true, time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 执行删除操作
	deleteResult, err := db.ExecWithResult(
		"db.users.deleteMany({username: {$regex: ?}})",
		"^delete",
	)
	if err != nil {
		t.Fatalf("删除操作失败: %v", err)
	}

	// 获取影响行数
	rowsAffected, err := deleteResult.RowsAffected()
	if err != nil {
		t.Fatalf("获取影响行数失败: %v", err)
	}

	if rowsAffected != 5 {
		t.Fatalf("删除操作验证失败，期望影响行数: 5, 实际影响行数: %d", rowsAffected)
	}

	// 验证删除结果
	var count int
	err = db.ScanRaw(&count, "db.users.count({username: {$regex: ?}})", "^delete")
	if err != nil {
		t.Fatalf("查询删除结果失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("删除结果验证失败，期望记录数: 0, 实际记录数: %d", count)
	}

	t.Logf("删除操作成功，删除记录数: %d", rowsAffected)
}

// 测试索引操作
func TestMongoIndex(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 创建索引
	err := db.Exec("db.users.createIndex({email: 1}, {unique: true, name: 'idx_email'})")
	if err != nil {
		t.Fatalf("创建索引失败: %v", err)
	}

	// 验证索引是否创建成功
	var indexes []map[string]interface{}
	rows, err := db.Query("db.users.getIndexes()")
	if err != nil {
		t.Fatalf("获取索引列表失败: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var index map[string]interface{}
		err := rows.Scan(&index)
		if err != nil {
			t.Fatalf("扫描索引结果失败: %v", err)
		}
		indexes = append(indexes, index)
	}

	// 检查是否存在名为idx_email的索引
	found := false
	for _, index := range indexes {
		if name, ok := index["name"].(string); ok && name == "idx_email" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("未找到创建的索引: idx_email")
	}

	t.Logf("索引操作成功，索引总数: %d", len(indexes))

	// 删除索引
	err = db.Exec("db.users.dropIndex('idx_email')")
	if err != nil {
		t.Fatalf("删除索引失败: %v", err)
	}

	t.Logf("删除索引成功")
}

// 测试聚合管道
func TestMongoAggregatePipeline(t *testing.T) {
	// 初始化数据库
	db := initMongoDB(t)
	defer db.Close()

	// 准备测试集合
	prepareMongoTestCollections(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)
		category := fmt.Sprintf("category%d", i%3+1)

		err := db.Exec(
			"db.users.insertOne({username: ?, email: ?, age: ?, category: ?, active: ?, created_at: ?, updated_at: ?})",
			username, email, 20+i, category, i%2 == 0, time.Now(), time.Now(),
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 执行聚合管道查询
	rows, err := db.Query(`
		db.users.aggregate([
			{ $match: { age: { $gt: 25 } } },
			{ $group: { 
				_id: "$category", 
				count: { $sum: 1 },
				avg_age: { $avg: "$age" },
				users: { $push: { username: "$username", age: "$age" } }
			}},
			{ $sort: { count: -1 } }
		])
	`)
	if err != nil {
		t.Fatalf("聚合管道查询失败: %v", err)
	}
	defer rows.Close()

	// 遍历结果集
	type AggregateResult struct {
		ID     string                   `db:"_id"`
		Count  int                      `db:"count"`
		AvgAge float64                  `db:"avg_age"`
		Users  []map[string]interface{} `db:"users"`
	}

	var results []AggregateResult
	for rows.Next() {
		var result AggregateResult
		err := rows.Scan(&result)
		if err != nil {
			t.Fatalf("扫描聚合结果失败: %v", err)
		}
		results = append(results, result)
	}

	// 检查遍历错误
	if err = rows.Err(); err != nil {
		t.Fatalf("遍历结果集错误: %v", err)
	}

	// 验证聚合结果
	if len(results) == 0 {
		t.Fatalf("聚合管道查询验证失败，未返回任何记录")
	}

	t.Logf("聚合管道查询成功，分组数: %d", len(results))
	for _, result := range results {
		t.Logf("分类: %s, 数量: %d, 平均年龄: %.2f, 用户数: %d",
			result.ID, result.Count, result.AvgAge, len(result.Users))
	}
}

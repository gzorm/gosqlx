package test

import (
	"context"
	"fmt"
	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/query"
	"testing"
	"time"
)

// 用户结构体
type OracleUser struct {
	ID        int64     `db:"ID"`
	Username  string    `db:"USERNAME"`
	Email     string    `db:"EMAIL"`
	Age       int       `db:"AGE"`
	Active    bool      `db:"ACTIVE"`
	CreatedAt time.Time `db:"CREATED_AT"`
}

// 文章结构体
type OracleArticle struct {
	ID        int64     `db:"ID"`
	UserID    int64     `db:"USER_ID"`
	Title     string    `db:"TITLE"`
	Content   string    `db:"CONTENT"`
	CreatedAt time.Time `db:"CREATED_AT"`
}

// 用户文章关联结构体
type OracleUserArticle struct {
	UserID       int64  `db:"USER_ID"`
	Username     string `db:"USERNAME"`
	ArticleID    int64  `db:"ARTICLE_ID"`
	ArticleTitle string `db:"ARTICLE_TITLE"`
}

func initOracleDB(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.Oracle,
		Driver:      "oracle",
		Source:      "username/password@//host:port/service_name",
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "oracle_test",
		Mode:    "rw",
		DBType:  gosqlx.Oracle,
		Timeout: time.Second * 10,
	}

	// 创建数据库实例
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("连接Oracle数据库失败: %v", err)
	}

	return db
}

// 准备测试表
func prepareOracleTestTables(t *testing.T, db *gosqlx.Database) {
	// 删除已存在的表
	err := db.Exec("DROP TABLE ARTICLES")
	if err != nil {
		t.Logf("删除ARTICLES表失败(可能不存在): %v", err)
	}

	err = db.Exec("DROP TABLE USERS")
	if err != nil {
		t.Logf("删除USERS表失败(可能不存在): %v", err)
	}

	// 创建用户表
	err = db.Exec(`
		CREATE TABLE USERS (
			ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			USERNAME VARCHAR2(100) NOT NULL,
			EMAIL VARCHAR2(100) NOT NULL,
			AGE NUMBER(3) DEFAULT 0,
			ACTIVE NUMBER(1) DEFAULT 0,
			CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("创建USERS表失败: %v", err)
	}

	// 创建文章表
	err = db.Exec(`
		CREATE TABLE ARTICLES (
			ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			USER_ID NUMBER NOT NULL,
			TITLE VARCHAR2(200) NOT NULL,
			CONTENT CLOB,
			CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT FK_ARTICLES_USER FOREIGN KEY (USER_ID) REFERENCES USERS(ID)
		)
	`)
	if err != nil {
		t.Fatalf("创建ARTICLES表失败: %v", err)
	}

	t.Log("测试表准备完成")
}

// 测试基本的CRUD操作
func TestOracleCRUD(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 测试插入
	result, err := db.ExecWithResult(
		"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
		"testuser", "test@example.com", 25, 1,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}
	fmt.Println(result)
	// 获取自增ID (Oracle需要特殊处理)
	var userID int64
	err = db.QueryRow("SELECT ID FROM USERS WHERE USERNAME = :1", "testuser").Scan(&userID)
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	t.Logf("插入用户成功，ID: %d", userID)

	// 测试查询
	var user OracleUser
	err = db.QueryRow(
		"SELECT ID, USERNAME, EMAIL, AGE, ACTIVE, CREATED_AT FROM USERS WHERE ID = :1",
		userID,
	).Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active, &user.CreatedAt)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}

	t.Logf("查询用户成功: %+v", user)

	// 测试更新
	err = db.Exec(
		"UPDATE USERS SET AGE = :1, EMAIL = :2 WHERE ID = :3",
		30, "updated@example.com", userID,
	)
	if err != nil {
		t.Fatalf("更新用户失败: %v", err)
	}

	// 验证更新
	var age int
	var email string
	err = db.QueryRow(
		"SELECT AGE, EMAIL FROM USERS WHERE ID = :1",
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
	err = db.Exec("DELETE FROM USERS WHERE ID = :1", userID)
	if err != nil {
		t.Fatalf("删除用户失败: %v", err)
	}

	// 验证删除
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM USERS WHERE ID = :1", userID).Scan(&count)
	if err != nil {
		t.Fatalf("验证删除失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("删除验证失败，期望记录数: 0, 实际记录数: %d", count)
	}

	t.Log("删除用户成功")
}

// 测试批量插入
func TestOracleBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

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
			"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
			username, email, 20+i, i%2,
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
	err = db.QueryRow("SELECT COUNT(*) FROM USERS").Scan(&count)
	if err != nil {
		t.Fatalf("验证批量插入失败: %v", err)
	}

	if count != 10 {
		t.Fatalf("批量插入验证失败，期望记录数: 10, 实际记录数: %d", count)
	}

	t.Logf("批量插入成功，记录数: %d", count)
}

// 测试分页查询
func TestOraclePagination(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
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

	// Oracle分页查询
	rows, err := db.Query(`
		SELECT * FROM (
			SELECT a.*, ROWNUM rnum FROM (
				SELECT ID, USERNAME, EMAIL, AGE, ACTIVE, CREATED_AT
				FROM USERS
				ORDER BY ID
			) a WHERE ROWNUM <= :1
		) WHERE rnum > :2
	`, offset+pageSize, offset)
	if err != nil {
		t.Fatalf("分页查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var users []OracleUser
	for rows.Next() {
		var user OracleUser
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
func TestOracleJoin(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 插入用户数据
	err := db.Exec(
		"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
		"joinuser", "join@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 获取用户ID
	var userID int64
	err = db.QueryRow("SELECT ID FROM USERS WHERE USERNAME = :1", "joinuser").Scan(&userID)
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for _, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO ARTICLES (USER_ID, TITLE, CONTENT) VALUES (:1, :2, :3)",
			userID, title, "这是"+title+"的内容",
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 关联查询
	rows, err := db.Query(`
		SELECT u.ID AS USER_ID, u.USERNAME, a.ID AS ARTICLE_ID, a.TITLE AS ARTICLE_TITLE
		FROM USERS u
		LEFT JOIN ARTICLES a ON u.ID = a.USER_ID
		WHERE u.ID = :1
		ORDER BY a.ID
	`, userID)
	if err != nil {
		t.Fatalf("关联查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var userArticles []OracleUserArticle
	for rows.Next() {
		var ua OracleUserArticle
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
func TestOracleTransaction(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 测试提交事务
	t.Run("Commit", func(t *testing.T) {
		// 开始事务
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("开始事务失败: %v", err)
		}

		// 插入用户
		err = tx.Exec(
			"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
			"txuser1", "tx1@example.com", 25, 1,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 获取用户ID
		var userID int64
		err = tx.QueryRow("SELECT ID FROM USERS WHERE USERNAME = :1", "txuser1").Scan(&userID)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中获取用户ID失败: %v", err)
		}

		// 插入文章
		err = tx.Exec(
			"INSERT INTO ARTICLES (USER_ID, TITLE, CONTENT) VALUES (:1, :2, :3)",
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
		err = db.QueryRow("SELECT COUNT(*) FROM ARTICLES WHERE USER_ID = :1", userID).Scan(&count)
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
		err = tx.Exec(
			"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
			"txuser2", "tx2@example.com", 30, 1,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 获取用户ID
		var userID int64
		err = tx.QueryRow("SELECT ID FROM USERS WHERE USERNAME = :1", "txuser2").Scan(&userID)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中获取用户ID失败: %v", err)
		}

		// 回滚事务
		err = tx.Rollback()
		if err != nil {
			t.Fatalf("回滚事务失败: %v", err)
		}

		// 验证事务回滚结果
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM USERS WHERE USERNAME = :1", "txuser2").Scan(&count)
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
func TestOracleQueryBuilderPagination(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
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
	q.Table("USERS").
		Select("ID", "USERNAME", "EMAIL", "AGE", "ACTIVE").
		OrderByAsc("ID").
		Page(page, pageSize)

	// 执行查询
	var users []OracleUser
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
func TestOracleQueryBuilderJoin(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 插入用户数据
	err := db.Exec(
		"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
		"joinuser", "join@example.com", 30, 1,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 获取用户ID
	var userID int64
	err = db.QueryRow("SELECT ID FROM USERS WHERE USERNAME = :1", "joinuser").Scan(&userID)
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for _, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO ARTICLES (USER_ID, TITLE, CONTENT) VALUES (:1, :2, :3)",
			userID, title, "这是"+title+"的内容",
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 使用Query构建器进行关联查询
	q := query.NewQuery(db.DB())
	q.Table("USERS u").
		Select("u.ID AS USER_ID", "u.USERNAME", "a.ID AS ARTICLE_ID", "a.TITLE AS ARTICLE_TITLE").
		LeftJoin("ARTICLES a", "u.ID = a.USER_ID").
		Where("u.ID = ?", userID).
		OrderByAsc("a.ID")

	// 执行查询
	var userArticles []OracleUserArticle
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
func TestOracleQueryBuilderAggregate(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)

		err := db.Exec(
			"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
			username, email, 20+i, i%2,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 测试AVG聚合函数
	q := query.NewQuery(db.DB())
	avgAge, err := q.Table("USERS").AvgNum("AGE")
	if err != nil {
		t.Fatalf("Query构建器AVG聚合查询失败: %v", err)
	}

	t.Logf("Query构建器AVG聚合查询成功，平均年龄: %.2f", avgAge)

	// 测试SUM聚合函数
	q = query.NewQuery(db.DB())
	sumAge, err := q.Table("USERS").SumNum("AGE")
	if err != nil {
		t.Fatalf("Query构建器SUM聚合查询失败: %v", err)
	}

	t.Logf("Query构建器SUM聚合查询成功，年龄总和: %.2f", sumAge)

	// 测试MAX聚合函数
	q = query.NewQuery(db.DB())
	maxAge, err := q.Table("USERS").MaxNum("AGE")
	if err != nil {
		t.Fatalf("Query构建器MAX聚合查询失败: %v", err)
	}

	t.Logf("Query构建器MAX聚合查询成功，最大年龄: %v", maxAge)

	// 测试MIN聚合函数
	q = query.NewQuery(db.DB())
	minAge, err := q.Table("USERS").MinNum("AGE")
	if err != nil {
		t.Fatalf("Query构建器MIN聚合查询失败: %v", err)
	}

	t.Logf("Query构建器MIN聚合查询成功，最小年龄: %v", minAge)
}

// 测试使用Query构建器进行事务操作
func TestOracleQueryBuilderTransaction(t *testing.T) {
	// 初始化数据库
	db := initOracleDB(t)
	defer db.Close()

	// 准备测试表
	prepareOracleTestTables(t, db)

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	// 在事务中使用Query构建器
	q := query.NewQuery(tx)

	// 插入用户
	err = tx.Exec(
		"INSERT INTO USERS (USERNAME, EMAIL, AGE, ACTIVE) VALUES (:1, :2, :3, :4)",
		"txbuilder", "txbuilder@example.com", 30, 1,
	)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中插入用户失败: %v", err)
	}

	// 获取用户ID
	var userID int64
	err = tx.QueryRow("SELECT ID FROM USERS WHERE USERNAME = :1", "txbuilder").Scan(&userID)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中获取用户ID失败: %v", err)
	}

	// 插入文章
	err = tx.Exec(
		"INSERT INTO ARTICLES (USER_ID, TITLE, CONTENT) VALUES (:1, :2, :3)",
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
	count, err := q.Table("ARTICLES").
		Where("USER_ID = ?", userID).
		CountNum()
	if err != nil {
		t.Fatalf("查询文章数量失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("事务验证失败，期望文章数: 1, 实际文章数: %d", count)
	}

	t.Logf("Query构建器事务操作成功，用户ID: %d", userID)
}

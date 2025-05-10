package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gzorm/gosqlx"
	"github.com/gzorm/gosqlx/query"
)

// 用户结构体
type SQLServerUser struct {
	ID        int64     `db:"ID"`
	Username  string    `db:"Username"`
	Email     string    `db:"Email"`
	Age       int       `db:"Age"`
	Active    bool      `db:"Active"`
	CreatedAt time.Time `db:"CreatedAt"`
}

// 文章结构体
type SQLServerArticle struct {
	ID        int64     `db:"ID"`
	UserID    int64     `db:"UserID"`
	Title     string    `db:"Title"`
	Content   string    `db:"Content"`
	CreatedAt time.Time `db:"CreatedAt"`
}

// 用户文章关联结构体
type SQLServerUserArticle struct {
	UserID       int64  `db:"UserID"`
	Username     string `db:"Username"`
	ArticleID    int64  `db:"ArticleID"`
	ArticleTitle string `db:"ArticleTitle"`
}

func initSQLServerDB(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.SQLServer,
		Driver:      "sqlserver",
		Source:      "server=localhost;user id=sa;password=YourPassword;database=TestDB",
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "sqlserver_test",
		Mode:    "rw",
		DBType:  gosqlx.SQLServer,
		Timeout: time.Second * 10,
	}

	// 创建数据库实例
	db, err := gosqlx.NewDatabase(ctx, config)
	if err != nil {
		t.Fatalf("连接SQL Server数据库失败: %v", err)
	}

	return db
}

// 准备测试表
func prepareSQLServerTestTables(t *testing.T, db *gosqlx.Database) {
	// 删除已存在的表
	err := db.Exec("IF OBJECT_ID('Articles', 'U') IS NOT NULL DROP TABLE Articles")
	if err != nil {
		t.Logf("删除Articles表失败: %v", err)
	}

	err = db.Exec("IF OBJECT_ID('Users', 'U') IS NOT NULL DROP TABLE Users")
	if err != nil {
		t.Logf("删除Users表失败: %v", err)
	}

	// 创建用户表
	err = db.Exec(`
		CREATE TABLE Users (
			ID INT IDENTITY(1,1) PRIMARY KEY,
			Username NVARCHAR(100) NOT NULL,
			Email NVARCHAR(100) NOT NULL,
			Age INT DEFAULT 0,
			Active BIT DEFAULT 0,
			CreatedAt DATETIME DEFAULT GETDATE()
		)
	`)
	if err != nil {
		t.Fatalf("创建Users表失败: %v", err)
	}

	// 创建文章表
	err = db.Exec(`
		CREATE TABLE Articles (
			ID INT IDENTITY(1,1) PRIMARY KEY,
			UserID INT NOT NULL,
			Title NVARCHAR(200) NOT NULL,
			Content NVARCHAR(MAX),
			CreatedAt DATETIME DEFAULT GETDATE(),
			CONSTRAINT FK_Articles_User FOREIGN KEY (UserID) REFERENCES Users(ID)
		)
	`)
	if err != nil {
		t.Fatalf("创建Articles表失败: %v", err)
	}

	t.Log("测试表准备完成")
}

// 测试基本的CRUD操作
func TestSQLServerCRUD(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 测试插入
	result, err := db.ExecWithResult(
		"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
		"testuser", "test@example.com", 25, true,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}
	fmt.Println(result)
	// 获取自增ID
	var userID int64
	err = db.QueryRow("SELECT SCOPE_IDENTITY()").Scan(&userID)
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	t.Logf("插入用户成功，ID: %d", userID)

	// 测试查询
	var user SQLServerUser
	err = db.QueryRow(
		"SELECT ID, Username, Email, Age, Active, CreatedAt FROM Users WHERE ID = @p1",
		userID,
	).Scan(&user.ID, &user.Username, &user.Email, &user.Age, &user.Active, &user.CreatedAt)
	if err != nil {
		t.Fatalf("查询用户失败: %v", err)
	}

	t.Logf("查询用户成功: %+v", user)

	// 测试更新
	err = db.Exec(
		"UPDATE Users SET Age = @p1, Email = @p2 WHERE ID = @p3",
		30, "updated@example.com", userID,
	)
	if err != nil {
		t.Fatalf("更新用户失败: %v", err)
	}

	// 验证更新
	var age int
	var email string
	err = db.QueryRow(
		"SELECT Age, Email FROM Users WHERE ID = @p1",
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
	err = db.Exec("DELETE FROM Users WHERE ID = @p1", userID)
	if err != nil {
		t.Fatalf("删除用户失败: %v", err)
	}

	// 验证删除
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM Users WHERE ID = @p1", userID).Scan(&count)
	if err != nil {
		t.Fatalf("验证删除失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("删除验证失败，期望记录数: 0, 实际记录数: %d", count)
	}

	t.Log("删除用户成功")
}

// 测试批量插入
func TestSQLServerBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

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
			"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
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
	err = db.QueryRow("SELECT COUNT(*) FROM Users").Scan(&count)
	if err != nil {
		t.Fatalf("验证批量插入失败: %v", err)
	}

	if count != 10 {
		t.Fatalf("批量插入验证失败，期望记录数: 10, 实际记录数: %d", count)
	}

	t.Logf("批量插入成功，记录数: %d", count)
}

// 测试分页查询
func TestSQLServerPagination(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
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

	// SQL Server分页查询
	rows, err := db.Query(`
		SELECT ID, Username, Email, Age, Active, CreatedAt
		FROM Users
		ORDER BY ID
		OFFSET @p1 ROWS
		FETCH NEXT @p2 ROWS ONLY
	`, offset, pageSize)
	if err != nil {
		t.Fatalf("分页查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var users []SQLServerUser
	for rows.Next() {
		var user SQLServerUser
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
func TestSQLServerJoin(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 插入用户数据
	err := db.Exec(
		"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
		"joinuser", "join@example.com", 30, true,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 获取用户ID
	var userID int64
	err = db.QueryRow("SELECT SCOPE_IDENTITY()").Scan(&userID)
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for _, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO Articles (UserID, Title, Content) VALUES (@p1, @p2, @p3)",
			userID, title, "这是"+title+"的内容",
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 关联查询
	rows, err := db.Query(`
		SELECT u.ID AS UserID, u.Username, a.ID AS ArticleID, a.Title AS ArticleTitle
		FROM Users u
		LEFT JOIN Articles a ON u.ID = a.UserID
		WHERE u.ID = @p1
		ORDER BY a.ID
	`, userID)
	if err != nil {
		t.Fatalf("关联查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	var userArticles []SQLServerUserArticle
	for rows.Next() {
		var ua SQLServerUserArticle
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
func TestSQLServerTransaction(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 测试提交事务
	t.Run("Commit", func(t *testing.T) {
		// 开始事务
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("开始事务失败: %v", err)
		}

		// 插入用户
		err = tx.Exec(
			"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
			"txuser1", "tx1@example.com", 25, true,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 获取用户ID
		var userID int64
		err = tx.QueryRow("SELECT SCOPE_IDENTITY()").Scan(&userID)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中获取用户ID失败: %v", err)
		}

		// 插入文章
		err = tx.Exec(
			"INSERT INTO Articles (UserID, Title, Content) VALUES (@p1, @p2, @p3)",
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
		err = db.QueryRow("SELECT COUNT(*) FROM Articles WHERE UserID = @p1", userID).Scan(&count)
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
			"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
			"txuser2", "tx2@example.com", 30, true,
		)
		if err != nil {
			tx.Rollback()
			t.Fatalf("事务中插入用户失败: %v", err)
		}

		// 获取用户ID
		var userID int64
		err = tx.QueryRow("SELECT SCOPE_IDENTITY()").Scan(&userID)
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
		err = db.QueryRow("SELECT COUNT(*) FROM Users WHERE Username = @p1", "txuser2").Scan(&count)
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
func TestSQLServerQueryBuilderPagination(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 20; i++ {
		username := fmt.Sprintf("page%d", i)
		email := fmt.Sprintf("page%d@example.com", i)

		err := db.Exec(
			"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
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
	q.Table("Users").
		Select("ID", "Username", "Email", "Age", "Active").
		OrderByAsc("ID").
		Page(page, pageSize)

	// 执行查询
	var users []SQLServerUser
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
func TestSQLServerQueryBuilderJoin(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 插入用户数据
	err := db.Exec(
		"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
		"joinuser", "join@example.com", 30, true,
	)
	if err != nil {
		t.Fatalf("插入用户数据失败: %v", err)
	}

	// 获取用户ID
	var userID int64
	err = db.QueryRow("SELECT SCOPE_IDENTITY()").Scan(&userID)
	if err != nil {
		t.Fatalf("获取用户ID失败: %v", err)
	}

	// 插入文章数据
	articleTitles := []string{"文章一", "文章二", "文章三"}
	for _, title := range articleTitles {
		err := db.Exec(
			"INSERT INTO Articles (UserID, Title, Content) VALUES (@p1, @p2, @p3)",
			userID, title, "这是"+title+"的内容",
		)
		if err != nil {
			t.Fatalf("插入文章数据失败: %v", err)
		}
	}

	// 使用Query构建器进行关联查询
	q := query.NewQuery(db.DB())
	q.Table("Users u").
		Select("u.ID AS UserID", "u.Username", "a.ID AS ArticleID", "a.Title AS ArticleTitle").
		LeftJoin("Articles a", "u.ID = a.UserID").
		Where("u.ID = ?", userID).
		OrderByAsc("a.ID")

	// 执行查询
	var userArticles []SQLServerUserArticle
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

// 测试SQL Server特有功能：表变量
func TestSQLServerTableVariable(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 使用表变量进行查询
	rows, err := db.Query(`
		DECLARE @Users TABLE (
			ID INT,
			Username NVARCHAR(100),
			Email NVARCHAR(100)
		);
		
		INSERT INTO @Users (ID, Username, Email)
		VALUES (1, 'user1', 'user1@example.com'),
			   (2, 'user2', 'user2@example.com'),
			   (3, 'user3', 'user3@example.com');
		
		SELECT ID, Username, Email FROM @Users ORDER BY ID;
	`)
	if err != nil {
		t.Fatalf("表变量查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type TableVarUser struct {
		ID       int
		Username string
		Email    string
	}

	var users []TableVarUser
	for rows.Next() {
		var user TableVarUser
		err := rows.Scan(&user.ID, &user.Username, &user.Email)
		if err != nil {
			t.Fatalf("扫描表变量查询结果失败: %v", err)
		}
		users = append(users, user)
	}

	// 验证查询结果
	if len(users) != 3 {
		t.Fatalf("表变量查询验证失败，期望记录数: 3, 实际记录数: %d", len(users))
	}

	t.Logf("表变量查询成功，记录数: %d", len(users))
}

// 测试SQL Server特有功能：临时表
func TestSQLServerTempTable(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 创建临时表
	err := db.Exec(`
		CREATE TABLE #TempUsers (
			ID INT IDENTITY(1,1) PRIMARY KEY,
			Username NVARCHAR(100) NOT NULL,
			Email NVARCHAR(100) NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("创建临时表失败: %v", err)
	}

	// 插入数据到临时表
	for i := 1; i <= 5; i++ {
		username := fmt.Sprintf("temp%d", i)
		email := fmt.Sprintf("temp%d@example.com", i)

		err := db.Exec(
			"INSERT INTO #TempUsers (Username, Email) VALUES (@p1, @p2)",
			username, email,
		)
		if err != nil {
			t.Fatalf("插入临时表数据失败: %v", err)
		}
	}

	// 查询临时表
	rows, err := db.Query("SELECT ID, Username, Email FROM #TempUsers ORDER BY ID")
	if err != nil {
		t.Fatalf("查询临时表失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type TempUser struct {
		ID       int
		Username string
		Email    string
	}

	var users []TempUser
	for rows.Next() {
		var user TempUser
		err := rows.Scan(&user.ID, &user.Username, &user.Email)
		if err != nil {
			t.Fatalf("扫描临时表查询结果失败: %v", err)
		}
		users = append(users, user)
	}

	// 验证查询结果
	if len(users) != 5 {
		t.Fatalf("临时表查询验证失败，期望记录数: 5, 实际记录数: %d", len(users))
	}

	t.Logf("临时表查询成功，记录数: %d", len(users))
}

// 测试SQL Server特有功能：OUTPUT子句
func TestSQLServerOutputClause(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 使用OUTPUT子句插入并返回插入的数据
	rows, err := db.Query(`
		INSERT INTO Users (Username, Email, Age, Active)
		OUTPUT INSERTED.ID, INSERTED.Username, INSERTED.Email
		VALUES (@p1, @p2, @p3, @p4)
	`, "outputuser", "output@example.com", 35, true)
	if err != nil {
		t.Fatalf("OUTPUT子句插入失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type OutputUser struct {
		ID       int64
		Username string
		Email    string
	}

	var users []OutputUser
	for rows.Next() {
		var user OutputUser
		err := rows.Scan(&user.ID, &user.Username, &user.Email)
		if err != nil {
			t.Fatalf("扫描OUTPUT子句结果失败: %v", err)
		}
		users = append(users, user)
	}

	// 验证OUTPUT子句结果
	if len(users) != 1 {
		t.Fatalf("OUTPUT子句验证失败，期望记录数: 1, 实际记录数: %d", len(users))
	}

	if users[0].Username != "outputuser" || users[0].Email != "output@example.com" {
		t.Fatalf("OUTPUT子句数据验证失败，期望: (outputuser, output@example.com), 实际: (%s, %s)",
			users[0].Username, users[0].Email)
	}

	t.Logf("OUTPUT子句测试成功，插入的ID: %d", users[0].ID)

	// 使用OUTPUT子句更新并返回更新前后的数据
	rows, err = db.Query(`
		UPDATE Users
		SET Age = Age + 10
		OUTPUT DELETED.ID, DELETED.Age AS OldAge, INSERTED.Age AS NewAge
		WHERE Username = @p1
	`, "outputuser")
	if err != nil {
		t.Fatalf("OUTPUT子句更新失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type OutputUpdate struct {
		ID     int64
		OldAge int
		NewAge int
	}

	var updates []OutputUpdate
	for rows.Next() {
		var update OutputUpdate
		err := rows.Scan(&update.ID, &update.OldAge, &update.NewAge)
		if err != nil {
			t.Fatalf("扫描OUTPUT子句更新结果失败: %v", err)
		}
		updates = append(updates, update)
	}

	// 验证OUTPUT子句更新结果
	if len(updates) != 1 {
		t.Fatalf("OUTPUT子句更新验证失败，期望记录数: 1, 实际记录数: %d", len(updates))
	}

	if updates[0].OldAge != 35 || updates[0].NewAge != 45 {
		t.Fatalf("OUTPUT子句更新数据验证失败，期望: (35, 45), 实际: (%d, %d)", updates[0].OldAge, updates[0].NewAge)
	}

	t.Logf("OUTPUT子句更新测试成功，旧年龄: %d, 新年龄: %d", updates[0].OldAge, updates[0].NewAge)

	// 使用OUTPUT子句删除并返回删除的数据
	rows, err = db.Query(`
		DELETE FROM Users
		OUTPUT DELETED.ID, DELETED.Username, DELETED.Email
		WHERE Username = @p1
	`, "outputuser")
	if err != nil {
		t.Fatalf("OUTPUT子句删除失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	users = nil
	for rows.Next() {
		var user OutputUser
		err := rows.Scan(&user.ID, &user.Username, &user.Email)
		if err != nil {
			t.Fatalf("扫描OUTPUT子句删除结果失败: %v", err)
		}
		users = append(users, user)
	}

	// 验证OUTPUT子句删除结果
	if len(users) != 1 {
		t.Fatalf("OUTPUT子句删除验证失败，期望记录数: 1, 实际记录数: %d", len(users))
	}

	t.Logf("OUTPUT子句删除测试成功，删除的用户: %s", users[0].Username)
}

// 测试SQL Server特有功能：CTE (公用表表达式)
func TestSQLServerCTE(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 5; i++ {
		err := db.Exec(
			"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
			fmt.Sprintf("cte%d", i), fmt.Sprintf("cte%d@example.com", i), 20+i, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 使用CTE查询
	rows, err := db.Query(`
		WITH UserCTE AS (
			SELECT ID, Username, Email, Age,
				ROW_NUMBER() OVER (ORDER BY Age) AS RowNum
			FROM Users
			WHERE Username LIKE 'cte%'
		)
		SELECT ID, Username, Email, Age, RowNum
		FROM UserCTE
		WHERE RowNum BETWEEN 2 AND 4
		ORDER BY RowNum
	`)
	if err != nil {
		t.Fatalf("CTE查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type CTEResult struct {
		ID       int64
		Username string
		Email    string
		Age      int
		RowNum   int
	}

	var results []CTEResult
	for rows.Next() {
		var result CTEResult
		err := rows.Scan(&result.ID, &result.Username, &result.Email, &result.Age, &result.RowNum)
		if err != nil {
			t.Fatalf("扫描CTE查询结果失败: %v", err)
		}
		results = append(results, result)
	}

	// 验证CTE查询结果
	if len(results) != 3 {
		t.Fatalf("CTE查询验证失败，期望记录数: 3, 实际记录数: %d", len(results))
	}

	for _, result := range results {
		t.Logf("CTE查询结果: 用户=%s, 年龄=%d, 行号=%d", result.Username, result.Age, result.RowNum)
	}

	t.Logf("CTE查询成功，记录数: %d", len(results))
}

// 测试SQL Server特有功能：MERGE语句
func TestSQLServerMerge(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 创建源表和目标表
	err := db.Exec(`
		CREATE TABLE SourceUsers (
			ID INT PRIMARY KEY,
			Username NVARCHAR(100) NOT NULL,
			Email NVARCHAR(100) NOT NULL,
			Age INT DEFAULT 0
		);

		CREATE TABLE TargetUsers (
			ID INT PRIMARY KEY,
			Username NVARCHAR(100) NOT NULL,
			Email NVARCHAR(100) NOT NULL,
			Age INT DEFAULT 0
		);
	`)
	if err != nil {
		t.Fatalf("创建MERGE测试表失败: %v", err)
	}

	// 插入源表数据
	err = db.Exec(`
		INSERT INTO SourceUsers (ID, Username, Email, Age)
		VALUES (1, 'user1', 'user1@example.com', 21),
			   (2, 'user2', 'user2@example.com', 22),
			   (3, 'user3', 'user3@example.com', 23);
	`)
	if err != nil {
		t.Fatalf("插入源表数据失败: %v", err)
	}

	// 插入目标表数据（部分重叠）
	err = db.Exec(`
		INSERT INTO TargetUsers (ID, Username, Email, Age)
		VALUES (1, 'user1', 'user1_old@example.com', 31),
			   (3, 'user3', 'user3_old@example.com', 33),
			   (4, 'user4', 'user4@example.com', 24);
	`)
	if err != nil {
		t.Fatalf("插入目标表数据失败: %v", err)
	}

	// 执行MERGE操作
	result, err := db.ExecWithResult(`
		MERGE TargetUsers AS target
		USING SourceUsers AS source
		ON (target.ID = source.ID)
		WHEN MATCHED THEN
			UPDATE SET target.Email = source.Email, target.Age = source.Age
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (ID, Username, Email, Age)
			VALUES (source.ID, source.Username, source.Email, source.Age)
		WHEN NOT MATCHED BY SOURCE THEN
			DELETE;
	`)
	if err != nil {
		t.Fatalf("执行MERGE操作失败: %v", err)
	}

	// 获取受影响的行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("获取受影响行数失败: %v", err)
	}

	t.Logf("MERGE操作成功，受影响行数: %d", rowsAffected)

	// 验证MERGE结果
	rows, err := db.Query("SELECT ID, Username, Email, Age FROM TargetUsers ORDER BY ID")
	if err != nil {
		t.Fatalf("查询MERGE结果失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type MergeResult struct {
		ID       int
		Username string
		Email    string
		Age      int
	}

	var results []MergeResult
	for rows.Next() {
		var result MergeResult
		err := rows.Scan(&result.ID, &result.Username, &result.Email, &result.Age)
		if err != nil {
			t.Fatalf("扫描MERGE结果失败: %v", err)
		}
		results = append(results, result)
	}

	// 验证MERGE结果
	if len(results) != 3 {
		t.Fatalf("MERGE结果验证失败，期望记录数: 3, 实际记录数: %d", len(results))
	}

	for _, result := range results {
		t.Logf("MERGE结果: ID=%d, 用户=%s, 邮箱=%s, 年龄=%d",
			result.ID, result.Username, result.Email, result.Age)
	}
}

// 测试SQL Server特有功能：PIVOT和UNPIVOT
func TestSQLServerPivotUnpivot(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 创建测试表
	err := db.Exec(`
		CREATE TABLE SalesData (
			ProductID INT,
			Quarter NVARCHAR(2),
			Amount DECIMAL(10,2)
		);
	`)
	if err != nil {
		t.Fatalf("创建PIVOT测试表失败: %v", err)
	}

	// 插入测试数据
	err = db.Exec(`
		INSERT INTO SalesData (ProductID, Quarter, Amount)
		VALUES (1, 'Q1', 1000), (1, 'Q2', 1200), (1, 'Q3', 1100), (1, 'Q4', 1300),
			   (2, 'Q1', 800), (2, 'Q2', 700), (2, 'Q3', 900), (2, 'Q4', 1100),
			   (3, 'Q1', 1200), (3, 'Q2', 1300), (3, 'Q3', 1400), (3, 'Q4', 1500);
	`)
	if err != nil {
		t.Fatalf("插入PIVOT测试数据失败: %v", err)
	}

	// 执行PIVOT查询
	rows, err := db.Query(`
		SELECT ProductID, Q1, Q2, Q3, Q4
		FROM (
			SELECT ProductID, Quarter, Amount
			FROM SalesData
		) AS SourceTable
		PIVOT (
			SUM(Amount)
			FOR Quarter IN (Q1, Q2, Q3, Q4)
		) AS PivotTable
		ORDER BY ProductID;
	`)
	if err != nil {
		t.Fatalf("执行PIVOT查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type PivotResult struct {
		ProductID int
		Q1        float64
		Q2        float64
		Q3        float64
		Q4        float64
	}

	var pivotResults []PivotResult
	for rows.Next() {
		var result PivotResult
		err := rows.Scan(&result.ProductID, &result.Q1, &result.Q2, &result.Q3, &result.Q4)
		if err != nil {
			t.Fatalf("扫描PIVOT查询结果失败: %v", err)
		}
		pivotResults = append(pivotResults, result)
	}

	// 验证PIVOT查询结果
	if len(pivotResults) != 3 {
		t.Fatalf("PIVOT查询验证失败，期望记录数: 3, 实际记录数: %d", len(pivotResults))
	}

	for _, result := range pivotResults {
		t.Logf("PIVOT结果: 产品ID=%d, Q1=%.2f, Q2=%.2f, Q3=%.2f, Q4=%.2f",
			result.ProductID, result.Q1, result.Q2, result.Q3, result.Q4)
	}

	// 执行UNPIVOT查询
	rows, err = db.Query(`
		SELECT ProductID, Quarter, Amount
		FROM (
			SELECT ProductID, Q1, Q2, Q3, Q4
			FROM (
				SELECT ProductID, Quarter, Amount
				FROM SalesData
			) AS SourceTable
			PIVOT (
				SUM(Amount)
				FOR Quarter IN (Q1, Q2, Q3, Q4)
			) AS PivotTable
		) p
		UNPIVOT (
			Amount FOR Quarter IN (Q1, Q2, Q3, Q4)
		) AS UnpivotTable
		ORDER BY ProductID, Quarter;
	`)
	if err != nil {
		t.Fatalf("执行UNPIVOT查询失败: %v", err)
	}
	defer rows.Close()

	// 读取结果
	type UnpivotResult struct {
		ProductID int
		Quarter   string
		Amount    float64
	}

	var unpivotResults []UnpivotResult
	for rows.Next() {
		var result UnpivotResult
		err := rows.Scan(&result.ProductID, &result.Quarter, &result.Amount)
		if err != nil {
			t.Fatalf("扫描UNPIVOT查询结果失败: %v", err)
		}
		unpivotResults = append(unpivotResults, result)
	}

	// 验证UNPIVOT查询结果
	if len(unpivotResults) != 12 {
		t.Fatalf("UNPIVOT查询验证失败，期望记录数: 12, 实际记录数: %d", len(unpivotResults))
	}

	t.Logf("UNPIVOT查询成功，记录数: %d", len(unpivotResults))
}

// 测试使用Query构建器进行聚合查询
func TestSQLServerQueryBuilderAggregate(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 插入测试数据
	for i := 1; i <= 10; i++ {
		username := fmt.Sprintf("agg%d", i)
		email := fmt.Sprintf("agg%d@example.com", i)

		err := db.Exec(
			"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
			username, email, 20+i, i%2 == 0,
		)
		if err != nil {
			t.Fatalf("插入测试数据失败: %v", err)
		}
	}

	// 测试AVG聚合函数
	q := query.NewQuery(db.DB())
	avgAge, err := q.Table("Users").AvgNum("Age")
	if err != nil {
		t.Fatalf("Query构建器AVG聚合查询失败: %v", err)
	}

	t.Logf("Query构建器AVG聚合查询成功，平均年龄: %.2f", avgAge)

	// 测试SUM聚合函数
	q = query.NewQuery(db.DB())
	sumAge, err := q.Table("Users").SumNum("Age")
	if err != nil {
		t.Fatalf("Query构建器SUM聚合查询失败: %v", err)
	}

	t.Logf("Query构建器SUM聚合查询成功，年龄总和: %.2f", sumAge)

	// 测试MAX聚合函数
	q = query.NewQuery(db.DB())
	maxAge, err := q.Table("Users").MaxNum("Age")
	if err != nil {
		t.Fatalf("Query构建器MAX聚合查询失败: %v", err)
	}

	t.Logf("Query构建器MAX聚合查询成功，最大年龄: %v", maxAge)

	// 测试MIN聚合函数
	q = query.NewQuery(db.DB())
	minAge, err := q.Table("Users").MinNum("Age")
	if err != nil {
		t.Fatalf("Query构建器MIN聚合查询失败: %v", err)
	}

	t.Logf("Query构建器MIN聚合查询成功，最小年龄: %v", minAge)
}

// 测试使用Query构建器进行事务操作
func TestSQLServerQueryBuilderTransaction(t *testing.T) {
	// 初始化数据库
	db := initSQLServerDB(t)
	defer db.Close()

	// 准备测试表
	prepareSQLServerTestTables(t, db)

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	// 在事务中使用Query构建器
	q := query.NewQuery(tx)

	// 插入用户
	err = tx.Exec(
		"INSERT INTO Users (Username, Email, Age, Active) VALUES (@p1, @p2, @p3, @p4)",
		"txbuilder", "txbuilder@example.com", 30, true,
	)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中插入用户失败: %v", err)
	}

	// 获取用户ID
	var userID int64
	err = tx.QueryRow("SELECT SCOPE_IDENTITY()").Scan(&userID)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中获取用户ID失败: %v", err)
	}

	// 插入文章
	err = tx.Exec(
		"INSERT INTO Articles (UserID, Title, Content) VALUES (@p1, @p2, @p3)",
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
	count, err := q.Table("Articles").
		Where("UserID = ?", userID).
		CountNum()
	if err != nil {
		t.Fatalf("查询文章数量失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("事务验证失败，期望文章数: 1, 实际文章数: %d", count)
	}

	t.Logf("Query构建器事务操作成功，用户ID: %d", userID)
}

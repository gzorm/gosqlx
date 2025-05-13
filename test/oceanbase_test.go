package test

import (
	"context"
	"fmt"
	"github.com/gzorm/gosqlx/adapter"
	"testing"
	"time"

	"github.com/gzorm/gosqlx"
)

// 初始化数据库连接
func initOceanBase(t *testing.T) *gosqlx.Database {
	// 创建数据库配置
	config := &gosqlx.Config{
		Type:        gosqlx.OceanBase,
		Driver:      "mysql", // OceanBase 使用 MySQL 驱动
		Source:      "root:root@tcp(localhost:2881)/testdb?charset=utf8mb4&parseTime=True&loc=Local",
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
		Debug:       true,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: context.Background(),
		Nick:    "test_oceanbase",
		Mode:    "rw",
		DBType:  gosqlx.OceanBase,
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
func prepareOceanBaseTestTables(t *testing.T, db *gosqlx.Database) {
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
func TestOceanBaseInsert(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

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
func TestOceanBaseBatchInsert(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

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
func TestOceanBaseQuery(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

	// 插入测试数据
	users := []User{
		{Username: "user1", Email: "user1@example.com", Age: 21, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user2", Email: "user2@example.com", Age: 22, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user3", Email: "user3@example.com", Age: 23, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	// 批量插入用户
	for _, user := range users {
		_, err := db.ExecWithResult(
			"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
			user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
		)
		if err != nil {
			t.Fatalf("插入用户失败: %v", err)
		}
	}

	// 测试查询单条记录
	var user User
	err := db.ScanRaw(&user, "SELECT id, username, email, age, active FROM users WHERE username = ?", "user1")
	if err != nil {
		t.Fatalf("查询单条记录失败: %v", err)
	}

	if user.Username != "user1" || user.Email != "user1@example.com" || user.Age != 21 {
		t.Fatalf("查询结果不匹配，期望: user1/user1@example.com/21, 实际: %s/%s/%d", user.Username, user.Email, user.Age)
	}

	// 测试查询多条记录
	var allUsers []User
	err = db.ScanRaw(&allUsers, "SELECT id, username, email, age, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("查询多条记录失败: %v", err)
	}

	if len(allUsers) != len(users) {
		t.Fatalf("查询结果数量不匹配，期望: %d, 实际: %d", len(users), len(allUsers))
	}
}

// 测试更新操作
func TestOceanBaseUpdate(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

	// 插入测试数据
	user := &User{
		Username:  "testuser",
		Email:     "test@example.com",
		Age:       25,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	// 执行更新
	newEmail := "updated@example.com"
	newAge := 30
	result, err = db.ExecWithResult(
		"UPDATE users SET email = ?, age = ? WHERE id = ?",
		newEmail, newAge, userID,
	)
	if err != nil {
		t.Fatalf("更新用户失败: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("获取影响行数失败: %v", err)
	}

	if rowsAffected != 1 {
		t.Fatalf("更新影响行数不匹配，期望: 1, 实际: %d", rowsAffected)
	}

	// 验证更新结果
	var updatedUser User
	err = db.ScanRaw(&updatedUser, "SELECT id, username, email, age, active FROM users WHERE id = ?", userID)
	if err != nil {
		t.Fatalf("查询更新后的用户失败: %v", err)
	}

	if updatedUser.Email != newEmail || updatedUser.Age != newAge {
		t.Fatalf("更新结果不匹配，期望: %s/%d, 实际: %s/%d", newEmail, newAge, updatedUser.Email, updatedUser.Age)
	}
}

// 测试删除操作
func TestOceanBaseDelete(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

	// 插入测试数据
	user := &User{
		Username:  "testuser",
		Email:     "test@example.com",
		Age:       25,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
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

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("获取影响行数失败: %v", err)
	}

	if rowsAffected != 1 {
		t.Fatalf("删除影响行数不匹配，期望: 1, 实际: %d", rowsAffected)
	}

	// 验证删除结果
	var count int
	err = db.ScanRaw(&count, "SELECT COUNT(*) FROM users WHERE id = ?", userID)
	if err != nil {
		t.Fatalf("查询删除后的用户失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("删除验证失败，期望记录数: 0, 实际记录数: %d", count)
	}
}

// 测试OceanBase特有的查询功能
func TestOceanBaseSpecificFeatures(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

	// 插入测试数据
	users := []User{
		{Username: "user1", Email: "user1@example.com", Age: 21, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user2", Email: "user2@example.com", Age: 22, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{Username: "user3", Email: "user3@example.com", Age: 23, Active: true, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	// 批量插入用户
	for _, user := range users {
		_, err := db.ExecWithResult(
			"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
			user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
		)
		if err != nil {
			t.Fatalf("插入用户失败: %v", err)
		}
	}

	// 使用标准查询方法，添加OceanBase特有的提示（如果需要）
	// 注意：这里使用了标准的查询方法，而不是特定于OceanBase的方法
	var result []User
	err := db.ScanRaw(&result, "SELECT /*+ PARALLEL(4) */ * FROM users WHERE age > ? ORDER BY age DESC LIMIT 10", 20)
	if err != nil {
		t.Fatalf("OceanBase特有查询失败: %v", err)
	}

	if len(result) != len(users) {
		t.Fatalf("查询结果数量不匹配，期望: %d, 实际: %d", len(users), len(result))
	}

	// 验证排序是否正确（按年龄降序）
	for i := 0; i < len(result)-1; i++ {
		if result[i].Age < result[i+1].Age {
			t.Fatalf("排序结果不正确，期望降序排列")
		}
	}
}

// 测试事务
func TestOceanBaseTransaction(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	// 在事务中执行插入
	user := &User{
		Username:  "txuser",
		Email:     "tx@example.com",
		Age:       30,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = tx.ExecWithResult(
		"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
	)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中插入用户失败: %v", err)
	}

	// 提交事务
	err = tx.Commit()
	if err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}

	// 验证事务提交后的结果
	var count int
	err = db.ScanRaw(&count, "SELECT COUNT(*) FROM users WHERE username = ?", user.Username)
	if err != nil {
		t.Fatalf("查询事务提交后的用户失败: %v", err)
	}

	if count != 1 {
		t.Fatalf("事务提交验证失败，期望记录数: 1, 实际记录数: %d", count)
	}

	// 测试事务回滚
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("开始第二个事务失败: %v", err)
	}

	rollbackUser := &User{
		Username:  "rollbackuser",
		Email:     "rollback@example.com",
		Age:       35,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	_, err = tx.ExecWithResult(
		"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		rollbackUser.Username, rollbackUser.Email, rollbackUser.Age, rollbackUser.Active, rollbackUser.CreatedAt, rollbackUser.UpdatedAt,
	)
	if err != nil {
		tx.Rollback()
		t.Fatalf("事务中插入回滚用户失败: %v", err)
	}

	// 回滚事务
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("回滚事务失败: %v", err)
	}

	// 验证事务回滚后的结果
	err = db.ScanRaw(&count, "SELECT COUNT(*) FROM users WHERE username = ?", rollbackUser.Username)
	if err != nil {
		t.Fatalf("查询事务回滚后的用户失败: %v", err)
	}

	if count != 0 {
		t.Fatalf("事务回滚验证失败，期望记录数: 0, 实际记录数: %d", count)
	}
}
func TestOceanBaseForUpdate(t *testing.T) {
	// 初始化数据库
	db := initOceanBase(t)
	defer db.Close()

	// 准备测试表
	prepareOceanBaseTestTables(t, db)

	// 插入测试数据
	user := &User{
		Username:  "lockuser",
		Email:     "lock@example.com",
		Age:       28,
		Active:    true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	result, err := db.ExecWithResult(
		"INSERT INTO users (username, email, age, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		user.Username, user.Email, user.Age, user.Active, user.CreatedAt, user.UpdatedAt,
	)
	if err != nil {
		t.Fatalf("插入用户失败: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("获取插入ID失败: %v", err)
	}

	// 开始事务
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开始事务失败: %v", err)
	}

	// 获取 OceanBase 适配器
	oceanbaseAdapter, ok := db.Adapter().(*adapter.OceanBase)
	if !ok {
		tx.Rollback()
		t.Fatalf("获取 OceanBase 适配器失败")
	}

	// 使用 FOR UPDATE 锁定行
	var lockedUser User
	forUpdateSQL := fmt.Sprintf("SELECT id, username, email, age, active FROM users WHERE id = ? %s", oceanbaseAdapter.ForUpdate())
	err = tx.ScanRaw(&lockedUser, forUpdateSQL, userID)
	if err != nil {
		tx.Rollback()
		t.Fatalf("锁定行失败: %v", err)
	}

	// 在另一个 goroutine 中尝试更新同一行（应该被阻塞）
	updateCompleted := make(chan bool)
	go func() {
		// 创建新的连接
		db2 := initOceanBase(t)
		defer db2.Close()

		// 尝试更新被锁定的行
		_, err := db2.ExecWithResult(
			"UPDATE users SET email = ? WHERE id = ?",
			"updated@example.com", userID,
		)

		// 通知更新完成
		updateCompleted <- (err == nil)
	}()

	// 等待一小段时间，确保第二个事务已经尝试更新
	time.Sleep(time.Second)

	// 提交第一个事务，释放锁
	err = tx.Commit()
	if err != nil {
		t.Fatalf("提交事务失败: %v", err)
	}

	// 等待第二个事务完成
	updateSuccess := <-updateCompleted

	// 验证第二个事务是否成功
	if !updateSuccess {
		t.Fatalf("第二个事务更新失败")
	}

	// 验证更新是否生效
	var updatedUser User
	err = db.ScanRaw(&updatedUser, "SELECT id, username, email, age, active FROM users WHERE id = ?", userID)
	if err != nil {
		t.Fatalf("查询更新后的用户失败: %v", err)
	}

	if updatedUser.Email != "updated@example.com" {
		t.Fatalf("更新结果不匹配，期望: %s, 实际: %s", "updated@example.com", updatedUser.Email)
	}
}

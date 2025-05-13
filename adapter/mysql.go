package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// MySQL 适配器结构体
type MySQL struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewMySQL 创建新的MySQL适配器
func NewMySQL(dsn string) *MySQL {
	return &MySQL{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (m *MySQL) WithMaxIdle(maxIdle int) *MySQL {
	m.MaxIdle = maxIdle
	return m
}

// WithMaxOpen 设置最大打开连接数
func (m *MySQL) WithMaxOpen(maxOpen int) *MySQL {
	m.MaxOpen = maxOpen
	return m
}

// WithMaxLifetime 设置连接最大生命周期
func (m *MySQL) WithMaxLifetime(maxLifetime time.Duration) *MySQL {
	m.MaxLifetime = maxLifetime
	return m
}

// WithDebug 设置调试模式
func (m *MySQL) WithDebug(debug bool) *MySQL {
	m.Debug = debug
	return m
}

// Connect 连接数据库
func (m *MySQL) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if m.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库
	db, err := gorm.Open(mysql.Open(m.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(m.MaxIdle)
	sqlDB.SetMaxOpenConns(m.MaxOpen)
	sqlDB.SetConnMaxLifetime(m.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (m *MySQL) BuildDSN(host string, port int, username, password, database string, params map[string]string) string {
	// 基本DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)

	// 添加参数
	if len(params) > 0 {
		var parameters []string
		for k, v := range params {
			parameters = append(parameters, fmt.Sprintf("%s=%s", k, v))
		}
		dsn = dsn + "?" + strings.Join(parameters, "&")
	} else {
		// 默认参数
		dsn = dsn + "?charset=utf8mb4&parseTime=True&loc=Local"
	}

	return dsn
}

// ForUpdate 生成FOR UPDATE锁定语句
func (m *MySQL) ForUpdate() string {
	return "FOR UPDATE"
}

// ForShare 生成FOR SHARE锁定语句
func (m *MySQL) ForShare() string {
	return "LOCK IN SHARE MODE"
}

// Limit 生成LIMIT语句
func (m *MySQL) Limit(offset, limit int) string {
	return fmt.Sprintf("LIMIT %d, %d", offset, limit)
}

// InsertIgnore 生成INSERT IGNORE语句
func (m *MySQL) InsertIgnore(db *gorm.DB, table string, values interface{}) *gorm.DB {
	return db.Clauses(clause.Insert{Modifier: "IGNORE"}).Table(table).Create(values)
}

//// OnDuplicateKeyUpdate 生成ON DUPLICATE KEY UPDATE语句
//func (m *MySQL) OnDuplicateKeyUpdate(db *gorm.DB, table string, values interface{}, updateColumns []string) *gorm.DB {
//	// 构建ON DUPLICATE KEY UPDATE子句
//	var updates = make(map[string]interface{})
//	for _, column := range updateColumns {
//		updates[column] = clause.Expr{SQL: fmt.Sprintf("VALUES(%s)", column)}
//	}
//
//	return db.Clauses(clause.OnConflict{
//		DoUpdates: clause.Assignments(updates),
//	}).Table(table).Create(values)
//}

// OnDuplicateKeyUpdate 生成ON DUPLICATE KEY UPDATE语句
func (m *MySQL) OnDuplicateKeyUpdate(db *gorm.DB, table string, values interface{}, updateColumns []string) *gorm.DB {
	return db.Clauses(clause.OnConflict{
		DoUpdates: clause.AssignmentColumns(updateColumns),
	}).Table(table).Create(values)
}

// BatchInsert 批量插入
func (m *MySQL) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// 构建SQL语句
	var placeholders []string
	for range values {
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.TrimRight(strings.Repeat("?,", len(columns)), ",")))
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 构建完整SQL
	sqlStr := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
	)

	// 执行SQL
	return db.Exec(sqlStr, flatValues...).Error
}

// BatchInsertOrUpdate 批量插入或更新
func (m *MySQL) BatchInsertOrUpdate(db *gorm.DB, table string, columns []string, values [][]interface{}, updateColumns []string) error {
	if len(values) == 0 {
		return nil
	}

	// 构建SQL语句
	var placeholders []string
	for range values {
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.TrimRight(strings.Repeat("?,", len(columns)), ",")))
	}

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 构建ON DUPLICATE KEY UPDATE子句
	var updates []string
	for _, column := range updateColumns {
		updates = append(updates, fmt.Sprintf("%s=VALUES(%s)", column, column))
	}

	// 构建完整SQL
	sqlStr := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		table,
		strings.Join(columns, ","),
		strings.Join(placeholders, ","),
		strings.Join(updates, ","),
	)

	// 执行SQL
	return db.Exec(sqlStr, flatValues...).Error
}

// MergeInto 合并插入（UPSERT）- MySQL实现
func (m *MySQL) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 || len(keyColumns) == 0 {
		return nil
	}

	// MySQL UPSERT (INSERT ... ON DUPLICATE KEY UPDATE)
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

	var placeholders []string
	var flatValues []interface{}

	for _, row := range values {
		var rowPlaceholders []string
		for _, val := range row {
			rowPlaceholders = append(rowPlaceholders, "?")
			flatValues = append(flatValues, val)
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	sqlBuilder.WriteString(strings.Join(placeholders, ", "))

	if len(updateColumns) > 0 {
		sqlBuilder.WriteString(" ON DUPLICATE KEY UPDATE ")
		var updates []string
		for _, col := range updateColumns {
			updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", col, col))
		}
		sqlBuilder.WriteString(strings.Join(updates, ", "))
	}

	return db.Exec(sqlBuilder.String(), flatValues...).Error
}

// CreateDatabase 创建数据库
func (m *MySQL) CreateDatabase(db *gorm.DB, name string, charset string, collation string) error {
	if charset == "" {
		charset = "utf8mb4"
	}
	if collation == "" {
		collation = "utf8mb4_general_ci"
	}

	sqlStr := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET %s COLLATE %s", name, charset, collation)
	return db.Exec(sqlStr).Error
}

// DropDatabase 删除数据库
func (m *MySQL) DropDatabase(db *gorm.DB, name string) error {
	sqlStr := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name)
	return db.Exec(sqlStr).Error
}

// ShowDatabases 显示所有数据库
func (m *MySQL) ShowDatabases(db *gorm.DB) ([]string, error) {
	var databases []string
	err := db.Raw("SHOW DATABASES").Scan(&databases).Error
	return databases, err
}

// ShowTables 显示所有表
func (m *MySQL) ShowTables(db *gorm.DB) ([]string, error) {
	var tables []string
	err := db.Raw("SHOW TABLES").Scan(&tables).Error
	return tables, err
}

// ShowCreateTable 显示创建表的SQL
func (m *MySQL) ShowCreateTable(db *gorm.DB, table string) (string, error) {
	var result struct {
		Table       string
		CreateTable string `gorm:"column:Create Table"`
	}

	err := db.Raw(fmt.Sprintf("SHOW CREATE TABLE `%s`", table)).Scan(&result).Error
	return result.CreateTable, err
}

// TruncateTable 清空表
func (m *MySQL) TruncateTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table)).Error
}

// OptimizeTable 优化表
func (m *MySQL) OptimizeTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("OPTIMIZE TABLE `%s`", table)).Error
}

// AnalyzeTable 分析表
func (m *MySQL) AnalyzeTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("ANALYZE TABLE `%s`", table)).Error
}

// CheckTable 检查表
func (m *MySQL) CheckTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("CHECK TABLE `%s`", table)).Error
}

// RepairTable 修复表
func (m *MySQL) RepairTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("REPAIR TABLE `%s`", table)).Error
}

// GetTableStatus 获取表状态
func (m *MySQL) GetTableStatus(db *gorm.DB, table string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := db.Raw(fmt.Sprintf("SHOW TABLE STATUS LIKE '%s'", table)).Scan(&result).Error
	return result, err
}

// GetVersion 获取MySQL版本
func (m *MySQL) GetVersion(db *gorm.DB) (string, error) {
	var version string
	err := db.Raw("SELECT VERSION()").Scan(&version).Error
	return version, err
}

// GetVariables 获取MySQL变量
func (m *MySQL) GetVariables(db *gorm.DB, pattern string) (map[string]string, error) {
	var results []struct {
		Variable string `gorm:"column:Variable_name"`
		Value    string `gorm:"column:Value"`
	}

	var err error
	if pattern == "" {
		err = db.Raw("SHOW VARIABLES").Scan(&results).Error
	} else {
		err = db.Raw("SHOW VARIABLES LIKE ?", pattern).Scan(&results).Error
	}

	if err != nil {
		return nil, err
	}

	variables := make(map[string]string)
	for _, result := range results {
		variables[result.Variable] = result.Value
	}

	return variables, nil
}

// GetStatus 获取MySQL状态
func (m *MySQL) GetStatus(db *gorm.DB, pattern string) (map[string]string, error) {
	var results []struct {
		Variable string `gorm:"column:Variable_name"`
		Value    string `gorm:"column:Value"`
	}

	var err error
	if pattern == "" {
		err = db.Raw("SHOW STATUS").Scan(&results).Error
	} else {
		err = db.Raw("SHOW STATUS LIKE ?", pattern).Scan(&results).Error
	}

	if err != nil {
		return nil, err
	}

	status := make(map[string]string)
	for _, result := range results {
		status[result.Variable] = result.Value
	}

	return status, nil
}

// GetProcessList 获取进程列表
func (m *MySQL) GetProcessList(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw("SHOW PROCESSLIST").Scan(&results).Error
	return results, err
}

// KillProcess 杀死进程
func (m *MySQL) KillProcess(db *gorm.DB, id int) error {
	return db.Exec("KILL ?", id).Error
}

// GetCharsets 获取字符集列表
func (m *MySQL) GetCharsets(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw("SHOW CHARACTER SET").Scan(&results).Error
	return results, err
}

// GetCollations 获取排序规则列表
func (m *MySQL) GetCollations(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw("SHOW COLLATION").Scan(&results).Error
	return results, err
}

// GetEngines 获取存储引擎列表
func (m *MySQL) GetEngines(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw("SHOW ENGINES").Scan(&results).Error
	return results, err
}

// GetPrivileges 获取权限列表
func (m *MySQL) GetPrivileges(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw("SHOW PRIVILEGES").Scan(&results).Error
	return results, err
}

// GetUsers 获取用户列表
func (m *MySQL) GetUsers(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw("SELECT * FROM mysql.user").Scan(&results).Error
	return results, err
}

// CreateUser 创建用户
func (m *MySQL) CreateUser(db *gorm.DB, username, password, host string) error {
	sqlStr := fmt.Sprintf("CREATE USER '%s'@'%s' IDENTIFIED BY '%s'", username, host, password)
	return db.Exec(sqlStr).Error
}

// DropUser 删除用户
func (m *MySQL) DropUser(db *gorm.DB, username, host string) error {
	sqlStr := fmt.Sprintf("DROP USER IF EXISTS '%s'@'%s'", username, host)
	return db.Exec(sqlStr).Error
}

// GrantPrivileges 授予权限
func (m *MySQL) GrantPrivileges(db *gorm.DB, username, host, database, table, privileges string) error {
	sqlStr := fmt.Sprintf("GRANT %s ON %s.%s TO '%s'@'%s'", privileges, database, table, username, host)
	return db.Exec(sqlStr).Error
}

// RevokePrivileges 撤销权限
func (m *MySQL) RevokePrivileges(db *gorm.DB, username, host, database, table, privileges string) error {
	sqlStr := fmt.Sprintf("REVOKE %s ON %s.%s FROM '%s'@'%s'", privileges, database, table, username, host)
	return db.Exec(sqlStr).Error
}

// FlushPrivileges 刷新权限
func (m *MySQL) FlushPrivileges(db *gorm.DB) error {
	return db.Exec("FLUSH PRIVILEGES").Error
}

// QueryPage 分页查询
func (m *MySQL) QueryPage(out interface{}, page, pageSize int, filter interface{}, opts ...interface{}) (int64, error) {
	// 参数验证
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}

	// 从 opts 中提取 db 和其他参数
	if len(opts) == 0 {
		return 0, fmt.Errorf("缺少必要参数：数据库连接")
	}

	db, ok := opts[0].(*gorm.DB)
	if !ok {
		return 0, fmt.Errorf("第一个可选参数必须是 *gorm.DB 类型")
	}

	// 处理 filter 参数
	var sqlStr string
	var values []interface{}

	switch f := filter.(type) {
	case string:
		// 如果 filter 是 SQL 字符串
		sqlStr = f
		// 提取剩余的参数作为 values
		if len(opts) > 1 {
			for _, v := range opts[1:] {
				values = append(values, v)
			}
		}
	case map[string]interface{}:
		// 如果 filter 是条件映射，构建 WHERE 子句
		// 这里需要根据您的具体需求实现
		return 0, fmt.Errorf("暂不支持 map 类型的过滤条件")
	default:
		return 0, fmt.Errorf("不支持的过滤条件类型")
	}

	// 计算偏移量
	offset := (page - 1) * pageSize

	// 查询总记录数
	var total int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_table", sqlStr)
	err := db.Raw(countSQL, values...).Count(&total).Error
	if err != nil {
		return 0, fmt.Errorf("查询总记录数失败: %w", err)
	}

	// 如果没有记录，直接返回
	if total == 0 {
		return 0, nil
	}

	// 查询分页数据
	pageSQL := fmt.Sprintf("%s LIMIT %d OFFSET %d", sqlStr, pageSize, offset)
	err = db.Raw(pageSQL, values...).Scan(out).Error
	if err != nil {
		return 0, fmt.Errorf("查询分页数据失败: %w", err)
	}

	return total, nil
}

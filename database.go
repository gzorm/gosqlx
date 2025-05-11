package gosqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	oracle "github.com/seelly/gorm-oracle"

	"gorm.io/driver/sqlite"

	"github.com/gzorm/gosqlx/adapter"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Database 数据库操作核心结构
type Database struct {
	db       *gorm.DB        // GORM数据库连接
	sqlDB    *sql.DB         // 原生SQL数据库连接
	dbType   DatabaseType    // 数据库类型
	deadlock *Deadlock       // 死锁检测器
	ctx      *Context        // 数据库上下文
	adapter  adapter.Adapter // 添加适配器字段
}

// Deadlock 死锁检测器
type Deadlock struct {
	ctx   *Context       // 上下文
	mutex sync.Mutex     // 互斥锁
	locks map[string]int // 锁定的表
}

// NewDeadlock 创建新的死锁检测器
func NewDeadlock(ctx *Context) *Deadlock {
	return &Deadlock{
		ctx:   ctx,
		locks: make(map[string]int),
	}
}

// Attach 添加锁定的表
func (d *Deadlock) Attach(table interface{}) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	tableName := reflectTableName(table)
	if _, ok := d.locks[tableName]; ok {
		d.locks[tableName]++
	} else {
		d.locks[tableName] = 1
	}
}

// Verify 验证是否有死锁风险
func (d *Deadlock) Verify() bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, count := range d.locks {
		if count > 1 {
			return false
		}
	}
	return true
}

// Print 打印死锁信息
func (d *Deadlock) Print() string {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var result strings.Builder
	result.WriteString(fmt.Sprintf("数据库(%s)可能存在死锁风险:\n", d.ctx.Nick))

	for table, count := range d.locks {
		if count > 1 {
			result.WriteString(fmt.Sprintf("  表(%s)被锁定了%d次\n", table, count))
		}
	}

	return result.String()
}

// NewDatabase 创建新的数据库操作实例
func NewDatabase(ctx *Context, config *Config) (*Database, error) {
	if ctx == nil {
		return nil, errors.New("上下文不能为空")
	}

	if config == nil {
		return nil, errors.New("配置不能为空")
	}

	// 创建GORM配置
	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	}

	if config.Debug {
		gormConfig.Logger = logger.Default.LogMode(logger.Info)
	}
	// MongoDB 需要特殊处理
	if config.Type == MongoDB {
		// 从连接字符串中解析数据库名称
		// MongoDB 连接字符串格式通常为：mongodb://user:pass@host:port/dbname
		dbName := ""
		parts := strings.Split(config.Source, "/")
		if len(parts) > 3 {
			dbNameParts := strings.Split(parts[3], "?")
			dbName = dbNameParts[0]
		}

		if dbName == "" {
			dbName = "admin" // 默认数据库名
		}

		// MongoDB 使用自定义适配器，不使用 GORM 的方言
		adapterInstance := adapter.NewMongoDB(config.Source, dbName)

		// 设置连接池参数
		adapterInstance.WithMaxIdle(config.MaxIdle)
		adapterInstance.WithMaxOpen(config.MaxOpen)
		adapterInstance.WithMaxLifetime(config.MaxLifetime)
		adapterInstance.WithDebug(config.Debug)

		// 连接 MongoDB
		_, _, err := adapterInstance.Connect()
		if err != nil {
			return nil, err
		}

		// 创建数据库操作实例
		database := &Database{
			db:       nil, // MongoDB 不使用 GORM
			sqlDB:    nil, // MongoDB 不使用标准 SQL
			dbType:   config.Type,
			deadlock: NewDeadlock(ctx),
			ctx:      ctx,
			adapter:  adapterInstance,
		}

		return database, nil
	}
	// 根据数据库类型创建方言
	var dialector gorm.Dialector
	switch config.Type {
	case MySQL:
		dialector = mysql.Open(config.Source)
	case PostgreSQL:
		dialector = postgres.Open(config.Source)
	case SQLServer:
		dialector = sqlserver.Open(config.Source)
	case SQLite:
		dialector = sqlite.Open(config.Source)
	case Oracle:
		dialector = oracle.Open(config.Source)
	case TiDB:
		// TiDB 使用 MySQL 驱动，但需要特殊处理
		dialector = mysql.Open(config.Source)
	default:
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.Type)
	}

	// 创建GORM连接
	db, err := gorm.Open(dialector, gormConfig)
	if err != nil {
		return nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(config.MaxIdle)
	sqlDB.SetMaxOpenConns(config.MaxOpen)
	sqlDB.SetConnMaxLifetime(config.MaxLifetime)

	// 创建适配器实例
	var adapterInstance adapter.Adapter
	switch config.Type {
	case MySQL:
		adapterInstance = adapter.NewMySQL(config.Source)
	case PostgreSQL:
		adapterInstance = adapter.NewPostgres(config.Source)
	case SQLite:
		adapterInstance = adapter.NewSQLite(config.Source)
	case SQLServer:
		adapterInstance = adapter.NewSQLServer(config.Source)
	case Oracle:
		adapterInstance = adapter.NewOracle(config.Source)
	case TiDB:
		adapterInstance = adapter.NewTiDB(config.Source)
	default:
		return nil, fmt.Errorf("不支持的数据库类型: %s", config.Type)
	}
	// 创建数据库操作实例
	database := &Database{
		db:       db,
		sqlDB:    sqlDB,
		dbType:   config.Type,
		deadlock: NewDeadlock(ctx),
		ctx:      ctx,
		adapter:  adapterInstance,
	}

	return database, nil
}

// DSN 返回数据库连接字符串
func (d *Database) DSN() string {
	switch adapter := d.adapter.(type) {
	case *adapter.MySQL:
		return adapter.DSN
	case *adapter.Postgres:
		return adapter.DSN
	case *adapter.SQLite:
		return adapter.DSN
	case *adapter.SQLServer:
		return adapter.DSN
	case *adapter.Oracle:
		return adapter.DSN
	case *adapter.MongoDB:
		return adapter.URI
	default:
		return ""
	}
}

// GetDBContext 获取数据库操作上下文
func (d *Database) GetDBContext() *DBContext {
	return NewDBContext(d.ctx, d.db, d.sqlDB)
}

// Adapter 返回底层数据库适配器
func (d *Database) Adapter() interface{} {
	return d.adapter // 假设Database结构体中有adapter字段
}

// Close 关闭数据库连接
func (d *Database) Close() error {
	return d.sqlDB.Close()
}

// Ping 测试数据库连接
func (d *Database) Ping() error {
	return d.sqlDB.Ping()
}

// 表操作相关方法

// Table 获取表操作对象
func (d *Database) Table(name string) *gorm.DB {
	return d.db.WithContext(d.ctx).Table(name)
}

// Model 获取模型操作对象
func (d *Database) Model(value interface{}) *gorm.DB {
	return d.db.WithContext(d.ctx).Model(value)
}

// 查询相关方法

// First 查询第一条记录
func (d *Database) First(out interface{}, where ...interface{}) error {
	return d.Model(out).First(out, where...).Error
}

// Find 查询多条记录
func (d *Database) Find(out interface{}, where ...interface{}) error {
	return d.Model(out).Find(out, where...).Error
}

// FindWhere 根据条件查询多条记录
func (d *Database) FindWhere(out interface{}, where string, values ...interface{}) error {
	return d.Model(out).Where(formatWhere(where), values...).Find(out).Error
}

// FindOrder 根据条件和排序查询多条记录
func (d *Database) FindOrder(out interface{}, order, where string, values ...interface{}) error {
	return d.Model(out).Where(formatWhere(where), values...).Order(order).Find(out).Error
}

// 锁相关方法

// Lock 锁定记录
func (d *Database) Lock(out interface{}, ids ...interface{}) error {
	// 添加死锁检测
	d.deadlock.Attach(out)
	// MongoDB 不支持标准的 FOR UPDATE 锁定
	if d.dbType == MongoDB {
		// 对于 MongoDB，可以使用 findAndModify 操作或事务来实现锁定
		// 这里简化处理，仅返回查询结果
		if len(ids) > 0 {
			return d.Model(out).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
		}
		return d.Model(out).FirstOrInit(out, ids...).Error
	}

	// 根据数据库类型选择锁定语法
	lockOption := "FOR UPDATE"
	switch d.dbType {
	case SQLServer:
		lockOption = "WITH (UPDLOCK, ROWLOCK)"
	case Oracle:
		lockOption = "FOR UPDATE NOWAIT"
	case PostgreSQL:
		lockOption = "FOR UPDATE"
	}

	// 多键查询
	if len(ids) > 0 {
		return d.Model(out).Set("gorm:query_option", lockOption).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
	}

	// 单键查询
	return d.Model(out).Set("gorm:query_option", lockOption).FirstOrInit(out, ids...).Error
}

// LockWhere 根据条件锁定记录
func (d *Database) LockWhere(out interface{}, where string, values ...interface{}) error {
	d.deadlock.Attach(out)

	// 根据数据库类型选择锁定语法
	lockOption := "FOR UPDATE"
	if d.dbType == SQLServer {
		lockOption = "WITH (UPDLOCK)"
	}

	return d.Model(out).Set("gorm:query_option", lockOption).Where(formatWhere(where), values...).FirstOrInit(out).Error
}

// LockOrder 按顺序锁定记录
func (d *Database) LockOrder(out interface{}, order, where string, values ...interface{}) error {
	d.deadlock.Attach(out)
	// MongoDB 不支持标准的 FOR UPDATE 锁定
	if d.dbType == MongoDB {
		// 对于 MongoDB，可以使用 findAndModify 操作或事务来实现锁定
		// 这里简化处理，仅返回查询结果
		return d.Model(out).Where(formatWhere(where), values...).Order(order).FirstOrInit(out).Error
	}
	// 根据数据库类型选择锁定语法
	lockOption := "FOR UPDATE"
	switch d.dbType {
	case SQLServer:
		lockOption = "WITH (UPDLOCK, ROWLOCK)"
	case Oracle:
		lockOption = "FOR UPDATE NOWAIT"
	case PostgreSQL:
		lockOption = "FOR UPDATE"
	}

	return d.Model(out).Set("gorm:query_option", lockOption).Where(formatWhere(where), values...).Order(order).FirstOrInit(out).Error
}

// LockShare 共享锁定记录
func (d *Database) LockShare(out interface{}, ids ...interface{}) error {
	d.deadlock.Attach(out)

	// 根据数据库类型选择共享锁语法
	lockOption := "FOR SHARE"
	switch d.dbType {
	case SQLServer:
		lockOption = "WITH (HOLDLOCK, ROWLOCK)"
	case Oracle:
		lockOption = "FOR UPDATE NOWAIT"
	case PostgreSQL:
		lockOption = "FOR SHARE"
	}

	// 多键查询
	if len(ids) > 0 {
		return d.Model(out).Set("gorm:query_option", lockOption).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
	}

	// 单键查询
	return d.Model(out).Set("gorm:query_option", lockOption).FirstOrInit(out, ids...).Error
}

// LockMulti 锁定多条记录
func (d *Database) LockMulti(out interface{}, where string, values ...interface{}) error {
	d.deadlock.Attach(out)

	// 根据数据库类型选择锁定语法
	lockOption := "FOR UPDATE"
	switch d.dbType {
	case SQLServer:
		lockOption = "WITH (UPDLOCK, ROWLOCK)"
	case Oracle:
		lockOption = "FOR UPDATE NOWAIT"
	case PostgreSQL:
		lockOption = "FOR UPDATE"
	}

	return d.Model(out).Set("gorm:query_option", lockOption).Where(formatWhere(where), values...).Find(out).Error
}

// BatchInsert 批量插入记录
func (d *Database) BatchInsert(table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	switch d.dbType {
	case MySQL:
		// MySQL 批量插入
		var sql strings.Builder
		sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

		var placeholders []string
		var flatValues []interface{}

		for _, row := range values {
			var rowPlaceholders []string
			for range columns {
				rowPlaceholders = append(rowPlaceholders, "?")
				flatValues = append(flatValues, row...)
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
		}

		sql.WriteString(strings.Join(placeholders, ", "))

		return d.db.Exec(sql.String(), flatValues...).Error

	case SQLServer:
		// 使用 SQLServer 适配器的批量插入
		adapter := &adapter.SQLServer{}
		return adapter.BatchInsert(d.db, table, columns, values)

	case Oracle:
		// 使用 Oracle 适配器的批量插入
		adapter := &adapter.Oracle{}
		return adapter.BatchInsert(d.db, table, columns, values)

	case PostgreSQL:
		// PostgreSQL 批量插入
		var sql strings.Builder
		sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

		var placeholders []string
		var flatValues []interface{}

		paramCount := 1
		for _, row := range values {
			var rowPlaceholders []string
			for _, val := range row {
				rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", paramCount))
				flatValues = append(flatValues, val)
				paramCount++
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
		}

		sql.WriteString(strings.Join(placeholders, ", "))

		return d.db.Exec(sql.String(), flatValues...).Error
	}

	return errors.New("不支持的数据库类型")
}

// MergeInto 合并插入记录（UPSERT）
func (d *Database) MergeInto(table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 || len(keyColumns) == 0 {
		return nil
	}

	switch d.dbType {
	case MySQL:
		// MySQL UPSERT (INSERT ... ON DUPLICATE KEY UPDATE)
		var sql strings.Builder
		sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

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

		sql.WriteString(strings.Join(placeholders, ", "))

		if len(updateColumns) > 0 {
			sql.WriteString(" ON DUPLICATE KEY UPDATE ")
			var updates []string
			for _, col := range updateColumns {
				updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", col, col))
			}
			sql.WriteString(strings.Join(updates, ", "))
		}

		return d.db.Exec(sql.String(), flatValues...).Error

	case SQLServer:
		// 使用 SQLServer 适配器的 MERGE INTO
		adapter := &adapter.SQLServer{}
		return adapter.MergeInto(d.db, table, columns, values, keyColumns, updateColumns)

	case Oracle:
		// 使用 Oracle 适配器的 MERGE INTO
		adapter := &adapter.Oracle{}
		return adapter.MergeInto(d.db, table, columns, values, keyColumns, updateColumns)

	case PostgreSQL:
		// PostgreSQL UPSERT (INSERT ... ON CONFLICT ... DO UPDATE)
		var sql strings.Builder
		sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

		var placeholders []string
		var flatValues []interface{}

		paramCount := 1
		for _, row := range values {
			var rowPlaceholders []string
			for _, val := range row {
				rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", paramCount))
				flatValues = append(flatValues, val)
				paramCount++
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
		}

		sql.WriteString(strings.Join(placeholders, ", "))

		if len(updateColumns) > 0 && len(keyColumns) > 0 {
			sql.WriteString(fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET ", strings.Join(keyColumns, ", ")))
			var updates []string
			for _, col := range updateColumns {
				updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
			sql.WriteString(strings.Join(updates, ", "))
		}

		return d.db.Exec(sql.String(), flatValues...).Error
	}

	return errors.New("不支持的数据库类型")
}

// Create 创建记录
func (d *Database) Create(value interface{}) error {
	return d.db.WithContext(d.ctx).Create(value).Error
}

// Save 保存记录
func (d *Database) Save(value interface{}) error {
	return d.db.WithContext(d.ctx).Save(value).Error
}

// Update 更新记录
func (d *Database) Update(value interface{}, attrs ...interface{}) error {
	return d.Model(value).Updates(attrs).Error
}

// UpdateMap 使用Map更新记录
func (d *Database) UpdateMap(value interface{}, attrs map[string]interface{}) error {
	return d.Model(value).Updates(attrs).Error
}

// Delete 删除记录
func (d *Database) Delete(value interface{}, where ...interface{}) error {
	return d.db.WithContext(d.ctx).Delete(value, where...).Error
}

// 事务相关方法
// Begin 开始一个新事务
func (d *Database) Begin() (*Database, error) {
	tx := d.db.WithContext(d.ctx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}

	return &Database{
		db:       tx,
		sqlDB:    d.sqlDB,
		dbType:   d.dbType,
		deadlock: d.deadlock,
		ctx:      d.ctx,
	}, nil
}

// Commit 提交事务
func (d *Database) Commit() error {
	return d.db.Commit().Error
}

// Rollback 回滚事务
func (d *Database) Rollback() error {
	return d.db.Rollback().Error
}

// Transaction 执行事务
func (d *Database) Transaction(fn func(tx *Database) error) error {
	return d.db.WithContext(d.ctx).Transaction(func(tx *gorm.DB) error {
		txDB := &Database{
			db:       tx,
			sqlDB:    d.sqlDB,
			dbType:   d.dbType,
			deadlock: d.deadlock,
			ctx:      d.ctx,
		}
		return fn(txDB)
	})
}

// 原生SQL相关方法

// Exec 执行原生SQL
func (d *Database) Exec(sql string, values ...interface{}) error {
	return d.db.WithContext(d.ctx).Exec(sql, values...).Error
}

// ExecWithResult 执行原生SQL返回结果
func (d *Database) ExecWithResult(sqlStr string, values ...interface{}) (sql.Result, error) {
	// 使用原生SQL连接执行语句
	return d.sqlDB.ExecContext(d.ctx, sqlStr, values...)
}

// Raw 执行原生查询
func (d *Database) Raw(sql string, values ...interface{}) *gorm.DB {
	return d.db.WithContext(d.ctx).Raw(sql, values...)
}

// ScanRaw 执行原生查询并扫描结果
func (d *Database) ScanRaw(out interface{}, sql string, values ...interface{}) error {
	return d.Raw(sql, values...).Scan(out).Error
}

// Query 执行查询并返回结果集(集合)
func (d *Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := d.db.WithContext(d.ctx).Raw(query, args...).Rows()
	return rows, err
}

// QueryRow 执行查询并返回单行结果
func (d *Database) QueryRow(query string, args ...interface{}) *sql.Row {
	row := d.db.WithContext(d.ctx).Raw(query, args...).Row()
	return row
}

// QueryRows 查询多条记录
func (d *Database) QueryRows(out interface{}, sql string, values ...interface{}) error {
	return d.Raw(sql, values...).Scan(out).Error
}

// QueryPage 分页查询
func (d *Database) QueryPage(out interface{}, page, pageSize int, sql string, values ...interface{}) (int64, error) {
	var total int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS t", sql)
	err := d.Raw(countSQL, values...).Scan(&total).Error
	if err != nil {
		return 0, err
	}

	if page > 0 && pageSize > 0 {
		offset := (page - 1) * pageSize
		sql = fmt.Sprintf("%s LIMIT %d OFFSET %d", sql, pageSize, offset)
	}

	err = d.Raw(sql, values...).Scan(out).Error
	return total, err
}

// Count 计数查询
func (d *Database) Count(out interface{}) (int64, error) {
	var count int64
	err := d.Model(out).Count(&count).Error
	return count, err
}

// CountWhere 条件计数查询
func (d *Database) CountWhere(out interface{}, where string, values ...interface{}) (int64, error) {
	var count int64
	err := d.Model(out).Where(formatWhere(where), values...).Count(&count).Error
	return count, err
}

// 批量操作相关方法

// BatchCreate 批量创建记录
func (d *Database) BatchCreate(values interface{}) error {
	return d.db.WithContext(d.ctx).CreateInBatches(values, 100).Error
}

func (db *Database) DB() interface{} {
	return db.db // 假设内部有一个 db 字段存储底层连接
}

// 辅助函数

// formatWhere 格式化WHERE条件
func formatWhere(where string) string {
	if strings.HasPrefix(where, "AND ") {
		return strings.Replace(where, "AND ", "", 1)
	}
	return where
}

// reflectTableName 反射获取表名
func reflectTableName(out interface{}) string {
	t := reflect.TypeOf(out)

	// 处理字符串类型
	if t.Kind() == reflect.String {
		return out.(string)
	}

	// 处理指针类型
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// 处理结构体类型
	if t.Kind() == reflect.Struct {
		// 尝试使用GORM的表名推断
		if tabler, ok := out.(interface{ TableName() string }); ok {
			return tabler.TableName()
		}

		// 使用类型名作为表名
		parts := strings.Split(t.String(), ".")
		return parts[len(parts)-1]
	}

	// 处理切片类型
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		parts := strings.Split(t.String(), ".")
		return parts[len(parts)-1]
	}

	return ""
}

// reflectKeys 反射获取主键条件
func reflectKeys(out interface{}) string {
	// 这里简化处理，假设主键为id
	// 在实际应用中，应该通过反射或GORM的API获取真实的主键
	return "id = ?"
}

// DatabaseManager 数据库管理器
type DatabaseManager struct {
	configManager *ConfigManager
	databases     map[string]*Database
	mutex         sync.RWMutex
}

// NewDatabaseManager 创建数据库管理器
func NewDatabaseManager(configManager *ConfigManager) *DatabaseManager {
	return &DatabaseManager{
		configManager: configManager,
		databases:     make(map[string]*Database),
	}
}

// GetDatabase 获取数据库连接
func (m *DatabaseManager) GetDatabase(ctx *Context) (*Database, error) {
	key := fmt.Sprintf("%s:%s", ctx.Nick, ctx.Mode)

	// 先尝试从缓存获取
	m.mutex.RLock()
	db, ok := m.databases[key]
	m.mutex.RUnlock()

	if ok {
		return db, nil
	}

	// 缓存中没有，创建新连接
	config, ok := m.configManager.GetConfig("default", ctx.Nick)
	if !ok {
		return nil, fmt.Errorf("找不到数据库配置: %s", ctx.Nick)
	}

	// 创建新的数据库连接
	db, err := NewDatabase(ctx, config)
	if err != nil {
		return nil, err
	}

	// 添加到缓存
	m.mutex.Lock()
	m.databases[key] = db
	m.mutex.Unlock()

	return db, nil
}

// CloseAll 关闭所有数据库连接
func (m *DatabaseManager) CloseAll() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, db := range m.databases {
		db.Close()
	}

	m.databases = make(map[string]*Database)
}

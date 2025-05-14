package gosqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/gzorm/gosqlx/adapter"
	oracle "github.com/seelly/gorm-oracle"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// ==================== 数据库核心结构 ====================

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

// ==================== 数据库管理器 ====================

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
	if ctx == nil {
		return nil, errors.New("上下文不能为空")
	}

	// 构建数据库键
	dbKey := fmt.Sprintf("%s_%s", ctx.Nick, ctx.Mode)

	// 尝试从缓存获取
	m.mutex.RLock()
	if db, ok := m.databases[dbKey]; ok {
		m.mutex.RUnlock()
		return db, nil
	}
	m.mutex.RUnlock()

	// 获取配置
	env := "development" // 默认环境
	dbName := ctx.Nick

	// 如果是只读模式，尝试获取只读数据库配置
	if ctx.IsReadOnly() {
		readOnlyDBName := fmt.Sprintf("%s_readonly", dbName)
		if _, ok := m.configManager.GetConfig(env, readOnlyDBName); ok {
			dbName = readOnlyDBName
		}
	}

	// 获取数据库配置
	config, ok := m.configManager.GetConfig(env, dbName)
	if !ok {
		return nil, fmt.Errorf("找不到数据库配置: %s", dbName)
	}

	// 创建数据库连接
	db, err := NewDatabase(ctx, config)
	if err != nil {
		return nil, err
	}

	// 缓存数据库连接
	m.mutex.Lock()
	m.databases[dbKey] = db
	m.mutex.Unlock()

	return db, nil
}

// CloseAll 关闭所有数据库连接
func (m *DatabaseManager) CloseAll() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var errs []string
	for key, db := range m.databases {
		if db.sqlDB != nil {
			if err := db.sqlDB.Close(); err != nil {
				errs = append(errs, fmt.Sprintf("关闭数据库(%s)失败: %v", key, err))
			}
		}
	}

	// 清空缓存
	m.databases = make(map[string]*Database)

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// ==================== 数据库初始化与连接 ====================

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
	case PostgresSQL:
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
	case MariaDB:
		// MariaDB 使用 MySQL 驱动
		dialector = mysql.Open(config.Source)
	case ClickHouse:
		dialector = clickhouse.Open(config.Source)
	case OceanBase:
		// OceanBase 使用 MySQL 驱动
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
		adapterInstance = adapter.NewMySQL(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case PostgresSQL:
		adapterInstance = adapter.NewPostgres(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case SQLServer:
		adapterInstance = adapter.NewSQLServer(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case SQLite:
		adapterInstance = adapter.NewSQLite(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case Oracle:
		adapterInstance = adapter.NewOracle(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case TiDB:
		adapterInstance = adapter.NewTiDB(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case MariaDB:
		adapterInstance = adapter.NewMariaDB(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case ClickHouse:
		adapterInstance = adapter.NewClickHouse(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
	case OceanBase:
		adapterInstance = adapter.NewOceanBase(config.Source).
			WithMaxIdle(config.MaxIdle).
			WithMaxOpen(config.MaxOpen).
			WithMaxLifetime(config.MaxLifetime).
			WithDebug(config.Debug)
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
	switch adapterInstance := d.adapter.(type) {
	case *adapter.MySQL:
		return adapterInstance.DSN
	case *adapter.Postgres:
		return adapterInstance.DSN
	case *adapter.SQLite:
		return adapterInstance.DSN
	case *adapter.SQLServer:
		return adapterInstance.DSN
	case *adapter.Oracle:
		return adapterInstance.DSN
	case *adapter.MongoDB:
		return adapterInstance.URI
	case *adapter.TiDB:
		return adapterInstance.DSN
	case *adapter.ClickHouse:
		return adapterInstance.DSN
	case *adapter.MariaDB:
		return adapterInstance.DSN
	default:
		return ""
	}
}

// ==================== 基础方法 ====================

// DB 获取GORM数据库连接
func (d *Database) DB() *gorm.DB {
	return d.db
}

// SqlDB 获取原生SQL数据库连接
func (d *Database) SqlDB() *sql.DB {
	return d.sqlDB
}

// Ping 测试数据库连接
func (d *Database) Ping() error {

	return d.sqlDB.Ping()
}

// Type 获取数据库类型
func (d *Database) Type() DatabaseType {
	return d.dbType
}

// Context 获取数据库上下文
func (d *Database) Context() *Context {
	return d.ctx
}

// Adapter 获取数据库适配器
func (d *Database) Adapter() adapter.Adapter {
	return d.adapter
}

// Model 设置模型
func (d *Database) Model(value interface{}) *gorm.DB {
	return d.db.Model(value)
}

// Table 设置表名
func (d *Database) Table(name string) *gorm.DB {
	return d.db.Table(name)
}

// ==================== 查询操作 ====================

// First 查询第一条记录
func (d *Database) First(out interface{}, where ...interface{}) error {
	return d.Model(out).First(out, where...).Error
}

// FirstOrInit 查询第一条记录，如果不存在则初始化
func (d *Database) FirstOrInit(out interface{}, where ...interface{}) error {
	return d.Model(out).FirstOrInit(out, where...).Error
}

// FirstOrCreate 查询第一条记录，如果不存在则创建
func (d *Database) FirstOrCreate(out interface{}, where ...interface{}) error {
	return d.Model(out).FirstOrCreate(out, where...).Error
}

// Find 查询多条记录
func (d *Database) Find(out interface{}, where ...interface{}) error {
	return d.Model(out).Find(out, where...).Error
}

// FindInBatches 批量查询
func (d *Database) FindInBatches(out interface{}, batchSize int, fc func(tx *gorm.DB, batch int) error) error {
	return d.Model(out).FindInBatches(out, batchSize, fc).Error
}

// Pluck 查询单个列
func (d *Database) Pluck(column string, out interface{}) error {
	return d.db.Pluck(column, out).Error
}

// Count 查询记录数
func (d *Database) Count(out interface{}) (int64, error) {
	var count int64
	err := d.Model(out).Count(&count).Error
	return count, err
}

// Exists 检查记录是否存在
func (d *Database) Exists(model interface{}, where ...interface{}) (bool, error) {
	var count int64
	err := d.Model(model).Where(where[0], where[1:]...).Count(&count).Error
	return count > 0, err
}

// Take 获取一条记录，不指定排序
func (d *Database) Take(out interface{}, where ...interface{}) error {
	return d.Model(out).Take(out, where...).Error
}

// Last 获取最后一条记录
func (d *Database) Last(out interface{}, where ...interface{}) error {
	return d.Model(out).Last(out, where...).Error
}

// Scan 将查询结果扫描到结构体
func (d *Database) Scan(dest interface{}) error {
	return d.db.Scan(dest).Error
}

// ScanRows 扫描行
func (d *Database) ScanRows(rows *sql.Rows, dest interface{}) error {
	return d.db.ScanRows(rows, dest)
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
func (d *Database) QueryRows(out interface{}, sqlStr string, values ...interface{}) error {
	return d.Raw(sqlStr, values...).Scan(out).Error
}

// Raw 执行原生SQL查询
func (d *Database) Raw(sql string, values ...interface{}) *gorm.DB {
	return d.db.Raw(sql, values...)
}

// ScanRaw 执行原生查询并扫描结果
func (d *Database) ScanRaw(out interface{}, sql string, values ...interface{}) error {
	return d.Raw(sql, values...).Scan(out).Error
}

// Exec 执行原生SQL
func (d *Database) Exec(sql string, values ...interface{}) error {
	return d.db.Exec(sql, values...).Error
}

// ExecWithResult 执行原生SQL返回结果
func (d *Database) ExecWithResult(sqlStr string, values ...interface{}) (sql.Result, error) {
	// 使用原生SQL连接执行语句
	return d.sqlDB.ExecContext(d.ctx, sqlStr, values...)
}

// QueryPage 分页查询
func (d *Database) QueryPage(dbOption interface{}, out interface{}, page, pageSize int, tableName string, orderBy []interface{}, filter ...interface{}) (int64, error) {
	// 使用适配器的分页查询
	if d.adapter != nil {
		if tableName == "" {
			tableName = reflectTableName(out)
		}

		return d.adapter.QueryPage(dbOption, out, page, pageSize, tableName, orderBy, filter)
	}

	// 计算总数
	var total int64
	err := d.Model(out).Where(filter).Count(&total).Error
	if err != nil {
		return 0, err
	}

	// 如果总数为0，直接返回
	if total == 0 {
		return 0, nil
	}

	// 计算偏移量
	offset := (page - 1) * pageSize

	// 查询数据
	err = d.Model(out).Where(filter).Offset(offset).Limit(pageSize).Find(out).Error
	if err != nil {
		return 0, err
	}

	return total, nil
}

// Lock 锁定记录
func (d *Database) Lock(out interface{}, ids ...interface{}) error {
	// 添加死锁检测
	d.deadlock.Attach(out)

	// MongoDB 不支持标准的锁定
	if d.dbType == MongoDB {
		if len(ids) > 0 {
			return d.Model(out).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
		}
		return d.Model(out).FirstOrInit(out, ids...).Error
	}

	// SQLServer 使用特殊语法
	if d.dbType == SQLServer {
		if len(ids) > 0 {
			return d.Model(out).Set("gorm:query_option", "WITH (UPDLOCK, ROWLOCK)").Where(reflectKeys(out), ids...).FirstOrInit(out).Error
		}
		return d.Model(out).Set("gorm:query_option", "WITH (UPDLOCK, ROWLOCK)").FirstOrInit(out, ids...).Error
	}

	// 其他数据库使用 clause.Locking
	var locking clause.Locking

	switch d.dbType {
	case Oracle:
		locking = clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}
	default:
		locking = clause.Locking{Strength: "UPDATE"}
	}

	if len(ids) > 0 {
		return d.Model(out).Clauses(locking).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
	}
	return d.Model(out).Clauses(locking).FirstOrInit(out, ids...).Error
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

	// MongoDB 不支持标准的锁定
	if d.dbType == MongoDB {
		// 对于 MongoDB，可以使用 findAndModify 操作或事务来实现锁定
		// 这里简化处理，仅返回查询结果
		return d.Model(out).Where(formatWhere(where), values...).Order(order).FirstOrInit(out).Error
	}

	// SQLServer 使用特殊语法
	if d.dbType == SQLServer {
		return d.Model(out).Set("gorm:query_option", "WITH (UPDLOCK, ROWLOCK)").Where(formatWhere(where), values...).Order(order).FirstOrInit(out).Error
	}

	// 其他数据库使用 clause.Locking
	var locking clause.Locking

	switch d.dbType {
	case Oracle:
		locking = clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}
	case SQLite:
		// SQLite 只在事务中支持 FOR UPDATE
		if d.db.Statement.ConnPool != nil && d.db.Statement.ConnPool.(*sql.Tx) != nil {
			locking = clause.Locking{Strength: "UPDATE"}
		} else {
			// 如果不在事务中，可以记录警告或自动开启事务
			locking = clause.Locking{Strength: "UPDATE"}
		}
	default:
		locking = clause.Locking{Strength: "UPDATE"}
	}

	return d.Model(out).Clauses(locking).Where(formatWhere(where), values...).Order(order).FirstOrInit(out).Error
}

// LockShare 共享锁定记录
func (d *Database) LockShare(out interface{}, ids ...interface{}) error {
	d.deadlock.Attach(out)

	// MongoDB 不支持标准的锁定
	if d.dbType == MongoDB {
		if len(ids) > 0 {
			return d.Model(out).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
		}
		return d.Model(out).FirstOrInit(out, ids...).Error
	}

	// SQLServer 使用特殊语法
	if d.dbType == SQLServer {
		if len(ids) > 0 {
			return d.Model(out).Set("gorm:query_option", "WITH (HOLDLOCK, ROWLOCK)").Where(reflectKeys(out), ids...).FirstOrInit(out).Error
		}
		return d.Model(out).Set("gorm:query_option", "WITH (HOLDLOCK, ROWLOCK)").FirstOrInit(out, ids...).Error
	}

	// Oracle 使用 UPDATE NOWAIT
	if d.dbType == Oracle {
		if len(ids) > 0 {
			return d.Model(out).Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
		}
		return d.Model(out).Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).FirstOrInit(out, ids...).Error
	}

	// SQLite 不支持 SHARE 锁，使用 UPDATE 锁代替
	if d.dbType == SQLite {
		if len(ids) > 0 {
			return d.Model(out).Clauses(clause.Locking{Strength: "UPDATE"}).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
		}
		return d.Model(out).Clauses(clause.Locking{Strength: "UPDATE"}).FirstOrInit(out, ids...).Error
	}

	// 其他数据库使用标准的 SHARE 锁
	if len(ids) > 0 {
		return d.Model(out).Clauses(clause.Locking{Strength: "SHARE"}).Where(reflectKeys(out), ids...).FirstOrInit(out).Error
	}
	return d.Model(out).Clauses(clause.Locking{Strength: "SHARE"}).FirstOrInit(out, ids...).Error
}

// LockMulti 锁定多条记录
func (d *Database) LockMulti(out interface{}, where string, values ...interface{}) error {
	d.deadlock.Attach(out)

	// MongoDB 不支持标准的锁定
	if d.dbType == MongoDB {
		// 对于 MongoDB，可以使用 findAndModify 操作或事务来实现锁定
		// 这里简化处理，仅返回查询结果
		return d.Model(out).Where(formatWhere(where), values...).Find(out).Error
	}

	// SQLServer 使用特殊语法
	if d.dbType == SQLServer {
		return d.Model(out).Set("gorm:query_option", "WITH (UPDLOCK, ROWLOCK)").Where(formatWhere(where), values...).Find(out).Error
	}

	// 其他数据库使用 clause.Locking
	var locking clause.Locking

	switch d.dbType {
	case Oracle:
		locking = clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}
	case SQLite:
		// SQLite 只在事务中支持 FOR UPDATE
		if d.db.Statement.ConnPool != nil && d.db.Statement.ConnPool.(*sql.Tx) != nil {
			locking = clause.Locking{Strength: "UPDATE"}
		} else {
			// 如果不在事务中，可以记录警告或自动开启事务
			locking = clause.Locking{Strength: "UPDATE"}
		}
	default:
		locking = clause.Locking{Strength: "UPDATE"}
	}

	return d.Model(out).Clauses(locking).Where(formatWhere(where), values...).Find(out).Error
}

// ==================== 插入操作 ====================

// Create 创建记录
func (d *Database) Create(value interface{}) error {
	return d.db.Create(value).Error
}

// CreateInBatches 批量创建记录
func (d *Database) CreateInBatches(value interface{}, batchSize int) error {
	return d.db.CreateInBatches(value, batchSize).Error
}

// Save 保存记录
func (d *Database) Save(value interface{}) error {
	return d.db.Save(value).Error
}

// BatchInsert 批量插入
func (d *Database) BatchInsert(table string, columns []string, values [][]interface{}) error {
	if d.adapter != nil {
		return d.adapter.BatchInsert(d.db, table, columns, values)
	}
	return errors.New("数据库适配器不支持批量插入")
}

// MergeInto 合并插入（UPSERT）
func (d *Database) MergeInto(table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if d.adapter != nil {
		return d.adapter.MergeInto(d.db, table, columns, values, keyColumns, updateColumns)
	}
	return errors.New("数据库适配器不支持合并插入")
}

// ==================== 更新操作 ====================

// Update 更新记录
func (d *Database) Update(model interface{}, column string, value interface{}) error {
	return d.Model(model).Update(column, value).Error
}

// Updates 批量更新记录
func (d *Database) Updates(model interface{}, values interface{}) error {
	return d.Model(model).Updates(values).Error
}

// UpdateColumn 更新列
func (d *Database) UpdateColumn(model interface{}, column string, value interface{}) error {
	return d.Model(model).UpdateColumn(column, value).Error
}

// UpdateColumns 批量更新列
func (d *Database) UpdateColumns(model interface{}, values interface{}) error {
	return d.Model(model).UpdateColumns(values).Error
}

// ==================== 删除操作 ====================

// Delete 删除记录
func (d *Database) Delete(value interface{}, where ...interface{}) error {
	return d.db.Delete(value, where...).Error
}

// Unscoped 不使用软删除
func (d *Database) Unscoped() *gorm.DB {
	return d.db.Unscoped()
}

// ==================== 事务操作 ====================

// Transaction 执行事务
func (d *Database) Transaction(fc func(tx *Database) error) error {
	return d.db.Transaction(func(tx *gorm.DB) error {
		// 创建事务数据库
		txDB := &Database{
			db:       tx,
			sqlDB:    d.sqlDB,
			dbType:   d.dbType,
			deadlock: d.deadlock,
			ctx:      d.ctx,
			adapter:  d.adapter,
		}
		return fc(txDB)
	})
}

// Begin 开始事务
func (d *Database) Begin() *Database {
	tx := d.db.Begin()
	return &Database{
		db:       tx,
		sqlDB:    d.sqlDB,
		dbType:   d.dbType,
		deadlock: d.deadlock,
		ctx:      d.ctx,
		adapter:  d.adapter,
	}
}

// Commit 提交事务
func (d *Database) Commit() error {
	return d.db.Commit().Error
}

// Rollback 回滚事务
func (d *Database) Rollback() error {
	return d.db.Rollback().Error
}

// ==================== 辅助函数 ====================
// formatWhere 格式化WHERE条件
func formatWhere(where string) string {
	if strings.HasPrefix(where, "AND ") {
		return strings.Replace(where, "AND ", "", 1)
	}
	return where
}

// reflectTableName 反射获取表名
func reflectTableName(value interface{}) string {
	if value == nil {
		return ""
	}

	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}

	// 尝试获取表名
	if t.Kind() == reflect.Struct {
		// 尝试调用TableName方法
		v := reflect.New(t)
		if method := v.MethodByName("TableName"); method.IsValid() {
			if result := method.Call(nil); len(result) > 0 {
				if tableName, ok := result[0].Interface().(string); ok {
					return tableName
				}
			}
		}
		// 使用结构体名作为表名
		return t.Name()
	}

	return ""
}

// reflectKeys 反射获取主键
func reflectKeys(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}

	// 尝试获取主键
	if t.Kind() == reflect.Struct {
		// 默认使用ID字段作为主键
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Name == "ID" || field.Name == "Id" || field.Name == "id" {
				return field.Name
			}
		}
	}

	return "id" // 默认主键名
}

// Close 关闭数据库连接
func (d *Database) Close() error {
	if d.sqlDB != nil {
		return d.sqlDB.Close()
	}
	return nil
}

// ShardingTable 返回分表后的 *gorm.DB
func (d *Database) ShardingTable(baseName string, shardingKey interface{}, tableCount int) *gorm.DB {
	tableName := ShardingTableName(baseName, shardingKey, tableCount)
	return d.db.Table(tableName)
}

// 分表插入
func (d *Database) ShardingCreate(baseName string, shardingKey interface{}, tableCount int, value interface{}) error {
	return d.ShardingTable(baseName, shardingKey, tableCount).Create(value).Error
}

// 分表查询
func (d *Database) ShardingFind(baseName string, shardingKey interface{}, tableCount int, out interface{}, where ...interface{}) error {
	return d.ShardingTable(baseName, shardingKey, tableCount).Find(out, where...).Error
}

// 分表更新
func (d *Database) ShardingUpdate(baseName string, shardingKey interface{}, tableCount int, model interface{}, column string, value interface{}) error {
	return d.ShardingTable(baseName, shardingKey, tableCount).Model(model).Update(column, value).Error
}

// 分表删除
func (d *Database) ShardingDelete(baseName string, shardingKey interface{}, tableCount int, model interface{}, where ...interface{}) error {
	return d.ShardingTable(baseName, shardingKey, tableCount).Delete(model, where...).Error
}

// ShardingTableName 根据分表键和分表数生成分表表名
func ShardingTableName(baseName string, shardingKey interface{}, tableCount int) string {
	keyStr := fmt.Sprintf("%v", shardingKey)
	h := fnv.New32a()
	h.Write([]byte(keyStr))
	idx := h.Sum32() % uint32(tableCount)
	return baseName + "_" + strconv.Itoa(int(idx))
}

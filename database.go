package gosqlx

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	oracle "github.com/seelly/gorm-oracle"
	"go.mongodb.org/mongo-driver/mongo/options"

	"gorm.io/driver/sqlite"

	"github.com/gzorm/gosqlx/adapter"

	"gorm.io/driver/clickhouse"
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
	case PostgresSQL:
		adapterInstance = adapter.NewPostgres(config.Source)
	case SQLite:
		adapterInstance = adapter.NewSQLite(config.Source)
	case SQLServer:
		adapterInstance = adapter.NewSQLServer(config.Source)
	case Oracle:
		adapterInstance = adapter.NewOracle(config.Source)
	case TiDB:
		adapterInstance = adapter.NewTiDB(config.Source)
	case ClickHouse:
		adapterInstance = adapter.NewClickHouse(config.Source)
	case MariaDB:
		adapterInstance = adapter.NewMariaDB(config.Source)
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
	case PostgresSQL:
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
	case MySQL:
		lockOption = "FOR UPDATE"
	case SQLServer:
		lockOption = "WITH (UPDLOCK, ROWLOCK)"
	case Oracle:
		lockOption = "FOR UPDATE NOWAIT"
	case PostgresSQL:
		lockOption = "FOR UPDATE"
	case TiDB:
		lockOption = "FOR UPDATE" // 目前与MySQL相同，但将来可能有特殊选项
	case SQLite:
		// SQLite 只在事务中支持 FOR UPDATE
		if d.db.Statement.ConnPool != nil && d.db.Statement.ConnPool.(*sql.Tx) != nil {
			lockOption = "FOR UPDATE"
		} else {
			// 如果不在事务中，可以记录警告或自动开启事务
			lockOption = "FOR UPDATE"
		}
	}

	return d.Model(out).Set("gorm:query_option", lockOption).Where(formatWhere(where), values...).Order(order).FirstOrInit(out).Error
}

// LockShare 共享锁定记录
func (d *Database) LockShare(out interface{}, ids ...interface{}) error {
	d.deadlock.Attach(out)

	// 根据数据库类型选择共享锁语法
	lockOption := "FOR SHARE"
	switch d.dbType {
	case MySQL:
		lockOption = "FOR SHARE"
	case SQLServer:
		lockOption = "WITH (HOLDLOCK, ROWLOCK)"
	case Oracle:
		lockOption = "FOR UPDATE NOWAIT"
	case PostgresSQL:
		lockOption = "FOR SHARE"
	case TiDB:
		lockOption = "FOR SHARE" // 目前与MySQL相同，但将来可能有特殊选项
	case SQLite:
		// SQLite 不支持 FOR SHARE，但会默默忽略它
		// 可以使用 FOR UPDATE 代替，或者不使用锁
		lockOption = "FOR UPDATE" // 使用排他锁代替共享锁
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

	// MongoDB 不支持标准的 FOR UPDATE 锁定
	if d.dbType == MongoDB {
		// 对于 MongoDB，可以使用 findAndModify 操作或事务来实现锁定
		// 这里简化处理，仅返回查询结果
		return d.Model(out).Where(formatWhere(where), values...).Find(out).Error
	}

	// 根据数据库类型选择锁定语法
	lockOption := "FOR UPDATE"
	switch d.dbType {
	case MySQL:
		lockOption = "FOR UPDATE"
	case SQLServer:
		lockOption = "WITH (UPDLOCK, ROWLOCK)"
	case Oracle:
		lockOption = "FOR UPDATE NOWAIT"
	case PostgresSQL:
		lockOption = "FOR UPDATE"
	case TiDB:
		lockOption = "FOR UPDATE" // 目前与MySQL相同，但将来可能有特殊选项
	case SQLite:
		// SQLite 只在事务中支持 FOR UPDATE
		if d.db.Statement.ConnPool != nil && d.db.Statement.ConnPool.(*sql.Tx) != nil {
			lockOption = "FOR UPDATE"
		} else {
			// 如果不在事务中，可以记录警告或自动开启事务
			lockOption = "FOR UPDATE"
		}
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
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

		var placeholders []string
		var flatValues []interface{}

		for _, row := range values {
			var rowPlaceholders []string
			for range columns {
				rowPlaceholders = append(rowPlaceholders, "?")
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
			flatValues = append(flatValues, row...)
		}
		sqlBuilder.WriteString(strings.Join(placeholders, ", "))

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case TiDB:
		// TiDB 与 MySQL 兼容，使用相同的批量插入语法
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

		var placeholders []string
		var flatValues []interface{}

		for _, row := range values {
			var rowPlaceholders []string
			for range columns {
				rowPlaceholders = append(rowPlaceholders, "?")
				flatValues = append(flatValues, row...)
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
			flatValues = append(flatValues, row...)
		}

		sqlBuilder.WriteString(strings.Join(placeholders, ", "))

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case SQLServer:
		// 使用 SQLServer 适配器的批量插入
		adapterInstance := &adapter.SQLServer{}
		return adapterInstance.BatchInsert(d.db, table, columns, values)

	case Oracle:
		// 使用 Oracle 适配器的批量插入
		adapterInstance := &adapter.Oracle{}
		return adapterInstance.BatchInsert(d.db, table, columns, values)

	case PostgresSQL:
		// PostgreSQL 批量插入
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

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
			flatValues = append(flatValues, row...)
		}

		sqlBuilder.WriteString(strings.Join(placeholders, ", "))

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case SQLite:
		// SQLite 批量插入
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

		var placeholders []string
		var flatValues []interface{}

		for _, row := range values {
			var rowPlaceholders []string
			for range columns {
				rowPlaceholders = append(rowPlaceholders, "?")
				flatValues = append(flatValues, row...)
			}
			placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
			flatValues = append(flatValues, row...)
		}

		sqlBuilder.WriteString(strings.Join(placeholders, ", "))

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case MongoDB:
		// MongoDB 批量插入
		// 使用 MongoDB 适配器进行批量插入
		if mongoAdapter, ok := d.adapter.(*adapter.MongoDB); ok {
			// 将列和值转换为文档格式
			docs := make([]interface{}, 0, len(values))
			for _, row := range values {
				doc := make(map[string]interface{})
				for i, col := range columns {
					if i < len(row) {
						doc[col] = row[i]
					}
				}
				docs = append(docs, doc)
			}
			_, err := mongoAdapter.InsertMany(table, docs)
			return err
		}
		return errors.New("MongoDB适配器类型断言失败")
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

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case TiDB:
		// TiDB 与 MySQL 兼容，使用相同的 UPSERT 语法
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

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case SQLServer:
		// 使用 SQLServer 适配器的 MERGE INTO
		adapterInstance := &adapter.SQLServer{}
		return adapterInstance.MergeInto(d.db, table, columns, values, keyColumns, updateColumns)

	case Oracle:
		// 使用 Oracle 适配器的 MERGE INTO
		adapterInstance := &adapter.Oracle{}
		return adapterInstance.MergeInto(d.db, table, columns, values, keyColumns, updateColumns)

	case PostgresSQL:
		// PostgreSQL UPSERT (INSERT ... ON CONFLICT ... DO UPDATE)
		var sqlBuilder strings.Builder
		sqlBuilder.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

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

		sqlBuilder.WriteString(strings.Join(placeholders, ", "))

		if len(updateColumns) > 0 && len(keyColumns) > 0 {
			sqlBuilder.WriteString(fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET ", strings.Join(keyColumns, ", ")))
			var updates []string
			for _, col := range updateColumns {
				updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
			sqlBuilder.WriteString(strings.Join(updates, ", "))
		}

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case SQLite:
		// SQLite 3.24.0 及以上版本支持 UPSERT
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

		if len(updateColumns) > 0 && len(keyColumns) > 0 {
			sqlBuilder.WriteString(fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET ", strings.Join(keyColumns, ", ")))
			var updates []string
			for _, col := range updateColumns {
				updates = append(updates, fmt.Sprintf("%s = excluded.%s", col, col))
			}
			sqlBuilder.WriteString(strings.Join(updates, ", "))
		}

		return d.db.Exec(sqlBuilder.String(), flatValues...).Error

	case MongoDB:
		// MongoDB 使用 upsert 操作
		if mongoAdapter, ok := d.adapter.(*adapter.MongoDB); ok {
			// 将列和值转换为文档格式
			docs := make([]interface{}, 0, len(values))
			for _, row := range values {
				doc := make(map[string]interface{})
				for i, col := range columns {
					if i < len(row) {
						doc[col] = row[i]
					}
				}
				docs = append(docs, doc)
			}

			// 构建查询条件（基于键列）
			filter := make(map[string]interface{})
			for _, keyCol := range keyColumns {
				filter[keyCol] = 1 // 使用键列作为过滤条件
			}

			// 创建 UpdateOptions 并设置 Upsert 为 true
			updateOpts := options.Update().SetUpsert(true)

			// 执行 upsert 操作
			_, err := mongoAdapter.UpdateMany(table, filter, docs, updateOpts)
			return err
		}
		return errors.New("MongoDB适配器类型断言失败")
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
func (d *Database) QueryRows(out interface{}, sqlStr string, values ...interface{}) error {
	return d.Raw(sqlStr, values...).Scan(out).Error
}

// QueryPage 分页查询
// out: 输出结果
// page: 页码（从1开始）
// pageSize: 每页记录数
// filter: 过滤条件（可以是SQL字符串或条件映射）
// opts: 可选参数，第一个参数通常是数据库连接
func (d *Database) QueryPage(out interface{}, page, pageSize int, filter interface{}, opts ...interface{}) (int64, error) {
	// 将数据库连接作为第一个可选参数传递给适配器
	newOpts := make([]interface{}, 0, len(opts)+1)
	newOpts = append(newOpts, d.db)
	newOpts = append(newOpts, opts...)

	// 根据数据库类型调用相应适配器的分页方法
	return d.adapter.QueryPage(out, page, pageSize, filter, newOpts...)
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
		_ = db.Close()
	}

	m.databases = make(map[string]*Database)
}

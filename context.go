package gosqlx

import (
	"context"
	"database/sql"
	"time"

	"gorm.io/gorm"
)

// 数据库访问模式常量
const (
	ModeReadWrite = "ReadWrite" // 读写模式
	ModeReadOnly  = "ReadOnly"  // 只读模式
)

// Context 数据库上下文
// 封装了数据库操作的上下文信息
type Context struct {
	context.Context               // 嵌入标准上下文
	Nick            string        // 数据库别名
	Mode            string        // 读写模式
	DBType          DatabaseType  // 数据库类型
	Timeout         time.Duration // 操作超时时间
}

// NewContext 创建新的数据库上下文
func NewContext(ctx context.Context, nick string, mode string) *Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return &Context{
		Context: ctx,
		Nick:    nick,
		Mode:    mode,
	}
}

// WithDBType 设置数据库类型
func (c *Context) WithDBType(dbType DatabaseType) *Context {
	c.DBType = dbType
	return c
}

// WithTimeout 设置操作超时时间
func (c *Context) WithTimeout(timeout time.Duration) *Context {
	c.Timeout = timeout
	return c
}

// IsReadOnly 判断是否为只读模式
func (c *Context) IsReadOnly() bool {
	return c.Mode == ModeReadOnly
}

// WithValue 创建带值的新上下文
func (c *Context) WithValue(key, val interface{}) *Context {
	return &Context{
		Context: context.WithValue(c.Context, key, val),
		Nick:    c.Nick,
		Mode:    c.Mode,
		DBType:  c.DBType,
		Timeout: c.Timeout,
	}
}

// WithCancel 创建可取消的上下文
func (c *Context) WithCancel() (*Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.Context)
	return &Context{
		Context: ctx,
		Nick:    c.Nick,
		Mode:    c.Mode,
		DBType:  c.DBType,
		Timeout: c.Timeout,
	}, cancel
}

// WithDeadline 创建带截止时间的上下文
func (c *Context) WithDeadline(d time.Time) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(c.Context, d)
	return &Context{
		Context: ctx,
		Nick:    c.Nick,
		Mode:    c.Mode,
		DBType:  c.DBType,
		Timeout: c.Timeout,
	}, cancel
}

// WithTimeout 创建带超时的上下文
func (c *Context) WithContextTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.Context, timeout)
	return &Context{
		Context: ctx,
		Nick:    c.Nick,
		Mode:    c.Mode,
		DBType:  c.DBType,
		Timeout: c.Timeout,
	}, cancel
}

// ExecuteWithTimeout 在指定超时时间内执行数据库操作
func (c *Context) ExecuteWithTimeout(timeout time.Duration, fn func(ctx *Context) error) error {
	ctx, cancel := c.WithContextTimeout(timeout)
	defer cancel()

	return fn(ctx)
}

// DBContext 数据库操作上下文
// 封装了GORM和原生SQL操作的上下文
type DBContext struct {
	*Context
	DB       *gorm.DB       // GORM数据库连接
	SqlDB    *sql.DB        // 原生SQL数据库连接
	TxOption *sql.TxOptions // 事务选项
}

// NewDBContext 创建新的数据库操作上下文
func NewDBContext(ctx *Context, db *gorm.DB, sqlDB *sql.DB) *DBContext {
	return &DBContext{
		Context: ctx,
		DB:      db,
		SqlDB:   sqlDB,
		TxOption: &sql.TxOptions{
			ReadOnly: ctx.IsReadOnly(),
		},
	}
}

// WithTxOption 设置事务选项
func (c *DBContext) WithTxOption(opt *sql.TxOptions) *DBContext {
	c.TxOption = opt
	return c
}

// Transaction 执行事务
func (c *DBContext) Transaction(fn func(tx *DBContext) error) error {
	// 使用GORM的事务
	return c.DB.WithContext(c.Context).Transaction(func(tx *gorm.DB) error {
		txCtx := &DBContext{
			Context:  c.Context,
			DB:       tx,
			SqlDB:    c.SqlDB,
			TxOption: c.TxOption,
		}
		return fn(txCtx)
	})
}

// RawTransaction 执行原生SQL事务
func (c *DBContext) RawTransaction(fn func(tx *sql.Tx) error) error {
	// 使用原生SQL的事务
	tx, err := c.SqlDB.BeginTx(c.Context, c.TxOption)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // 重新抛出panic
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

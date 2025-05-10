package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	oracle "github.com/seelly/gorm-oracle"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// Oracle 适配器结构体
type Oracle struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewOracle 创建新的Oracle适配器
func NewOracle(dsn string) *Oracle {
	return &Oracle{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (o *Oracle) WithMaxIdle(maxIdle int) *Oracle {
	o.MaxIdle = maxIdle
	return o
}

// WithMaxOpen 设置最大打开连接数
func (o *Oracle) WithMaxOpen(maxOpen int) *Oracle {
	o.MaxOpen = maxOpen
	return o
}

// WithMaxLifetime 设置连接最大生命周期
func (o *Oracle) WithMaxLifetime(maxLifetime time.Duration) *Oracle {
	o.MaxLifetime = maxLifetime
	return o
}

// WithDebug 设置调试模式
func (o *Oracle) WithDebug(debug bool) *Oracle {
	o.Debug = debug
	return o
}

// Connect 连接数据库
func (o *Oracle) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if o.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库
	db, err := gorm.Open(oracle.Open(o.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(o.MaxIdle)
	sqlDB.SetMaxOpenConns(o.MaxOpen)
	sqlDB.SetConnMaxLifetime(o.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (o *Oracle) BuildDSN(host string, port int, username, password, serviceName string, params map[string]string) string {
	// 基本DSN
	dsn := fmt.Sprintf("%s/%s@%s:%d/%s", username, password, host, port, serviceName)

	// 添加参数
	if len(params) > 0 {
		var parameters []string
		for k, v := range params {
			parameters = append(parameters, fmt.Sprintf("%s=%s", k, v))
		}
		dsn = dsn + "?" + strings.Join(parameters, "&")
	}

	return dsn
}

// ForUpdate 生成FOR UPDATE锁定语句
func (o *Oracle) ForUpdate() string {
	return "FOR UPDATE"
}

// ForShare 生成共享锁语句
func (o *Oracle) ForShare() string {
	return "FOR UPDATE NOWAIT"
}

// Limit 生成分页语句
func (o *Oracle) Limit(offset, limit int) string {
	// Oracle 使用 ROWNUM 或 ROW_NUMBER() 实现分页
	return fmt.Sprintf("OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit)
}

// BatchInsert 批量插入
func (o *Oracle) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// Oracle 批量插入使用 INSERT ALL 语法
	var sql strings.Builder
	sql.WriteString("INSERT ALL ")

	// 构建每一行的 INTO 语句
	for range values {
		sql.WriteString(fmt.Sprintf("INTO %s (%s) VALUES (", table, strings.Join(columns, ", ")))
		for i := range columns {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString("?")
		}
		sql.WriteString(") ")
	}

	// 添加 SELECT 子句
	sql.WriteString("SELECT 1 FROM DUAL")

	// 展平值数组
	var flatValues []interface{}
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// 执行SQL
	return db.Exec(sql.String(), flatValues...).Error
}

// MergeInto 实现Oracle的MERGE INTO功能（相当于MySQL的ON DUPLICATE KEY UPDATE）
func (o *Oracle) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 || len(keyColumns) == 0 {
		return nil
	}

	// 为每一行数据生成一个MERGE语句
	for _, row := range values {
		// 构建MERGE INTO语句
		var sql strings.Builder
		sql.WriteString(fmt.Sprintf("MERGE INTO %s t USING (SELECT ", table))

		// 构建VALUES部分
		for i, col := range columns {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("? AS %s", col))
		}

		sql.WriteString(" FROM DUAL) s ON (")

		// 构建ON条件
		for i, key := range keyColumns {
			if i > 0 {
				sql.WriteString(" AND ")
			}
			sql.WriteString(fmt.Sprintf("t.%s = s.%s", key, key))
		}

		sql.WriteString(") ")

		// 如果匹配则更新
		if len(updateColumns) > 0 {
			sql.WriteString("WHEN MATCHED THEN UPDATE SET ")
			for i, col := range updateColumns {
				if i > 0 {
					sql.WriteString(", ")
				}
				sql.WriteString(fmt.Sprintf("t.%s = s.%s", col, col))
			}
			sql.WriteString(" ")
		}

		// 如果不匹配则插入
		sql.WriteString("WHEN NOT MATCHED THEN INSERT (")
		sql.WriteString(strings.Join(columns, ", "))
		sql.WriteString(") VALUES (")
		for i, col := range columns {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("s.%s", col))
		}
		sql.WriteString(")")

		// 执行SQL
		if err := db.Exec(sql.String(), row...).Error; err != nil {
			return err
		}
	}

	return nil
}

// CreateSequence 创建序列
func (o *Oracle) CreateSequence(db *gorm.DB, name string, startWith int, incrementBy int) error {
	sql := fmt.Sprintf("CREATE SEQUENCE %s START WITH %d INCREMENT BY %d", name, startWith, incrementBy)
	return db.Exec(sql).Error
}

// DropSequence 删除序列
func (o *Oracle) DropSequence(db *gorm.DB, name string) error {
	sql := fmt.Sprintf("DROP SEQUENCE %s", name)
	return db.Exec(sql).Error
}

// NextVal 获取序列的下一个值
func (o *Oracle) NextVal(db *gorm.DB, name string) (int64, error) {
	var result int64
	sql := fmt.Sprintf("SELECT %s.NEXTVAL FROM DUAL", name)
	err := db.Raw(sql).Scan(&result).Error
	return result, err
}

// CurrVal 获取序列的当前值
func (o *Oracle) CurrVal(db *gorm.DB, name string) (int64, error) {
	var result int64
	sql := fmt.Sprintf("SELECT %s.CURRVAL FROM DUAL", name)
	err := db.Raw(sql).Scan(&result).Error
	return result, err
}

// CreateDatabase Oracle不支持CREATE DATABASE语句，需要通过其他方式创建
func (o *Oracle) CreateDatabase(db *gorm.DB, name string) error {
	return fmt.Errorf("Oracle does not support CREATE DATABASE via SQL, please use Oracle tools")
}

// DropDatabase Oracle不支持DROP DATABASE语句，需要通过其他方式删除
func (o *Oracle) DropDatabase(db *gorm.DB, name string) error {
	return fmt.Errorf("Oracle does not support DROP DATABASE via SQL, please use Oracle tools")
}

// ShowDatabases 获取所有数据库（在Oracle中为用户/模式）
func (o *Oracle) ShowDatabases(db *gorm.DB) ([]string, error) {
	var schemas []string
	err := db.Raw("SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME").Scan(&schemas).Error
	return schemas, err
}

// ShowTables 获取所有表
func (o *Oracle) ShowTables(db *gorm.DB) ([]string, error) {
	var tables []string
	err := db.Raw("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME").Scan(&tables).Error
	return tables, err
}

// ShowCreateTable 获取创建表的DDL
func (o *Oracle) ShowCreateTable(db *gorm.DB, table string) (string, error) {
	var result string
	err := db.Raw("SELECT DBMS_METADATA.GET_DDL('TABLE', ?) FROM DUAL", strings.ToUpper(table)).Scan(&result).Error
	return result, err
}

// TruncateTable 清空表
func (o *Oracle) TruncateTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
}

// GetTableStatus 获取表状态
func (o *Oracle) GetTableStatus(db *gorm.DB, table string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := db.Raw(`
		SELECT 
			t.TABLE_NAME, 
			t.NUM_ROWS, 
			t.BLOCKS, 
			t.EMPTY_BLOCKS,
			t.AVG_ROW_LEN, 
			t.LAST_ANALYZED
		FROM 
			USER_TABLES t
		WHERE 
			t.TABLE_NAME = ?
	`, strings.ToUpper(table)).Scan(&result).Error
	return result, err
}

// GetVersion 获取Oracle版本
func (o *Oracle) GetVersion(db *gorm.DB) (string, error) {
	var version string
	err := db.Raw("SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle%'").Scan(&version).Error
	return version, err
}

// GetTableColumns 获取表的列信息
func (o *Oracle) GetTableColumns(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			COLUMN_NAME, 
			DATA_TYPE, 
			DATA_LENGTH, 
			DATA_PRECISION, 
			DATA_SCALE, 
			NULLABLE, 
			COLUMN_ID
		FROM 
			USER_TAB_COLUMNS 
		WHERE 
			TABLE_NAME = ? 
		ORDER BY 
			COLUMN_ID
	`, strings.ToUpper(table)).Scan(&results).Error
	return results, err
}

// GetTableIndexes 获取表的索引信息
func (o *Oracle) GetTableIndexes(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			i.INDEX_NAME, 
			i.INDEX_TYPE, 
			i.UNIQUENESS, 
			i.STATUS, 
			i.FUNCIDX_STATUS, 
			i.JOIN_INDEX, 
			i.COLUMNS
		FROM 
			USER_INDEXES i,
			(SELECT 
				INDEX_NAME, 
				LISTAGG(COLUMN_NAME, ',') WITHIN GROUP (ORDER BY COLUMN_POSITION) AS COLUMNS
			FROM 
				USER_IND_COLUMNS 
			WHERE 
				TABLE_NAME = ?
			GROUP BY 
				INDEX_NAME) c
		WHERE 
			i.TABLE_NAME = ? 
			AND i.INDEX_NAME = c.INDEX_NAME
	`, strings.ToUpper(table), strings.ToUpper(table)).Scan(&results).Error
	return results, err
}

// GetTableConstraints 获取表的约束信息
func (o *Oracle) GetTableConstraints(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			c.CONSTRAINT_NAME, 
			c.CONSTRAINT_TYPE, 
			c.STATUS, 
			c.VALIDATED, 
			c.GENERATED, 
			cc.COLUMNS
		FROM 
			USER_CONSTRAINTS c,
			(SELECT 
				CONSTRAINT_NAME, 
				LISTAGG(COLUMN_NAME, ',') WITHIN GROUP (ORDER BY POSITION) AS COLUMNS
			FROM 
				USER_CONS_COLUMNS 
			WHERE 
				TABLE_NAME = ?
			GROUP BY 
				CONSTRAINT_NAME) cc
		WHERE 
			c.TABLE_NAME = ? 
			AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
	`, strings.ToUpper(table), strings.ToUpper(table)).Scan(&results).Error
	return results, err
}

// GetProcessList 获取会话列表
func (o *Oracle) GetProcessList(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			s.SID, 
			s.SERIAL#, 
			s.USERNAME, 
			s.STATUS, 
			s.SCHEMANAME, 
			s.OSUSER, 
			s.MACHINE, 
			s.TERMINAL, 
			s.PROGRAM, 
			s.TYPE, 
			s.MODULE, 
			s.ACTION, 
			s.LOGON_TIME, 
			s.LAST_CALL_ET
		FROM 
			V$SESSION s
		WHERE 
			s.TYPE = 'USER'
		ORDER BY 
			s.LOGON_TIME
	`).Scan(&results).Error
	return results, err
}

// KillProcess 终止会话
func (o *Oracle) KillProcess(db *gorm.DB, sid int, serial int) error {
	return db.Exec("ALTER SYSTEM KILL SESSION '??,??'", sid, serial).Error
}

// GetTablespace 获取表空间信息
func (o *Oracle) GetTablespace(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			df.TABLESPACE_NAME, 
			df.BYTES / 1024 / 1024 AS SIZE_MB, 
			(df.BYTES - fs.BYTES) / 1024 / 1024 AS USED_MB, 
			fs.BYTES / 1024 / 1024 AS FREE_MB, 
			ROUND(100 * (fs.BYTES / df.BYTES), 2) AS FREE_PCT
		FROM 
			(SELECT 
				TABLESPACE_NAME, 
				SUM(BYTES) AS BYTES 
			FROM 
				DBA_DATA_FILES 
			GROUP BY 
				TABLESPACE_NAME) df,
			(SELECT 
				TABLESPACE_NAME, 
				SUM(BYTES) AS BYTES 
			FROM 
				DBA_FREE_SPACE 
			GROUP BY 
				TABLESPACE_NAME) fs
		WHERE 
			df.TABLESPACE_NAME = fs.TABLESPACE_NAME
		ORDER BY 
			df.TABLESPACE_NAME
	`).Scan(&results).Error
	return results, err
}

// GetUsers 获取用户列表
func (o *Oracle) GetUsers(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			USERNAME, 
			USER_ID, 
			ACCOUNT_STATUS, 
			LOCK_DATE, 
			EXPIRY_DATE, 
			DEFAULT_TABLESPACE, 
			TEMPORARY_TABLESPACE, 
			CREATED, 
			PROFILE
		FROM 
			DBA_USERS
		ORDER BY 
			USERNAME
	`).Scan(&results).Error
	return results, err
}

// CreateUser 创建用户
func (o *Oracle) CreateUser(db *gorm.DB, username, password string, defaultTablespace, temporaryTablespace string) error {
	sql := fmt.Sprintf(
		"CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE %s TEMPORARY TABLESPACE %s",
		username, password, defaultTablespace, temporaryTablespace,
	)
	return db.Exec(sql).Error
}

// DropUser 删除用户
func (o *Oracle) DropUser(db *gorm.DB, username string, cascade bool) error {
	sql := fmt.Sprintf("DROP USER %s", username)
	if cascade {
		sql += " CASCADE"
	}
	return db.Exec(sql).Error
}

// GrantPrivileges 授予权限
func (o *Oracle) GrantPrivileges(db *gorm.DB, privileges string, objects string, username string) error {
	sql := fmt.Sprintf("GRANT %s ON %s TO %s", privileges, objects, username)
	return db.Exec(sql).Error
}

// RevokePrivileges 撤销权限
func (o *Oracle) RevokePrivileges(db *gorm.DB, privileges string, objects string, username string) error {
	sql := fmt.Sprintf("REVOKE %s ON %s FROM %s", privileges, objects, username)
	return db.Exec(sql).Error
}

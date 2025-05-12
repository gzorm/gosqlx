package adapter

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// SQLServer 适配器结构体
type SQLServer struct {
	// 基础配置
	DSN         string        // 数据源名称
	MaxIdle     int           // 最大空闲连接数
	MaxOpen     int           // 最大打开连接数
	MaxLifetime time.Duration // 连接最大生命周期
	Debug       bool          // 调试模式
}

// NewSQLServer 创建新的SQLServer适配器
func NewSQLServer(dsn string) *SQLServer {
	return &SQLServer{
		DSN:         dsn,
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// WithMaxIdle 设置最大空闲连接数
func (s *SQLServer) WithMaxIdle(maxIdle int) *SQLServer {
	s.MaxIdle = maxIdle
	return s
}

// WithMaxOpen 设置最大打开连接数
func (s *SQLServer) WithMaxOpen(maxOpen int) *SQLServer {
	s.MaxOpen = maxOpen
	return s
}

// WithMaxLifetime 设置连接最大生命周期
func (s *SQLServer) WithMaxLifetime(maxLifetime time.Duration) *SQLServer {
	s.MaxLifetime = maxLifetime
	return s
}

// WithDebug 设置调试模式
func (s *SQLServer) WithDebug(debug bool) *SQLServer {
	s.Debug = debug
	return s
}

// Connect 连接数据库
func (s *SQLServer) Connect() (*gorm.DB, *sql.DB, error) {
	// 创建GORM配置
	config := &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 使用单数表名
		},
		Logger: logger.Default.LogMode(logger.Silent),
	}

	// 如果开启调试模式，设置日志级别
	if s.Debug {
		config.Logger = logger.Default.LogMode(logger.Info)
	}

	// 连接数据库
	db, err := gorm.Open(sqlserver.Open(s.DSN), config)
	if err != nil {
		return nil, nil, err
	}

	// 获取原生SQL连接
	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, err
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(s.MaxIdle)
	sqlDB.SetMaxOpenConns(s.MaxOpen)
	sqlDB.SetConnMaxLifetime(s.MaxLifetime)

	return db, sqlDB, nil
}

// BuildDSN 构建DSN连接字符串
func (s *SQLServer) BuildDSN(server string, port int, username, password, database string, params map[string]string) string {
	// 基本DSN
	dsn := fmt.Sprintf("server=%s;port=%d;database=%s;user id=%s;password=%s", server, port, database, username, password)

	// 添加参数
	if len(params) > 0 {
		for k, v := range params {
			dsn = dsn + ";" + k + "=" + v
		}
	}

	return dsn
}

// ForUpdate 生成锁定语句
func (s *SQLServer) ForUpdate() string {
	return "WITH (UPDLOCK, ROWLOCK)"
}

// ForShare 生成共享锁语句
func (s *SQLServer) ForShare() string {
	return "WITH (HOLDLOCK, ROWLOCK)"
}

// Limit 生成分页语句
func (s *SQLServer) Limit(offset, limit int) string {
	// SQL Server 2012+ 使用 OFFSET-FETCH
	return fmt.Sprintf("OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit)
}

// BatchInsert 批量插入
func (s *SQLServer) BatchInsert(db *gorm.DB, table string, columns []string, values [][]interface{}) error {
	if len(values) == 0 {
		return nil
	}

	// SQL Server 批量插入
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", table, strings.Join(columns, ", ")))

	// 构建占位符
	var placeholders []string
	var flatValues []interface{}

	for i, row := range values {
		var rowPlaceholders []string
		for j := range columns {
			// SQL Server 使用 @p1, @p2 等参数
			paramName := fmt.Sprintf("@p%d", i*len(columns)+j+1)
			rowPlaceholders = append(rowPlaceholders, paramName)
			flatValues = append(flatValues, row[j])
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
	}

	sql.WriteString(strings.Join(placeholders, ", "))

	// 执行SQL
	return db.Exec(sql.String(), flatValues...).Error
}

// MergeInto 实现SQL Server的MERGE INTO功能
func (s *SQLServer) MergeInto(db *gorm.DB, table string, columns []string, values [][]interface{}, keyColumns []string, updateColumns []string) error {
	if len(values) == 0 || len(keyColumns) == 0 {
		return nil
	}

	// 创建临时表
	tempTableName := fmt.Sprintf("#Temp_%s", table)
	createTempTableSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tempTableName, s.generateColumnDefinitions(db, table))
	if err := db.Exec(createTempTableSQL).Error; err != nil {
		return err
	}

	// 插入数据到临时表
	if err := s.BatchInsert(db, tempTableName, columns, values); err != nil {
		return err
	}

	// 构建MERGE语句
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("MERGE INTO %s AS target USING %s AS source ON ", table, tempTableName))

	// 构建ON条件
	var onConditions []string
	for _, key := range keyColumns {
		onConditions = append(onConditions, fmt.Sprintf("target.%s = source.%s", key, key))
	}
	sql.WriteString(strings.Join(onConditions, " AND "))

	// 如果匹配则更新
	if len(updateColumns) > 0 {
		sql.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		var updates []string
		for _, col := range updateColumns {
			updates = append(updates, fmt.Sprintf("target.%s = source.%s", col, col))
		}
		sql.WriteString(strings.Join(updates, ", "))
	}

	// 如果不匹配则插入
	sql.WriteString(" WHEN NOT MATCHED THEN INSERT (")
	sql.WriteString(strings.Join(columns, ", "))
	sql.WriteString(") VALUES (")
	var sourceColumns []string
	for _, col := range columns {
		sourceColumns = append(sourceColumns, "source."+col)
	}
	sql.WriteString(strings.Join(sourceColumns, ", "))
	sql.WriteString(");")

	// 执行MERGE
	if err := db.Exec(sql.String()).Error; err != nil {
		return err
	}

	// 删除临时表
	return db.Exec(fmt.Sprintf("DROP TABLE %s", tempTableName)).Error
}

// generateColumnDefinitions 生成列定义
func (s *SQLServer) generateColumnDefinitions(db *gorm.DB, table string) string {
	var result struct {
		ColumnDefinitions string
	}

	// 获取表的列定义
	query := `
		SELECT STRING_AGG(COLUMN_NAME + ' ' + DATA_TYPE + 
			CASE 
				WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')' 
				WHEN NUMERIC_PRECISION IS NOT NULL THEN '(' + CAST(NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(NUMERIC_SCALE AS VARCHAR) + ')' 
				ELSE '' 
			END + 
			CASE WHEN IS_NULLABLE = 'NO' THEN ' NOT NULL' ELSE ' NULL' END, 
			', ') AS ColumnDefinitions
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = ?
	`

	db.Raw(query, table).Scan(&result)
	return result.ColumnDefinitions
}

// CreateDatabase 创建数据库
func (s *SQLServer) CreateDatabase(db *gorm.DB, name string) error {
	return db.Exec(fmt.Sprintf("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'%s') CREATE DATABASE [%s]", name, name)).Error
}

// DropDatabase 删除数据库
func (s *SQLServer) DropDatabase(db *gorm.DB, name string) error {
	// 先关闭所有连接
	closeConnectionsSQL := fmt.Sprintf(`
		USE master;
		IF EXISTS (SELECT name FROM sys.databases WHERE name = N'%s')
		BEGIN
			ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		END
	`, name, name)

	if err := db.Exec(closeConnectionsSQL).Error; err != nil {
		return err
	}

	// 删除数据库
	return db.Exec(fmt.Sprintf("IF EXISTS (SELECT name FROM sys.databases WHERE name = N'%s') DROP DATABASE [%s]", name, name)).Error
}

// ShowDatabases 获取所有数据库
func (s *SQLServer) ShowDatabases(db *gorm.DB) ([]string, error) {
	var databases []string
	err := db.Raw("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') ORDER BY name").Scan(&databases).Error
	return databases, err
}

// ShowTables 获取所有表
func (s *SQLServer) ShowTables(db *gorm.DB) ([]string, error) {
	var tables []string
	err := db.Raw("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME").Scan(&tables).Error
	return tables, err
}

// ShowCreateTable 获取创建表的DDL
func (s *SQLServer) ShowCreateTable(db *gorm.DB, table string) (string, error) {
	var result string
	err := db.Raw(fmt.Sprintf("EXEC sp_helptext '%s'", table)).Scan(&result).Error
	if err != nil {
		// 如果sp_helptext失败，尝试自己构建CREATE TABLE语句
		var createTableSQL strings.Builder
		createTableSQL.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", table))

		// 获取列信息
		type Column struct {
			Name         string
			Type         string
			MaxLength    int
			Precision    int
			Scale        int
			IsNullable   string
			DefaultValue string
		}

		var columns []Column
		err = db.Raw(`
			SELECT 
				COLUMN_NAME as Name, 
				DATA_TYPE as Type, 
				CHARACTER_MAXIMUM_LENGTH as MaxLength, 
				NUMERIC_PRECISION as Precision, 
				NUMERIC_SCALE as Scale, 
				IS_NULLABLE as IsNullable,
				COLUMN_DEFAULT as DefaultValue
			FROM 
				INFORMATION_SCHEMA.COLUMNS 
			WHERE 
				TABLE_NAME = ? 
			ORDER BY 
				ORDINAL_POSITION
		`, table).Scan(&columns).Error

		if err != nil {
			return "", err
		}

		// 构建列定义
		for i, col := range columns {
			createTableSQL.WriteString(fmt.Sprintf("    %s %s", col.Name, col.Type))

			// 添加长度/精度/小数位
			if col.MaxLength > 0 && col.MaxLength < 8000 {
				createTableSQL.WriteString(fmt.Sprintf("(%d)", col.MaxLength))
			} else if col.Precision > 0 {
				createTableSQL.WriteString(fmt.Sprintf("(%d,%d)", col.Precision, col.Scale))
			}

			// 添加NULL/NOT NULL
			if col.IsNullable == "NO" {
				createTableSQL.WriteString(" NOT NULL")
			} else {
				createTableSQL.WriteString(" NULL")
			}

			// 添加默认值
			if col.DefaultValue != "" {
				createTableSQL.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultValue))
			}

			// 添加逗号
			if i < len(columns)-1 {
				createTableSQL.WriteString(",\n")
			}
		}

		// 获取主键信息
		type PrimaryKey struct {
			ConstraintName string
			ColumnName     string
		}

		var primaryKeys []PrimaryKey
		err = db.Raw(`
			SELECT 
				kcu.CONSTRAINT_NAME,
				kcu.COLUMN_NAME
			FROM 
				INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			JOIN 
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			WHERE 
				tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
			ORDER BY 
				kcu.ORDINAL_POSITION
		`, table).Scan(&primaryKeys).Error

		if err != nil {
			return "", err
		}

		// 添加主键约束
		if len(primaryKeys) > 0 {
			var pkColumns []string
			for _, pk := range primaryKeys {
				pkColumns = append(pkColumns, pk.ColumnName)
			}

			createTableSQL.WriteString(",\n    CONSTRAINT ")
			createTableSQL.WriteString(primaryKeys[0].ConstraintName)
			createTableSQL.WriteString(" PRIMARY KEY (")
			createTableSQL.WriteString(strings.Join(pkColumns, ", "))
			createTableSQL.WriteString(")")
		}

		createTableSQL.WriteString("\n)")

		return createTableSQL.String(), nil
	}

	return result, nil
}

// TruncateTable 清空表
func (s *SQLServer) TruncateTable(db *gorm.DB, table string) error {
	return db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
}

// GetTableStatus 获取表状态
func (s *SQLServer) GetTableStatus(db *gorm.DB, table string) (map[string]interface{}, error) {
	var result map[string]interface{}
	err := db.Raw(`
		SELECT 
			t.name AS TableName,
			p.rows AS RowCount,
			SUM(a.total_pages) * 8 AS TotalSpaceKB,
			SUM(a.used_pages) * 8 AS UsedSpaceKB,
			(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
		FROM 
			sys.tables t
		INNER JOIN      
			sys.indexes i ON t.object_id = i.object_id
		INNER JOIN 
			sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		INNER JOIN 
			sys.allocation_units a ON p.partition_id = a.container_id
		WHERE 
			t.name = ? AND t.is_ms_shipped = 0 AND i.object_id > 255
		GROUP BY 
			t.name, p.rows
	`, table).Scan(&result).Error
	return result, err
}

// GetVersion 获取SQL Server版本
func (s *SQLServer) GetVersion(db *gorm.DB) (string, error) {
	var version string
	err := db.Raw("SELECT @@VERSION").Scan(&version).Error
	return version, err
}

// GetTableColumns 获取表的列信息
func (s *SQLServer) GetTableColumns(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			c.COLUMN_NAME, 
			c.DATA_TYPE, 
			c.CHARACTER_MAXIMUM_LENGTH, 
			c.NUMERIC_PRECISION, 
			c.NUMERIC_SCALE, 
			c.IS_NULLABLE, 
			c.COLUMN_DEFAULT,
			c.ORDINAL_POSITION,
			COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
		FROM 
			INFORMATION_SCHEMA.COLUMNS c
		WHERE 
			c.TABLE_NAME = ?
		ORDER BY 
			c.ORDINAL_POSITION
	`, table).Scan(&results).Error
	return results, err
}

// GetTableIndexes 获取表的索引信息
func (s *SQLServer) GetTableIndexes(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			i.name AS IndexName,
			i.type_desc AS IndexType,
			i.is_unique AS IsUnique,
			i.is_primary_key AS IsPrimaryKey,
			STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS ColumnNames
		FROM 
			sys.indexes i
		INNER JOIN 
			sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
		INNER JOIN 
			sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		INNER JOIN 
			sys.tables t ON i.object_id = t.object_id
		WHERE 
			t.name = ?
		GROUP BY 
			i.name, i.type_desc, i.is_unique, i.is_primary_key
		ORDER BY 
			i.name
	`, table).Scan(&results).Error
	return results, err
}

// GetTableConstraints 获取表的约束信息
func (s *SQLServer) GetTableConstraints(db *gorm.DB, table string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			con.name AS ConstraintName,
			con.type_desc AS ConstraintType,
			STRING_AGG(col.name, ', ') WITHIN GROUP (ORDER BY conCol.constraint_column_id) AS ColumnNames,
			OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable,
			STRING_AGG(refCol.name, ', ') WITHIN GROUP (ORDER BY conCol.constraint_column_id) AS ReferencedColumns
		FROM 
			sys.tables tab
		INNER JOIN 
			sys.objects con ON con.parent_object_id = tab.object_id AND con.type_desc LIKE '%CONSTRAINT'
		INNER JOIN 
			sys.sysconstraints sc ON sc.constid = con.object_id
		INNER JOIN 
			sys.columns col ON col.object_id = tab.object_id AND col.column_id = sc.colid
		LEFT JOIN 
			sys.foreign_keys fk ON fk.object_id = con.object_id
		LEFT JOIN 
			sys.foreign_key_columns fkCol ON fkCol.constraint_object_id = fk.object_id
		LEFT JOIN 
			sys.columns refCol ON refCol.object_id = fk.referenced_object_id AND refCol.column_id = fkCol.referenced_column_id
		LEFT JOIN 
			sys.constraint_column_usage conCol ON conCol.constraint_object_id = con.object_id AND conCol.column_id = col.column_id
		WHERE 
			tab.name = ?
		GROUP BY 
			con.name, con.type_desc, fk.referenced_object_id
		ORDER BY 
			con.name
	`, table).Scan(&results).Error
	return results, err
}

// GetProcessList 获取会话列表
func (s *SQLServer) GetProcessList(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			s.session_id,
			s.login_name,
			s.host_name,
			s.program_name,
			DB_NAME(s.database_id) AS database_name,
			s.status,
			s.cpu_time,
			s.memory_usage,
			s.total_elapsed_time,
			s.last_request_start_time,
			s.last_request_end_time,
			t.text AS sql_text
		FROM 
			sys.dm_exec_sessions s
		LEFT JOIN 
			sys.dm_exec_connections c ON s.session_id = c.session_id
		OUTER APPLY 
			sys.dm_exec_sql_text(c.most_recent_sql_handle) t
		WHERE 
			s.is_user_process = 1
		ORDER BY 
			s.session_id
	`).Scan(&results).Error
	return results, err
}

// KillProcess 终止会话
func (s *SQLServer) KillProcess(db *gorm.DB, sessionID int) error {
	return db.Exec(fmt.Sprintf("KILL %d", sessionID)).Error
}

// GetUsers 获取用户列表
func (s *SQLServer) GetUsers(db *gorm.DB) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(`
		SELECT 
			p.name AS LoginName,
			p.type_desc AS LoginType,
			p.create_date AS CreateDate,
			p.is_disabled AS IsDisabled,
			CASE WHEN p.type = 'S' THEN 'SQL Login' ELSE 'Windows Login' END AS AuthenticationType
		FROM 
			sys.server_principals p
		WHERE 
			p.type IN ('S', 'U', 'G')
			AND p.name NOT LIKE '##%'
		ORDER BY 
			p.name
	`).Scan(&results).Error
	return results, err
}

// CreateUser 创建用户
func (s *SQLServer) CreateUser(db *gorm.DB, loginName, username, defaultSchema string, isSysAdmin bool) error {
	// 创建登录
	if err := db.Exec(fmt.Sprintf("CREATE LOGIN [%s] WITH PASSWORD = '%s'", loginName, loginName)).Error; err != nil {
		return err
	}

	// 创建用户
	sql := fmt.Sprintf("CREATE USER [%s] FOR LOGIN [%s]", username, loginName)
	if defaultSchema != "" {
		sql += fmt.Sprintf(" WITH DEFAULT_SCHEMA = [%s]", defaultSchema)
	}

	if err := db.Exec(sql).Error; err != nil {
		return err
	}

	// 如果是系统管理员，添加到sysadmin角色
	if isSysAdmin {
		if err := db.Exec(fmt.Sprintf("EXEC sp_addsrvrolemember '%s', 'sysadmin'", loginName)).Error; err != nil {
			return err
		}
	}

	return nil
}

// DropUser 删除用户
func (s *SQLServer) DropUser(db *gorm.DB, username string) error {
	// 删除用户
	if err := db.Exec(fmt.Sprintf("IF EXISTS (SELECT name FROM sys.database_principals WHERE name = N'%s') DROP USER [%s]", username, username)).Error; err != nil {
		return err
	}

	// 删除登录
	return db.Exec(fmt.Sprintf("IF EXISTS (SELECT name FROM sys.server_principals WHERE name = N'%s') DROP LOGIN [%s]", username, username)).Error
}

// GrantPrivileges 授予权限
func (s *SQLServer) GrantPrivileges(db *gorm.DB, privileges string, objects string, username string) error {
	sql := fmt.Sprintf("GRANT %s ON %s TO [%s]", privileges, objects, username)
	return db.Exec(sql).Error
}

// RevokePrivileges 撤销权限
func (s *SQLServer) RevokePrivileges(db *gorm.DB, privileges string, objects string, username string) error {
	sql := fmt.Sprintf("REVOKE %s ON %s FROM [%s]", privileges, objects, username)
	return db.Exec(sql).Error
}

// GetDatabaseFiles 获取数据库文件信息
func (s *SQLServer) GetDatabaseFiles(db *gorm.DB, database string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	err := db.Raw(fmt.Sprintf(`
		USE [%s];
		SELECT 
			name AS FileName,
			physical_name AS PhysicalName,
			type_desc AS FileType,
			size/128 AS SizeMB,
			growth/128 AS GrowthMB,
			CASE WHEN is_percent_growth = 1 THEN 'Percent' ELSE 'MB' END AS GrowthType,
			max_size/128 AS MaxSizeMB
		FROM 
			sys.database_files
		ORDER BY 
			type, name
	`, database)).Scan(&results).Error
	return results, err
}

// BackupDatabase 备份数据库
func (s *SQLServer) BackupDatabase(db *gorm.DB, database, backupFile string) error {
	sql := fmt.Sprintf("BACKUP DATABASE [%s] TO DISK = N'%s' WITH NOFORMAT, NOINIT, NAME = N'%s-Full Database Backup', SKIP, NOREWIND, NOUNLOAD, STATS = 10", database, backupFile, database)
	return db.Exec(sql).Error
}

// RestoreDatabase 恢复数据库
func (s *SQLServer) RestoreDatabase(db *gorm.DB, database, backupFile string) error {
	// 先关闭所有连接
	closeConnectionsSQL := fmt.Sprintf(`
		USE master;
		IF EXISTS (SELECT name FROM sys.databases WHERE name = N'%s')
		BEGIN
			ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		END
	`, database, database)

	if err := db.Exec(closeConnectionsSQL).Error; err != nil {
		return err
	}

	// 恢复数据库
	restoreSQL := fmt.Sprintf("RESTORE DATABASE [%s] FROM DISK = N'%s' WITH FILE = 1, NOUNLOAD, REPLACE, STATS = 5", database, backupFile)
	if err := db.Exec(restoreSQL).Error; err != nil {
		return err
	}

	// 恢复多用户模式
	return db.Exec(fmt.Sprintf("ALTER DATABASE [%s] SET MULTI_USER", database)).Error
}

// QueryPage 分页查询
func (s *SQLServer) QueryPage(out interface{}, page, pageSize int, filter interface{}, opts ...interface{}) (int64, error) {
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
	var sql string
	var values []interface{}

	switch f := filter.(type) {
	case string:
		// 如果 filter 是 SQL 字符串
		sql = f
		// 提取剩余的参数作为 values
		if len(opts) > 1 {
			for _, v := range opts[1:] {
				values = append(values, v)
			}
		}
	case map[string]interface{}:
		// 如果 filter 是条件映射，构建 WHERE 子句
		// 这里简单实现，实际应用中可能需要更复杂的处理
		var conditions []string
		for k, v := range f {
			// SQL Server 使用 @p1, @p2 等参数
			paramName := fmt.Sprintf("@p%d", len(values)+1)
			conditions = append(conditions, fmt.Sprintf("%s = %s", k, paramName))
			values = append(values, v)
		}

		// 假设 opts[1] 是基础 SQL（如果提供）
		baseSQL := "SELECT * FROM unknown_table"
		if len(opts) > 1 {
			if s, ok := opts[1].(string); ok {
				baseSQL = s
			}
		}

		if len(conditions) > 0 {
			if strings.Contains(strings.ToUpper(baseSQL), " WHERE ") {
				sql = fmt.Sprintf("%s AND %s", baseSQL, strings.Join(conditions, " AND "))
			} else {
				sql = fmt.Sprintf("%s WHERE %s", baseSQL, strings.Join(conditions, " AND "))
			}
		} else {
			sql = baseSQL
		}
	default:
		return 0, fmt.Errorf("不支持的过滤条件类型")
	}

	// 计算偏移量
	offset := (page - 1) * pageSize

	// 查询总记录数
	var total int64
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_table", sql)
	err := db.Raw(countSQL, values...).Count(&total).Error
	if err != nil {
		return 0, fmt.Errorf("查询总记录数失败: %w", err)
	}

	// 如果没有记录，直接返回
	if total == 0 {
		return 0, nil
	}

	// 查询分页数据
	// SQL Server 2012+ 使用 OFFSET-FETCH 语法
	// 注意：SQL Server 要求 ORDER BY 子句
	if !strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		// 如果原始SQL没有ORDER BY子句，添加一个默认的
		sql = fmt.Sprintf("%s ORDER BY (SELECT NULL)", sql)
	}

	pageSQL := fmt.Sprintf("%s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", sql, offset, pageSize)
	err = db.Raw(pageSQL, values...).Scan(out).Error
	if err != nil {
		return 0, fmt.Errorf("查询分页数据失败: %w", err)
	}

	return total, nil
}

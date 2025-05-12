package gosqlx

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// 常量定义
const (
	// 批量操作相关常量
	BatchSize = 100 // 每批处理的记录数

	// 事件类型常量
	EventCreate = 1 // 创建事件
	EventUpdate = 3 // 更新事件

	// 排序类型常量
	OrderAscending  = 3 // 升序
	OrderDescending = 1 // 降序
	OrderCustomize  = 5 // 自定义排序
)

// FormatWhere 格式化WHERE条件
// 如果条件以"AND "开头，则移除"AND "前缀
func FormatWhere(where string) string {
	if strings.HasPrefix(where, "AND ") {
		return strings.Replace(where, "AND ", "", 1)
	}
	return where
}

// ReflectTableName 反射获取表名
// 支持字符串、结构体和切片类型
func ReflectTableName(out interface{}) string {
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

// ReflectPrimaryKeys 反射获取主键条件
// 返回主键字段的条件字符串，如 "id = ?"
func ReflectPrimaryKeys(out interface{}) string {
	// 这里简化处理，假设主键为id
	// 在实际应用中，应该通过反射或GORM的API获取真实的主键
	return "id = ?"
}

// ReflectPrimaryValues 反射获取主键值
// 返回主键字段的值数组
func ReflectPrimaryValues(out interface{}) []interface{} {
	// 这里简化处理，假设主键为id字段
	// 在实际应用中，应该通过反射获取真实的主键值
	v := reflect.Indirect(reflect.ValueOf(out))

	// 尝试获取ID字段
	idField := v.FieldByName("ID")
	if idField.IsValid() {
		return []interface{}{idField.Interface()}
	}

	// 尝试获取Id字段
	idField = v.FieldByName("Id")
	if idField.IsValid() {
		return []interface{}{idField.Interface()}
	}

	return nil
}

// ReflectUpdateSQL 反射生成更新SQL
// 返回更新SQL语句和参数值
func ReflectUpdateSQL(out interface{}) (string, []interface{}) {
	// 特别处理指针
	v := reflect.Indirect(reflect.ValueOf(out))

	// 相关定义
	var fields, pkFields []string
	var values, pkValues []interface{}
	t := v.Type()

	// 遍历字段
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i).Interface()

		// 跳过非导出字段
		if !field.IsExported() {
			continue
		}

		// 获取字段名
		fieldName := field.Name

		// 检查是否为主键
		if fieldName == "ID" || fieldName == "Id" {
			pkFields = append(pkFields, fmt.Sprintf("`%s` = ?", fieldName))
			pkValues = append(pkValues, value)
			continue
		}

		// 追加普通字段
		fields = append(fields, fmt.Sprintf("`%s` = ?", fieldName))
		values = append(values, value)
	}

	// 如果没有找到主键，返回空
	if len(pkFields) == 0 {
		return "", nil
	}

	// 生成SQL
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;",
		ReflectTableName(out),
		strings.Join(fields, ", "),
		strings.Join(pkFields, " AND "),
	)

	// 合并参数
	values = append(values, pkValues...)

	return sql, values
}

// BatchProcess 批量处理数据
// db: 数据库连接
// event: 事件类型（创建或更新）
// batchSize: 每批处理的记录数
// rows: 要处理的数据行
func BatchProcess(db *Database, event int, batchSize int, rows ...interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// 如果未指定批量大小，使用默认值
	if batchSize <= 0 {
		batchSize = BatchSize
	}

	// 计算批次数
	total := len(rows)
	batches := (total + batchSize - 1) / batchSize

	// 按批次处理
	for i := 0; i < batches; i++ {
		start := i * batchSize
		end := (i + 1) * batchSize
		if end > total {
			end = total
		}

		// 获取当前批次的数据
		batch := rows[start:end]

		// 使用 ReflectBatchSQL 生成 SQL
		sql, values := ReflectBatchSQL(event, batch...)
		if sql == "" {
			continue
		}

		// 执行 SQL
		if err := db.Exec(sql, values...); err != nil {
			return err
		}
	}

	return nil
}

// ReflectBatchSQL 反射生成批量插入SQL
// 支持插入和更新操作
func ReflectBatchSQL(event int, rows ...interface{}) (string, []interface{}) {
	// 校验为空
	if len(rows) == 0 {
		return "", nil
	}

	// 相关定义
	var duplicateClause string
	meta := rows[0]

	// 获取字段名
	fields := ReflectFields(meta)

	// 生成占位符
	placeholder := fmt.Sprintf("(%s), ", strings.TrimRight(strings.Repeat("?, ", len(fields)), ", "))
	placeholders := strings.TrimRight(strings.Repeat(placeholder, len(rows)), ", ")

	// 如果是更新操作，生成ON DUPLICATE KEY UPDATE子句
	if event == EventUpdate {
		duplicateClause = GenerateDuplicateClause(fields...)
	}

	// 收集所有值
	var values []interface{}
	for _, row := range rows {
		values = append(values, ReflectValues(row)...)
	}

	// 生成SQL
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s %s;",
		ReflectTableName(meta),
		fmt.Sprintf("`%s`", strings.Join(fields, "`, `")),
		placeholders,
		duplicateClause,
	)

	return sql, values
}

// ReflectFields 反射获取结构体的字段名
func ReflectFields(obj interface{}) []string {
	v := reflect.Indirect(reflect.ValueOf(obj))
	t := v.Type()

	var fields []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// 跳过非导出字段
		if !field.IsExported() {
			continue
		}

		fields = append(fields, field.Name)
	}

	return fields
}

// ReflectValues 反射获取结构体的字段值
func ReflectValues(obj interface{}) []interface{} {
	v := reflect.Indirect(reflect.ValueOf(obj))
	t := v.Type()

	var values []interface{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// 跳过非导出字段
		if !field.IsExported() {
			continue
		}

		values = append(values, v.Field(i).Interface())
	}

	return values
}

// GenerateDuplicateClause 生成ON DUPLICATE KEY UPDATE子句
func GenerateDuplicateClause(fields ...string) string {
	var buffer bytes.Buffer
	buffer.WriteString(" ON DUPLICATE KEY UPDATE ")

	for _, field := range fields {
		buffer.WriteString(fmt.Sprintf("`%s` = VALUES(`%s`), ", field, field))
	}

	return strings.TrimRight(buffer.String(), ", ")
}

// CountSQL 将查询SQL转换为计数SQL
func CountSQL(sql string) string {
	// 移除ORDER BY子句
	orderRegex := regexp.MustCompile(`(?i)order by[^\)]*$`)
	if orderRegex.MatchString(sql) {
		sql = orderRegex.ReplaceAllString(sql, "")
	}

	// 替换SELECT子句为COUNT(*)
	selectRegex := regexp.MustCompile(`(?i)(?U)^\s*select[\s|\S]+\sfrom\s`)
	if selectRegex.MatchString(sql) && !strings.Contains(strings.ToUpper(sql), "GROUP BY") {
		return selectRegex.ReplaceAllString(sql, `SELECT COUNT(*) FROM `)
	}

	// 对于复杂查询，使用子查询
	return fmt.Sprintf(`SELECT COUNT(*) FROM (%s) C`, sql)
}

// FormatLikeValue 格式化LIKE查询的值
func FormatLikeValue(value string) string {
	return "%" + value + "%"
}

// FormatDateRange 格式化日期范围
// start为true时返回日期的开始时间，否则返回结束时间
func FormatDateRange(dateStr string, start bool) string {
	// 简单的日期格式处理
	if len(dateStr) <= 10 { // 只有日期部分
		if start {
			return dateStr + " 00:00:00.000"
		}
		return dateStr + " 23:59:59.999"
	}

	return dateStr
}

// IsNullOrEmpty 检查字符串是否为空
func IsNullOrEmpty(s string) bool {
	return len(strings.TrimSpace(s)) == 0
}

// IsZeroValue 检查值是否为零值
func IsZeroValue(value interface{}) bool {
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Slice, reflect.Map, reflect.Array:
		return v.Len() == 0
	case reflect.Struct:
		// 特殊处理time.Time
		if t, ok := value.(time.Time); ok {
			return t.IsZero()
		}
		return false
	default:
		return false
	}
}

// ConvertToSlice 将任意值转换为切片
func ConvertToSlice(value interface{}) []interface{} {
	v := reflect.ValueOf(value)

	// 如果已经是切片或数组，转换每个元素
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		result := make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			result[i] = v.Index(i).Interface()
		}
		return result
	}

	// 单个值转换为只有一个元素的切片
	return []interface{}{value}
}

// JoinInts 将整数切片连接为字符串
func JoinInts(ints []int, separator string) string {
	if len(ints) == 0 {
		return ""
	}

	strs := make([]string, len(ints))
	for i, v := range ints {
		strs[i] = fmt.Sprintf("%d", v)
	}

	return strings.Join(strs, separator)
}

// JoinInt64s 将int64切片连接为字符串
func JoinInt64s(ints []int64, separator string) string {
	if len(ints) == 0 {
		return ""
	}

	strs := make([]string, len(ints))
	for i, v := range ints {
		strs[i] = fmt.Sprintf("%d", v)
	}

	return strings.Join(strs, separator)
}

// JoinStrings 将字符串切片连接为字符串，并添加引号
func JoinStrings(strs []string, separator string) string {
	if len(strs) == 0 {
		return ""
	}

	quoted := make([]string, len(strs))
	for i, v := range strs {
		quoted[i] = fmt.Sprintf("'%s'", v)
	}

	return strings.Join(quoted, separator)
}

// GeneratePlaceholders 生成SQL占位符
func GeneratePlaceholders(count int) string {
	if count <= 0 {
		return ""
	}

	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}

	return strings.Join(placeholders, ", ")
}

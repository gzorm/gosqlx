package query

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gzorm/gosqlx/builder"
)

// Query 查询构建器
type Query struct {
	db        interface{}    // 数据库连接
	table     string         // 表名
	alias     string         // 表别名
	columns   []string       // 查询列
	joins     []string       // 连接语句
	where     *builder.Where // 条件构建器
	group     string         // 分组语句
	having    string         // 过滤语句
	order     *builder.Order // 排序构建器
	limit     int            // 限制数
	offset    int            // 偏移量
	forUpdate bool           // 行锁
	forShare  bool           // 共享锁
	distinct  bool           // 去重
	count     string         // 计数字段
	sum       string         // 求和字段
	avg       string         // 平均值字段
	max       string         // 最大值字段
	min       string         // 最小值字段
	args      []interface{}  // 参数值
}

// NewQuery 创建查询构建器
func NewQuery(db interface{}) *Query {
	return &Query{
		db:      db,
		where:   builder.NewWhere(),
		order:   builder.NewOrder(),
		columns: []string{"*"},
		args:    make([]interface{}, 0),
	}
}

// Table 设置表名
func (q *Query) Table(table string) *Query {
	q.table = table
	return q
}

// Alias 设置表别名
func (q *Query) Alias(alias string) *Query {
	q.alias = alias
	return q
}

// Select 设置查询列
func (q *Query) Select(columns ...string) *Query {
	if len(columns) > 0 {
		q.columns = columns
	}
	return q
}

// SelectRaw 设置原始查询列
func (q *Query) SelectRaw(query string, args ...interface{}) *Query {
	q.columns = []string{query}
	q.args = append(q.args, args...)
	return q
}

// WhereRaw 添加原始SQL条件
func (q *Query) WhereRaw(query string, args ...interface{}) *Query {
	q.where.WhereRaw(query, args...)
	return q
}

// Join 添加连接
func (q *Query) Join(table, condition string) *Query {
	q.joins = append(q.joins, fmt.Sprintf("JOIN %s ON %s", table, condition))
	return q
}

// LeftJoin 添加左连接
func (q *Query) LeftJoin(table, condition string) *Query {
	q.joins = append(q.joins, fmt.Sprintf("LEFT JOIN %s ON %s", table, condition))
	return q
}

// RightJoin 添加右连接
func (q *Query) RightJoin(table, condition string) *Query {
	q.joins = append(q.joins, fmt.Sprintf("RIGHT JOIN %s ON %s", table, condition))
	return q
}

// InnerJoin 添加内连接
func (q *Query) InnerJoin(table, condition string) *Query {
	q.joins = append(q.joins, fmt.Sprintf("INNER JOIN %s ON %s", table, condition))
	return q
}

// Where 添加条件
func (q *Query) Where(query string, args ...interface{}) *Query {
	q.where.Where(query, args...)
	return q
}

// WhereGroup 添加条件组
func (q *Query) WhereGroup(fn func(w *builder.Where)) *Query {
	fn(q.where)
	return q
}

// WhereIf 条件性添加条件
func (q *Query) WhereIf(condition bool, query string, args ...interface{}) *Query {
	q.where.WhereIf(condition, query, args...)
	return q
}

// WhereIn 添加IN条件
func (q *Query) WhereIn(field string, values interface{}) *Query {
	q.where.WhereIn(field, values)
	return q
}

// WhereNotIn 添加NOT IN条件
func (q *Query) WhereNotIn(field string, values interface{}) *Query {
	q.where.WhereNotIn(field, values)
	return q
}

// WhereBetween 添加BETWEEN条件
func (q *Query) WhereBetween(field string, min, max interface{}) *Query {
	q.where.WhereBetween(field, min, max)
	return q
}

// WhereNotBetween 添加NOT BETWEEN条件
func (q *Query) WhereNotBetween(field string, min, max interface{}) *Query {
	q.where.WhereNotBetween(field, min, max)
	return q
}

// WhereLike 添加LIKE条件
func (q *Query) WhereLike(field string, value string) *Query {
	q.where.WhereLike(field, value)
	return q
}

// WhereNotLike 添加NOT LIKE条件
func (q *Query) WhereNotLike(field string, value string) *Query {
	q.where.WhereNotLike(field, value)
	return q
}

// WhereNull 添加IS NULL条件
func (q *Query) WhereNull(field string) *Query {
	q.where.WhereNull(field)
	return q
}

// WhereNotNull 添加IS NOT NULL条件
func (q *Query) WhereNotNull(field string) *Query {
	q.where.WhereNotNull(field)
	return q
}

// Group 添加分组
func (q *Query) Group(group string) *Query {
	q.group = group
	return q
}

// Having 添加过滤
func (q *Query) Having(having string, args ...interface{}) *Query {
	q.having = having
	q.args = append(q.args, args...)
	return q
}

// OrderBy 设置排序
func (q *Query) OrderBy(order string) *Query {
	q.order.OrderBy(order)
	return q
}

// OrderByAsc 按字段升序排序
func (q *Query) OrderByAsc(field string) *Query {
	q.order.OrderByAsc(field)
	return q
}

// OrderByDesc 按字段降序排序
func (q *Query) OrderByDesc(field string) *Query {
	q.order.OrderByDesc(field)
	return q
}

// Limit 设置限制数
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

// Offset 设置偏移量
func (q *Query) Offset(offset int) *Query {
	q.offset = offset
	return q
}

// Page 设置分页
func (q *Query) Page(page, pageSize int) *Query {
	if page <= 0 {
		page = 1
	}

	if pageSize <= 0 {
		pageSize = 10
	}

	q.limit = pageSize
	q.offset = (page - 1) * pageSize
	return q
}

// ForUpdate 设置行锁
func (q *Query) ForUpdate() *Query {
	q.forUpdate = true
	return q
}

// ForShare 设置共享锁
func (q *Query) ForShare() *Query {
	q.forShare = true
	return q
}

// Distinct 设置去重
func (q *Query) Distinct() *Query {
	q.distinct = true
	return q
}

// Count 设置计数
func (q *Query) Count(field ...string) *Query {
	if len(field) > 0 {
		q.count = field[0]
	} else {
		q.count = "*"
	}
	return q
}

// Sum 设置求和
func (q *Query) Sum(field string) *Query {
	q.sum = field
	return q
}

// Avg 设置平均值
func (q *Query) Avg(field string) *Query {
	q.avg = field
	return q
}

// Max 设置最大值
func (q *Query) Max(field string) *Query {
	q.max = field
	return q
}

// Min 设置最小值
func (q *Query) Min(field string) *Query {
	q.min = field
	return q
}

// Get 获取多条记录
func (q *Query) Get(out interface{}) error {
	sql, args := q.BuildSelect()
	return q.execQuery(sql, args, out)
}

// First 获取单条记录
func (q *Query) First(out interface{}) error {
	q.limit = 1
	sql, args := q.BuildSelect()
	return q.execQuery(sql, args, out)
}

// Value 获取单个值
func (q *Query) Value(column string) (interface{}, error) {
	q.columns = []string{column}
	q.limit = 1
	sql, args := q.BuildSelect()

	var value interface{}
	err := q.execQueryRow(sql, args, &value)
	return value, err
}

// Pluck 获取单列值
func (q *Query) Pluck(column string, out interface{}) error {
	q.columns = []string{column}
	sqlSelect, args := q.BuildSelect()
	return q.execQuery(sqlSelect, args, out)
}

// Exists 判断是否存在
func (q *Query) Exists() (bool, error) {
	q.columns = []string{"1"}
	q.limit = 1
	sqlSelect, args := q.BuildSelect()

	var value interface{}
	err := q.execQueryRow(sqlSelect, args, &value)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CountNum 获取记录数
func (q *Query) CountNum() (int64, error) {
	oldColumns := q.columns
	oldLimit := q.limit
	oldOffset := q.offset
	oldOrder := q.order

	q.columns = []string{"COUNT(*) as count"}
	q.limit = 0
	q.offset = 0
	q.order = builder.NewOrder()

	sql, args := q.BuildSelect()

	var count int64
	err := q.execQueryRow(sql, args, &count)

	q.columns = oldColumns
	q.limit = oldLimit
	q.offset = oldOffset
	q.order = oldOrder

	return count, err
}

// SumNum 获取求和
func (q *Query) SumNum(field string) (float64, error) {
	oldColumns := q.columns
	oldLimit := q.limit
	oldOffset := q.offset

	q.columns = []string{fmt.Sprintf("SUM(%s) as sum", field)}
	q.limit = 0
	q.offset = 0

	sql, args := q.BuildSelect()

	var sum float64
	err := q.execQueryRow(sql, args, &sum)

	q.columns = oldColumns
	q.limit = oldLimit
	q.offset = oldOffset

	return sum, err
}

// AvgNum 获取平均值
func (q *Query) AvgNum(field string) (float64, error) {
	oldColumns := q.columns
	oldLimit := q.limit
	oldOffset := q.offset

	q.columns = []string{fmt.Sprintf("AVG(%s) as avg", field)}
	q.limit = 0
	q.offset = 0

	sql, args := q.BuildSelect()

	var avg float64
	err := q.execQueryRow(sql, args, &avg)

	q.columns = oldColumns
	q.limit = oldLimit
	q.offset = oldOffset

	return avg, err
}

// MaxNum 获取最大值
func (q *Query) MaxNum(field string) (interface{}, error) {
	oldColumns := q.columns
	oldLimit := q.limit
	oldOffset := q.offset

	q.columns = []string{fmt.Sprintf("MAX(%s) as max", field)}
	q.limit = 0
	q.offset = 0

	sql, args := q.BuildSelect()

	var max interface{}
	err := q.execQueryRow(sql, args, &max)

	q.columns = oldColumns
	q.limit = oldLimit
	q.offset = oldOffset

	return max, err
}

// MinNum 获取最小值
func (q *Query) MinNum(field string) (interface{}, error) {
	oldColumns := q.columns
	oldLimit := q.limit
	oldOffset := q.offset

	q.columns = []string{fmt.Sprintf("MIN(%s) as min", field)}
	q.limit = 0
	q.offset = 0

	sql, args := q.BuildSelect()

	var min interface{}
	err := q.execQueryRow(sql, args, &min)

	q.columns = oldColumns
	q.limit = oldLimit
	q.offset = oldOffset

	return min, err
}

// BuildSelect 构建SELECT语句
func (q *Query) BuildSelect() (string, []interface{}) {
	var query strings.Builder
	var args []interface{}

	// SELECT
	query.WriteString("SELECT ")
	if q.distinct {
		query.WriteString("DISTINCT ")
	}

	// 处理聚合函数
	if q.count != "" {
		query.WriteString(fmt.Sprintf("COUNT(%s)", q.count))
	} else if q.sum != "" {
		query.WriteString(fmt.Sprintf("SUM(%s)", q.sum))
	} else if q.avg != "" {
		query.WriteString(fmt.Sprintf("AVG(%s)", q.avg))
	} else if q.max != "" {
		query.WriteString(fmt.Sprintf("MAX(%s)", q.max))
	} else if q.min != "" {
		query.WriteString(fmt.Sprintf("MIN(%s)", q.min))
	} else {
		query.WriteString(strings.Join(q.columns, ", "))
	}

	// FROM
	query.WriteString(" FROM ")
	query.WriteString(q.table)
	if q.alias != "" {
		query.WriteString(" AS ")
		query.WriteString(q.alias)
	}

	// JOIN
	if len(q.joins) > 0 {
		query.WriteString(" ")
		query.WriteString(strings.Join(q.joins, " "))
	}

	// WHERE
	whereStr, whereArgs := q.where.Build()
	if whereStr != "" {
		query.WriteString(" WHERE ")
		query.WriteString(whereStr)
		args = append(args, whereArgs...)
	}

	// GROUP BY
	if q.group != "" {
		query.WriteString(" GROUP BY ")
		query.WriteString(q.group)
	}

	// HAVING
	if q.having != "" {
		query.WriteString(" HAVING ")
		query.WriteString(q.having)
	}

	// ORDER BY
	orderStr := q.order.String()
	if orderStr != "" {
		query.WriteString(" ")
		query.WriteString(orderStr)
	}

	// LIMIT & OFFSET
	if q.limit > 0 {
		query.WriteString(fmt.Sprintf(" LIMIT %d", q.limit))
		if q.offset > 0 {
			query.WriteString(fmt.Sprintf(" OFFSET %d", q.offset))
		}
	}

	// FOR UPDATE / FOR SHARE
	if q.forUpdate {
		query.WriteString(" FOR UPDATE")
	} else if q.forShare {
		query.WriteString(" FOR SHARE")
	}

	// 合并参数
	args = append(args, q.args...)

	return query.String(), args
}

// execQuery 执行查询
func (q *Query) execQuery(sqlStr string, args []interface{}, out interface{}) error {
	// 检查输出参数
	if out == nil {
		return errors.New("输出参数不能为空")
	}

	// 检查数据库连接
	if q.db == nil {
		return errors.New("数据库连接不能为空")
	}

	// 根据数据库连接类型执行查询
	switch db := q.db.(type) {
	case *sql.DB:
		rows, err := db.Query(sqlStr, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return scanRows(rows, out)
	case *sql.Tx:
		rows, err := db.Query(sqlStr, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return scanRows(rows, out)
	default:
		return fmt.Errorf("不支持的数据库连接类型: %T", q.db)
	}
}

// execQueryRow 执行单行查询
func (q *Query) execQueryRow(sqlStr string, args []interface{}, out interface{}) error {
	// 检查输出参数
	if out == nil {
		return errors.New("输出参数不能为空")
	}

	// 检查数据库连接
	if q.db == nil {
		return errors.New("数据库连接不能为空")
	}

	// 根据数据库连接类型执行查询
	switch db := q.db.(type) {
	case *sql.DB:
		return db.QueryRow(sqlStr, args...).Scan(out)
	case *sql.Tx:
		return db.QueryRow(sqlStr, args...).Scan(out)
	default:
		return fmt.Errorf("不支持的数据库连接类型: %T", q.db)
	}
}

// scanRows 扫描结果集
func scanRows(rows *sql.Rows, out interface{}) error {
	// 获取输出参数的反射值
	outValue := reflect.ValueOf(out)
	if outValue.Kind() != reflect.Ptr {
		return errors.New("输出参数必须是指针类型")
	}

	// 获取指针指向的值
	outValue = outValue.Elem()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// 处理切片类型
	if outValue.Kind() == reflect.Slice {
		// 获取切片元素类型
		elemType := outValue.Type().Elem()

		// 创建切片
		slice := reflect.MakeSlice(outValue.Type(), 0, 0)

		// 遍历结果集
		for rows.Next() {
			// 创建元素
			elem := reflect.New(elemType).Elem()

			// 扫描行数据
			if err := scanRow(rows, columns, elem); err != nil {
				return err
			}

			// 添加到切片
			slice = reflect.Append(slice, elem)
		}

		// 设置输出值
		outValue.Set(slice)
		return nil
	}

	// 处理结构体类型
	if outValue.Kind() == reflect.Struct {
		// 检查是否有数据
		if !rows.Next() {
			return sql.ErrNoRows
		}

		// 扫描行数据
		if err := scanRow(rows, columns, outValue); err != nil {
			return err
		}

		return nil
	}

	// 处理基本类型
	if !rows.Next() {
		return sql.ErrNoRows
	}

	// 扫描单个值
	return rows.Scan(out)
}

// scanRow 扫描单行数据
func scanRow(rows *sql.Rows, columns []string, outValue reflect.Value) error {
	// 创建扫描目标
	targets := make([]interface{}, len(columns))
	for i := range columns {
		targets[i] = new(interface{})
	}

	// 扫描行数据
	if err := rows.Scan(targets...); err != nil {
		return err
	}

	// 设置结构体字段值
	if outValue.Kind() == reflect.Struct {
		for i, column := range columns {
			// 查找字段
			field := findField(outValue, column)
			if !field.IsValid() {
				continue
			}

			// 获取扫描值
			scanValue := reflect.ValueOf(targets[i]).Elem().Interface()
			if scanValue == nil {
				continue
			}

			// 设置字段值
			if err := setFieldValue(field, scanValue); err != nil {
				return err
			}
		}
		return nil
	}

	// 设置基本类型值
	scanValue := reflect.ValueOf(targets[0]).Elem().Interface()
	if scanValue == nil {
		return nil
	}

	// 设置值
	return setFieldValue(outValue, scanValue)
}

// findField 查找结构体字段
func findField(outValue reflect.Value, column string) reflect.Value {
	// 获取结构体类型
	outType := outValue.Type()

	// 遍历字段
	for i := 0; i < outType.NumField(); i++ {
		field := outType.Field(i)

		// 检查标签
		tag := field.Tag.Get("db")
		if tag == column {
			return outValue.Field(i)
		}

		// 检查字段名
		if strings.EqualFold(field.Name, column) {
			return outValue.Field(i)
		}
	}

	return reflect.Value{}
}

// setFieldValue 设置字段值
func setFieldValue(field reflect.Value, value interface{}) error {
	// 处理nil值
	if value == nil {
		return nil
	}

	// 若 field 是指针，初始化并取其 Elem
	if field.Kind() == reflect.Ptr {
		elemType := field.Type().Elem()
		field.Set(reflect.New(elemType))
		return setFieldValue(field.Elem(), value)
	}

	// 检查字段是否可设置
	if !field.CanSet() {
		return nil
	}

	valueValue := reflect.ValueOf(value)

	switch field.Kind() {
	case reflect.String:
		switch v := value.(type) {
		case string:
			field.SetString(v)
		case []byte:
			field.SetString(string(v))
		default:
			field.SetString(fmt.Sprintf("%v", v))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case int64:
			field.SetInt(v)
		case int:
			field.SetInt(int64(v))
		case float64:
			field.SetInt(int64(v))
		case string:
			i, err := parseInt(v)
			if err != nil {
				return err
			}
			field.SetInt(i)
		case []byte:
			i, err := parseInt(string(v))
			if err != nil {
				return err
			}
			field.SetInt(i)
		default:
			return fmt.Errorf("无法将 %T 转换为 %s", value, field.Kind())
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := value.(type) {
		case uint64:
			field.SetUint(v)
		case uint:
			field.SetUint(uint64(v))
		case int64:
			field.SetUint(uint64(v))
		case int:
			field.SetUint(uint64(v))
		case float64:
			field.SetUint(uint64(v))
		case string:
			i, err := parseUint(v)
			if err != nil {
				return err
			}
			field.SetUint(i)
		case []byte:
			i, err := parseUint(string(v))
			if err != nil {
				return err
			}
			field.SetUint(i)
		default:
			return fmt.Errorf("无法将 %T 转换为 %s", value, field.Kind())
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case float64:
			field.SetFloat(v)
		case float32:
			field.SetFloat(float64(v))
		case int64:
			field.SetFloat(float64(v))
		case int:
			field.SetFloat(float64(v))
		case string:
			f, err := parseFloat(v)
			if err != nil {
				return err
			}
			field.SetFloat(f)
		case []byte:
			f, err := parseFloat(string(v))
			if err != nil {
				return err
			}
			field.SetFloat(f)
		default:
			return fmt.Errorf("无法将 %T 转换为 %s", value, field.Kind())
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			field.SetBool(v)
		case int64:
			field.SetBool(v != 0)
		case int:
			field.SetBool(v != 0)
		case string:
			b, err := parseBool(v)
			if err != nil {
				return err
			}
			field.SetBool(b)
		case []byte:
			b, err := parseBool(string(v))
			if err != nil {
				return err
			}
			field.SetBool(b)
		default:
			return fmt.Errorf("无法将 %T 转换为 %s", value, field.Kind())
		}
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			switch v := value.(type) {
			case time.Time:
				field.Set(reflect.ValueOf(v))
			case string:
				t, err := parseTime(v)
				if err != nil {
					return err
				}
				field.Set(reflect.ValueOf(t))
			case []byte:
				t, err := parseTime(string(v))
				if err != nil {
					return err
				}
				field.Set(reflect.ValueOf(t))
			default:
				return fmt.Errorf("无法将 %T 转换为 time.Time", value)
			}
		} else {
			return fmt.Errorf("不支持的 struct 类型: %s", field.Type().Name())
		}
	case reflect.Interface:
		field.Set(valueValue)
	default:
		return fmt.Errorf("不支持的字段类型: %s", field.Kind())
	}

	return nil
}

// parseInt 解析整数
func parseInt(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// parseUint 解析无符号整数
func parseUint(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

// parseFloat 解析浮点数
func parseFloat(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

// parseBool 解析布尔值
func parseBool(s string) (bool, error) {
	if s == "" {
		return false, nil
	}

	s = strings.ToLower(s)
	switch s {
	case "1", "t", "true", "yes", "y", "on":
		return true, nil
	case "0", "f", "false", "no", "n", "off":
		return false, nil
	default:
		return false, fmt.Errorf("无法将 %q 解析为布尔值", s)
	}
}

// parseTime 解析时间
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}

	// 尝试多种常见的时间格式
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"15:04:05",
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999",
	}

	for _, format := range formats {
		t, err := time.Parse(format, s)
		if err == nil {
			return t, nil
		}
	}

	// 尝试解析 Unix 时间戳
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(i, 0), nil
	}

	return time.Time{}, fmt.Errorf("无法将 %q 解析为时间", s)
}

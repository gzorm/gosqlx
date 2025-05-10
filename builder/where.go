package builder

import (
	"fmt"
	"reflect"
	"strings"
)

// Where 条件构建器
type Where struct {
	wheres []string      // 条件语句
	values []interface{} // 参数值
}

// NewWhere 创建新的条件构建器
func NewWhere() *Where {
	return &Where{
		wheres: make([]string, 0),
		values: make([]interface{}, 0),
	}
}

// Where 添加条件
// 示例: Where("id = ?", 1)
func (w *Where) Where(query string, args ...interface{}) *Where {
	if query != "" {
		w.wheres = append(w.wheres, query)
		w.values = append(w.values, args...)
	}
	return w
}

// WhereIf 条件性添加条件
// 示例: WhereIf(id > 0, "id = ?", id)
func (w *Where) WhereIf(condition bool, query string, args ...interface{}) *Where {
	if condition && query != "" {
		w.wheres = append(w.wheres, query)
		w.values = append(w.values, args...)
	}
	return w
}

// And 添加AND条件
// 示例: And("name = ?", "张三")
func (w *Where) And(query string, args ...interface{}) *Where {
	return w.Where(query, args...)
}

// AndIf 条件性添加AND条件
// 示例: AndIf(name != "", "name = ?", name)
func (w *Where) AndIf(condition bool, query string, args ...interface{}) *Where {
	return w.WhereIf(condition, query, args...)
}

// Or 添加OR条件
// 示例: Or("status = ?", 1)
func (w *Where) Or(query string, args ...interface{}) *Where {
	if query != "" {
		if len(w.wheres) > 0 {
			lastIndex := len(w.wheres) - 1
			w.wheres[lastIndex] = fmt.Sprintf("(%s) OR (%s)", w.wheres[lastIndex], query)
			w.values = append(w.values, args...)
		} else {
			w.wheres = append(w.wheres, query)
			w.values = append(w.values, args...)
		}
	}
	return w
}

// OrIf 条件性添加OR条件
// 示例: OrIf(status > 0, "status = ?", status)
func (w *Where) OrIf(condition bool, query string, args ...interface{}) *Where {
	if condition {
		return w.Or(query, args...)
	}
	return w
}

// WhereIn 添加IN条件
// 示例: WhereIn("id", []int{1, 2, 3})
func (w *Where) WhereIn(field string, values interface{}) *Where {
	if field == "" || values == nil {
		return w
	}

	// 处理切片类型
	rv := reflect.ValueOf(values)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return w
	}

	// 空切片直接返回
	if rv.Len() == 0 {
		return w
	}

	// 构建占位符
	var placeholders []string
	var args []interface{}

	for i := 0; i < rv.Len(); i++ {
		placeholders = append(placeholders, "?")
		args = append(args, rv.Index(i).Interface())
	}

	query := fmt.Sprintf("%s IN (%s)", field, strings.Join(placeholders, ", "))
	return w.Where(query, args...)
}

// WhereInIf 条件性添加IN条件
// 示例: WhereInIf(len(ids) > 0, "id", ids)
func (w *Where) WhereInIf(condition bool, field string, values interface{}) *Where {
	if condition {
		return w.WhereIn(field, values)
	}
	return w
}

// WhereNotIn 添加NOT IN条件
// 示例: WhereNotIn("id", []int{1, 2, 3})
func (w *Where) WhereNotIn(field string, values interface{}) *Where {
	if field == "" || values == nil {
		return w
	}

	// 处理切片类型
	rv := reflect.ValueOf(values)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return w
	}

	// 空切片直接返回
	if rv.Len() == 0 {
		return w
	}

	// 构建占位符
	var placeholders []string
	var args []interface{}

	for i := 0; i < rv.Len(); i++ {
		placeholders = append(placeholders, "?")
		args = append(args, rv.Index(i).Interface())
	}

	query := fmt.Sprintf("%s NOT IN (%s)", field, strings.Join(placeholders, ", "))
	return w.Where(query, args...)
}

// WhereNotInIf 条件性添加NOT IN条件
// 示例: WhereNotInIf(len(ids) > 0, "id", ids)
func (w *Where) WhereNotInIf(condition bool, field string, values interface{}) *Where {
	if condition {
		return w.WhereNotIn(field, values)
	}
	return w
}

// WhereBetween 添加BETWEEN条件
// 示例: WhereBetween("age", 18, 30)
func (w *Where) WhereBetween(field string, min, max interface{}) *Where {
	if field == "" {
		return w
	}

	query := fmt.Sprintf("%s BETWEEN ? AND ?", field)
	return w.Where(query, min, max)
}

// WhereBetweenIf 条件性添加BETWEEN条件
// 示例: WhereBetweenIf(min > 0 && max > 0, "age", min, max)
func (w *Where) WhereBetweenIf(condition bool, field string, min, max interface{}) *Where {
	if condition {
		return w.WhereBetween(field, min, max)
	}
	return w
}

// WhereNotBetween 添加NOT BETWEEN条件
// 示例: WhereNotBetween("age", 18, 30)
func (w *Where) WhereNotBetween(field string, min, max interface{}) *Where {
	if field == "" {
		return w
	}

	query := fmt.Sprintf("%s NOT BETWEEN ? AND ?", field)
	return w.Where(query, min, max)
}

// WhereNotBetweenIf 条件性添加NOT BETWEEN条件
// 示例: WhereNotBetweenIf(min > 0 && max > 0, "age", min, max)
func (w *Where) WhereNotBetweenIf(condition bool, field string, min, max interface{}) *Where {
	if condition {
		return w.WhereNotBetween(field, min, max)
	}
	return w
}

// WhereLike 添加LIKE条件
// 示例: WhereLike("name", "%张%")
func (w *Where) WhereLike(field string, value string) *Where {
	if field == "" || value == "" {
		return w
	}

	query := fmt.Sprintf("%s LIKE ?", field)
	return w.Where(query, value)
}

// WhereLikeIf 条件性添加LIKE条件
// 示例: WhereLikeIf(name != "", "name", "%"+name+"%")
func (w *Where) WhereLikeIf(condition bool, field string, value string) *Where {
	if condition {
		return w.WhereLike(field, value)
	}
	return w
}

// WhereNotLike 添加NOT LIKE条件
// 示例: WhereNotLike("name", "%张%")
func (w *Where) WhereNotLike(field string, value string) *Where {
	if field == "" || value == "" {
		return w
	}

	query := fmt.Sprintf("%s NOT LIKE ?", field)
	return w.Where(query, value)
}

// WhereNotLikeIf 条件性添加NOT LIKE条件
// 示例: WhereNotLikeIf(name != "", "name", "%"+name+"%")
func (w *Where) WhereNotLikeIf(condition bool, field string, value string) *Where {
	if condition {
		return w.WhereNotLike(field, value)
	}
	return w
}

// WhereNull 添加IS NULL条件
// 示例: WhereNull("deleted_at")
func (w *Where) WhereNull(field string) *Where {
	if field == "" {
		return w
	}

	query := fmt.Sprintf("%s IS NULL", field)
	return w.Where(query)
}

// WhereNullIf 条件性添加IS NULL条件
// 示例: WhereNullIf(includeDeleted, "deleted_at")
func (w *Where) WhereNullIf(condition bool, field string) *Where {
	if condition {
		return w.WhereNull(field)
	}
	return w
}

// WhereNotNull 添加IS NOT NULL条件
// 示例: WhereNotNull("deleted_at")
func (w *Where) WhereNotNull(field string) *Where {
	if field == "" {
		return w
	}

	query := fmt.Sprintf("%s IS NOT NULL", field)
	return w.Where(query)
}

// WhereNotNullIf 条件性添加IS NOT NULL条件
// 示例: WhereNotNullIf(excludeDeleted, "deleted_at")
func (w *Where) WhereNotNullIf(condition bool, field string) *Where {
	if condition {
		return w.WhereNotNull(field)
	}
	return w
}

// WhereExists 添加EXISTS条件
// 示例: WhereExists("SELECT 1 FROM users WHERE users.id = posts.user_id")
func (w *Where) WhereExists(subquery string, args ...interface{}) *Where {
	if subquery == "" {
		return w
	}

	query := fmt.Sprintf("EXISTS (%s)", subquery)
	return w.Where(query, args...)
}

// WhereExistsIf 条件性添加EXISTS条件
// 示例: WhereExistsIf(condition, "SELECT 1 FROM users WHERE users.id = posts.user_id")
func (w *Where) WhereExistsIf(condition bool, subquery string, args ...interface{}) *Where {
	if condition {
		return w.WhereExists(subquery, args...)
	}
	return w
}

// WhereNotExists 添加NOT EXISTS条件
// 示例: WhereNotExists("SELECT 1 FROM users WHERE users.id = posts.user_id")
func (w *Where) WhereNotExists(subquery string, args ...interface{}) *Where {
	if subquery == "" {
		return w
	}

	query := fmt.Sprintf("NOT EXISTS (%s)", subquery)
	return w.Where(query, args...)
}

// WhereNotExistsIf 条件性添加NOT EXISTS条件
// 示例: WhereNotExistsIf(condition, "SELECT 1 FROM users WHERE users.id = posts.user_id")
func (w *Where) WhereNotExistsIf(condition bool, subquery string, args ...interface{}) *Where {
	if condition {
		return w.WhereNotExists(subquery, args...)
	}
	return w
}

// WhereRaw 添加原始SQL条件
// 示例: WhereRaw("DATE_FORMAT(created_at, '%Y-%m-%d') = ?", "2023-01-01")
func (w *Where) WhereRaw(sql string, args ...interface{}) *Where {
	return w.Where(sql, args...)
}

// WhereRawIf 条件性添加原始SQL条件
// 示例: WhereRawIf(condition, "DATE_FORMAT(created_at, '%Y-%m-%d') = ?", "2023-01-01")
func (w *Where) WhereRawIf(condition bool, sql string, args ...interface{}) *Where {
	if condition {
		return w.WhereRaw(sql, args...)
	}
	return w
}

// Group 添加条件组
// 示例: Group(func(w *Where) { w.Where("status = ?", 1).Or("status = ?", 2) })
func (w *Where) Group(fn func(*Where)) *Where {
	if fn == nil {
		return w
	}

	// 创建子条件构建器
	subWhere := NewWhere()
	fn(subWhere)

	// 如果子条件为空，直接返回
	if len(subWhere.wheres) == 0 {
		return w
	}

	// 构建子条件
	var subConditions []string
	for _, condition := range subWhere.wheres {
		subConditions = append(subConditions, condition)
	}

	// 添加到主条件
	groupCondition := fmt.Sprintf("(%s)", strings.Join(subConditions, " AND "))
	w.wheres = append(w.wheres, groupCondition)
	w.values = append(w.values, subWhere.values...)

	return w
}

// GroupIf 条件性添加条件组
// 示例: GroupIf(condition, func(w *Where) { w.Where("status = ?", 1).Or("status = ?", 2) })
func (w *Where) GroupIf(condition bool, fn func(*Where)) *Where {
	if condition {
		return w.Group(fn)
	}
	return w
}

// OrGroup 添加OR条件组
// 示例: OrGroup(func(w *Where) { w.Where("status = ?", 1).Or("status = ?", 2) })
func (w *Where) OrGroup(fn func(*Where)) *Where {
	if fn == nil {
		return w
	}

	// 创建子条件构建器
	subWhere := NewWhere()
	fn(subWhere)

	// 如果子条件为空，直接返回
	if len(subWhere.wheres) == 0 {
		return w
	}

	// 构建子条件
	var subConditions []string
	for _, condition := range subWhere.wheres {
		subConditions = append(subConditions, condition)
	}

	// 添加到主条件
	groupCondition := fmt.Sprintf("(%s)", strings.Join(subConditions, " AND "))

	if len(w.wheres) > 0 {
		lastIndex := len(w.wheres) - 1
		w.wheres[lastIndex] = fmt.Sprintf("(%s) OR %s", w.wheres[lastIndex], groupCondition)
	} else {
		w.wheres = append(w.wheres, groupCondition)
	}

	w.values = append(w.values, subWhere.values...)

	return w
}

// OrGroupIf 条件性添加OR条件组
// 示例: OrGroupIf(condition, func(w *Where) { w.Where("status = ?", 1).Or("status = ?", 2) })
func (w *Where) OrGroupIf(condition bool, fn func(*Where)) *Where {
	if condition {
		return w.OrGroup(fn)
	}
	return w
}

// Clear 清空条件
func (w *Where) Clear() *Where {
	w.wheres = make([]string, 0)
	w.values = make([]interface{}, 0)
	return w
}

// IsEmpty 判断是否为空
func (w *Where) IsEmpty() bool {
	return len(w.wheres) == 0
}

// GetWheres 获取条件语句
func (w *Where) GetWheres() []string {
	return w.wheres
}

// GetValues 获取参数值
func (w *Where) GetValues() []interface{} {
	return w.values
}

// String 获取条件语句
func (w *Where) String() string {
	if len(w.wheres) == 0 {
		return ""
	}
	return strings.Join(w.wheres, " AND ")
}

// Build 构建条件语句
func (w *Where) Build() (string, []interface{}) {
	if len(w.wheres) == 0 {
		return "", nil
	}
	return strings.Join(w.wheres, " AND "), w.values
}

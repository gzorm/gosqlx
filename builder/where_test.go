package builder

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// 测试创建新的条件构建器
func TestNewWhere(t *testing.T) {
	w := NewWhere()
	if w == nil {
		t.Fatal("NewWhere() 返回了 nil")
	}
	if len(w.wheres) != 0 {
		t.Errorf("期望 wheres 长度为 0，实际为 %d", len(w.wheres))
	}
	if len(w.values) != 0 {
		t.Errorf("期望 values 长度为 0，实际为 %d", len(w.values))
	}
}

// 测试基本的 Where 条件
func TestWhere(t *testing.T) {
	w := NewWhere()
	w.Where("id = ?", 1)

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "id = ?" {
		t.Errorf("期望条件为 'id = ?'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 1 {
		t.Errorf("期望 values 长度为 1，实际为 %d", len(w.values))
	}
	if w.values[0] != 1 {
		t.Errorf("期望参数值为 1，实际为 %v", w.values[0])
	}
}

// 测试条件性添加 Where 条件
func TestWhereIf(t *testing.T) {
	// 条件为 true 的情况
	w1 := NewWhere()
	w1.WhereIf(true, "id = ?", 1)

	if len(w1.wheres) != 1 {
		t.Errorf("条件为 true 时，期望 wheres 长度为 1，实际为 %d", len(w1.wheres))
	}

	// 条件为 false 的情况
	w2 := NewWhere()
	w2.WhereIf(false, "id = ?", 1)

	if len(w2.wheres) != 0 {
		t.Errorf("条件为 false 时，期望 wheres 长度为 0，实际为 %d", len(w2.wheres))
	}
}

// 测试 And 条件
func TestAnd(t *testing.T) {
	w := NewWhere()
	w.Where("id = ?", 1).And("name = ?", "张三")

	if len(w.wheres) != 2 {
		t.Errorf("期望 wheres 长度为 2，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "id = ?" || w.wheres[1] != "name = ?" {
		t.Errorf("期望条件为 'id = ?' 和 'name = ?'，实际为 '%s' 和 '%s'", w.wheres[0], w.wheres[1])
	}
	if len(w.values) != 2 {
		t.Errorf("期望 values 长度为 2，实际为 %d", len(w.values))
	}
	if w.values[0] != 1 || w.values[1] != "张三" {
		t.Errorf("期望参数值为 1 和 '张三'，实际为 %v 和 %v", w.values[0], w.values[1])
	}
}

// 测试条件性添加 And 条件
func TestAndIf(t *testing.T) {
	// 条件为 true 的情况
	w1 := NewWhere()
	w1.Where("id = ?", 1).AndIf(true, "name = ?", "张三")

	if len(w1.wheres) != 2 {
		t.Errorf("条件为 true 时，期望 wheres 长度为 2，实际为 %d", len(w1.wheres))
	}

	// 条件为 false 的情况
	w2 := NewWhere()
	w2.Where("id = ?", 1).AndIf(false, "name = ?", "张三")

	if len(w2.wheres) != 1 {
		t.Errorf("条件为 false 时，期望 wheres 长度为 1，实际为 %d", len(w2.wheres))
	}
}

// 测试 Or 条件
func TestOr(t *testing.T) {
	w := NewWhere()
	w.Where("id = ?", 1).Or("name = ?", "张三")

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "(id = ?) OR (name = ?)" {
		t.Errorf("期望条件为 '(id = ?) OR (name = ?)'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 2 {
		t.Errorf("期望 values 长度为 2，实际为 %d", len(w.values))
	}
}

// 测试条件性添加 Or 条件
func TestOrIf(t *testing.T) {
	// 条件为 true 的情况
	w1 := NewWhere()
	w1.Where("id = ?", 1).OrIf(true, "name = ?", "张三")

	if len(w1.wheres) != 1 {
		t.Errorf("条件为 true 时，期望 wheres 长度为 1，实际为 %d", len(w1.wheres))
	}
	if w1.wheres[0] != "(id = ?) OR (name = ?)" {
		t.Errorf("期望条件为 '(id = ?) OR (name = ?)'，实际为 '%s'", w1.wheres[0])
	}

	// 条件为 false 的情况
	w2 := NewWhere()
	w2.Where("id = ?", 1).OrIf(false, "name = ?", "张三")

	if len(w2.wheres) != 1 {
		t.Errorf("条件为 false 时，期望 wheres 长度为 1，实际为 %d", len(w2.wheres))
	}
	if w2.wheres[0] != "id = ?" {
		t.Errorf("期望条件为 'id = ?'，实际为 '%s'", w2.wheres[0])
	}
}

// 测试 WhereIn 条件
func TestWhereIn(t *testing.T) {
	w := NewWhere()
	w.WhereIn("id", []int{1, 2, 3})

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "id IN (?, ?, ?)" {
		t.Errorf("期望条件为 'id IN (?, ?, ?)'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 3 {
		t.Errorf("期望 values 长度为 3，实际为 %d", len(w.values))
	}

	// 测试空切片
	w2 := NewWhere()
	w2.WhereIn("id", []int{})

	if len(w2.wheres) != 0 {
		t.Errorf("空切片时，期望 wheres 长度为 0，实际为 %d", len(w2.wheres))
	}

	// 测试非切片类型
	w3 := NewWhere()
	w3.WhereIn("id", 1)

	if len(w3.wheres) != 0 {
		t.Errorf("非切片类型时，期望 wheres 长度为 0，实际为 %d", len(w3.wheres))
	}
}

// 测试条件性添加 WhereIn 条件
func TestWhereInIf(t *testing.T) {
	// 条件为 true 的情况
	w1 := NewWhere()
	w1.WhereInIf(true, "id", []int{1, 2, 3})

	if len(w1.wheres) != 1 {
		t.Errorf("条件为 true 时，期望 wheres 长度为 1，实际为 %d", len(w1.wheres))
	}

	// 条件为 false 的情况
	w2 := NewWhere()
	w2.WhereInIf(false, "id", []int{1, 2, 3})

	if len(w2.wheres) != 0 {
		t.Errorf("条件为 false 时，期望 wheres 长度为 0，实际为 %d", len(w2.wheres))
	}
}

// 测试 WhereNotIn 条件
func TestWhereNotIn(t *testing.T) {
	w := NewWhere()
	w.WhereNotIn("id", []int{1, 2, 3})

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "id NOT IN (?, ?, ?)" {
		t.Errorf("期望条件为 'id NOT IN (?, ?, ?)'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 3 {
		t.Errorf("期望 values 长度为 3，实际为 %d", len(w.values))
	}
}

// 测试 WhereBetween 条件
func TestWhereBetween(t *testing.T) {
	w := NewWhere()
	w.WhereBetween("age", 18, 30)

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "age BETWEEN ? AND ?" {
		t.Errorf("期望条件为 'age BETWEEN ? AND ?'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 2 {
		t.Errorf("期望 values 长度为 2，实际为 %d", len(w.values))
	}
	if w.values[0] != 18 || w.values[1] != 30 {
		t.Errorf("期望参数值为 18 和 30，实际为 %v 和 %v", w.values[0], w.values[1])
	}
}

// 测试 WhereLike 条件
func TestWhereLike(t *testing.T) {
	w := NewWhere()
	w.WhereLike("name", "%张%")

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "name LIKE ?" {
		t.Errorf("期望条件为 'name LIKE ?'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 1 {
		t.Errorf("期望 values 长度为 1，实际为 %d", len(w.values))
	}
	if w.values[0] != "%张%" {
		t.Errorf("期望参数值为 '%%张%%'，实际为 %v", w.values[0])
	}
}

// 测试 WhereNull 条件
func TestWhereNull(t *testing.T) {
	w := NewWhere()
	w.WhereNull("deleted_at")

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "deleted_at IS NULL" {
		t.Errorf("期望条件为 'deleted_at IS NULL'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 0 {
		t.Errorf("期望 values 长度为 0，实际为 %d", len(w.values))
	}
}

// 测试 WhereNotNull 条件
func TestWhereNotNull(t *testing.T) {
	w := NewWhere()
	w.WhereNotNull("deleted_at")

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "deleted_at IS NOT NULL" {
		t.Errorf("期望条件为 'deleted_at IS NOT NULL'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 0 {
		t.Errorf("期望 values 长度为 0，实际为 %d", len(w.values))
	}
}

// 测试 WhereExists 条件
func TestWhereExists(t *testing.T) {
	w := NewWhere()
	w.WhereExists("SELECT 1 FROM users WHERE users.id = posts.user_id")

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "EXISTS (SELECT 1 FROM users WHERE users.id = posts.user_id)" {
		t.Errorf("期望条件为 'EXISTS (SELECT 1 FROM users WHERE users.id = posts.user_id)'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 0 {
		t.Errorf("期望 values 长度为 0，实际为 %d", len(w.values))
	}
}

// 测试 WhereNotExists 条件
func TestWhereNotExists(t *testing.T) {
	w := NewWhere()
	w.WhereNotExists("SELECT 1 FROM users WHERE users.id = posts.user_id")

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "NOT EXISTS (SELECT 1 FROM users WHERE users.id = posts.user_id)" {
		t.Errorf("期望条件为 'NOT EXISTS (SELECT 1 FROM users WHERE users.id = posts.user_id)'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 0 {
		t.Errorf("期望 values 长度为 0，实际为 %d", len(w.values))
	}
}

// 测试 WhereRaw 条件
func TestWhereRaw(t *testing.T) {
	w := NewWhere()
	w.WhereRaw("DATE_FORMAT(created_at, '%Y-%m-%d') = ?", "2023-01-01")

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "DATE_FORMAT(created_at, '%Y-%m-%d') = ?" {
		t.Errorf("期望条件为 'DATE_FORMAT(created_at, '%%Y-%%m-%%d') = ?'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 1 {
		t.Errorf("期望 values 长度为 1，实际为 %d", len(w.values))
	}
	if w.values[0] != "2023-01-01" {
		t.Errorf("期望参数值为 '2023-01-01'，实际为 %v", w.values[0])
	}
}

// 测试条件组
func TestGroup(t *testing.T) {
	w := NewWhere()
	w.Where("id > ?", 10).Group(func(w *Where) {
		w.Where("status = ?", 1).Or("status = ?", 2)
	})

	if len(w.wheres) != 2 {
		t.Errorf("期望 wheres 长度为 2，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "id > ?" {
		t.Errorf("期望第一个条件为 'id > ?'，实际为 '%s'", w.wheres[0])
	}
	if w.wheres[1] != "((status = ?) OR (status = ?))" {
		t.Errorf("期望第二个条件为 '((status = ?) OR (status = ?))'，实际为 '%s'", w.wheres[1])
	}
	if len(w.values) != 3 {
		t.Errorf("期望 values 长度为 3，实际为 %d", len(w.values))
	}
}

// 测试 OR 条件组
func TestOrGroup(t *testing.T) {
	w := NewWhere()
	w.Where("id > ?", 10).OrGroup(func(w *Where) {
		w.Where("status = ?", 1).And("type = ?", "A")
	})

	if len(w.wheres) != 1 {
		t.Errorf("期望 wheres 长度为 1，实际为 %d", len(w.wheres))
	}
	if w.wheres[0] != "(id > ?) OR ((status = ?) AND (type = ?))" {
		t.Errorf("期望条件为 '(id > ?) OR ((status = ?) AND (type = ?))'，实际为 '%s'", w.wheres[0])
	}
	if len(w.values) != 3 {
		t.Errorf("期望 values 长度为 3，实际为 %d", len(w.values))
	}
}

// 测试清空条件
func TestClear(t *testing.T) {
	w := NewWhere()
	w.Where("id = ?", 1).And("name = ?", "张三")

	if len(w.wheres) != 2 {
		t.Errorf("清空前，期望 wheres 长度为 2，实际为 %d", len(w.wheres))
	}

	w.Clear()

	if len(w.wheres) != 0 {
		t.Errorf("清空后，期望 wheres 长度为 0，实际为 %d", len(w.wheres))
	}
	if len(w.values) != 0 {
		t.Errorf("清空后，期望 values 长度为 0，实际为 %d", len(w.values))
	}
}

// 测试判断是否为空
func TestIsEmpty(t *testing.T) {
	w1 := NewWhere()
	if !w1.IsEmpty() {
		t.Errorf("新建的 Where 应该为空")
	}

	w2 := NewWhere()
	w2.Where("id = ?", 1)
	if w2.IsEmpty() {
		t.Errorf("添加条件后的 Where 不应该为空")
	}
}

// 测试获取条件语句
func TestString(t *testing.T) {
	w := NewWhere()
	w.Where("id = ?", 1).And("name = ?", "张三")

	expected := "id = ? AND name = ?"
	if w.String() != expected {
		t.Errorf("期望条件字符串为 '%s'，实际为 '%s'", expected, w.String())
	}
}

// 测试构建条件语句
func TestBuild(t *testing.T) {
	w := NewWhere()
	w.Where("id = ?", 1).And("name = ?", "张三")

	expectedWhere := "id = ? AND name = ?"
	expectedValues := []interface{}{1, "张三"}

	where, values := w.Build()

	if where != expectedWhere {
		t.Errorf("期望条件字符串为 '%s'，实际为 '%s'", expectedWhere, where)
	}

	if !reflect.DeepEqual(values, expectedValues) {
		t.Errorf("期望参数值为 %v，实际为 %v", expectedValues, values)
	}
}

// 测试复杂查询条件组合
func TestComplexWhere(t *testing.T) {
	w := NewWhere()
	w.Where("status = ?", 1).
		AndIf(true, "type = ?", "A").
		AndIf(false, "ignored = ?", "value").
		WhereIn("category_id", []int{1, 2, 3}).
		WhereNotIn("tag_id", []int{4, 5}).
		WhereBetween("created_at", "2023-01-01", "2023-12-31").
		WhereLike("name", "%测试%").
		WhereNull("deleted_at").
		Group(func(w *Where) {
			w.Where("price > ?", 100).Or("is_featured = ?", true)
		})

	where, values := w.Build()

	// 检查条件数量
	conditions := strings.Split(where, " AND ")
	if len(conditions) != 7 {
		t.Errorf("期望条件数量为 7，实际为 %d", len(conditions))
	}

	// 检查参数数量
	if len(values) != 9 {
		t.Errorf("期望参数数量为 9，实际为 %d", len(values))
	}

	t.Logf("复杂条件: %s", where)
	t.Logf("参数值: %v", values)
}

// 模拟数据库查询的测试
type User struct {
	ID       int
	Username string
	Email    string
	Age      int
}

// 模拟单条记录查询
func TestMockSingleQuery(t *testing.T) {
	// 构建查询条件
	w := NewWhere()
	w.Where("id = ?", 1)

	where, values := w.Build()

	// 模拟 SQL 查询
	sql := fmt.Sprintf("SELECT * FROM users WHERE %s", where)

	// 验证 SQL 和参数
	expectedSQL := "SELECT * FROM users WHERE id = ?"
	if sql != expectedSQL {
		t.Errorf("期望 SQL 为 '%s'，实际为 '%s'", expectedSQL, sql)
	}

	if len(values) != 1 || values[0] != 1 {
		t.Errorf("期望参数为 [1]，实际为 %v", values)
	}

	// 在实际应用中，这里会执行数据库查询
	t.Logf("模拟执行 SQL: %s, 参数: %v", sql, values)
}

// 模拟多条记录查询
func TestMockMultiQuery(t *testing.T) {
	// 构建查询条件
	w := NewWhere()
	w.Where("age > ?", 18).
		WhereIn("status", []int{1, 2}).
		WhereLike("username", "%test%")

	where, values := w.Build()

	// 模拟 SQL 查询
	sql := fmt.Sprintf("SELECT * FROM users WHERE %s", where)

	// 验证 SQL 和参数
	expectedSQL := "SELECT * FROM users WHERE age > ? AND status IN (?, ?) AND username LIKE ?"
	if sql != expectedSQL {
		t.Errorf("期望 SQL 为 '%s'，实际为 '%s'", expectedSQL, sql)
	}

	if len(values) != 4 {
		t.Errorf("期望参数数量为 4，实际为 %d", len(values))
	}

	// 在实际应用中，这里会执行数据库查询
	t.Logf("模拟执行 SQL: %s, 参数: %v", sql, values)
}

// 模拟分页查询
func TestMockPagedQuery(t *testing.T) {
	// 构建查询条件
	w := NewWhere()
	w.Where("active = ?", true).
		WhereNotNull("email")

	where, values := w.Build()

	// 分页参数
	pageIndex := 2
	pageSize := 10
	offset := (pageIndex - 1) * pageSize

	// 模拟分页 SQL 查询
	sql := fmt.Sprintf("SELECT * FROM users WHERE %s LIMIT %d OFFSET %d", where, pageSize, offset)

	// 验证 SQL 和参数
	expectedSQL := "SELECT * FROM users WHERE active = ? AND email IS NOT NULL LIMIT 10 OFFSET 10"
	if sql != expectedSQL {
		t.Errorf("期望 SQL 为 '%s'，实际为 '%s'", expectedSQL, sql)
	}

	if len(values) != 1 || values[0] != true {
		t.Errorf("期望参数为 [true]，实际为 %v", values)
	}

	// 在实际应用中，这里会执行数据库查询
	t.Logf("模拟执行分页 SQL: %s, 参数: %v", sql, values)
}

// 模拟关联查询
func TestMockJoinQuery(t *testing.T) {
	// 构建查询条件
	w := NewWhere()
	w.Where("users.active = ?", true).
		And("orders.status = ?", "completed")

	where, values := w.Build()

	// 模拟关联查询 SQL
	sql := fmt.Sprintf(`
		SELECT users.*, orders.id as order_id, orders.amount
		FROM users
		JOIN orders ON users.id = orders.user_id
		WHERE %s
	`, where)

	// 在实际应用中，这里会执行数据库查询
	t.Logf("模拟执行关联查询 SQL: %s, 参数: %v", sql, values)
}

// 模拟 COUNT 查询
func TestMockCountQuery(t *testing.T) {
	// 构建查询条件
	w := NewWhere()
	w.Where("age > ?", 18).
		WhereIn("status", []int{1, 2})

	where, values := w.Build()

	// 模拟 COUNT SQL 查询
	sql := fmt.Sprintf("SELECT COUNT(*) FROM users WHERE %s", where)

	// 验证 SQL 和参数
	expectedSQL := "SELECT COUNT(*) FROM users WHERE age > ? AND status IN (?, ?)"
	if sql != expectedSQL {
		t.Errorf("期望 SQL 为 '%s'，实际为 '%s'", expectedSQL, sql)
	}

	// 在实际应用中，这里会执行数据库查询
	t.Logf("模拟执行 COUNT SQL: %s, 参数: %v", sql, values)
}

// 模拟子查询
func TestMockSubQuery(t *testing.T) {
	// 构建子查询条件
	subWhere := NewWhere()
	subWhere.Where("amount > ?", 1000)
	subWhereStr, subValues := subWhere.Build()

	// 构建主查询条件
	w := NewWhere()
	w.Where("active = ?", true).
		WhereExists(fmt.Sprintf("SELECT 1 FROM orders WHERE orders.user_id = users.id AND %s", subWhereStr), subValues...)

	where, values := w.Build()

	// 模拟 SQL 查询
	sql := fmt.Sprintf("SELECT * FROM users WHERE %s", where)

	// 在实际应用中，这里会执行数据库查询
	t.Logf("模拟执行子查询 SQL: %s, 参数: %v", sql, values)
}

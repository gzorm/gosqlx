package builder

import (
	"fmt"
	"strings"
)

// Order 排序构建器
type Order struct {
	orderBy string // 排序语句
}

// NewOrder 创建新的排序构建器
func NewOrder() *Order {
	return &Order{}
}

// OrderBy 设置排序字段和方向
// 示例: OrderBy("id DESC")
func (o *Order) OrderBy(order string) *Order {
	if order != "" {
		o.orderBy = order
	}
	return o
}

// OrderByAsc 按字段升序排序
// 示例: OrderByAsc("id")
func (o *Order) OrderByAsc(field string) *Order {
	if field != "" {
		o.orderBy = fmt.Sprintf("%s ASC", field)
	}
	return o
}

// OrderByDesc 按字段降序排序
// 示例: OrderByDesc("id")
func (o *Order) OrderByDesc(field string) *Order {
	if field != "" {
		o.orderBy = fmt.Sprintf("%s DESC", field)
	}
	return o
}

// OrderByMulti 设置多个排序字段
// 示例: OrderByMulti([]string{"id DESC", "name ASC"})
func (o *Order) OrderByMulti(orders []string) *Order {
	if len(orders) > 0 {
		o.orderBy = strings.Join(orders, ", ")
	}
	return o
}

// OrderByField 按字段值的特定顺序排序
// 示例: OrderByField("status", []interface{}{1, 2, 3})
func (o *Order) OrderByField(field string, values []interface{}) *Order {
	if field != "" && len(values) > 0 {
		var cases []string
		for i, v := range values {
			switch v.(type) {
			case string:
				cases = append(cases, fmt.Sprintf("WHEN '%v' THEN %d", v, i))
			default:
				cases = append(cases, fmt.Sprintf("WHEN %v THEN %d", v, i))
			}
		}
		o.orderBy = fmt.Sprintf("CASE %s %s END", field, strings.Join(cases, " "))
	}
	return o
}

// OrderByRandom 随机排序
func (o *Order) OrderByRandom() *Order {
	o.orderBy = "RAND()"
	return o
}

// MySqlOrderByRandom 添加 MySQL 特定的随机排序
func (o *Order) MySqlOrderByRandom() *Order {
	return o.OrderBy("RAND()")
}

// PostgreSQLOrderByRandom 添加 PostgreSQL 特定的随机排序
func (o *Order) PostgreSQLOrderByRandom() *Order {
	return o.OrderBy("RANDOM()")
}

// OrderByIf 条件排序
// 示例: OrderByIf(true, "id DESC")
func (o *Order) OrderByIf(condition bool, order string) *Order {
	if condition && order != "" {
		o.orderBy = order
	}
	return o
}

// OrderByAscIf 条件升序排序
// 示例: OrderByAscIf(true, "id")
func (o *Order) OrderByAscIf(condition bool, field string) *Order {
	if condition && field != "" {
		o.orderBy = fmt.Sprintf("%s ASC", field)
	}
	return o
}

// OrderByDescIf 条件降序排序
// 示例: OrderByDescIf(true, "id")
func (o *Order) OrderByDescIf(condition bool, field string) *Order {
	if condition && field != "" {
		o.orderBy = fmt.Sprintf("%s DESC", field)
	}
	return o
}

// AppendOrderBy 追加排序条件
// 示例: AppendOrderBy("name ASC")
func (o *Order) AppendOrderBy(order string) *Order {
	if order != "" {
		if o.orderBy != "" {
			o.orderBy = fmt.Sprintf("%s, %s", o.orderBy, order)
		} else {
			o.orderBy = order
		}
	}
	return o
}

// AppendOrderByIf 条件追加排序
// 示例: AppendOrderByIf(true, "name ASC")
func (o *Order) AppendOrderByIf(condition bool, order string) *Order {
	if condition && order != "" {
		if o.orderBy != "" {
			o.orderBy = fmt.Sprintf("%s, %s", o.orderBy, order)
		} else {
			o.orderBy = order
		}
	}
	return o
}

// Clear 清空排序条件
func (o *Order) Clear() *Order {
	o.orderBy = ""
	return o
}

// IsEmpty 判断是否为空
func (o *Order) IsEmpty() bool {
	return o.orderBy == ""
}

// String 获取排序语句
func (o *Order) String() string {
	if o.orderBy == "" {
		return ""
	}
	return fmt.Sprintf("ORDER BY %s", o.orderBy)
}

// GetOrderBy 获取排序语句（兼容旧版本）
func (o *Order) GetOrderBy(prefix bool) string {
	if o.orderBy == "" {
		return ""
	}

	if prefix {
		return fmt.Sprintf("ORDER BY %s", o.orderBy)
	}
	return o.orderBy
}

// Build 构建排序语句
func (o *Order) Build() string {
	return o.String()
}

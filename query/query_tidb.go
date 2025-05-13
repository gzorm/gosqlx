package query

import (
	"fmt"
	"strings"
)

// // TiDBQuery TiDB特定查询构建器
// type TiDBQuery struct {
// 	*Query
// }

// // NewTiDBQuery 创建TiDB查询构建器
// func NewTiDBQuery(db interface{}) *TiDBQuery {
// 	return &TiDBQuery{
// 		Query: NewQuery(db),
// 	}
// }

// Partition 指定分区
func (q *Query) Partition(partitions ...string) *Query {
	if len(partitions) > 0 {
		partitionClause := fmt.Sprintf("PARTITION(%s)", strings.Join(partitions, ", "))
		q.joins = append(q.joins, partitionClause)
	}
	return q
}

// IndexHint 添加索引提示
// 示例: IndexHint("USE", "idx_name")
func (q *Query) IndexHint(hintType string, indexes ...string) *Query {
	if len(indexes) > 0 {
		hint := fmt.Sprintf("%s INDEX(%s)", hintType, strings.Join(indexes, ", "))
		q.joins = append(q.joins, hint)
	}
	return q
}

// ForceIndex 强制使用索引
func (q *Query) ForceIndex(indexes ...string) *Query {
	return q.IndexHint("FORCE", indexes...)
}

// UseIndex 使用索引
func (q *Query) UseIndex(indexes ...string) *Query {
	return q.IndexHint("USE", indexes...)
}

// IgnoreIndex 忽略索引
func (q *Query) IgnoreIndex(indexes ...string) *Query {
	return q.IndexHint("IGNORE", indexes...)
}

// Hint 添加优化器提示
// 示例: Hint("/*+ TIDB_SMJ(t1, t2) */")
func (q *Query) Hint(hint string) *Query {
	if hint != "" {
		q.joins = append(q.joins, hint)
	}
	return q
}

// TimeZone 设置时区
func (q *Query) TimeZone(tz string) *Query {
	if tz != "" {
		q.joins = append(q.joins, fmt.Sprintf("/*+ TIME_ZONE=%s */", tz))
	}
	return q
}

// ReadFromReplica 从副本读取
func (q *Query) ReadFromReplica() *Query {
	q.joins = append(q.joins, "/*+ READ_FROM_REPLICA() */")
	return q
}

// ReadFromStorage 从指定存储引擎读取
// 示例: ReadFromStorage("tiFlash", "t1", "t2")
func (q *Query) ReadFromStorage(engine string, tables ...string) *Query {
	if engine != "" && len(tables) > 0 {
		hint := fmt.Sprintf("/*+ READ_FROM_STORAGE(%s, %s) */",
			engine, strings.Join(tables, ", "))
		q.joins = append(q.joins, hint)
	}
	return q
}

// MergeJoin 使用归并连接
func (q *Query) MergeJoin(tables ...string) *Query {
	if len(tables) > 0 {
		hint := fmt.Sprintf("/*+ TIDB_SMJ(%s) */", strings.Join(tables, ", "))
		q.joins = append(q.joins, hint)
	}
	return q
}

// HashJoin 使用哈希连接
func (q *Query) HashJoin(tables ...string) *Query {
	if len(tables) > 0 {
		hint := fmt.Sprintf("/*+ TIDB_HJ(%s) */", strings.Join(tables, ", "))
		q.joins = append(q.joins, hint)
	}
	return q
}

// IndexJoin 使用索引连接
func (q *Query) IndexJoin(tables ...string) *Query {
	if len(tables) > 0 {
		hint := fmt.Sprintf("/*+ TIDB_INLJ(%s) */", strings.Join(tables, ", "))
		q.joins = append(q.joins, hint)
	}
	return q
}

// StreamAgg 使用流式聚合
func (q *Query) StreamAgg() *Query {
	q.joins = append(q.joins, "/*+ STREAM_AGG() */")
	return q
}

// HashAgg 使用哈希聚合
func (q *Query) HashAgg() *Query {
	q.joins = append(q.joins, "/*+ HASH_AGG() */")
	return q
}

//// BuildSelect 重写构建SELECT语句，添加TiDB特定功能
//func (q *Query) BuildSelect() (string, []interface{}) {
//	return q.BuildSelect()
//}

package query

import (
	"fmt"
	"strings"
)

/*
// 创建OceanBase查询构建器
obQuery := query.NewOceanBaseQuery(db)

// 使用OceanBase特有的功能
var users []User
err := obQuery.Table("users").
    ParallelDegree(4).           // 设置并行度
    TableGroup("tg_users").      // 设置表组
    Partition("p_202301").       // 设置分区
    Where("status = ?", "active").
    OrderByDesc("created_at").
    Limit(10).
    Get(&users)
*/

// OceanBaseQuery OceanBase数据库查询构建器
type OceanBaseQuery struct {
	*Query
	// OceanBase特有的属性
	parallelDegree int    // 并行度
	tableGroup     string // 表组
	partition      string // 分区
}

// NewOceanBaseQuery 创建OceanBase查询构建器
func NewOceanBaseQuery(db interface{}) *OceanBaseQuery {
	return &OceanBaseQuery{
		Query: NewQuery(db),
	}
}

// ParallelDegree 设置并行度
func (q *OceanBaseQuery) ParallelDegree(degree int) *OceanBaseQuery {
	q.parallelDegree = degree
	return q
}

// TableGroup 设置表组
func (q *OceanBaseQuery) TableGroup(group string) *OceanBaseQuery {
	q.tableGroup = group
	return q
}

// Partition 设置分区
func (q *OceanBaseQuery) Partition(partition string) *OceanBaseQuery {
	q.partition = partition
	return q
}

// BuildSelect 重写构建SELECT语句方法
func (q *OceanBaseQuery) BuildSelect() (string, []interface{}) {
	sqlStr, args := q.Query.BuildSelect()

	// 添加OceanBase特有的语法
	var additions []string

	// 添加并行度
	if q.parallelDegree > 0 {
		additions = append(additions, fmt.Sprintf("/*+ PARALLEL(%d) */", q.parallelDegree))
	}

	// 添加表组
	if q.tableGroup != "" {
		// 在FROM子句后添加表组
		sqlStr = strings.Replace(sqlStr,
			fmt.Sprintf("FROM %s", q.table),
			fmt.Sprintf("FROM %s@%s", q.table, q.tableGroup),
			1)
	}

	// 添加分区
	if q.partition != "" {
		// 在FROM子句后添加分区
		sqlStr = strings.Replace(sqlStr,
			fmt.Sprintf("FROM %s", q.table),
			fmt.Sprintf("FROM %s PARTITION(%s)", q.table, q.partition),
			1)
	}

	// 添加OceanBase特有的提示
	if len(additions) > 0 {
		hint := strings.Join(additions, " ")
		// 在SELECT后添加提示
		sqlStr = strings.Replace(sqlStr,
			"SELECT ",
			fmt.Sprintf("SELECT %s ", hint),
			1)
	}

	return sqlStr, args
}

// Get 获取多条记录
func (q *OceanBaseQuery) Get(out interface{}) error {
	sqlStr, args := q.BuildSelect()
	return q.execQuery(sqlStr, args, out)
}

// First 获取单条记录
func (q *OceanBaseQuery) First(out interface{}) error {
	q.limit = 1
	sqlStr, args := q.BuildSelect()
	return q.execQuery(sqlStr, args, out)
}

// 其他方法可以根据OceanBase的特性进行扩展

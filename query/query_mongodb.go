package query

import (
	"fmt"
	"reflect"
	"strings"
)

// MongoDB 特有的聚合管道操作

// PipelineStage MongoDB 聚合管道阶段
type PipelineStage struct {
	Name   string
	Value  interface{}
	Fields []string
}

// Lookup 实现 MongoDB 的 $lookup 操作（类似 SQL 的 JOIN）
// from: 要关联的集合
// localField: 主集合中的字段
// foreignField: 关联集合中的字段
// as: 结果数组字段名
func (q *Query) Lookup(from, localField, foreignField, as string) *Query {
	stage := fmt.Sprintf(`{"$lookup": {"from": "%s", "localField": "%s", "foreignField": "%s", "as": "%s"}}`,
		from, localField, foreignField, as)
	q.joins = append(q.joins, stage)
	return q
}

// Unwind 实现 MongoDB 的 $unwind 操作（展开数组）
// path: 要展开的数组字段路径
func (q *Query) Unwind(path string) *Query {
	stage := fmt.Sprintf(`{"$unwind": "$%s"}`, path)
	q.joins = append(q.joins, stage)
	return q
}

// Match 实现 MongoDB 的 $match 操作（筛选文档）
// field: 字段名
// value: 匹配值
func (q *Query) Match(field string, value interface{}) *Query {
	var matchStage string

	// 处理不同类型的值
	switch v := value.(type) {
	case []interface{}:
		if len(v) > 0 && reflect.TypeOf(v[0]).Kind() == reflect.Map {
			// 处理复杂条件
			matchStage = fmt.Sprintf(`{"$match": {"%s": %v}}`, field, v[0])
		} else {
			// 处理数组条件
			matchStage = fmt.Sprintf(`{"$match": {"%s": {"$in": %v}}}`, field, v)
		}
	case map[string]interface{}:
		// 处理对象条件
		matchStage = fmt.Sprintf(`{"$match": {"%s": %v}}`, field, v)
	default:
		// 处理简单条件
		if reflect.TypeOf(value).Kind() == reflect.String {
			matchStage = fmt.Sprintf(`{"$match": {"%s": "%v"}}`, field, value)
		} else {
			matchStage = fmt.Sprintf(`{"$match": {"%s": %v}}`, field, value)
		}
	}

	q.joins = append(q.joins, matchStage)
	return q
}

// AddField 实现 MongoDB 的 $addFields 操作（添加字段）
// field: 新字段名
// value: 字段值表达式
func (q *Query) AddField(field string, value interface{}) *Query {
	stage := fmt.Sprintf(`{"$addFields": {"%s": %v}}`, field, value)
	q.joins = append(q.joins, stage)
	return q
}

// Project 实现 MongoDB 的 $project 操作（投影）
// fields: 字段映射
func (q *Query) Project(fields map[string]interface{}) *Query {
	// 构建投影对象
	projectObj := "{"
	i := 0
	for k, v := range fields {
		if i > 0 {
			projectObj += ", "
		}

		switch val := v.(type) {
		case bool:
			projectObj += fmt.Sprintf(`"%s": %v`, k, val)
		case int:
			projectObj += fmt.Sprintf(`"%s": %v`, k, val)
		case string:
			if val[0] == '$' {
				projectObj += fmt.Sprintf(`"%s": "%s"`, k, val)
			} else {
				projectObj += fmt.Sprintf(`"%s": "%s"`, k, val)
			}
		default:
			projectObj += fmt.Sprintf(`"%s": %v`, k, val)
		}
		i++
	}
	projectObj += "}"

	stage := fmt.Sprintf(`{"$project": %s}`, projectObj)
	q.joins = append(q.joins, stage)
	return q
}

// Sort 实现 MongoDB 的 $sort 操作（排序）
// fields: 排序字段映射，1为升序，-1为降序
func (q *Query) Sort(fields map[string]int) *Query {
	// 构建排序对象
	sortObj := "{"
	i := 0
	for k, v := range fields {
		if i > 0 {
			sortObj += ", "
		}
		sortObj += fmt.Sprintf(`"%s": %d`, k, v)
		i++
	}
	sortObj += "}"

	stage := fmt.Sprintf(`{"$sort": %s}`, sortObj)
	q.joins = append(q.joins, stage)
	return q
}

// GroupBy 实现 MongoDB 的 $group 操作（分组）
// id: 分组键，可以是字段名或表达式
func (q *Query) GroupBy(id string) *Query {
	q.group = fmt.Sprintf(`{"_id": "$%s"}`, id)
	return q
}

// GroupCount 添加分组计数字段
func (q *Query) GroupCount(field string) *Query {
	if q.group == "" {
		q.group = `{"_id": null}`
	}

	// 从 JSON 字符串中移除结尾的 }
	q.group = q.group[:len(q.group)-1]
	q.group += fmt.Sprintf(`, "%s": {"$sum": 1}}`, field)
	return q
}

// GroupSum 添加分组求和字段
func (q *Query) GroupSum(sourceField, targetField string) *Query {
	if q.group == "" {
		q.group = `{"_id": null}`
	}

	// 从 JSON 字符串中移除结尾的 }
	q.group = q.group[:len(q.group)-1]
	q.group += fmt.Sprintf(`, "%s": {"$sum": "$%s"}}`, targetField, sourceField)
	return q
}

// GroupAvg 添加分组平均值字段
func (q *Query) GroupAvg(sourceField, targetField string) *Query {
	if q.group == "" {
		q.group = `{"_id": null}`
	}

	// 从 JSON 字符串中移除结尾的 }
	q.group = q.group[:len(q.group)-1]
	q.group += fmt.Sprintf(`, "%s": {"$avg": "$%s"}}`, targetField, sourceField)
	return q
}

// GroupMax 添加分组最大值字段
func (q *Query) GroupMax(sourceField, targetField string) *Query {
	if q.group == "" {
		q.group = `{"_id": null}`
	}

	// 从 JSON 字符串中移除结尾的 }
	q.group = q.group[:len(q.group)-1]
	q.group += fmt.Sprintf(`, "%s": {"$max": "$%s"}}`, targetField, sourceField)
	return q
}

// GroupMin 添加分组最小值字段
func (q *Query) GroupMin(sourceField, targetField string) *Query {
	if q.group == "" {
		q.group = `{"_id": null}`
	}

	// 从 JSON 字符串中移除结尾的 }
	q.group = q.group[:len(q.group)-1]
	q.group += fmt.Sprintf(`, "%s": {"$min": "$%s"}}`, targetField, sourceField)
	return q
}

// GroupPush 添加分组数组字段
func (q *Query) GroupPush(sourceField, targetField string) *Query {
	if q.group == "" {
		q.group = `{"_id": null}`
	}

	// 从 JSON 字符串中移除结尾的 }
	q.group = q.group[:len(q.group)-1]
	q.group += fmt.Sprintf(`, "%s": {"$push": "$%s"}}`, targetField, sourceField)
	return q
}

// Skip 实现 MongoDB 的 $skip 操作（跳过文档）
func (q *Query) Skip(n int) *Query {
	q.offset = n
	return q
}

// Facet 实现 MongoDB 的 $facet 操作（多管道处理）
func (q *Query) Facet(facets map[string][]string) *Query {
	// 构建 facet 对象
	facetObj := "{"
	i := 0
	for k, v := range facets {
		if i > 0 {
			facetObj += ", "
		}
		facetObj += fmt.Sprintf(`"%s": [%s]`, k, join(v, ", "))
		i++
	}
	facetObj += "}"

	stage := fmt.Sprintf(`{"$facet": %s}`, facetObj)
	q.joins = append(q.joins, stage)
	return q
}

// 辅助函数：连接字符串数组
func join(arr []string, sep string) string {
	if len(arr) == 0 {
		return ""
	}

	result := arr[0]
	for i := 1; i < len(arr); i++ {
		result += sep + arr[i]
	}
	return result
}

// BuildAggregate 构建 MongoDB 聚合管道查询
func (q *Query) BuildAggregate() (string, []interface{}) {
	var pipeline []string
	var args []interface{}

	// 添加 $match 阶段（如果有 where 条件）
	if q.where != nil {
		whereStr, whereArgs := q.where.Build()
		if whereStr != "" {
			matchStage := fmt.Sprintf(`{"$match": {%s}}`, whereStr)
			pipeline = append(pipeline, matchStage)
			args = append(args, whereArgs...)
		}
	}

	// 添加 $lookup 等阶段（存储在 joins 中）
	pipeline = append(pipeline, q.joins...)

	// 添加 $group 阶段（如果有分组）
	if q.group != "" {
		groupStage := fmt.Sprintf(`{"$group": %s}`, q.group)
		pipeline = append(pipeline, groupStage)
	}

	// 添加 $sort 阶段（如果有排序）
	if q.order != nil {
		orderStr := q.order.String()
		if orderStr != "" {
			// 将 SQL 排序转换为 MongoDB 排序
			// 例如："ORDER BY field1 ASC, field2 DESC" -> {"$sort": {"field1": 1, "field2": -1}}
			orderParts := strings.Split(strings.Replace(orderStr, "ORDER BY ", "", 1), ", ")
			sortObj := "{"
			for i, part := range orderParts {
				if i > 0 {
					sortObj += ", "
				}

				parts := strings.Split(part, " ")
				field := parts[0]
				direction := 1 // 默认升序
				if len(parts) > 1 && strings.ToUpper(parts[1]) == "DESC" {
					direction = -1
				}

				sortObj += fmt.Sprintf(`"%s": %d`, field, direction)
			}
			sortObj += "}"

			sortStage := fmt.Sprintf(`{"$sort": %s}`, sortObj)
			pipeline = append(pipeline, sortStage)
		}
	}

	// 添加 $skip 阶段（如果有偏移量）
	if q.offset > 0 {
		skipStage := fmt.Sprintf(`{"$skip": %d}`, q.offset)
		pipeline = append(pipeline, skipStage)
	}

	// 添加 $limit 阶段（如果有限制数）
	if q.limit > 0 {
		limitStage := fmt.Sprintf(`{"$limit": %d}`, q.limit)
		pipeline = append(pipeline, limitStage)
	}

	// 添加 $project 阶段（如果有指定列）
	if len(q.columns) > 0 && q.columns[0] != "*" {
		projectObj := "{"
		for i, column := range q.columns {
			if i > 0 {
				projectObj += ", "
			}
			projectObj += fmt.Sprintf(`"%s": 1`, column)
		}
		projectObj += "}"

		projectStage := fmt.Sprintf(`{"$project": %s}`, projectObj)
		pipeline = append(pipeline, projectStage)
	}

	// 构建最终的聚合管道查询
	query := fmt.Sprintf(`db.%s.aggregate([%s])`, q.table, join(pipeline, ", "))

	return query, args
}

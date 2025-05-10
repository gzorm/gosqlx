package model

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Model 基础模型结构
type Model struct {
	ID        uint       `gorm:"primary_key" json:"id"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `sql:"index" json:"deleted_at,omitempty"`
}

// TableName 表名接口
type TableName interface {
	TableName() string
}

// BeforeCreate 创建前钩子接口
type BeforeCreate interface {
	BeforeCreate() error
}

// AfterCreate 创建后钩子接口
type AfterCreate interface {
	AfterCreate() error
}

// BeforeUpdate 更新前钩子接口
type BeforeUpdate interface {
	BeforeUpdate() error
}

// AfterUpdate 更新后钩子接口
type AfterUpdate interface {
	AfterUpdate() error
}

// BeforeDelete 删除前钩子接口
type BeforeDelete interface {
	BeforeDelete() error
}

// AfterDelete 删除后钩子接口
type AfterDelete interface {
	AfterDelete() error
}

// BeforeSave 保存前钩子接口
type BeforeSave interface {
	BeforeSave() error
}

// AfterSave 保存后钩子接口
type AfterSave interface {
	AfterSave() error
}

// BeforeFind 查询前钩子接口
type BeforeFind interface {
	BeforeFind() error
}

// AfterFind 查询后钩子接口
type AfterFind interface {
	AfterFind() error
}

// JSON 自定义JSON类型
type JSON json.RawMessage

// Value 实现driver.Valuer接口
func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return string(j), nil
}

// Scan 实现sql.Scanner接口
func (j *JSON) Scan(value interface{}) error {
	if value == nil {
		*j = JSON("null")
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("无法将 %T 转换为 JSON", value)
	}

	*j = JSON(bytes)
	return nil
}

// MarshalJSON 实现json.Marshaler接口
func (j JSON) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte("null"), nil
	}
	return j, nil
}

// UnmarshalJSON 实现json.Unmarshaler接口
func (j *JSON) UnmarshalJSON(data []byte) error {
	*j = JSON(data)
	return nil
}

// JSONMap JSON映射类型
type JSONMap map[string]interface{}

// Value 实现driver.Valuer接口
func (m JSONMap) Value() (driver.Value, error) {
	if m == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return string(bytes), nil
}

// Scan 实现sql.Scanner接口
func (m *JSONMap) Scan(value interface{}) error {
	if value == nil {
		*m = make(JSONMap)
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("无法将 %T 转换为 JSONMap", value)
	}

	return json.Unmarshal(bytes, m)
}

// JSONArray JSON数组类型
type JSONArray []interface{}

// Value 实现driver.Valuer接口
func (a JSONArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	bytes, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}

	return string(bytes), nil
}

// Scan 实现sql.Scanner接口
func (a *JSONArray) Scan(value interface{}) error {
	if value == nil {
		*a = make(JSONArray, 0)
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("无法将 %T 转换为 JSONArray", value)
	}

	return json.Unmarshal(bytes, a)
}

// SoftDelete 软删除接口
type SoftDelete interface {
	SoftDelete() error
}

// Pagination 分页结构
type Pagination struct {
	Total    int64       `json:"total"`     // 总记录数
	Page     int         `json:"page"`      // 当前页码
	PageSize int         `json:"page_size"` // 每页记录数
	Data     interface{} `json:"data"`      // 数据
}

// NewPagination 创建分页结构
func NewPagination(data interface{}, total int64, page, pageSize int) *Pagination {
	return &Pagination{
		Total:    total,
		Page:     page,
		PageSize: pageSize,
		Data:     data,
	}
}

// GetOffset 获取偏移量
func (p *Pagination) GetOffset() int {
	if p.Page <= 0 {
		p.Page = 1
	}

	if p.PageSize <= 0 {
		p.PageSize = 10
	}

	return (p.Page - 1) * p.PageSize
}

// GetLimit 获取限制数
func (p *Pagination) GetLimit() int {
	if p.PageSize <= 0 {
		p.PageSize = 10
	}

	return p.PageSize
}

// GetTotalPages 获取总页数
func (p *Pagination) GetTotalPages() int {
	if p.PageSize <= 0 {
		p.PageSize = 10
	}

	totalPages := int(p.Total) / p.PageSize
	if int(p.Total)%p.PageSize > 0 {
		totalPages++
	}

	return totalPages
}

// HasPrevious 是否有上一页
func (p *Pagination) HasPrevious() bool {
	return p.Page > 1
}

// HasNext 是否有下一页
func (p *Pagination) HasNext() bool {
	return p.Page < p.GetTotalPages()
}

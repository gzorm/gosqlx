package gosqlx

import (
	"time"
)

// DatabaseType 表示支持的数据库类型
type DatabaseType string

// 支持的数据库类型常量
const (
	MySQL      DatabaseType = "mysql"
	PostgreSQL DatabaseType = "postgres"
	Oracle     DatabaseType = "oracle"
	SQLServer  DatabaseType = "sqlserver"
	SQLite     DatabaseType = "sqlite3"
	MongoDB    DatabaseType = "mongodb"
	TiDB       DatabaseType = "tidb"
)

// Config 数据库配置结构
type Config struct {
	// 数据库类型
	Type DatabaseType `json:"type"`

	// 连接信息
	Driver string `json:"driver"`
	Source string `json:"source"`

	// 连接池配置
	MaxIdle     int           `json:"maxIdle"`
	MaxOpen     int           `json:"maxOpen"`
	MaxLifetime time.Duration `json:"maxLifetime"`

	// 调试模式
	Debug bool `json:"debug"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Type:        MySQL,
		Driver:      "mysql",
		MaxIdle:     10,
		MaxOpen:     100,
		MaxLifetime: time.Hour,
		Debug:       false,
	}
}

// ConfigMap 是一个配置映射，用于存储多个数据库配置
// 格式为: map[环境][数据库名]配置
type ConfigMap map[string]map[string]*Config

// ConfigProvider 定义了配置提供者接口
type ConfigProvider interface {
	// GetConfig 获取指定环境和数据库名的配置
	GetConfig(env, dbName string) (*Config, bool)

	// GetAllConfigs 获取所有配置
	GetAllConfigs() ConfigMap
}

// DefaultConfigProvider 默认配置提供者实现
type DefaultConfigProvider struct {
	configs ConfigMap
}

// NewConfigProvider 创建新的配置提供者
func NewConfigProvider(configs ConfigMap) ConfigProvider {
	return &DefaultConfigProvider{
		configs: configs,
	}
}

// GetConfig 获取指定环境和数据库名的配置
func (p *DefaultConfigProvider) GetConfig(env, dbName string) (*Config, bool) {
	if envConfigs, ok := p.configs[env]; ok {
		if config, ok := envConfigs[dbName]; ok {
			return config, true
		}
	}
	return nil, false
}

// GetAllConfigs 获取所有配置
func (p *DefaultConfigProvider) GetAllConfigs() ConfigMap {
	return p.configs
}

// ConfigLoader 配置加载器接口
type ConfigLoader interface {
	// Load 加载配置
	Load() (ConfigMap, error)
}

// FileConfigLoader 从文件加载配置
type FileConfigLoader struct {
	filePath string
}

// NewFileConfigLoader 创建文件配置加载器
func NewFileConfigLoader(filePath string) ConfigLoader {
	return &FileConfigLoader{
		filePath: filePath,
	}
}

// Load 从文件加载配置
func (l *FileConfigLoader) Load() (ConfigMap, error) {
	// 这里可以实现从文件加载配置的逻辑
	// 暂时返回一个空的配置映射
	return ConfigMap{}, nil
}

// 配置管理器
type ConfigManager struct {
	provider ConfigProvider
}

// NewConfigManager 创建配置管理器
func NewConfigManager(provider ConfigProvider) *ConfigManager {
	return &ConfigManager{
		provider: provider,
	}
}

// GetConfig 获取指定环境和数据库名的配置
func (m *ConfigManager) GetConfig(env, dbName string) (*Config, bool) {
	return m.provider.GetConfig(env, dbName)
}

// GetAllConfigs 获取所有配置
func (m *ConfigManager) GetAllConfigs() ConfigMap {
	return m.provider.GetAllConfigs()
}

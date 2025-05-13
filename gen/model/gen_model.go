package model

import "fmt"

// Generator 表结构生成器接口
type Generator interface {
	Generate() error
	Close() error
}

// GenerateModels 生成模型
func GenerateModels(config *Config) error {
	var generator Generator
	var err error

	switch config.DBType {
	case "mysql":
		generator, err = NewMySQLGenerator(config)
	case "postgres":
		generator, err = NewPostgresGenerator(config)
	case "sqlserver":
		generator, err = NewSQLServerGenerator(config)
	case "oracle":
		generator, err = NewOracleGenerator(config)
	case "tidb":
		generator, err = NewTiDBGenerator(config)
	case "sqlite":
		generator, err = NewSQLiteGenerator(config)
	case "mongodb":
		generator, err = NewMongoDBGenerator(config)
	case "mariadb":
		generator, err = NewMariaDBGenerator(config)
	case "clickhouse":
		generator, err = NewClickHouseGenerator(config)
	default:
		return fmt.Errorf("不支持的数据库类型: %s", config.DBType)
	}

	if err != nil {
		return err
	}
	defer generator.Close()

	return generator.Generate()
}

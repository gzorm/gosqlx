package doc

import (
	"database/sql"
	"fmt"
	"github.com/unidoc/unioffice/color"
	"github.com/unidoc/unioffice/measurement"
	"time"

	"github.com/gzorm/gosqlx"
	"github.com/unidoc/unioffice/document"
	"github.com/unidoc/unioffice/schema/soo/wml"
	"github.com/xuri/excelize/v2"
)

// Config 文档生成配置
type Config struct {
	// 数据库配置
	DBType gosqlx.DatabaseType // 数据库类型
	Source string              // 数据库连接字符串
	DBName string              // 数据库名称

	// 输出配置
	OutputPath string // 输出文件路径
	Title      string // 文档标题
	Author     string // 文档作者
	Company    string // 公司名称
}

// TableDoc 表文档信息
type TableDoc struct {
	TableName    string      // 表名
	TableComment string      // 表注释
	Columns      []ColumnDoc // 列信息
	PrimaryKeys  []string    // 主键
	Indexes      []IndexDoc  // 索引
}

// ColumnDoc 列文档信息
type ColumnDoc struct {
	ColumnName    string // 列名
	DataType      string // 数据类型
	IsNullable    string // 是否可为空
	ColumnDefault string // 默认值
	ColumnComment string // 列注释
	ColumnKey     string // 键类型
	Extra         string // 额外信息
}

// IndexDoc 索引文档信息
type IndexDoc struct {
	IndexName string   // 索引名称
	Columns   []string // 索引列
	IndexType string   // 索引类型
	IsUnique  bool     // 是否唯一
}

// GenerateDBDoc 生成数据库文档
func GenerateDBDoc(config *Config) error {
	// 创建数据库连接
	db, err := createDBConnection(config)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %v", err)
	}
	defer db.Close()

	// 获取所有表信息
	tables, err := getAllTables(db, config.DBName)
	if err != nil {
		return fmt.Errorf("获取表信息失败: %v", err)
	}

	// 生成Word文档
	err = generateWordDoc(tables, config)
	if err != nil {
		return fmt.Errorf("生成Word文档失败: %v", err)
	}

	return nil
}

// createDBConnection 创建数据库连接
func createDBConnection(config *Config) (*sql.DB, error) {
	// 创建数据库配置
	dbConfig := &gosqlx.Config{
		Type:        config.DBType,
		Source:      config.Source,
		MaxIdle:     5,
		MaxOpen:     10,
		MaxLifetime: time.Hour,
	}

	// 创建数据库上下文
	ctx := &gosqlx.Context{
		Context: nil,
		Nick:    "db_doc_generator",
		Mode:    "ro", // 只读模式
		DBType:  config.DBType,
		Timeout: time.Second * 30,
	}

	// 创建数据库实例
	database, err := gosqlx.NewDatabase(ctx, dbConfig)
	if err != nil {
		return nil, err
	}

	// 获取原生SQL连接
	return database.GetDBContext().SqlDB, nil
}

// getAllTables 获取所有表信息
func getAllTables(db *sql.DB, dbName string) ([]TableDoc, error) {
	// 获取所有表名
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}

	// 获取每个表的详细信息
	var tables []TableDoc
	for _, tableName := range tableNames {
		table, err := getTableInfo(db, dbName, tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// getTableInfo 获取表信息
func getTableInfo(db *sql.DB, dbName, tableName string) (TableDoc, error) {
	// 获取表注释
	var tableComment string
	err := db.QueryRow(`
		SELECT table_comment 
		FROM information_schema.tables 
		WHERE table_schema = ? AND table_name = ?
	`, dbName, tableName).Scan(&tableComment)
	if err != nil {
		return TableDoc{}, err
	}

	// 获取列信息
	columns, err := getColumnInfo(db, dbName, tableName)
	if err != nil {
		return TableDoc{}, err
	}

	// 获取主键
	primaryKeys, err := getPrimaryKeys(db, dbName, tableName)
	if err != nil {
		return TableDoc{}, err
	}

	// 获取索引
	indexes, err := getIndexes(db, dbName, tableName)
	if err != nil {
		return TableDoc{}, err
	}

	return TableDoc{
		TableName:    tableName,
		TableComment: tableComment,
		Columns:      columns,
		PrimaryKeys:  primaryKeys,
		Indexes:      indexes,
	}, nil
}

// getColumnInfo 获取列信息
func getColumnInfo(db *sql.DB, dbName, tableName string) ([]ColumnDoc, error) {
	query := `
		SELECT 
			column_name, data_type, 
			is_nullable, column_default, 
			column_comment, column_key, extra
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`

	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnDoc
	for rows.Next() {
		var col ColumnDoc
		var defaultValue sql.NullString
		if err := rows.Scan(
			&col.ColumnName, &col.DataType,
			&col.IsNullable, &defaultValue,
			&col.ColumnComment, &col.ColumnKey, &col.Extra,
		); err != nil {
			return nil, err
		}

		if defaultValue.Valid {
			col.ColumnDefault = defaultValue.String
		} else {
			col.ColumnDefault = "NULL"
		}

		columns = append(columns, col)
	}

	return columns, nil
}

// getPrimaryKeys 获取主键
func getPrimaryKeys(db *sql.DB, dbName, tableName string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position
	`

	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	return primaryKeys, nil
}

// getIndexes 获取索引
func getIndexes(db *sql.DB, dbName, tableName string) ([]IndexDoc, error) {
	query := `
		SELECT 
			index_name, column_name, 
			non_unique, index_type
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ?
		ORDER BY index_name, seq_in_index
	`

	rows, err := db.Query(query, dbName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 临时存储索引信息
	indexMap := make(map[string]*IndexDoc)
	for rows.Next() {
		var indexName, columnName, indexType string
		var nonUnique int
		if err := rows.Scan(&indexName, &columnName, &nonUnique, &indexType); err != nil {
			return nil, err
		}

		// 如果是新索引，创建索引记录
		if _, exists := indexMap[indexName]; !exists {
			indexMap[indexName] = &IndexDoc{
				IndexName: indexName,
				Columns:   []string{},
				IndexType: indexType,
				IsUnique:  nonUnique == 0,
			}
		}

		// 添加列到索引
		indexMap[indexName].Columns = append(indexMap[indexName].Columns, columnName)
	}

	// 转换为切片
	var indexes []IndexDoc
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// generateWordDoc 生成Word文档
func generateWordDoc(tables []TableDoc, config *Config) error {
	// 创建新的Word文档
	doc := document.New()

	// 添加标题
	title := doc.AddParagraph()
	title.Properties().SetAlignment(wml.ST_JcCenter)
	titleRun := title.AddRun()
	titleRun.Properties().SetBold(true)
	titleRun.Properties().SetSize(24)
	titleRun.AddText(config.Title)

	// 添加文档信息
	info := doc.AddParagraph()
	info.Properties().SetAlignment(wml.ST_JcCenter)
	infoRun := info.AddRun()
	infoRun.Properties().SetSize(12)
	infoRun.AddText(fmt.Sprintf("作者: %s   公司: %s   生成时间: %s",
		config.Author, config.Company, time.Now().Format("2006-01-02 15:04:05")))

	// 添加分隔线
	doc.AddParagraph()

	// 添加数据库信息
	dbInfo := doc.AddParagraph()
	dbInfoRun := dbInfo.AddRun()
	dbInfoRun.Properties().SetBold(true)
	dbInfoRun.Properties().SetSize(14)
	dbInfoRun.AddText(fmt.Sprintf("数据库名称: %s", config.DBName))

	doc.AddParagraph()

	// 添加表信息
	for _, table := range tables {
		// 表标题
		tableTitle := doc.AddParagraph()
		tableTitle.Properties().SetAlignment(wml.ST_JcLeft)
		tableTitleRun := tableTitle.AddRun()
		tableTitleRun.Properties().SetBold(true)
		tableTitleRun.Properties().SetSize(14)
		tableTitleRun.AddText(fmt.Sprintf("表名: %s", table.TableName))

		// 表注释
		if table.TableComment != "" {
			tableComment := doc.AddParagraph()
			tableCommentRun := tableComment.AddRun()
			tableCommentRun.Properties().SetItalic(true)
			tableCommentRun.AddText(fmt.Sprintf("注释: %s", table.TableComment))
		}

		// 添加列信息表格
		columnTable := doc.AddTable()
		// 设置表格边框
		border := columnTable.Properties().Borders()
		border.SetAll(wml.ST_BorderSingle, color.Auto, 1*measurement.Point)

		//// 设置表格外观
		//tblLook := columnTable.Properties().TableLook()
		//tblLook.FirstRow = wml.ST_OnOffTrue
		//tblLook.FirstColumn = wml.ST_OnOffTrue
		//tblLook.LastRow = wml.ST_OnOffFalse
		//tblLook.LastColumn = wml.ST_OnOffFalse
		//tblLook.NoHBand = wml.ST_OnOffFalse
		//tblLook.NoVBand = wml.ST_OnOffTrue

		// 添加表头
		headerRow := columnTable.AddRow()
		headers := []string{"列名", "数据类型", "允许空值", "默认值", "键类型", "额外信息", "注释"}
		for _, header := range headers {
			cell := headerRow.AddCell()
			cellPara := cell.AddParagraph()
			cellRun := cellPara.AddRun()
			cellRun.Properties().SetBold(true)
			cellRun.AddText(header)
		}

		// 添加列数据
		for _, col := range table.Columns {
			row := columnTable.AddRow()

			// 列名
			nameCell := row.AddCell()
			nameCell.AddParagraph().AddRun().AddText(col.ColumnName)

			// 数据类型
			typeCell := row.AddCell()
			typeCell.AddParagraph().AddRun().AddText(col.DataType)

			// 允许空值
			nullableCell := row.AddCell()
			nullableCell.AddParagraph().AddRun().AddText(col.IsNullable)

			// 默认值
			defaultCell := row.AddCell()
			defaultCell.AddParagraph().AddRun().AddText(col.ColumnDefault)

			// 键类型
			keyCell := row.AddCell()
			keyCell.AddParagraph().AddRun().AddText(col.ColumnKey)

			// 额外信息
			extraCell := row.AddCell()
			extraCell.AddParagraph().AddRun().AddText(col.Extra)

			// 注释
			commentCell := row.AddCell()
			commentCell.AddParagraph().AddRun().AddText(col.ColumnComment)
		}

		// 添加主键信息
		if len(table.PrimaryKeys) > 0 {
			pkInfo := doc.AddParagraph()
			pkRun := pkInfo.AddRun()
			pkRun.Properties().SetBold(true)
			pkRun.AddText("主键: ")
			pkRun = pkInfo.AddRun()
			pkRun.AddText(fmt.Sprintf("%s", table.PrimaryKeys))
		}

		// 添加索引信息
		if len(table.Indexes) > 0 {
			idxTitle := doc.AddParagraph()
			idxTitleRun := idxTitle.AddRun()
			idxTitleRun.Properties().SetBold(true)
			idxTitleRun.AddText("索引:")

			for _, idx := range table.Indexes {
				if idx.IndexName == "PRIMARY" {
					continue // 主键索引已经显示过了
				}

				idxInfo := doc.AddParagraph()
				// 设置左缩进 720 twips（0.5英寸）
				para := idxInfo.X()
				if para.PPr == nil {
					para.PPr = wml.NewCT_PPr()
				}
				if para.PPr.Ind == nil {
					para.PPr.Ind = wml.NewCT_Ind()
				}
				val := int64(720)

				left := wml.ST_SignedTwipsMeasure{
					Int64:               &val,
					ST_UniversalMeasure: nil,
				}
				para.PPr.Ind.LeftAttr = &left

				idxRun := idxInfo.AddRun()
				idxRun.Properties().SetBold(true)
				idxRun.AddText(fmt.Sprintf("%s: ", idx.IndexName))

				idxRun = idxInfo.AddRun()
				idxType := "普通索引"
				if idx.IsUnique {
					idxType = "唯一索引"
				}
				idxRun.AddText(fmt.Sprintf("类型=%s, 列=%s", idxType, idx.Columns))
				//idxInfo.Properties().SetIndentation(wml.NewCT_Ind())
				//idxInfo.Properties().Indentation().SetLeft(720) // 缩进0.5英寸

				idxRun = idxInfo.AddRun()
				idxRun.Properties().SetBold(true)
				idxRun.AddText(fmt.Sprintf("%s: ", idx.IndexName))

				idxRun = idxInfo.AddRun()
				idxType = "普通索引"
				if idx.IsUnique {
					idxType = "唯一索引"
				}
				idxRun.AddText(fmt.Sprintf("类型=%s, 列=%s", idxType, idx.Columns))
			}
		}

		// 添加分隔段落
		doc.AddParagraph()
	}

	// 保存文档
	return doc.SaveToFile(config.OutputPath)
}

// GenerateExcelDoc 生成Excel格式的数据库文档
func GenerateExcelDoc(config *Config) error {
	// 创建数据库连接
	db, err := createDBConnection(config)
	if err != nil {
		return fmt.Errorf("连接数据库失败: %v", err)
	}
	defer db.Close()

	// 获取所有表信息
	tables, err := getAllTables(db, config.DBName)
	if err != nil {
		return fmt.Errorf("获取表信息失败: %v", err)
	}

	// 创建Excel文件
	f := excelize.NewFile()

	// 创建概览工作表
	f.SetSheetName("Sheet1", "概览")
	f.SetCellValue("概览", "A1", "表名")
	f.SetCellValue("概览", "B1", "注释")
	f.SetCellValue("概览", "C1", "列数")

	// 设置表头样式
	headerStyle, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#DDEBF7"}, Pattern: 1},
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
		},
	})
	f.SetCellStyle("概览", "A1", "C1", headerStyle)

	// 填充概览数据
	for i, table := range tables {
		row := i + 2
		f.SetCellValue("概览", fmt.Sprintf("A%d", row), table.TableName)
		f.SetCellValue("概览", fmt.Sprintf("B%d", row), table.TableComment)
		f.SetCellValue("概览", fmt.Sprintf("C%d", row), len(table.Columns))

		// 为每个表创建工作表
		f.NewSheet(table.TableName)

		// 设置表头
		headers := []string{"列名", "数据类型", "允许空值", "默认值", "键类型", "额外信息", "注释"}
		for j, header := range headers {
			col := string(rune('A' + j))
			f.SetCellValue(table.TableName, fmt.Sprintf("%s1", col), header)
		}
		f.SetCellStyle(table.TableName, "A1", string(rune('A'+len(headers)-1))+"1", headerStyle)

		// 填充列数据
		for j, col := range table.Columns {
			row := j + 2
			f.SetCellValue(table.TableName, fmt.Sprintf("A%d", row), col.ColumnName)
			f.SetCellValue(table.TableName, fmt.Sprintf("B%d", row), col.DataType)
			f.SetCellValue(table.TableName, fmt.Sprintf("C%d", row), col.IsNullable)
			f.SetCellValue(table.TableName, fmt.Sprintf("D%d", row), col.ColumnDefault)
			f.SetCellValue(table.TableName, fmt.Sprintf("E%d", row), col.ColumnKey)
			f.SetCellValue(table.TableName, fmt.Sprintf("F%d", row), col.Extra)
			f.SetCellValue(table.TableName, fmt.Sprintf("G%d", row), col.ColumnComment)
		}

		// 添加索引信息
		indexRow := len(table.Columns) + 3
		f.SetCellValue(table.TableName, fmt.Sprintf("A%d", indexRow), "索引信息")
		f.SetCellStyle(table.TableName, fmt.Sprintf("A%d", indexRow), fmt.Sprintf("A%d", indexRow), headerStyle)

		for j, idx := range table.Indexes {
			if idx.IndexName == "PRIMARY" {
				continue
			}
			row := indexRow + j + 1
			idxType := "普通索引"
			if idx.IsUnique {
				idxType = "唯一索引"
			}
			f.SetCellValue(table.TableName, fmt.Sprintf("A%d", row), idx.IndexName)
			f.SetCellValue(table.TableName, fmt.Sprintf("B%d", row), idxType)
			f.SetCellValue(table.TableName, fmt.Sprintf("C%d", row), fmt.Sprintf("%v", idx.Columns))
		}

		// 自动调整列宽
		for j := 0; j < len(headers); j++ {
			col := string(rune('A' + j))
			f.SetColWidth(table.TableName, col, col, 15)
		}
	}

	// 保存Excel文件
	return f.SaveAs(config.OutputPath)
}

package postgres

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"gorm.io/gorm"
)

var dataTypes = map[string]string{
	"double precision": "FLOAT",
}

func generateCreateTableStatement(db *gorm.DB, tableName string) (string, error) {
	var columns []map[string]interface{}
	tx := db.Raw(`
		SELECT
			column_name,
			data_type,
			is_nullable,
			column_default,
			character_maximum_length,
			numeric_precision,
			numeric_scale
		FROM
			information_schema.columns
		WHERE
			table_name = ? AND table_schema = 'public'
	`, tableName).Scan(&columns)
	if tx.Error != nil {
		return "", fmt.Errorf("failed to fetch table columns: %w", tx.Error)
	}

	var foreignKeys []map[string]interface{}
	tx = db.Raw(`
		SELECT
			tc.constraint_name,
			tc.table_name,
			kcu.column_name,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column
		FROM
			information_schema.table_constraints tc
		JOIN
			information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		JOIN
			information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
		WHERE
			tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = ? AND tc.table_schema = 'public'
	`, tableName).Scan(&foreignKeys)
	if tx.Error != nil {
		return "", fmt.Errorf("failed to fetch foreign keys: %w", tx.Error)
	}

	createTableStatement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", tableName)

	for _, column := range columns {
		columnName := column["column_name"].(string)
		dataType := column["data_type"].(string)
		// if v, ok := dataTypes[dataType]; ok {
		// 	dataType = v
		// }
		isNullable := column["is_nullable"].(string) == "f"
		columnDefault := column["column_default"]
		characterMaxLength := column["character_maximum_length"]
		numericPrecision := column["numeric_precision"]
		numericScale := column["numeric_scale"]
		if columnName == "user" {
			columnName = "\"user\""
		}
		columnDefinition := fmt.Sprintf("%s %s", columnName, dataType)
		if characterMaxLength != nil {
			columnDefinition += fmt.Sprintf("(%v)", characterMaxLength)
		}
		if numericPrecision != nil && dataType != "double precision" {
			if numericScale != nil {
				columnDefinition += fmt.Sprintf("(%v,%s)", numericPrecision.(int32), numericScale.(string))
			} else {
				columnDefinition += fmt.Sprintf("(%v)", numericPrecision.(int32))
			}
		}
		if isNullable {
			columnDefinition += " NOT NULL"
		}

		// Add default value only if it's not NULL
		if columnDefault != nil && columnDefault != "NULL" {
			columnDefinition += fmt.Sprintf(" DEFAULT %s", columnDefault)
		}

		createTableStatement += fmt.Sprintf("  %s,\n", columnDefinition)
	}

	// Add foreign keys
	for _, fk := range foreignKeys {
		constraintName := fk["constraint_name"].(string)
		column := fk["column_name"].(string)
		referencedTable := fk["referenced_table"].(string)
		referencedColumn := fk["referenced_column"].(string)
		createTableStatement += fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s),\n", constraintName, column, referencedTable, referencedColumn)
	}

	// Remove trailing comma and add closing parenthesis
	createTableStatement = strings.TrimSuffix(createTableStatement, ",\n") + ");\n"

	return createTableStatement, nil
}

// func main() {
// 	// Connect to your PostgreSQL database
// 	db, err := gorm.Open(gorm.Open("postgres"), "your_database_connection_string")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer db.Close()

// 	// Example usage:
// 	tableName := "your_table_name"
// 	createTableStatement, err := generateCreateTableStatement(db, tableName)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println(createTableStatement)
// }

// UpdatePostgresDSN updates the database name in a PostgreSQL DSN
func UpdatePostgresDSN(dsn string, newDatabase string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("error parsing DSN: %w", err)
	}

	if u.Scheme != "postgresql" {
		return "", errors.New("invalid DSN scheme, expected postgres")
	}

	u.Path = "/" + newDatabase

	return u.String(), nil
}

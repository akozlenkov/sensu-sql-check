package main

import "C"
import (
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jawher/mow.cli"
	_ "github.com/prestodb/presto-go-client/presto"
	"gopkg.in/Knetic/govaluate.v2"
	"os"
	"strings"
)

type Result struct {
	output string
	retCode	int
}

var stopwords = []string{
	"ALTER",
	"CREATE",
	"DELETE",
	"DROP",
	"EXEC",
	"TRUNCATE",
	"UPDATE",
	"INSERT",
}

func main() {
	app := cli.App("go-sql-check", "")
	app.Spec = "[--driver] [--connection-url] --query --expression"

	var (
		driver = app.StringOpt("d driver", "clickhouse", "One of clickhouse, presto")
		connectionUrl = app.StringOpt("c connection-url", "tcp://127.0.0.1:9000", "DSN for connection")
		query = app.StringOpt("q query", "", "")
		expr = app.StringOpt("e expression", "", "")
	)

	app.Action = func() {
		for _, w := range stopwords {
			if strings.Contains(strings.ToLower(*query), strings.ToLower(w)) {
				fatalOnError(fmt.Errorf("Only select allowed"))
			}
		}

		connect, err := sql.Open(*driver, *connectionUrl)
		fatalOnError(err)
		fatalOnError(connect.Ping())

		rows, err := connect.Query(*query)
		fatalOnError(err)

		defer rows.Close()

		f, err := getFirst(rows)
		fatalOnError(err)

		functions := map[string]govaluate.ExpressionFunction {
			"info": func(arguments ...interface{}) (i interface{}, err error) {
				return Result{
					output:  fmt.Sprint(arguments...),
					retCode: 0,
				}, nil
			},
			"warn": func(arguments ...interface{}) (i interface{}, err error) {
				return Result{
					output:  fmt.Sprint(arguments...),
					retCode: 1,
				}, nil
			},
			"error": func(arguments ...interface{}) (i interface{}, err error) {
				return Result{
					output:  fmt.Sprint(arguments...),
					retCode: 2,
				}, nil
			},
		}

		expression, err := govaluate.NewEvaluableExpressionWithFunctions(*expr, functions)
		fatalOnError(err)

		result, err := expression.Evaluate(f)
		fatalOnError(err)

		switch result.(type) {
		case Result:
			r := result.(Result)
			fmt.Println(r.output)
			cli.Exit(r.retCode)
		default:
			cli.Exit(0)
		}
	}
	app.Run(os.Args)

}

func fatalOnError(err error)  {
	if err != nil {
		fmt.Println(err)
		cli.Exit(2)
	}
}

func getFirst(rows *sql.Rows) (map[string]interface{}, error) {
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))

		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		types, _ := rows.ColumnTypes()

		m := make(map[string]interface{})
		for i, colName := range cols {
			t := types[i].ScanType()
			switch t.String() {
			default:
				m[colName] = *columnPointers[i].(*interface{})
			}
		}
		return m, nil
	}
	return nil, nil
}

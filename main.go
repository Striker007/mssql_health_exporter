package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type Config map[string]interface{}

var db *sql.DB

func recordMetrics(config Config) float64 {

	start := time.Now()
	elapsed := float64(-1)

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
		config["server"], config["user"], config["password"], config["port"], config["database"])
	// TODO use timeout
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		fmt.Println("No connection")
		log.Fatal("Error creating connection pool: ", err.Error())
		return elapsed
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		fmt.Println("No Ping")
		log.Fatal(err.Error())
		return elapsed
	}

	tsql := fmt.Sprintf("SELECT TOP 1 name FROM sys.databases")
	rows, err := db.Query(tsql)
	if err != nil {
		fmt.Println("No query")
		log.Fatal(err.Error())
		return elapsed
	}
	defer rows.Close()

	for rows.Next() {
		// symulate reading
		var name string
		err = rows.Scan(&id)
		if err != nil {
			fmt.Println("No query rows Scan result")
			log.Fatal(err.Error())
			return elapsed
		}
	}

	elapsed = float64(time.Since(start))
	return elapsed

}

func main() {

	arg := os.Args[1]

	var config Config
	config, err := ReadConfig(arg)
	if err != nil {
		fmt.Println("No config")
		fmt.Println(err)
	}
	// TODO parse connect string or use it
	config["server"] = "127.0.0.1"
	config["database"] = "test"

	result := recordMetrics(config)

	err = writeMetricsToPrometheusFile("/var/lib/node_exporter/textfile_collector/mssql_query.prom", result)
	if err != nil {
		fmt.Println("Write issue")
		fmt.Println(err)
	}

}

func writeMetricsToPrometheusFile(filename string, result float64) error {

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	r := fmt.Sprintf("%f", result)
	file.WriteString("mssql_response_time " + r + "\n")
	return nil
}

func ReadConfig(filename string) (map[string]interface{}, error) {
	config := Config{
		"user":     "admin",
		"password": "abc123",
		"server":   "",
		"port":     1433,
	}
	if len(filename) == 0 {
		return config, nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')

		if equal := strings.Index(line, "="); equal >= 0 {
			if key := strings.TrimSpace(line[:equal]); len(key) > 0 {
				value := ""
				if len(line) > equal {
					value = strings.TrimSpace(line[equal+1:])
				}
				config[key] = value
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return config, nil
}

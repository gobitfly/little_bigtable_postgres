/*
little_bigtable launches the sqlite3 backed Bigtable emulator on the given address.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/gobitfly/little_bigtable_postgres/bttest"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	maxMsgSize = 256 * 1024 * 1024 // 256 MiB
	version    = "0.1.0"
)

func main() {
	host := flag.String("host", "localhost", "the address to bind to on the local machine")
	port := flag.Int("port", 9000, "the port number to bind to on the local machine")
	dbUsername := flag.String("db-username", "", "db user name")
	dbPassword := flag.String("db-password", "", "db user password")
	dbHost := flag.String("db-host", "localhost", "db host")
	dbPort := flag.String("db-port", "5432", "db port")
	dbName := flag.String("db-name", "bigtable", "db name")
	showVersion := flag.Bool("version", false, "show version")

	ctx := context.Background()
	grpc.EnableTracing = false
	flag.Parse()

	if *showVersion {
		fmt.Printf("little_bigtable v%s (built w/%s)", version, runtime.Version())
		os.Exit(0)
	}

	dbConnString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", *dbUsername, *dbPassword, *dbHost, *dbPort, *dbName)
	db, err := sqlx.Open("pgx", dbConnString)
	if err != nil {
		logrus.Fatalf("failed creating postgres connection %v", err)
	}
	db.SetMaxOpenConns(1)

	err = bttest.CreateTables(ctx, db)
	if err != nil {
		logrus.Fatalf("%v", err)
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	}
	srv, err := bttest.NewServer(fmt.Sprintf("%s:%d", *host, *port), db, opts...)
	if err != nil {
		logrus.Fatalf("failed to start emulator: %v", err)
	}

	logrus.Printf("\"little\" Bigtable emulator running. DB:%s Connect with environment variable BIGTABLE_EMULATOR_HOST=%q", dbConnString, srv.Addr)
	select {}
}

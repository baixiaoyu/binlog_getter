package main

import (
	"binlog_getter/replication"
	"context"

	"flag"

	. "github.com/go-mysql-org/go-mysql/mysql"
)

func main() {

	var start_file string
	var end_file string

	flag.StringVar(&start_file, "s", "", "开始binlog")
	flag.StringVar(&end_file, "e", "", "结束binlog")

	//解析命令行参数
	flag.Parse()

	replication.Start_binlog_file = start_file
	replication.End_binlog_file = end_file

	println(start_file, end_file)
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     20996,
		User:     "msandbox",
		Password: "msandbox",
		// ReadTimeout: 10 * time.Second,
	}
	syncer := replication.NewBinlogSyncer(cfg)

	// Start sync with specified binlog file and position

	streamer, _ := syncer.StartSync(Position{replication.Start_binlog_file, 0})
	for {
		_, err := streamer.GetEvent(context.Background())
		if err != nil {
			break
		}
	}
}

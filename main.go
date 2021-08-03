package main

import (
	"context"
	"flag"

	. "github.com/go-mysql-org/go-mysql/mysql"

	"binlog_getter/replication"
)

// const (
// 	COM_SLEEP byte = iota
// 	COM_QUIT
// 	COM_INIT_DB
// 	COM_QUERY
// 	COM_FIELD_LIST
// 	COM_CREATE_DB
// 	COM_DROP_DB
// 	COM_REFRESH
// 	COM_SHUTDOWN
// 	COM_STATISTICS
// 	COM_PROCESS_INFO
// 	COM_CONNECT
// 	COM_PROCESS_KILL
// 	COM_DEBUG
// 	COM_PING
// 	COM_TIME
// 	COM_DELAYED_INSERT
// 	COM_CHANGE_USER
// 	COM_BINLOG_DUMP
// 	COM_TABLE_DUMP
// 	COM_CONNECT_OUT
// 	COM_REGISTER_SLAVE
// 	COM_STMT_PREPARE
// 	COM_STMT_EXECUTE
// 	COM_STMT_SEND_LONG_DATA
// 	COM_STMT_CLOSE
// 	COM_STMT_RESET
// 	COM_SET_OPTION
// 	COM_STMT_FETCH
// 	COM_DAEMON
// 	COM_BINLOG_DUMP_GTID
// 	COM_RESET_CONNECTION
// )

// type BinlogSyncerConfig struct {
// 	// ServerID is the unique ID in cluster.
// 	ServerID uint32

// 	// Host is for MySQL server host.
// 	Host string
// 	// Port is for MySQL server port.
// 	Port uint16
// 	// User is for MySQL user.
// 	User string
// 	// Password is for MySQL password.
// 	Password   string
// 	Flavor     string
// 	binlogfile string
// 	binlogpos  uint32
// 	// master heartbeat period
// 	HeartbeatPeriod time.Duration

// 	// read timeout
// 	ReadTimeout time.Duration

// 	// maximum number of attempts to re-establish a broken connection, zero or negative number means infinite retry.
// 	// this configuration will not work if DisableRetrySync is true
// 	MaxReconnectAttempts int

// 	// DumpCommandFlag is used to send binglog dump command. Default 0, aka BINLOG_DUMP_NEVER_STOP.
// 	// For MySQL, BINLOG_DUMP_NEVER_STOP and BINLOG_DUMP_NON_BLOCK are available.
// 	// https://dev.mysql.com/doc/internals/en/com-binlog-dump.html#binlog-dump-non-block
// 	// For MariaDB, BINLOG_DUMP_NEVER_STOP, BINLOG_DUMP_NON_BLOCK and BINLOG_SEND_ANNOTATE_ROWS_EVENT are available.
// 	// https://mariadb.com/kb/en/library/com_binlog_dump/
// 	// https://mariadb.com/kb/en/library/annotate_rows_event/
// 	DumpCommandFlag uint16

// 	//Option function is used to set outside of BinlogSyncerConfig， between mysql connection and COM_REGISTER_SLAVE
// 	//For MariaDB: slave_gtid_ignore_duplicates、skip_replication、slave_until_gtid
// 	Option func(*client.Conn) error
// }
// type FakeSlave struct {
// 	//m sync.RWMutex

// 	cfg BinlogSyncerConfig
// 	c   *client.Conn
// 	dns string
// }

// func newFakeSlave(cfg BinlogSyncerConfig) *FakeSlave {
// 	pass := cfg.Password
// 	cfg.Password = ""
// 	log.Infof("create Fakeslave with config %v", cfg)
// 	cfg.Password = pass
// 	b := new(FakeSlave)
// 	b.cfg = cfg
// 	return b

// }

// func (b *FakeSlave) registerSlave() error {
// 	if b.c != nil {
// 		b.c.Close()
// 	}

// 	var err error
// 	b.c, err = b.newConnection()
// 	if err != nil {
// 		return errors.Trace(err)
// 	}

// 	if b.cfg.Option != nil {
// 		if err = b.cfg.Option(b.c); err != nil {
// 			return errors.Trace(err)
// 		}
// 	}

// 	if len(b.cfg.Charset) != 0 {
// 		if err = b.c.SetCharset(b.cfg.Charset); err != nil {
// 			return errors.Trace(err)
// 		}
// 	}

// 	//set read timeout
// 	if b.cfg.ReadTimeout > 0 {
// 		_ = b.c.SetReadDeadline(time.Now().Add(b.cfg.ReadTimeout))
// 	}

// 	if b.cfg.RecvBufferSize > 0 {
// 		if tcp, ok := b.c.Conn.Conn.(*net.TCPConn); ok {
// 			_ = tcp.SetReadBuffer(b.cfg.RecvBufferSize)
// 		}
// 	}

// 	// kill last connection id
// 	if b.lastConnectionID > 0 {
// 		b.killConnection(b.c, b.lastConnectionID)
// 	}

// 	// save last last connection id for kill
// 	b.lastConnectionID = b.c.GetConnectionID()

// 	//for mysql 5.6+, binlog has a crc32 checksum
// 	//before mysql 5.6, this will not work, don't matter.:-)
// 	if r, err := b.c.Execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'"); err != nil {
// 		return errors.Trace(err)
// 	} else {
// 		s, _ := r.GetString(0, 1)
// 		if s != "" {
// 			// maybe CRC32 or NONE

// 			// mysqlbinlog.cc use NONE, see its below comments:
// 			// Make a notice to the server that this client
// 			// is checksum-aware. It does not need the first fake Rotate
// 			// necessary checksummed.
// 			// That preference is specified below.

// 			if _, err = b.c.Execute(`SET @master_binlog_checksum='NONE'`); err != nil {
// 				return errors.Trace(err)
// 			}

// 			// if _, err = b.c.Execute(`SET @master_binlog_checksum=@@global.binlog_checksum`); err != nil {
// 			// 	return errors.Trace(err)
// 			// }
// 		}
// 	}

// 	if b.cfg.Flavor == MariaDBFlavor {
// 		// Refer https://github.com/alibaba/canal/wiki/BinlogChange(MariaDB5&10)
// 		// Tell the server that we understand GTIDs by setting our slave capability
// 		// to MARIA_SLAVE_CAPABILITY_GTID = 4 (MariaDB >= 10.0.1).
// 		if _, err := b.c.Execute("SET @mariadb_slave_capability=4"); err != nil {
// 			return errors.Errorf("failed to set @mariadb_slave_capability=4: %v", err)
// 		}
// 	}

// 	if b.cfg.HeartbeatPeriod > 0 {
// 		_, err = b.c.Execute(fmt.Sprintf("SET @master_heartbeat_period=%d;", b.cfg.HeartbeatPeriod))
// 		if err != nil {
// 			log.Errorf("failed to set @master_heartbeat_period=%d, err: %v", b.cfg.HeartbeatPeriod, err)
// 			return errors.Trace(err)
// 		}
// 	}

// 	if err = b.writeRegisterSlaveCommand(); err != nil {
// 		return errors.Trace(err)
// 	}

// 	if _, err = b.c.ReadOKPacket(); err != nil {
// 		return errors.Trace(err)
// 	}

// 	return nil
// }

// func (b *FakeSlave) Close() {
// 	b.m.Lock()
// 	defer b.m.Unlock()

// 	b.close()
// }

// func (b *FakeSlave) close() {
// 	if b.isClosed() {
// 		return
// 	}

// 	log.Info("fakeslave is closing...")

// 	b.running = false
// 	b.cancel()

// 	if b.c != nil {
// 		err := b.c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
// 		if err != nil {
// 			log.Warnf(`could not set read deadline: %s`, err)
// 		}
// 	}

// 	// kill last connection id
// 	if b.lastConnectionID > 0 {
// 		// Use a new connection to kill the binlog syncer
// 		// because calling KILL from the same connection
// 		// doesn't actually disconnect it.
// 		c, err := b.newConnection()
// 		if err == nil {
// 			b.killConnection(c, b.lastConnectionID)
// 			c.Close()
// 		}
// 	}

// 	b.wg.Wait()

// 	if b.c != nil {
// 		b.c.Close()
// 	}

// 	log.Info("fakesalve is closed")
// }

// func (b *FakeSlave) isClosed() bool {
// 	select {
// 	case <-b.ctx.Done():
// 		return true
// 	default:
// 		return false
// 	}
// }

// func (this *FakeSlave) startreplication() (err error) {

// 	if err = this.writeBinlogDumpCommand(); err != nil {
// 		return errors.Trace(err)
// 	}
// 	res, err := this.c.ReadPacket()
// 	if err != nil {
// 		return errors.Trace(err)
// 	} else {

// 		fmt.Printf("====%+v\n", res)
// 	}

// 	return nil
// }

// func (b *FakeSlave) writeBinlogDumpCommand(p Position) error {
// 	b.c.ResetSequence()

// 	data := make([]byte, 4+1+4+2+4+len(p.Name))

// 	pos := 4
// 	data[pos] = COM_BINLOG_DUMP
// 	pos++

// 	binary.LittleEndian.PutUint32(data[pos:], p.Pos)
// 	pos += 4

// 	binary.LittleEndian.PutUint16(data[pos:], b.cfg.DumpCommandFlag)
// 	pos += 2

// 	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
// 	pos += 4

// 	copy(data[pos:], p.Name)

// 	return b.c.WritePacket(data)
// }

// func (b *FakeSlave) writeBinlogDumpMysqlGTIDCommand(gset GTIDSet) error {
// 	p := Position{Name: "", Pos: 4}
// 	gtidData := gset.Encode()

// 	b.c.ResetSequence()

// 	data := make([]byte, 4+1+2+4+4+len(p.Name)+8+4+len(gtidData))
// 	pos := 4
// 	data[pos] = COM_BINLOG_DUMP_GTID
// 	pos++

// 	binary.LittleEndian.PutUint16(data[pos:], 0)
// 	pos += 2

// 	binary.LittleEndian.PutUint32(data[pos:], b.cfg.ServerID)
// 	pos += 4

// 	binary.LittleEndian.PutUint32(data[pos:], uint32(len(p.Name)))
// 	pos += 4

// 	n := copy(data[pos:], p.Name)
// 	pos += n

// 	binary.LittleEndian.PutUint64(data[pos:], uint64(p.Pos))
// 	pos += 8

// 	binary.LittleEndian.PutUint32(data[pos:], uint32(len(gtidData)))
// 	pos += 4
// 	n = copy(data[pos:], gtidData)
// 	pos += n

// 	data = data[0:pos]

// 	return b.c.WritePacket(data)
// }

// func (b *FakeSlave) prepare() error {
// 	if b.isClosed() {
// 		return errors.Trace(ErrSyncClosed)
// 	}

// 	if err := b.registerSlave(); err != nil {
// 		return errors.Trace(err)
// 	}

// 	if err := b.enableSemiSync(); err != nil {
// 		return errors.Trace(err)
// 	}

// 	return nil
// }

// func (b *FakeSlave) prepareSyncPos(pos Position) error {
// 	// always start from position 4
// 	if pos.Pos < 4 {
// 		pos.Pos = 4
// 	}

// 	if err := b.prepare(); err != nil {
// 		return errors.Trace(err)
// 	}

// 	if err := b.writeBinlogDumpCommand(pos); err != nil {
// 		return errors.Trace(err)
// 	}

// 	return nil
// }

// func (this *FakeSlave) writeRegisterSlaveCommand() error {
// 	this.c.ResetSequence()

// 	hostname := "FakeSlave"

// 	// This should be the name of slave host not the host we are connecting to.
// 	data := make([]byte, 4+1+4+1+len(hostname)+1+len(this.cfg.User)+1+len(this.cfg.Password)+2+4+4)
// 	pos := 4

// 	data[pos] = COM_REGISTER_SLAVE
// 	pos++

// 	binary.LittleEndian.PutUint32(data[pos:], this.cfg.ServerID)
// 	pos += 4

// 	// This should be the name of slave hostname not the host we are connecting to.
// 	data[pos] = uint8(len(hostname))
// 	pos++
// 	n := copy(data[pos:], hostname)
// 	pos += n

// 	data[pos] = uint8(len(this.cfg.User))
// 	pos++
// 	n = copy(data[pos:], this.cfg.User)
// 	pos += n

// 	data[pos] = uint8(len(this.cfg.Password))
// 	pos++
// 	n = copy(data[pos:], this.cfg.Password)
// 	pos += n

// 	binary.LittleEndian.PutUint16(data[pos:], this.cfg.Port)
// 	pos += 2

// 	//replication rank, not used
// 	binary.LittleEndian.PutUint32(data[pos:], 0)
// 	pos += 4

// 	// master ID, 0 is OK
// 	binary.LittleEndian.PutUint32(data[pos:], 0)

// 	return this.c.WritePacket(data)
// }

// func (this *FakeSlave) newConnection() (*client.Conn, error) {
// 	var addr string
// 	if this.cfg.Port != 0 {
// 		addr = fmt.Sprintf("%s:%d", this.cfg.Host, this.cfg.Port)
// 	} else {
// 		addr = this.cfg.Host
// 	}

// 	return client.Connect(addr, this.cfg.User, this.cfg.Password, "", func(c *client.Conn) {
// 		//c.SetTLSConfig(this.cfg.TLSConfig)
// 	})
// }

func main() {

	var start_file string
	var end_file string
	var s_gtidset string
	var e_gtidset string

	flag.StringVar(&start_file, "s", "", "开始binlog")
	flag.StringVar(&end_file, "e", "", "结束binlog")
	flag.StringVar(&s_gtidset, "sg", "", "开始gtid")
	flag.StringVar(&e_gtidset, "eg", "", "结束gtid")

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

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

const (
	topic1 = "dbserver1.bank.tbl_user_pin"
	topic2 = "dbserver1.bank.tbl_activation"
	topic3 = "dbserver1.bank.tbl_activation_mnt_log"
)

var (
	db          *sql.DB
	stmtIns1    *sql.Stmt
	stmtUpd1    *sql.Stmt
	stmtIns2    *sql.Stmt
	stmtUpd2    *sql.Stmt
	stmtIns3    *sql.Stmt
	stmtUpd3    *sql.Stmt
	logLocation string
)

// DataTblUserPin struct tabel activation.
type DataTblUserPin struct {
	ID   int    // ID
	Nama string //Nama
}

// DataTblActivation tabel activation
type DataTblActivation struct {
	ID     int
	Lokasi string
	Alasan string
}

// DataTblActivationMntLog tabel activation mnt log
type DataTblActivationMntLog struct {
	ID                 int
	Lokasi_log         string
	Alasan_penghapusan string
	Date               int64
}

// Befores untuk data tbluserpin
type Befores struct {
	Before DataTblUserPin
}

// Befores1 untuk data tbluserpin
type Befores1 struct {
	Before DataTblActivation
}

// Befores2 untuk data tbluserpin
type Befores2 struct {
	Before DataTblActivationMntLog
}

// StatusEvent untuk cek status yang delete
type StatusEvent struct {
	Op string `json:"op"`
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

func main() {
	var err error
	db, err = sql.Open("mysql", "debezium:debezium@tcp(localhost:3306)/bank_del")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("connected to MySQL...")

	// Insert dan update tabel tbl_user_pin
	stmtIns1, err = db.Prepare("INSERT INTO tbl_user_pin (`id`,`nama`) VALUES(?,?)")
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns1.Close()
	stmtUpd1, err = db.Prepare("UPDATE tbl_user_pin SET nama=? WHERE id=?")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpd1.Close()

	// Insert dan update tabel tbl_activation
	stmtIns2, err = db.Prepare("INSERT INTO tbl_activation (`id`,`lokasi`,`alasan`) VALUES(?,?,?)")
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns2.Close()
	stmtUpd2, err = db.Prepare("UPDATE tbl_activation SET lokasi=?, alasan=? WHERE id=?")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpd2.Close()

	// Insert dan update tabel tbl_activation_mnt_log
	stmtIns3, err = db.Prepare("INSERT INTO tbl_activation_mnt_log (`id`,`lokasi_log`,`alasan_penghapusan`,`date`) VALUES(?,?,?,?)")
	if err != nil {
		panic(err.Error())
	}
	defer stmtIns3.Close()
	stmtUpd3, err = db.Prepare("UPDATE tbl_activation_mnt_log SET lokasi_log=?, alasan_penghapusan=?, date=? WHERE id=?")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpd3.Close()

	// Untuk pencatatan log berupa json
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("Failed to determine working directory: %s", err)
	}
	runID := time.Now().Format("run-2006-01-02-15-04-05")
	logLocation := filepath.Join(cwd, runID+".log")
	logFile, err := os.OpenFile(logLocation, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file %s for output: %s", logLocation, err)
	}
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(io.MultiWriter(os.Stderr, logFile))
	log.RegisterExitHandler(func() {
		if logFile == nil {
			return
		}
		logFile.Close()
	})

	// Config sarama
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := []string{"localhost:9092"}
	topics := []string{topic1, topic2, topic3}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(brokers, "group_consumer_1", config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		if string(message.Value) != "" {
			log.Printf("value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		}
		session.MarkMessage(message, "")

		switch message.Topic {
		case topic1:
			convert := string(message.Value)
			var read StatusEvent
			_ = json.Unmarshal([]byte(convert), &read)
			if read.Op == "d" {
				var getdata Befores
				_ = json.Unmarshal([]byte(convert), &getdata)
				// Execute the query
				id := getdata.Before.ID
				var ids int
				row := db.QueryRow("SELECT id FROM tbl_user_pin WHERE id=?", id)
				err := row.Scan(&ids)
				if err != nil {
					_, err = stmtIns1.Exec(getdata.Before.ID, getdata.Before.Nama)
					if err != nil {
						panic(err.Error())
					} else {
						fmt.Println("Insert Record tabel tbl_user_pin Push To Database")
					}
				} else {
					_, err = stmtUpd1.Exec(getdata.Before.Nama, getdata.Before.ID)
					if err != nil {
						panic(err.Error())
					} else {
						fmt.Println("Update Record tabel tbl_user_pin Push To Database where id =", getdata.Before.ID, "Nama =", getdata.Before.Nama)
					}
				}
			}
		case topic2:
			convert := string(message.Value)
			var read StatusEvent
			_ = json.Unmarshal([]byte(convert), &read)
			if read.Op == "d" {
				var getdata Befores1
				_ = json.Unmarshal([]byte(convert), &getdata)
				// Execute the query
				id := getdata.Before.ID
				var ids int
				row := db.QueryRow("SELECT id FROM tbl_activation WHERE id=?", id)
				err := row.Scan(&ids)
				if err != nil {
					_, err = stmtIns2.Exec(getdata.Before.ID, getdata.Before.Lokasi, getdata.Before.Alasan)
					if err != nil {
						panic(err.Error())
					} else {
						fmt.Println("Insert Record tabel tbl_activation Push To Database")
					}
				} else {
					_, err = stmtUpd2.Exec(getdata.Before.Lokasi, getdata.Before.Alasan, getdata.Before.ID)
					if err != nil {
						panic(err.Error())
					} else {
						fmt.Println("Update Record tabel tbl_activation Push To Database where id", getdata.Before.ID, "Lokasi =", getdata.Before.Lokasi, "Alasan =", getdata.Before.Alasan)
					}
				}
			}
		case topic3:
			convert := string(message.Value)
			var read StatusEvent
			_ = json.Unmarshal([]byte(convert), &read)
			if read.Op == "d" {
				var getdata Befores2
				_ = json.Unmarshal([]byte(convert), &getdata)
				// Execute the query
				id := getdata.Before.ID
				var ids int
				row := db.QueryRow("SELECT id FROM tbl_activation_mnt_log WHERE id=?", id)
				err := row.Scan(&ids)
				if err != nil {
					// convert date milisecond to datetime
					tm2 := time.Unix(0, getdata.Before.Date*int64(time.Millisecond))
					loc, _ := time.LoadLocation("Asia/Jakarta")
					fmt.Println(tm2.Format("2006-01-02 15:04:05"))
					_, err = stmtIns3.Exec(getdata.Before.ID, getdata.Before.Lokasi_log, getdata.Before.Alasan_penghapusan, tm2.In(loc).Format("2006-01-02 15:04:05"))
					if err != nil {
						panic(err.Error())
					} else {
						fmt.Println("Insert Record tabel tbl_activation_mnt_log Push To Database")
					}
				} else {
					// convert date milisecond to datetime
					tm2 := time.Unix(0, getdata.Before.Date*int64(time.Millisecond))
					loc, _ := time.LoadLocation("Asia/Jakarta")
					fmt.Println(tm2.Format("2006-01-02 15:04:05"))
					_, err = stmtUpd3.Exec(getdata.Before.Lokasi_log, getdata.Before.Alasan_penghapusan, tm2.In(loc).Format("2006-01-02 15:04:05"), getdata.Before.ID)
					if err != nil {
						panic(err.Error())
					} else {
						fmt.Println("Update Record tabel tbl_activation_mnt_log Push To Database where id =", getdata.Before.ID, "Lokasi Log =", getdata.Before.Lokasi_log, "Alasan Penghapusan =", getdata.Before.Alasan_penghapusan, "Tanggal =", tm2.In(loc).Format("2006-01-02 15:04:05"))
					}
				}
			}
		default:
			log.Printf("unknown topic")
		}
	}
	return nil
}

package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/xbitgo/components/MQ"
	"github.com/xbitgo/components/asyncbus"
	"github.com/xbitgo/components/crontab"
	"github.com/xbitgo/core/di"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Task1 struct {
}

func (t Task1) At() time.Time {
	//return tool_time.ParseDateTime("2006-01-02 15:04:05")
	return time.Now().Add(10 * time.Second)
}
func (t Task1) Run() error {
	fmt.Printf("task[%s] run ... %s", t.TaskName(), time.Now().Format("2006-01-02 15:04:05"))
	return nil
}

func (t Task1) TaskName() string {
	return "test1"
}

type Task2 struct {
}

func (t Task2) CronSpec() string {
	return "*/2 * * * * ?"
}

func (t Task2) CronJob() error {
	fmt.Printf("run %s ,%s \n", t.TaskName(), time.Now().Format("2006-01-02 15:04:05"))
	return nil
}

func (t Task2) TaskName() string {
	return "task2"
}

type Task3 struct {
	MQC MQ.Consumer `di:"kafka.mqc"`
}

func (t Task3) MQConsumer() MQ.Consumer {
	return t.MQC
}

func (t Task3) ConsumeJob(buf []byte) error {
	fmt.Printf("run %s ,%s,%s \n", t.TaskName(), buf, time.Now().Format("2006-01-02 15:04:05"))
	time.Sleep(100 * time.Millisecond)
	return nil
}

func (t Task3) TaskName() string {
	return "Task3"
}

type Task4 struct {
}

func main() {
	cli, err := etcdv3.New(etcdv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	mqc := MQ.NewKafkaConsumer(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		GroupID:        "test1",
		Topic:          "test",
		MinBytes:       10e3, // 10k
		MaxWait:        10 * time.Second,
		MaxBytes:       10e6, // 10MB
		CommitInterval: 100 * time.Millisecond,
	})
	di.Register("kafka.mqc", mqc)
	fmt.Println("mqc", mqc)

	cronCtl := crontab.NewByEtcd(cli, "asyncbus")
	bus := asyncbus.NewBus(cronCtl)

	bus.Register(&Task1{})
	bus.Register(&Task2{})
	//bus.Register(&Task3{})
	bus.Register(asyncbus.WithPool(&Task3{}, 100)) // 使用xiec
	//bus.Register(&Task3{})
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
	go signalExit(func() {
		bus.Close()
	})
	go func() {
		bus.Start()
	}()
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
	select {
	case <-time.After(60 * time.Second):
		//bus.Close()
	}
}

func signalExit(cls func()) {
	// signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	for {
		s := <-c
		log.Printf("service get a signal: %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT, syscall.SIGHUP:
			cls()
			log.Printf("service closed")
			os.Exit(0)
			return
		default:
			return
		}
	}
}

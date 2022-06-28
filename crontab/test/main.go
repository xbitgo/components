package main

import (
	"github.com/xbitgo/components/crontab"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	startJob()
	select {}
}

func startJob() {
	cli, err := etcdv3.New(etcdv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}

	cron := crontab.NewByEtcd(cli, "etcdcron")
	cron.AddFunc("0 * * * * ?", func() {
		log.Println("doing 0 * * * * ?")
	})
	cron.AddFunc("@every 1s", func() {
		log.Println("doing @every 1s")
	})
	cron.AddFunc("@every 3s", func() {
		//log.Println("doing @every 3s")
		panic("doing @every 3s panic")
	})

	// 退出信号量处理
	go signalExit(func() {
		cron.Close()
	})
	go cron.SafeStart()
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
			log.Printf("service Closed")
			os.Exit(0)
			return
		default:
			return
		}
	}
}

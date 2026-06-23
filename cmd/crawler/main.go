package main

import (
	"context"
	"flag"
	"fmt"
	"gfap/internal/metrics"
	"io"
	"log"
	"os"

	"gfap/internal/config"
	"gfap/internal/crawler"
	"gfap/internal/storage"
)

func main() {
	testMode := flag.Bool("test", false, "run bounded test crawl")
	freshMode := flag.Bool("fresh", false, "first run: seed from seeds.txt")
	flag.Parse()

	cfg := config.Load()

	logFile, err := os.OpenFile("crawler.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer logFile.Close()

	if *testMode {
		log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	} else {
		log.SetOutput(logFile)
	}

	redis, err := storage.NewRedis(cfg.RedisAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer redis.Close()

	mongo, err := storage.NewMongo(cfg.MongoURI, cfg.MongoDB, cfg.MongoCol)
	if err != nil {
		log.Fatal(err)
	}
	defer mongo.Close()

	c := crawler.New(cfg, redis, mongo)
	go metrics.Serve(cfg.MetricsPort, c.Stop)
	if err := c.Login(); err != nil {
		log.Fatalf("[FATAL] login failed: %v", err)
	}
	log.Println("[INFO] crawler logged in successfully")

	if *testMode {
		log.Println("[INFO] Running in test mode")
		c.InitTest()
		if err := redis.BloomInit(context.Background()); err != nil { // Bloom need after Clear()
			log.Fatalf("[ERROR] Bloom init failed: %v\n", err)
		}
		c.RunTest(cfg.TestUrl)
		if err := c.SaveTest(); err != nil {
			log.Printf("[ERROR] Failed to save crawler data: %v", err)
		}
		res := fmt.Sprintf("Visited %d videos, target %d\n", c.Count(), c.TargetCount())
		log.Print(res)
		fmt.Print(res)
	} else {
		if err := redis.BloomInit(context.Background()); err != nil {
			log.Fatalf("[ERROR] Bloom init failed: %v\n", err)
		}
		if err := redis.BloomVerify(context.Background()); err != nil {
			log.Fatalf("[ERROR] Bloom verify failed: %v\n", err)
		}
		log.Println("[INFO] Running in production mode")
		c.Resume()
		if *freshMode {
			c.Seed("seeds.txt")
		}
		c.Run(cfg.BaseUrl)
	}
}

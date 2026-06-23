package config

import (
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Workers   int
	QueueSize int

	BaseUrl      string
	TestUrl      string
	VideoPattern string
	TitleSuffix  string
	OutputFile   string
	RateLimit    time.Duration
	CutoffDate   time.Time

	RedisAddr string

	MongoURI string
	MongoDB  string
	MongoCol string

	LoginURL string
	Username string
	Password string

	MetricsPort string

	StaticProxyURLs  []string
	RotatingProxyURL string
}

func Load() *Config {
	godotenv.Load()

	staticURLs := []string{}
	if raw := os.Getenv("STATIC_PROXY_URLS"); raw != "" {
		staticURLs = strings.Split(raw, ",")
	}

	return &Config{
		Workers:      20, // ~0.5 req/s per each worker
		QueueSize:    10_000,
		BaseUrl:      "https://www.vidlii.com",
		TestUrl:      "https://www.vidlii.com/user/rinkomania",
		VideoPattern: "/watch?v=",
		TitleSuffix:  " - VidLii",
		OutputFile:   "targets.json",
		RateLimit:    15 * time.Second,
		CutoffDate:   time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),

		RedisAddr: "localhost:6379",

		MongoURI: "mongodb://localhost:27017",
		MongoDB:  "vidlii",
		MongoCol: "videos",

		LoginURL: "https://www.vidlii.com/login",
		Username: "bennyc",
		Password: "abc123456",

		MetricsPort: "2112",

		StaticProxyURLs:  staticURLs,
		RotatingProxyURL: os.Getenv("ROTATING_PROXY_URL"),
	}
}

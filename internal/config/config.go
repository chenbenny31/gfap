package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Workers int

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

	// --- frontier tuning ---
	// Raw seconds/hours; converted to time.Duration at the FrontierConfig
	// construction site in main.go, keeping this package free of a storage import.
	LeaseTimeoutSec     int
	MaxStrikes          int
	SchedulerBatchLimit int

	RateLimitBackoffSec []int // seconds, indexed by attempts: default 5m,15m,45m
	StrikeBackoffSec    []int // seconds, indexed by attempts: default 1m,5m,25m

	ListingBaseTTLHours    int
	SeedTTLHours           int
	ListingMaxTTLHours     int
	QuarantineListingHours int
}

func Load() *Config {
	godotenv.Load()

	staticURLs := []string{}
	if raw := os.Getenv("STATIC_PROXY_URLS"); raw != "" {
		staticURLs = strings.Split(raw, ",")
	}

	redisAddr := "localhost:6379"
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		redisAddr = v
	}

	mongoURI := "mongodb://localhost:27017"
	if v := os.Getenv("MONGO_URI"); v != "" {
		mongoURI = v
	}

	username := os.Getenv("VIDLII_USERNAME")
	password := os.Getenv("VIDLII_PASSWORD")

	return &Config{
		Workers:          20, // ~0.5 req/s per each worker
		BaseUrl:          "https://www.vidlii.com",
		TestUrl:          "https://www.vidlii.com/user/rinkomania",
		VideoPattern:     "/watch?v=",
		TitleSuffix:      " - VidLii",
		OutputFile:       "targets.json",
		RateLimit:        15 * time.Second,
		CutoffDate:       time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
		RedisAddr:        redisAddr,
		MongoURI:         mongoURI,
		MongoDB:          "vidlii",
		MongoCol:         "videos",
		LoginURL:         "https://www.vidlii.com/login",
		Username:         username,
		Password:         password,
		MetricsPort:      "2112",
		StaticProxyURLs:  staticURLs,
		RotatingProxyURL: os.Getenv("ROTATING_PROXY_URL"),

		LeaseTimeoutSec:     getEnvInt("LEASE_TIMEOUT_SEC", 600),
		MaxStrikes:          getEnvInt("MAX_STRIKES", 5),
		SchedulerBatchLimit: getEnvInt("SCHEDULER_BATCH_LIMIT", 100),

		RateLimitBackoffSec: getEnvIntSlice("RATE_LIMIT_BACKOFF_SEC", []int{300, 900, 2700}),
		StrikeBackoffSec:    getEnvIntSlice("STRIKE_BACKOFF_SEC", []int{60, 300, 1500}),

		ListingBaseTTLHours:    getEnvInt("LISTING_BASE_TTL_HOURS", 72),
		SeedTTLHours:           getEnvInt("SEED_TTL_HOURS", 24),
		ListingMaxTTLHours:     getEnvInt("LISTING_MAX_TTL_HOURS", 21*24),
		QuarantineListingHours: getEnvInt("QUARANTINE_LISTING_HOURS", 7*24),
	}
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getEnvIntSlice(key string, def []int) []int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return def // malformed override - fall back entirely, don't partially apply
		}
		out = append(out, n)
	}
	return out
}

package redisx

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient 统一的Redis客户端接口 (适配v8版本)
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	TTL(ctx context.Context, key string) *redis.DurationCmd
	Rename(ctx context.Context, key, newKey string) *redis.StatusCmd
	RenameNX(ctx context.Context, key, newKey string) *redis.BoolCmd
	Type(ctx context.Context, key string) *redis.StatusCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd

	// String operations
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	MSet(ctx context.Context, pairs ...interface{}) *redis.StatusCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd
	Decr(ctx context.Context, key string) *redis.IntCmd
	DecrBy(ctx context.Context, key string, value int64) *redis.IntCmd

	// List operations
	LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	LPop(ctx context.Context, key string) *redis.StringCmd
	RPop(ctx context.Context, key string) *redis.StringCmd
	LLen(ctx context.Context, key string) *redis.IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd
	LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd

	// Hash operations
	HGet(ctx context.Context, key, field string) *redis.StringCmd
	HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd
	HExists(ctx context.Context, key, field string) *redis.BoolCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	HKeys(ctx context.Context, key string) *redis.StringSliceCmd
	HVals(ctx context.Context, key string) *redis.StringSliceCmd
	HLen(ctx context.Context, key string) *redis.IntCmd
	HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd

	// Set operations
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd
	SCard(ctx context.Context, key string) *redis.IntCmd

	// Sorted Set operations
	ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd
	ZScore(ctx context.Context, key, member string) *redis.FloatCmd
	ZCard(ctx context.Context, key string) *redis.IntCmd
	ZCount(ctx context.Context, key string, min, max string) *redis.IntCmd
	ZRank(ctx context.Context, key, member string) *redis.IntCmd
	ZRevRank(ctx context.Context, key, member string) *redis.IntCmd
	ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd

	// Scan
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd

	// Bitmap operations
	GetBit(ctx context.Context, key string, offset int64) *redis.IntCmd
	SetBit(ctx context.Context, key string, offset int64, value int) *redis.IntCmd
	BitCount(ctx context.Context, key string, bitCount *redis.BitCount) *redis.IntCmd

	// Pipeline and Lua script support
	Pipeline() redis.Pipeliner
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd

	// Health check
	Ping(ctx context.Context) *redis.StatusCmd
	Close() error
}

// RedisStats Redis统计信息
type RedisStats struct {
	totalOps  int64
	errorOps  int64
	startTime time.Time
	mu        sync.RWMutex
}

// NewRedisStats 创建新的Redis统计
func NewRedisStats() *RedisStats {
	return &RedisStats{
		startTime: time.Now(),
	}
}

// IncrTotal 增加总操作数
func (s *RedisStats) IncrTotal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalOps++
}

// IncrError 增加错误操作数
func (s *RedisStats) IncrError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errorOps++
}

// GetStats 获取统计信息
func (s *RedisStats) GetStats() (total, errors int64, uptime time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalOps, s.errorOps, time.Since(s.startTime)
}

// Proc 处理统计信息（打印或记录）
func (s *RedisStats) Proc() {
	total, errors, uptime := s.GetStats()
	log.Printf("Redis Stats - Total: %d, Errors: %d, Uptime: %v, Error Rate: %.2f%%",
		total, errors, uptime, float64(errors)/float64(total)*100)
}

// RedisManager Redis管理器
type RedisManager struct {
	config       *RedisConfig
	client       RedisClient
	isHealthy    bool
	stats        *RedisStats
	scripts      map[string]string // Lua脚本缓存
	scriptsMutex sync.RWMutex
	ctx          context.Context    // 默认context
	cancel       context.CancelFunc // 取消函数

	// 健康检查和统计
	healthTicker *time.Ticker
	statsTicker  *time.Ticker
	done         chan struct{}
	mu           sync.RWMutex
}

// NewRedisManager 创建Redis管理器
func NewRedisManager(config *RedisConfig) (*RedisManager, error) {
	// 验证配置
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	// 设置默认值
	config.SetDefaults()

	// 创建context
	ctx, cancel := context.WithCancel(context.Background())

	manager := &RedisManager{
		config:  config,
		stats:   NewRedisStats(),
		scripts: make(map[string]string),
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}

	// 初始化客户端
	if err := manager.initClient(); err != nil {
		cancel()
		return nil, fmt.Errorf("初始化Redis客户端失败: %w", err)
	}

	// 启动健康检查
	go manager.startHealthCheck()

	// 启动统计输出（如果启用）
	if config.Common.EnableStats {
		go manager.startStatsOutput()
	}

	// 注册所有Lua脚本
	RegisterAllScripts(manager)

	return manager, nil
}

// initClient 初始化Redis客户端
func (rm *RedisManager) initClient() error {
	switch rm.config.Mode {
	case ModeSingle:
		return rm.initSingleClient()
	case ModeMasterSlave:
		return rm.initMasterSlaveClient()
	case ModeCluster:
		return rm.initClusterClient()
	default:
		return ErrInvalidConfig.WithMessage(fmt.Sprintf("unsupported mode: %s", rm.config.Mode))
	}
}

// initSingleClient 初始化单例Redis客户端
func (rm *RedisManager) initSingleClient() error {
	opts := &redis.Options{
		Addr:            rm.config.Single.Addr,
		Password:        rm.config.Single.Password,
		DB:              rm.config.Single.Database,
		PoolSize:        rm.config.Common.PoolSize,
		MinIdleConns:    rm.config.Common.MinIdleConns,
		PoolTimeout:     rm.config.Common.PoolTimeout,
		DialTimeout:     rm.config.Common.DialTimeout,
		ReadTimeout:     rm.config.Common.ReadTimeout,
		WriteTimeout:    rm.config.Common.WriteTimeout,
		MaxRetries:      rm.config.Common.MaxRetries,
		MinRetryBackoff: rm.config.Common.MinRetryBackoff,
		MaxRetryBackoff: rm.config.Common.MaxRetryBackoff,
	}

	client := redis.NewClient(opts)

	// 测试连接
	if err := client.Ping(rm.ctx).Err(); err != nil {
		_ = client.Close()
		return ErrConnectionFailed.WithError(err)
	}

	rm.client = client
	rm.isHealthy = true
	log.Printf("Redis single client initialized successfully, addr: %s", rm.config.Single.Addr)
	return nil
}

// initMasterSlaveClient 初始化主从客户端
func (rm *RedisManager) initMasterSlaveClient() error {
	config := rm.config.MasterSlave

	// 如果启用了哨兵，使用哨兵模式
	if config.Sentinel != nil && config.Sentinel.Enabled {
		return rm.initSentinelClient()
	}

	// 否则使用普通主从模式
	return rm.initRingClient()
}

// initSentinelClient 初始化哨兵客户端
func (rm *RedisManager) initSentinelClient() error {
	config := rm.config.MasterSlave

	opts := &redis.FailoverOptions{
		// 哨兵配置
		MasterName:       config.Sentinel.MasterName,
		SentinelAddrs:    config.Sentinel.SentinelAddrs,
		SentinelPassword: config.Sentinel.SentinelPassword,
		SentinelUsername: config.Sentinel.SentinelUsername,

		// Redis连接配置（复用主从配置）
		Password: config.Password,
		DB:       config.Database,

		// 读写分离配置
		RouteRandomly:  config.Sentinel.RouteRandomly, // 随机路由读操作到从节点
		RouteByLatency: config.Sentinel.RouteByLatency,

		// 通用配置
		PoolSize:        rm.config.Common.PoolSize,
		MinIdleConns:    rm.config.Common.MinIdleConns,
		PoolTimeout:     rm.config.Common.PoolTimeout,
		DialTimeout:     rm.config.Common.DialTimeout,
		ReadTimeout:     rm.config.Common.ReadTimeout,
		WriteTimeout:    rm.config.Common.WriteTimeout,
		MaxRetries:      rm.config.Common.MaxRetries,
		MinRetryBackoff: rm.config.Common.MinRetryBackoff,
		MaxRetryBackoff: rm.config.Common.MaxRetryBackoff,
	}

	client := redis.NewFailoverClusterClient(opts)

	// 测试连接
	if err := client.Ping(rm.ctx).Err(); err != nil {
		_ = client.Close()
		return ErrConnectionFailed.WithError(err)
	}

	rm.client = client
	rm.isHealthy = true
	log.Printf("Redis sentinel client initialized successfully, master: %s, sentinels: %s",
		config.Sentinel.MasterName, strings.Join(config.Sentinel.SentinelAddrs, ","))
	return nil
}

// initRingClient 初始化Ring客户端（手动主从模式）
func (rm *RedisManager) initRingClient() error {
	config := rm.config.MasterSlave

	// 构建Ring配置 - 使用配置中的实际地址
	addrs := make(map[string]string)
	for i, addr := range config.Addrs {
		addrs[fmt.Sprintf("%d", i)] = addr
	}

	opts := &redis.RingOptions{
		Addrs:    addrs,
		Password: config.Password,
		DB:       config.Database,

		// 通用配置
		PoolSize:        rm.config.Common.PoolSize,
		MinIdleConns:    rm.config.Common.MinIdleConns,
		PoolTimeout:     rm.config.Common.PoolTimeout,
		DialTimeout:     rm.config.Common.DialTimeout,
		ReadTimeout:     rm.config.Common.ReadTimeout,
		WriteTimeout:    rm.config.Common.WriteTimeout,
		MaxRetries:      rm.config.Common.MaxRetries,
		MinRetryBackoff: rm.config.Common.MinRetryBackoff,
		MaxRetryBackoff: rm.config.Common.MaxRetryBackoff,
	}

	client := redis.NewRing(opts)

	// 测试连接
	if err := client.Ping(rm.ctx).Err(); err != nil {
		_ = client.Close()
		return ErrConnectionFailed.WithError(err)
	}

	rm.client = client
	rm.isHealthy = true
	log.Printf("Redis ring client initialized successfully, addrs: %s",
		strings.Join(config.Addrs, ","))
	return nil
}

// initClusterClient 初始化集群Redis客户端
func (rm *RedisManager) initClusterClient() error {
	opts := &redis.ClusterOptions{
		// 基础配置
		Addrs:    rm.config.Cluster.Addrs,
		Password: rm.config.Cluster.Password,

		// 集群特定配置
		MaxRedirects:   rm.config.Cluster.MaxRedirects,
		ReadOnly:       rm.config.Cluster.ReadOnly,
		RouteByLatency: rm.config.Cluster.RouteByLatency,
		RouteRandomly:  rm.config.Cluster.RouteRandomly,

		// 连接池配置
		PoolSize:        rm.config.Common.PoolSize,
		MinIdleConns:    rm.config.Common.MinIdleConns,
		PoolTimeout:     rm.config.Common.PoolTimeout,
		DialTimeout:     rm.config.Common.DialTimeout,
		ReadTimeout:     rm.config.Common.ReadTimeout,
		WriteTimeout:    rm.config.Common.WriteTimeout,
		MaxRetries:      rm.config.Common.MaxRetries,
		MinRetryBackoff: rm.config.Common.MinRetryBackoff,
		MaxRetryBackoff: rm.config.Common.MaxRetryBackoff,
	}

	// 设置集群默认值
	if opts.MaxRedirects == 0 {
		opts.MaxRedirects = 3
	}

	client := redis.NewClusterClient(opts)

	// 测试连接
	if err := client.Ping(rm.ctx).Err(); err != nil {
		_ = client.Close()
		return ErrConnectionFailed.WithError(err)
	}

	rm.client = client
	rm.isHealthy = true

	if rm.config.Cluster.ReadOnly {
		log.Printf("Redis cluster client initialized successfully, addrs: %s, read_from_replica: enabled",
			strings.Join(rm.config.Cluster.Addrs, ","))
	} else {
		log.Printf("Redis cluster client initialized successfully, addrs: %s",
			strings.Join(rm.config.Cluster.Addrs, ","))
	}
	return nil
}

// startHealthCheck 启动健康检查
func (rm *RedisManager) startHealthCheck() {
	rm.healthTicker = time.NewTicker(rm.config.Common.HealthCheckInterval)
	go rm.healthCheckLoop()
}

// startStatsOutput 启动统计信息输出
func (rm *RedisManager) startStatsOutput() {
	rm.statsTicker = time.NewTicker(rm.config.Common.StatsInterval)
	go rm.statsOutputLoop()
}

// healthCheckLoop 健康检查循环
func (rm *RedisManager) healthCheckLoop() {
	for {
		select {
		case <-rm.healthTicker.C:
			rm.performHealthCheck()
		case <-rm.done:
			return
		}
	}
}

// statsOutputLoop 统计信息输出循环
func (rm *RedisManager) statsOutputLoop() {
	for {
		select {
		case <-rm.statsTicker.C:
			rm.stats.Proc()
		case <-rm.done:
			return
		}
	}
}

// performHealthCheck 执行健康检查
func (rm *RedisManager) performHealthCheck() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.client == nil {
		rm.isHealthy = false
		return
	}

	var err error
	switch rm.config.Mode {
	case ModeCluster:
		// 集群模式：检查集群状态
		if clusterClient, ok := rm.client.(*redis.ClusterClient); ok {
			// 检查集群节点状态
			err = clusterClient.ForEachMaster(rm.ctx, func(ctx context.Context, master *redis.Client) error {
				return master.Ping(ctx).Err()
			})
		} else {
			err = rm.client.Ping(rm.ctx).Err()
		}
	case ModeMasterSlave:
		// 主从模式：根据是否启用哨兵采用不同检查策略
		if rm.config.MasterSlave.Sentinel != nil && rm.config.MasterSlave.Sentinel.Enabled {
			// 哨兵模式：检查故障转移客户端
			err = rm.client.Ping(rm.ctx).Err()
		} else {
			// Ring模式：检查Ring客户端
			if ringClient, ok := rm.client.(*redis.Ring); ok {
				err = ringClient.Ping(rm.ctx).Err()
			} else {
				err = rm.client.Ping(rm.ctx).Err()
			}
		}
	default:
		// 单例模式：简单ping检查
		err = rm.client.Ping(rm.ctx).Err()
	}

	wasHealthy := rm.isHealthy
	rm.isHealthy = err == nil

	if !rm.isHealthy && wasHealthy {
		log.Printf("Redis health check failed (mode: %s): %v", rm.config.Mode, err)
		rm.stats.IncrError()
	} else if rm.isHealthy && !wasHealthy {
		log.Printf("Redis health check recovered (mode: %s)", rm.config.Mode)
	}
}

// IsHealthy 检查Redis连接是否健康
func (rm *RedisManager) IsHealthy() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.isHealthy
}

// GetStats 获取统计信息
func (rm *RedisManager) GetStats() *RedisStats {
	return rm.stats
}

// Close 关闭Redis连接和管理器
func (rm *RedisManager) Close() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// 停止健康检查和统计输出
	close(rm.done)

	if rm.healthTicker != nil {
		rm.healthTicker.Stop()
	}

	if rm.statsTicker != nil {
		rm.statsTicker.Stop()
	}

	// 关闭Redis客户端
	if rm.client != nil {
		err := rm.client.Close()
		rm.client = nil
		rm.isHealthy = false
		log.Printf("Redis manager closed")
		return err
	}

	return nil
}

// GetClient 获取Redis客户端（用于高级操作）
func (rm *RedisManager) GetClient() RedisClient {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.client
}

// RegisterScript 注册Lua脚本
func (rm *RedisManager) RegisterScript(name, script string) {
	rm.scriptsMutex.Lock()
	defer rm.scriptsMutex.Unlock()
	rm.scripts[name] = script
}

// GetScript 获取注册的Lua脚本
func (rm *RedisManager) GetScript(name string) (string, bool) {
	rm.scriptsMutex.RLock()
	defer rm.scriptsMutex.RUnlock()
	script, exists := rm.scripts[name]
	return script, exists
}

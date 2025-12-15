package redis

import "time"

// RedisMode 定义Redis连接模式
type RedisMode string

const (
	// ModeSingle 单例模式
	ModeSingle RedisMode = "single"
	// ModeMasterSlave 主从模式（可选哨兵）
	ModeMasterSlave RedisMode = "master_slave"
	// ModeCluster 集群模式
	ModeCluster RedisMode = "cluster"
)

// RedisConfig Redis配置
type RedisConfig struct {
	// 连接模式：single、master_slave 或 cluster
	Mode RedisMode `json:"mode" yaml:"mode"`

	// 单例模式配置
	Single *SingleConfig `json:"single,omitempty" yaml:"single,omitempty"`

	// 主从模式配置（可选哨兵）
	MasterSlave *MasterSlaveConfig `json:"master_slave,omitempty" yaml:"master_slave,omitempty"`

	// 集群模式配置
	Cluster *ClusterConfig `json:"cluster,omitempty" yaml:"cluster,omitempty"`

	// 通用配置
	Common CommonConfig `json:"common" yaml:"common"`
}

// SingleConfig 单例Redis配置
type SingleConfig struct {
	Addr     string `json:"addr" yaml:"addr"`         // Redis地址，如 "localhost:6379"
	Password string `json:"password" yaml:"password"` // 密码
	Database int    `json:"database" yaml:"database"` // 数据库编号 (0-15)
}

// MasterSlaveConfig 主从配置
type MasterSlaveConfig struct {
	// 主从节点配置
	Addrs    []string `json:"addrs" yaml:"addrs"`       // 节点地址
	Password string   `json:"password" yaml:"password"` // Redis密码
	Database int      `json:"database" yaml:"database"` // 数据库编号

	// 哨兵配置（可选 - 仅添加监控和故障转移能力）
	Sentinel *SentinelConfig `json:"sentinel,omitempty" yaml:"sentinel,omitempty"`
}

// SentinelConfig 哨兵配置
type SentinelConfig struct {
	Enabled          bool     `json:"enabled" yaml:"enabled"`                                         // 是否启用哨兵
	MasterName       string   `json:"master_name" yaml:"master_name"`                                 // 主节点名称
	SentinelAddrs    []string `json:"sentinel_addrs" yaml:"sentinel_addrs"`                           // 哨兵地址列表
	SentinelPassword string   `json:"sentinel_password,omitempty" yaml:"sentinel_password,omitempty"` // 哨兵密码
	SentinelUsername string   `json:"sentinel_username,omitempty" yaml:"sentinel_username,omitempty"` // 哨兵用户名

	// 故障转移配置
	RouteRandomly  bool `json:"route_randomly,omitempty" yaml:"route_randomly,omitempty"`     // 把只读命令随机到一个节点
	RouteByLatency bool `json:"route_by_latency,omitempty" yaml:"route_by_latency,omitempty"` // 把只读命令发送到响应最快的节点
	ReplicaOnly    bool `json:"replica_only,omitempty" yaml:"replica_only,omitempty"`         // 把所有命令发送到发送到只读节点
}

// ClusterConfig Redis集群配置（支持主从结构）
type ClusterConfig struct {
	// 集群节点地址（包含所有主从节点）
	Addrs    []string `json:"addrs" yaml:"addrs"`       // 集群所有节点地址列表
	Password string   `json:"password" yaml:"password"` // 密码

	// 集群特定配置
	MaxRedirects   int  `json:"max_redirects" yaml:"max_redirects"`                           // 最大重定向次数，默认3
	ReadOnly       bool `json:"read_only,omitempty" yaml:"read_only,omitempty"`               // 启用从节点处理只读命令，go-redis会把只读命令发给从节点
	RouteByLatency bool `json:"route_by_latency,omitempty" yaml:"route_by_latency,omitempty"` // 把只读命令发送到响应最快的节点，自动启用 `ReadOnly` 选项
	RouteRandomly  bool `json:"route_randomly,omitempty" yaml:"route_randomly,omitempty"`     // 把只读命令随机到一个节点，自动启用 `ReadOnly` 选项
}

// CommonConfig 通用配置
type CommonConfig struct {
	// 连接池配置
	PoolSize     int           `json:"pool_size" yaml:"pool_size"`           // 连接池大小，默认10
	MinIdleConns int           `json:"min_idle_conns" yaml:"min_idle_conns"` // 最小空闲连接数，默认5
	PoolTimeout  time.Duration `json:"pool_timeout" yaml:"pool_timeout"`     // 获取连接超时时间，默认5秒

	// 操作超时配置
	DialTimeout  time.Duration `json:"dial_timeout" yaml:"dial_timeout"`   // 连接超时，默认5秒
	ReadTimeout  time.Duration `json:"read_timeout" yaml:"read_timeout"`   // 读超时，默认3秒
	WriteTimeout time.Duration `json:"write_timeout" yaml:"write_timeout"` // 写超时，默认3秒

	// 重试配置
	MaxRetries      int           `json:"max_retries" yaml:"max_retries"`             // 最大重试次数，默认3
	MinRetryBackoff time.Duration `json:"min_retry_backoff" yaml:"min_retry_backoff"` // 最小重试间隔，默认8ms
	MaxRetryBackoff time.Duration `json:"max_retry_backoff" yaml:"max_retry_backoff"` // 最大重试间隔，默认512ms

	// 健康检查配置
	HealthCheck         bool          `json:"health_check" yaml:"health_check"`                   // 是否启用健康检查，默认true
	HealthCheckInterval time.Duration `json:"health_check_interval" yaml:"health_check_interval"` // 健康检查间隔，默认30秒

	// 统计配置
	EnableStats   bool          `json:"enable_stats" yaml:"enable_stats"`     // 是否启用统计，默认false
	StatsInterval time.Duration `json:"stats_interval" yaml:"stats_interval"` // 统计输出间隔，默认60秒
}

// SetDefaults 设置默认值
func (c *RedisConfig) SetDefaults() {
	if c.Common.PoolSize == 0 {
		c.Common.PoolSize = 10
	}
	if c.Common.MinIdleConns == 0 {
		c.Common.MinIdleConns = 5
	}
	if c.Common.PoolTimeout == 0 {
		c.Common.PoolTimeout = 5 * time.Second
	}
	if c.Common.DialTimeout == 0 {
		c.Common.DialTimeout = 5 * time.Second
	}
	if c.Common.ReadTimeout == 0 {
		c.Common.ReadTimeout = 3 * time.Second
	}
	if c.Common.WriteTimeout == 0 {
		c.Common.WriteTimeout = 3 * time.Second
	}
	if c.Common.MaxRetries == 0 {
		c.Common.MaxRetries = 3
	}
	if c.Common.MinRetryBackoff == 0 {
		c.Common.MinRetryBackoff = 8 * time.Millisecond
	}
	if c.Common.MaxRetryBackoff == 0 {
		c.Common.MaxRetryBackoff = 512 * time.Millisecond
	}
	if c.Common.HealthCheckInterval == 0 {
		c.Common.HealthCheckInterval = 30 * time.Second
	}
	if c.Common.StatsInterval == 0 {
		c.Common.StatsInterval = 60 * time.Second
	}

	// 默认启用健康检查和统计
	c.Common.HealthCheck = true
}

// Validate 验证配置
func (c *RedisConfig) Validate() error {
	if c.Mode == "" {
		c.Mode = ModeSingle
	}

	switch c.Mode {
	case ModeSingle:
		if c.Single == nil {
			return ErrInvalidConfig.WithMessage("single config is required when mode is single")
		}
		if c.Single.Addr == "" {
			return ErrInvalidConfig.WithMessage("single.addr is required")
		}
	case ModeMasterSlave:
		if c.MasterSlave == nil {
			return ErrInvalidConfig.WithMessage("master_slave config is required when mode is master_slave")
		}

		// 验证基本主从配置
		if len(c.MasterSlave.Addrs) == 0 {
			return ErrInvalidConfig.WithMessage("master_slave.slave_addrs is required")
		}

		// 验证哨兵配置（如果启用）
		if c.MasterSlave.Sentinel != nil && c.MasterSlave.Sentinel.Enabled {
			if c.MasterSlave.Sentinel.MasterName == "" {
				return ErrInvalidConfig.WithMessage("sentinel.master_name is required when sentinel is enabled")
			}

			if len(c.MasterSlave.Sentinel.SentinelAddrs) == 0 {
				return ErrInvalidConfig.WithMessage("sentinel.sentinel_addrs is required when sentinel is enabled")
			}
		}
	case ModeCluster:
		if c.Cluster == nil {
			return ErrInvalidConfig.WithMessage("cluster config is required when mode is cluster")
		}
		if len(c.Cluster.Addrs) == 0 {
			return ErrInvalidConfig.WithMessage("cluster.addrs is required")
		}
	default:
		return ErrInvalidConfig.WithMessage("invalid mode, must be 'single', 'master_slave' or 'cluster'")
	}

	return nil
}

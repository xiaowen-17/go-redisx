# Redis Client Package

`go-redisx` 是一个基于 `go-redis/v9` 封装的 Redis 客户端库，提供了统一的接口、健康检查、统计监控以及对多种 Redis 部署模式（单机、主从、集群）的支持。

## 特性

*   **多模式支持**: 支持单机 (Single)、主从/哨兵 (Master-Slave/Sentinel) 和集群 (Cluster) 模式。
*   **统一接口**: 提供 `RedisClient` 接口，屏蔽底层实现差异。
*   **类型安全**: 提供强类型的操作方法（如 `GetS` 返回字符串, `GetB` 返回字节数组），减少类型断言。
*   **健康检查**: 内置后台健康检查机制，自动检测连接状态。
*   **统计监控**: 记录操作总数、错误数、运行时间等统计信息。
*   **Lua 脚本管理**: 提供 Lua 脚本的注册和管理功能。
*   **Pipeline 支持**: 支持 Redis Pipeline 操作。

## 安装

```go
import "github.com/xiaowen-17/go-redisx"
```

## 配置

使用 `RedisConfig` 结构体进行配置：

```go
config := &redisx.RedisConfig{
    Mode: redisx.ModeSingle, // 或 ModeMasterSlave, ModeCluster
    Single: redisx.SingleConfig{
        Addr:     "127.0.0.1:6379",
        Password: "",
        Database: 0,
    },
    Common: redisx.CommonConfig{
        PoolSize:     100,
        MinIdleConns: 10,
        // ... 其他通用配置
    },
}
```

## 使用示例

### 初始化

```go
manager, err := redisx.NewRedisManager(config)
if err != nil {
    log.Fatal(err)
}
defer manager.Close()
```

### 基本操作

```go
ctx := context.Background()

// 设置值
manager.SetS("key", "value", time.Minute)

// 获取值
result := manager.GetS("key")
if result.IsSuccess() {
    fmt.Println(result.Val())
} else {
    fmt.Println("Error:", result.Err())
}

// 整数自增
manager.Incr("counter")
```

### 使用原生客户端

如果封装的方法无法满足需求，可以获取底层的 `go-redis` 客户端：

```go
client := manager.GetClient()
client.Set(ctx, "key", "value", 0)
```

### Lua 脚本

```go
// 注册脚本
manager.RegisterScript("myscript", `return redis.call("GET", KEYS[1])`)

// 获取并执行
script, exists := manager.GetScript("myscript")
if exists {
    manager.GetClient().Eval(ctx, script, []string{"key"})
}
```

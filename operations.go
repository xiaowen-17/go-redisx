package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// CodecType 表示用于Redis操作的数据类型
type CodecType int

const (
	// StringType 表示字符串操作
	StringType CodecType = iota
	// ByteArrayType 表示二进制运算
	ByteArrayType
)

// Redis操作接口，封装常用的Redis操作
// 提供了统一的错误处理和统计记录

// ==== String Operations ====

// get 内部方法：获取值（支持字符串和字节数组）
func (rm *RedisManager) get(codecType CodecType, key string) interface{} {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		if codecType == StringType {
			return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
		}
		return NewCacheError[[]byte](CONNECTION_FAILED, ErrConnectionFailed)
	}

	var val interface{}
	var err error
	switch codecType {
	case StringType:
		val, err = rm.client.Get(rm.ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			return NewCacheError[string](KEY_NOT_FOUND, ErrKeyNotFound)
		} else if err != nil {
			rm.stats.IncrError()
			return NewCacheError[string](REDIS_INNER_ERROR, err)
		}
		return NewCacheResult(val.(string))
	case ByteArrayType:
		val, err = rm.client.Get(rm.ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			return NewCacheError[[]byte](KEY_NOT_FOUND, ErrKeyNotFound)
		} else if err != nil {
			rm.stats.IncrError()
			return NewCacheError[[]byte](REDIS_INNER_ERROR, err)
		}
		return NewCacheResult(val.([]byte))
	}

	// 默认返回
	return NewCacheError[interface{}](INVALID_OPERATION, ErrInvalidOperation)

}

// GetS 获取字符串值
func (rm *RedisManager) GetS(key string) CacheResult[string] {
	return rm.get(StringType, key).(CacheResult[string])
}

// GetB 获取字节数组值
func (rm *RedisManager) GetB(key string) CacheResult[[]byte] {
	return rm.get(ByteArrayType, key).(CacheResult[[]byte])
}

// set 内部方法：设置值（支持字符串和字节数组）
func (rm *RedisManager) set(codecType CodecType, key string, value interface{}, expiration time.Duration) CacheResult[string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Set(rm.ctx, key, value, expiration).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// SetS 设置字符串值
func (rm *RedisManager) SetS(key string, value string, expiration time.Duration) CacheResult[string] {
	return rm.set(StringType, key, value, expiration)
}

// SetB 设置字节数组值
func (rm *RedisManager) SetB(key string, value []byte, expiration time.Duration) CacheResult[string] {
	return rm.set(ByteArrayType, key, value, expiration)
}

// SetNX 仅当键不存在时设置值（分布式锁常用）
func (rm *RedisManager) SetNX(key string, value string, expiration time.Duration) CacheResult[bool] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[bool](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.SetNX(rm.ctx, key, value, expiration).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[bool](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// GetSet 设置新值并返回旧值
func (rm *RedisManager) GetSet(key string, value string) CacheResult[string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.GetSet(rm.ctx, key, value).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return NewCacheError[string](KEY_NOT_FOUND, ErrKeyNotFound)
		}
		rm.stats.IncrError()
		return NewCacheError[string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// mget 内部方法：批量获取多个键的值（支持字符串和字节数组）
func (rm *RedisManager) mget(codecType CodecType, keys ...string) interface{} {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		switch codecType {
		case StringType:
			return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
		case ByteArrayType:
			return NewCacheError[[][]byte](CONNECTION_FAILED, ErrConnectionFailed)
		}
		return NewCacheError[interface{}](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.MGet(rm.ctx, keys...).Result()
	if err != nil {
		rm.stats.IncrError()
		switch codecType {
		case StringType:
			return NewCacheError[[]string](REDIS_INNER_ERROR, err)
		case ByteArrayType:
			return NewCacheError[[][]byte](REDIS_INNER_ERROR, err)
		}
		return NewCacheError[interface{}](REDIS_INNER_ERROR, err)
	}

	switch codecType {
	case StringType:
		// 转换 []interface{} 为 []string
		result := make([]string, len(val))
		for i, v := range val {
			if v != nil {
				result[i] = v.(string)
			}
		}
		return NewCacheResult(result)
	case ByteArrayType:
		// 转换 []interface{} 为 [][]byte
		result := make([][]byte, len(val))
		for i, v := range val {
			if v != nil {
				result[i] = []byte(v.(string))
			}
		}
		return NewCacheResult(result)
	}

	return NewCacheError[interface{}](INVALID_OPERATION, ErrInvalidOperation)
}

// MGetS 批量获取多个键的字符串值
func (rm *RedisManager) MGetS(keys ...string) CacheResult[[]string] {
	return rm.mget(StringType, keys...).(CacheResult[[]string])
}

// MGetB 批量获取多个键的字节数组值
func (rm *RedisManager) MGetB(keys ...string) CacheResult[[][]byte] {
	return rm.mget(ByteArrayType, keys...).(CacheResult[[][]byte])
}

// MSet 批量设置多个键值对
func (rm *RedisManager) MSet(pairs ...interface{}) CacheResult[string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.MSet(rm.ctx, pairs...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// Incr 整数值自增1
func (rm *RedisManager) Incr(key string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Incr(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// IncrBy 整数值自增指定值
func (rm *RedisManager) IncrBy(key string, value int64) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.IncrBy(rm.ctx, key, value).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// Decr 整数值自减1
func (rm *RedisManager) Decr(key string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Decr(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// DecrBy 整数值自减指定值
func (rm *RedisManager) DecrBy(key string, value int64) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.DecrBy(rm.ctx, key, value).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ==== Key Operations ====

// Del 删除一个或多个键
func (rm *RedisManager) Del(keys ...string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Del(rm.ctx, keys...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// DelCtx 删除一个或多个键（支持context）
func (rm *RedisManager) DelCtx(ctx context.Context, keys ...string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Del(ctx, keys...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// Rename 重命名键
func (rm *RedisManager) Rename(oldKey, newKey string) CacheResult[bool] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[bool](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Rename(rm.ctx, oldKey, newKey).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[bool](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val == newKey)
}

// RenameNX 仅当新键不存在时重命名键
func (rm *RedisManager) RenameNX(oldKey, newKey string) CacheResult[bool] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[bool](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.RenameNX(rm.ctx, oldKey, newKey).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[bool](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// Exists 检查键是否存在
func (rm *RedisManager) Exists(keys ...string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Exists(rm.ctx, keys...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	if val == 0 {
		return NewCacheError[int64](KEY_NOT_FOUND, ErrKeyNotFound)
	}

	return NewCacheResult(val)
}

// Expire 设置键的过期时间
func (rm *RedisManager) Expire(key string, expiration time.Duration) CacheResult[bool] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[bool](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Expire(rm.ctx, key, expiration).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[bool](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// TTL 获取键的剩余生存时间
func (rm *RedisManager) TTL(key string) CacheResult[time.Duration] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[time.Duration](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.TTL(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[time.Duration](REDIS_INNER_ERROR, err)
	}

	if val == -2*time.Second {
		return NewCacheError[time.Duration](KEY_NOT_FOUND, ErrKeyNotFound)
	}

	return NewCacheResult(val)
}

// Type 获取键的数据类型
func (rm *RedisManager) Type(key string) CacheResult[string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Type(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[string](REDIS_INNER_ERROR, err)
	}

	if val == "none" {
		return NewCacheError[string](KEY_NOT_FOUND, ErrKeyNotFound)
	}

	return NewCacheResult(val)
}

// Keys 查找匹配模式的键
func (rm *RedisManager) Keys(pattern string) CacheResult[[]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Keys(rm.ctx, pattern).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ==== List Operations ====

// LPush 从左侧推入
func (rm *RedisManager) LPush(key string, values ...interface{}) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.LPush(rm.ctx, key, values...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// RPush 从右侧推入
func (rm *RedisManager) RPush(key string, values ...interface{}) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.RPush(rm.ctx, key, values...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// LPop 从左侧弹出
func (rm *RedisManager) LPop(key string) CacheResult[string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.LPop(rm.ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return NewCacheError[string](KEY_NOT_FOUND, ErrKeyNotFound)
	} else if err != nil {
		rm.stats.IncrError()
		return NewCacheError[string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// LRange 获取范围内的元素
func (rm *RedisManager) LRange(key string, start, stop int64) CacheResult[[]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.LRange(rm.ctx, key, start, stop).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// LLen 获取列表长度
func (rm *RedisManager) LLen(key string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.LLen(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	if val == 0 {
		return NewCacheError[int64](KEY_NOT_FOUND, ErrKeyNotFound)
	}

	return NewCacheResult(val)
}

// ==== Hash Operations ====

// hset 内部方法：设置哈希字段（支持字符串和字节数组）
func (rm *RedisManager) hset(codecType CodecType, key, field string, value interface{}) CacheResult[bool] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[bool](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HSet(rm.ctx, key, field, value).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[bool](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val > 0)
}

// HSetS 设置哈希字段（字符串值）
func (rm *RedisManager) HSetS(key, field string, value string) CacheResult[bool] {
	return rm.hset(StringType, key, field, value)
}

// HSetB 设置哈希字段（字节数组值）
func (rm *RedisManager) HSetB(key, field string, value []byte) CacheResult[bool] {
	return rm.hset(ByteArrayType, key, field, value)
}

// HMSet 批量设置哈希字段
func (rm *RedisManager) HMSet(key string, fields map[string]interface{}) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	if len(fields) == 0 {
		return NewCacheError[int64](INVALID_OPERATION, ErrInvalidOperation)
	}
	result, err := rm.client.HSet(rm.ctx, key, fields).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(result)
}

// hmget 内部方法：批量获取哈希字段（支持字符串和字节数组）
func (rm *RedisManager) hmget(codecType CodecType, key string, fields ...string) interface{} {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		switch codecType {
		case StringType:
			return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
		case ByteArrayType:
			return NewCacheError[[][]byte](CONNECTION_FAILED, ErrConnectionFailed)
		}
		return NewCacheError[[]interface{}](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HMGet(rm.ctx, key, fields...).Result()
	if err != nil {
		rm.stats.IncrError()
		switch codecType {
		case StringType:
			return NewCacheError[[]string](REDIS_INNER_ERROR, err)
		case ByteArrayType:
			return NewCacheError[[][]byte](REDIS_INNER_ERROR, err)
		}
		return NewCacheError[[]interface{}](REDIS_INNER_ERROR, err)
	}

	switch codecType {
	case StringType:
		// 转换 []interface{} 为 []string
		result := make([]string, len(val))
		for i, v := range val {
			if v != nil {
				result[i] = v.(string)
			}
		}
		return NewCacheResult(result)
	case ByteArrayType:
		// 转换 []interface{} 为 [][]byte
		result := make([][]byte, len(val))
		for i, v := range val {
			if v != nil {
				if str, ok := v.(string); ok {
					result[i] = []byte(str)
				}
			}
		}
		return NewCacheResult(result)
	}

	// 原始接口类型返回（保持向后兼容）
	return NewCacheResult(val)
}

// HMGet 批量获取哈希字段
func (rm *RedisManager) HMGet(key string, fields ...string) CacheResult[[]interface{}] {
	return rm.hmget(-1, key, fields...).(CacheResult[[]interface{}]) // 使用-1表示原始interface{}类型
}

// HMGetS 批量获取哈希字段（返回字符串切片）
func (rm *RedisManager) HMGetS(key string, fields ...string) CacheResult[[]string] {
	return rm.hmget(StringType, key, fields...).(CacheResult[[]string])
}

// HMGetB 批量获取哈希字段（返回[]byte切片）
// 专门用于获取二进制数据，将Redis返回的string自动转换为[]byte
func (rm *RedisManager) HMGetB(key string, fields ...string) CacheResult[[][]byte] {
	return rm.hmget(ByteArrayType, key, fields...).(CacheResult[[][]byte])
}

// HExists 检查哈希字段是否存在
func (rm *RedisManager) HExists(key, field string) CacheResult[bool] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[bool](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HExists(rm.ctx, key, field).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[bool](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// HKeys 获取哈希的所有字段名
func (rm *RedisManager) HKeys(key string) CacheResult[[]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HKeys(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// HVals 获取哈希的所有值
func (rm *RedisManager) HVals(key string) CacheResult[[]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HVals(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// HLen 获取哈希字段数量
func (rm *RedisManager) HLen(key string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HLen(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	if val == 0 {
		return NewCacheError[int64](KEY_NOT_FOUND, ErrKeyNotFound)
	}

	return NewCacheResult(val)
}

// hget 内部方法：获取哈希字段（支持字符串和字节数组）
func (rm *RedisManager) hget(codecType CodecType, key, field string) interface{} {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		switch codecType {
		case StringType:
			return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
		case ByteArrayType:
			return NewCacheError[[]byte](CONNECTION_FAILED, ErrConnectionFailed)
		}
		return NewCacheError[interface{}](CONNECTION_FAILED, ErrConnectionFailed)
	}

	var val interface{}
	var err error
	switch codecType {
	case StringType:
		val, err = rm.client.HGet(rm.ctx, key, field).Result()
	case ByteArrayType:
		val, err = rm.client.HGet(rm.ctx, key, field).Bytes()
	}
	if errors.Is(err, redis.Nil) {
		switch codecType {
		case StringType:
			return NewCacheError[string](KEY_NOT_FOUND, ErrKeyNotFound)
		case ByteArrayType:
			return NewCacheError[[]byte](KEY_NOT_FOUND, ErrKeyNotFound)
		}
	} else if err != nil {
		rm.stats.IncrError()
		switch codecType {
		case StringType:
			return NewCacheError[string](REDIS_INNER_ERROR, err)
		case ByteArrayType:
			return NewCacheError[[]byte](REDIS_INNER_ERROR, err)
		}
	}

	// 根据类型返回正确的结果
	switch codecType {
	case StringType:
		return NewCacheResult(val.(string))
	case ByteArrayType:
		return NewCacheResult(val.([]byte))
	}

	// 默认返回
	return NewCacheError[interface{}](INVALID_OPERATION, ErrInvalidOperation)
}

// HGetS 获取哈希字段（字符串值）
func (rm *RedisManager) HGetS(key, field string) CacheResult[string] {
	return rm.hget(StringType, key, field).(CacheResult[string])
}

// HGetB 获取哈希字段（字节数组值）
func (rm *RedisManager) HGetB(key, field string) CacheResult[[]byte] {
	return rm.hget(ByteArrayType, key, field).(CacheResult[[]byte])
}

// HGetAll 获取所有哈希字段和值
func (rm *RedisManager) HGetAll(key string) CacheResult[map[string]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[map[string]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HGetAll(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[map[string]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// HDel 删除哈希字段
func (rm *RedisManager) HDel(key string, fields ...string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HDel(rm.ctx, key, fields...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// HIncrBy 原子递增哈希字段的数值
func (rm *RedisManager) HIncrBy(key, field string, incr int64) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.HIncrBy(rm.ctx, key, field, incr).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ==== Set Operations ====

// SAdd 添加集合成员
func (rm *RedisManager) SAdd(key string, members ...interface{}) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.SAdd(rm.ctx, key, members...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// SRem 移除集合成员
func (rm *RedisManager) SRem(key string, members ...interface{}) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.SRem(rm.ctx, key, members...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// SMembers 获取所有集合成员
func (rm *RedisManager) SMembers(key string) CacheResult[[]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.SMembers(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// SIsMember 检查是否是集合成员
func (rm *RedisManager) SIsMember(key string, member string) CacheResult[bool] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[bool](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.SIsMember(rm.ctx, key, member).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[bool](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// SCard 获取集合成员数量
func (rm *RedisManager) SCard(key string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.SCard(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	if val == 0 {
		return NewCacheError[int64](KEY_NOT_FOUND, ErrKeyNotFound)
	}

	return NewCacheResult(val)
}

// ==== Sorted Set Operations ====

// ZAdd 添加有序集合成员
func (rm *RedisManager) ZAdd(key string, score float64, member string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZAdd(rm.ctx, key, redis.Z{Score: score, Member: member}).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZAddMultiple 批量添加有序集合成员
func (rm *RedisManager) ZAddMultiple(key string, members ...redis.Z) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZAdd(rm.ctx, key, members...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZRem 删除有序集合成员
func (rm *RedisManager) ZRem(key string, members ...interface{}) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZRem(rm.ctx, key, members...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZRange 按索引范围获取有序集合成员
func (rm *RedisManager) ZRange(key string, start, stop int64) CacheResult[[]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZRange(rm.ctx, key, start, stop).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZRangeWithScores 按索引范围获取有序集合成员及分数
func (rm *RedisManager) ZRangeWithScores(key string, start, stop int64) CacheResult[[]redis.Z] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]redis.Z](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZRangeWithScores(rm.ctx, key, start, stop).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]redis.Z](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZRevRange 按索引范围获取有序集合成员（逆序）
func (rm *RedisManager) ZRevRange(key string, start, stop int64) CacheResult[[]string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZRevRange(rm.ctx, key, start, stop).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZRevRangeWithScores 按索引范围获取有序集合成员及分数（逆序）
func (rm *RedisManager) ZRevRangeWithScores(key string, start, stop int64) CacheResult[[]redis.Z] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[[]redis.Z](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZRangeWithScores(rm.ctx, key, start, stop).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[[]redis.Z](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZScore 获取成员分数
func (rm *RedisManager) ZScore(key string, member string) CacheResult[float64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[float64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZScore(rm.ctx, key, member).Result()
	if errors.Is(err, redis.Nil) {
		return NewCacheError[float64](KEY_NOT_FOUND, ErrKeyNotFound)
	} else if err != nil {
		rm.stats.IncrError()
		return NewCacheError[float64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZCard 获取有序集合成员数量
func (rm *RedisManager) ZCard(key string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZCard(rm.ctx, key).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	if val == 0 {
		return NewCacheError[int64](KEY_NOT_FOUND, ErrKeyNotFound)
	}

	return NewCacheResult(val)
}

// ZCount 统计分数范围内的成员数量
func (rm *RedisManager) ZCount(key string, min, max string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZCount(rm.ctx, key, min, max).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZRank 获取成员排名（从小到大）
func (rm *RedisManager) ZRank(key string, member string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZRank(rm.ctx, key, member).Result()
	if errors.Is(err, redis.Nil) {
		return NewCacheError[int64](KEY_NOT_FOUND, ErrKeyNotFound)
	} else if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZRevRank 获取成员排名（从大到小）
func (rm *RedisManager) ZRevRank(key string, member string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZRevRank(rm.ctx, key, member).Result()
	if errors.Is(err, redis.Nil) {
		return NewCacheError[int64](KEY_NOT_FOUND, ErrKeyNotFound)
	} else if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ZIncrBy 原子递增有序集合成员的分数
func (rm *RedisManager) ZIncrBy(key string, increment float64, member string) CacheResult[float64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[float64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.ZIncrBy(rm.ctx, key, increment, member).Result()
	if err != nil {
		rm.stats.IncrError()

		return NewCacheError[float64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

type ScanResult struct {
	Keys   []string
	Cursor uint64
}

// Scan 扫描键
func (rm *RedisManager) Scan(cursor uint64, match string, count int64) CacheResult[ScanResult] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[ScanResult](CONNECTION_FAILED, ErrConnectionFailed)
	}

	page, cursor, err := rm.client.Scan(rm.ctx, cursor, match, count).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[ScanResult](REDIS_INNER_ERROR, err)
	}

	res := ScanResult{
		Keys:   page,
		Cursor: cursor,
	}

	return NewCacheResult(res)
}

// GetBit 获取位
func (rm *RedisManager) GetBit(key string, offset int64) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.GetBit(rm.ctx, key, offset).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// SetBit 设置位
func (rm *RedisManager) SetBit(key string, offset int64, value int) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.SetBit(rm.ctx, key, offset, value).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// BitCount 统计位
func (rm *RedisManager) BitCount(key string) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.BitCount(rm.ctx, key, nil).Result()

	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// BitCountWithRange 统计位
func (rm *RedisManager) BitCountWithRange(key string, start, end int64) CacheResult[int64] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[int64](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.BitCount(rm.ctx, key, &redis.BitCount{
		Start: start,
		End:   end,
	}).Result()

	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[int64](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// ==== Script Operations ====

// Eval 执行Lua脚本
func (rm *RedisManager) Eval(script string, keys []string, args ...interface{}) CacheResult[interface{}] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[interface{}](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Eval(rm.ctx, script, keys, args...).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[interface{}](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

// EvalScript 执行注册的Lua脚本
func (rm *RedisManager) EvalScript(name string, keys []string, args ...interface{}) CacheResult[interface{}] {
	script, exists := rm.GetScript(name)
	if !exists {
		return NewCacheError[interface{}](INVALID_OPERATION, ErrInvalidOperation.WithMessage("script not found: "+name))
	}

	return rm.Eval(script, keys, args...)
}

// ==== Utility Operations ====

// Ping 测试连接
func (rm *RedisManager) Ping() CacheResult[string] {
	rm.stats.IncrTotal()

	if !rm.IsHealthy() {
		return NewCacheError[string](CONNECTION_FAILED, ErrConnectionFailed)
	}

	val, err := rm.client.Ping(rm.ctx).Result()
	if err != nil {
		rm.stats.IncrError()
		return NewCacheError[string](REDIS_INNER_ERROR, err)
	}

	return NewCacheResult(val)
}

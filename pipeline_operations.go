package redisx

import (
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisPipeline 包装的Pipeline，提供统一的错误处理
type RedisPipeline struct {
	pipe redis.Pipeliner
	rm   *RedisManager
}

// Pipeline 获取包装的Pipeline
func (rm *RedisManager) Pipeline() *RedisPipeline {
	return &RedisPipeline{
		pipe: rm.client.Pipeline(),
		rm:   rm,
	}
}

// ==== Pipeline Operations ====

// Exec 执行Pipeline并统一处理错误
func (rp *RedisPipeline) Exec() CacheResult[[]redis.Cmder] {
	rp.rm.stats.IncrTotal()

	if !rp.rm.IsHealthy() {
		return NewCacheError[[]redis.Cmder](CONNECTION_FAILED, ErrConnectionFailed)
	}

	cmders, err := rp.pipe.Exec(rp.rm.ctx)
	if err != nil {

		if errors.Is(err, redis.Nil) {
			return NewCacheError[[]redis.Cmder](KEY_NOT_FOUND, ErrKeyNotFound)
		} else {
			rp.rm.stats.IncrError()
			return NewCacheError[[]redis.Cmder](REDIS_INNER_ERROR, err)
		}

	}

	return NewCacheResult(cmders)
}

// 代理方法：将所有 Pipeliner 的方法转发给内部的 pipe
func (rp *RedisPipeline) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return rp.pipe.Set(rp.rm.ctx, key, value, expiration)
}

func (rp *RedisPipeline) Get(key string) *redis.StringCmd {
	return rp.pipe.Get(rp.rm.ctx, key)
}

func (rp *RedisPipeline) Del(keys ...string) *redis.IntCmd {
	return rp.pipe.Del(rp.rm.ctx, keys...)
}

func (rp *RedisPipeline) Exists(keys ...string) *redis.IntCmd {
	return rp.pipe.Exists(rp.rm.ctx, keys...)
}

func (rp *RedisPipeline) Expire(key string, expiration time.Duration) *redis.BoolCmd {
	return rp.pipe.Expire(rp.rm.ctx, key, expiration)
}

func (rp *RedisPipeline) TTL(key string) *redis.DurationCmd {
	return rp.pipe.TTL(rp.rm.ctx, key)
}

func (rp *RedisPipeline) Rename(key, newKey string) *redis.StatusCmd {
	return rp.pipe.Rename(rp.rm.ctx, key, newKey)
}

func (rp *RedisPipeline) RenameNX(key, newKey string) *redis.BoolCmd {
	return rp.pipe.RenameNX(rp.rm.ctx, key, newKey)
}

func (rp *RedisPipeline) Type(key string) *redis.StatusCmd {
	return rp.pipe.Type(rp.rm.ctx, key)
}

func (rp *RedisPipeline) Keys(pattern string) *redis.StringSliceCmd {
	return rp.pipe.Keys(rp.rm.ctx, pattern)
}

func (rp *RedisPipeline) GetSet(key string, value interface{}) *redis.StringCmd {
	return rp.pipe.GetSet(rp.rm.ctx, key, value)
}

// String operations
func (rp *RedisPipeline) SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return rp.pipe.SetNX(rp.rm.ctx, key, value, expiration)
}

func (rp *RedisPipeline) MGet(keys ...string) *redis.SliceCmd {
	return rp.pipe.MGet(rp.rm.ctx, keys...)
}

func (rp *RedisPipeline) MSet(pairs ...interface{}) *redis.StatusCmd {
	return rp.pipe.MSet(rp.rm.ctx, pairs...)
}

func (rp *RedisPipeline) Incr(key string) *redis.IntCmd {
	return rp.pipe.Incr(rp.rm.ctx, key)
}

func (rp *RedisPipeline) IncrBy(key string, value int64) *redis.IntCmd {
	return rp.pipe.IncrBy(rp.rm.ctx, key, value)
}

func (rp *RedisPipeline) Decr(key string) *redis.IntCmd {
	return rp.pipe.Decr(rp.rm.ctx, key)
}

func (rp *RedisPipeline) DecrBy(key string, value int64) *redis.IntCmd {
	return rp.pipe.DecrBy(rp.rm.ctx, key, value)
}

// List operations
func (rp *RedisPipeline) LPush(key string, values ...interface{}) *redis.IntCmd {
	return rp.pipe.LPush(rp.rm.ctx, key, values...)
}

func (rp *RedisPipeline) RPush(key string, values ...interface{}) *redis.IntCmd {
	return rp.pipe.RPush(rp.rm.ctx, key, values...)
}

func (rp *RedisPipeline) LPop(key string) *redis.StringCmd {
	return rp.pipe.LPop(rp.rm.ctx, key)
}

func (rp *RedisPipeline) RPop(key string) *redis.StringCmd {
	return rp.pipe.RPop(rp.rm.ctx, key)
}

func (rp *RedisPipeline) LLen(key string) *redis.IntCmd {
	return rp.pipe.LLen(rp.rm.ctx, key)
}

func (rp *RedisPipeline) LRange(key string, start, stop int64) *redis.StringSliceCmd {
	return rp.pipe.LRange(rp.rm.ctx, key, start, stop)
}

func (rp *RedisPipeline) LRem(key string, count int64, value interface{}) *redis.IntCmd {
	return rp.pipe.LRem(rp.rm.ctx, key, count, value)
}

func (rp *RedisPipeline) LTrim(key string, start, stop int64) *redis.StatusCmd {
	return rp.pipe.LTrim(rp.rm.ctx, key, start, stop)
}

// Hash operations
func (rp *RedisPipeline) HGet(key, field string) *redis.StringCmd {
	return rp.pipe.HGet(rp.rm.ctx, key, field)
}

func (rp *RedisPipeline) HMGet(key string, fields ...string) *redis.SliceCmd {
	return rp.pipe.HMGet(rp.rm.ctx, key, fields...)
}

func (rp *RedisPipeline) HSet(key string, values ...interface{}) *redis.IntCmd {
	return rp.pipe.HSet(rp.rm.ctx, key, values...)
}

func (rp *RedisPipeline) HMSet(key string, values map[string]interface{}) *redis.BoolCmd {
	if len(values) == 0 {
		return nil
	}

	return rp.pipe.HMSet(rp.rm.ctx, key, values)
}

func (rp *RedisPipeline) HDel(key string, fields ...string) *redis.IntCmd {
	return rp.pipe.HDel(rp.rm.ctx, key, fields...)
}

func (rp *RedisPipeline) HExists(key, field string) *redis.BoolCmd {
	return rp.pipe.HExists(rp.rm.ctx, key, field)
}

func (rp *RedisPipeline) HGetAll(key string) *redis.MapStringStringCmd {
	return rp.pipe.HGetAll(rp.rm.ctx, key)
}

func (rp *RedisPipeline) HKeys(key string) *redis.StringSliceCmd {
	return rp.pipe.HKeys(rp.rm.ctx, key)
}

func (rp *RedisPipeline) HVals(key string) *redis.StringSliceCmd {
	return rp.pipe.HVals(rp.rm.ctx, key)
}

func (rp *RedisPipeline) HLen(key string) *redis.IntCmd {
	return rp.pipe.HLen(rp.rm.ctx, key)
}

func (rp *RedisPipeline) HIncrBy(key, field string, incr int64) *redis.IntCmd {
	return rp.pipe.HIncrBy(rp.rm.ctx, key, field, incr)
}

// Set operations
func (rp *RedisPipeline) SAdd(key string, members ...interface{}) *redis.IntCmd {
	return rp.pipe.SAdd(rp.rm.ctx, key, members...)
}

func (rp *RedisPipeline) SRem(key string, members ...interface{}) *redis.IntCmd {
	return rp.pipe.SRem(rp.rm.ctx, key, members...)
}

func (rp *RedisPipeline) SMembers(key string) *redis.StringSliceCmd {
	return rp.pipe.SMembers(rp.rm.ctx, key)
}

func (rp *RedisPipeline) SIsMember(key string, member interface{}) *redis.BoolCmd {
	return rp.pipe.SIsMember(rp.rm.ctx, key, member)
}

func (rp *RedisPipeline) SCard(key string) *redis.IntCmd {
	return rp.pipe.SCard(rp.rm.ctx, key)
}

// Sorted Set operations
func (rp *RedisPipeline) ZAddMultiple(key string, members ...redis.Z) *redis.IntCmd {
	return rp.pipe.ZAdd(rp.rm.ctx, key, members...)
}

func (rp *RedisPipeline) ZAdd(key string, member interface{}, score float64) *redis.IntCmd {
	return rp.pipe.ZAdd(rp.rm.ctx, key, redis.Z{Score: score, Member: member})
}

func (rp *RedisPipeline) ZRem(key string, members ...interface{}) *redis.IntCmd {
	return rp.pipe.ZRem(rp.rm.ctx, key, members...)
}

func (rp *RedisPipeline) ZRange(key string, start, stop int64) *redis.StringSliceCmd {
	return rp.pipe.ZRange(rp.rm.ctx, key, start, stop)
}

func (rp *RedisPipeline) ZRevRange(key string, start, stop int64) *redis.StringSliceCmd {
	return rp.pipe.ZRevRange(rp.rm.ctx, key, start, stop)
}

func (rp *RedisPipeline) ZRemRangeByRank(key string, start, stop int64) *redis.IntCmd {
	return rp.pipe.ZRemRangeByRank(rp.rm.ctx, key, start, stop)
}

func (rp *RedisPipeline) ZRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	return rp.pipe.ZRangeWithScores(rp.rm.ctx, key, start, stop)
}

func (rp *RedisPipeline) ZRevRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	return rp.pipe.ZRevRangeWithScores(rp.rm.ctx, key, start, stop)
}

func (rp *RedisPipeline) ZScore(key, member string) *redis.FloatCmd {
	return rp.pipe.ZScore(rp.rm.ctx, key, member)
}

func (rp *RedisPipeline) ZCard(key string) *redis.IntCmd {
	return rp.pipe.ZCard(rp.rm.ctx, key)
}

func (rp *RedisPipeline) ZCount(key string, min, max string) *redis.IntCmd {
	return rp.pipe.ZCount(rp.rm.ctx, key, min, max)
}

func (rp *RedisPipeline) ZRank(key, member string) *redis.IntCmd {
	return rp.pipe.ZRank(rp.rm.ctx, key, member)
}

func (rp *RedisPipeline) ZRevRank(key, member string) *redis.IntCmd {
	return rp.pipe.ZRevRank(rp.rm.ctx, key, member)
}

func (rp *RedisPipeline) ZIncrBy(key string, increment float64, member string) *redis.FloatCmd {
	return rp.pipe.ZIncrBy(rp.rm.ctx, key, increment, member)
}

// GetBit
func (rp *RedisPipeline) GetBit(key string, offset int64) *redis.IntCmd {
	return rp.pipe.GetBit(rp.rm.ctx, key, offset)
}

// SetBit
func (rp *RedisPipeline) SetBit(key string, offset int64, value int) *redis.IntCmd {
	return rp.pipe.SetBit(rp.rm.ctx, key, offset, value)
}

// BitCount
func (rp *RedisPipeline) BitCount(key string, bitStart, bitEnd int64) *redis.IntCmd {
	return rp.pipe.BitCount(rp.rm.ctx, key, &redis.BitCount{
		Start: bitStart,
		End:   bitEnd,
	})
}

// 获取原始的Pipeliner（用于高级用法）
func (rp *RedisPipeline) GetPipeliner() redis.Pipeliner {
	return rp.pipe
}

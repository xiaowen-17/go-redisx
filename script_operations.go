package redisx

import (
	"fmt"
	"time"
)

// ==== 便利的脚本操作方法 ====

// SafeDecr 安全减值操作
// 只有当前值大于等于要减少的值时才执行减操作
func (rm *RedisManager) SafeDecr(key string, decr int64) CacheResult[int64] {
	result := rm.EvalScript(ScriptKeyDecr, []string{key}, decr)
	if !result.IsOK() {
		return NewCacheError[int64](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[int64](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val)
}

// SafeIncr 安全增值操作
// 只有当前值小于最大值时才执行增操作
func (rm *RedisManager) SafeIncr(key string, incr, max int64) CacheResult[int64] {
	result := rm.EvalScript(ScriptKeyIncr, []string{key}, incr, max)
	if !result.IsOK() {
		return NewCacheError[int64](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[int64](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val)
}

// IncrWithLimitAndExpire 带上限和过期时间的原子性递增操作
// 参数:
//   - key: Redis键
//   - incr: 递增值
//   - max: 最大值上限
//   - ttl: 过期时间
//
// 返回:
//   - 成功: 递增后的新值
//   - 失败: -1 表示已达上限，不再递增
//
// 特性:
//   - 原子性: 检查、递增、设置过期时间在同一个 Lua 脚本中完成
//   - 上限保证: 即使高并发也不会超过 max 值
//   - 自动过期: 第一次递增时自动设置过期时间
func (rm *RedisManager) IncrWithLimitAndExpire(key string, incr, max int64, ttl time.Duration) CacheResult[int64] {
	result := rm.EvalScript(ScriptKeyIncrWithLimitAndExpire, []string{key}, incr, max, int64(ttl.Seconds()))
	if !result.IsOK() {
		return NewCacheError[int64](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[int64](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val)
}

// SafeHIncr 安全Hash增值操作, 只有当前值小于最大值时才执行增操作
func (rm *RedisManager) SafeHIncr(key string, field string, incr, max int64) CacheResult[int64] {
	result := rm.EvalScript(ScriptKeyHIncr, []string{key, field}, incr, max)
	if !result.IsOK() {
		return NewCacheError[int64](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[int64](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val)
}

// SafeHDecr 安全的Hash减值操作, 只有当前值大于等于要减少的值时才执行减操作
func (rm *RedisManager) SafeHDecr(key string, field string, decr int64) CacheResult[int64] {
	result := rm.EvalScript(ScriptKeyHDecr, []string{key, field}, decr)
	if !result.IsOK() {
		return NewCacheError[int64](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[int64](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}
	return NewCacheResult(val)
}

// SetExpireIfExists 如果键存在则设置过期时间
func (rm *RedisManager) SetExpireIfExists(key string, ttl time.Duration) CacheResult[bool] {
	result := rm.EvalScript(ScriptKeyCheckExpire, []string{key}, int64(ttl.Seconds()))
	if !result.IsOK() {
		return NewCacheError[bool](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val == 1)
}

// DeleteIfValueMatches 如果值匹配则删除键
func (rm *RedisManager) DeleteIfValueMatches(key, expectedValue string) CacheResult[bool] {
	result := rm.EvalScript(ScriptKeyCheckValueAndDel, []string{key}, expectedValue)
	if !result.IsOK() {
		return NewCacheError[bool](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val == 1)
}

// TestAddition 测试脚本 - 两数相加
func (rm *RedisManager) TestAddition(a, b int64) CacheResult[int64] {
	result := rm.EvalScript(ScriptKeyTest, []string{}, a, b)
	if !result.IsOK() {
		return NewCacheError[int64](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[int64](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val)
}

// TryLock 尝试获取分布式锁
func (rm *RedisManager) TryLock(lockKey, lockValue string, expiration time.Duration) CacheResult[bool] {
	result := rm.EvalScript(ScriptKeyLock, []string{lockKey}, lockValue, expiration.Milliseconds())
	if !result.IsOK() {
		return NewCacheError[bool](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	// 处理返回值：1=成功，0=失败，-1=参数错误
	if val == -1 {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("invalid lock parameters"))
	}

	return NewCacheResult(val == 1)
}

// ReleaseLock 释放分布式锁
func (rm *RedisManager) ReleaseLock(lockKey, lockValue string) CacheResult[bool] {
	result := rm.EvalScript(ScriptKeyUnlock, []string{lockKey}, lockValue)
	if !result.IsOK() {
		return NewCacheError[bool](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val == 1)
}

// RenewLock 续期分布式锁
func (rm *RedisManager) RenewLock(lockKey, lockValue string, expiration time.Duration) CacheResult[bool] {
	result := rm.EvalScript(ScriptKeyRenewLock, []string{lockKey}, lockValue, expiration.Milliseconds())
	if !result.IsOK() {
		return NewCacheError[bool](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	// 处理返回值：1=成功，0=失败，-1=参数错误
	if val == -1 {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("invalid renew lock parameters"))
	}

	return NewCacheResult(val == 1)
}

// TryMultiLock 尝试获取多个分布式锁
func (rm *RedisManager) TryMultiLock(lockKeys []string, lockValue string, expiration time.Duration) CacheResult[bool] {
	result := rm.EvalScript(ScriptKeyMultiLock, lockKeys, lockValue, expiration.Milliseconds())
	if !result.IsOK() {
		return NewCacheError[bool](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	// 处理返回值：1=成功，0=失败，-1=参数错误
	if val == -1 {
		return NewCacheError[bool](REDIS_INNER_ERROR, fmt.Errorf("invalid multi-lock parameters"))
	}

	return NewCacheResult(val == 1)
}

// ReleaseMultiLock 释放多个分布式锁
// 返回实际解锁的锁数量
func (rm *RedisManager) ReleaseMultiLock(lockKeys []string, lockValue string) CacheResult[int64] {
	result := rm.EvalScript(ScriptKeyMultiUnlock, lockKeys, lockValue)
	if !result.IsOK() {
		return NewCacheError[int64](result.ErrCode, result.Err)
	}

	val, ok := result.Val.(int64)
	if !ok {
		return NewCacheError[int64](REDIS_INNER_ERROR, fmt.Errorf("unexpected return type"))
	}

	return NewCacheResult(val)
}

package redisx

// Lua脚本常量定义
// 基于Java代码中的RedisLuaScript类转换而来

const (
	// ScriptKeyDecr 减值脚本的键名
	ScriptKeyDecr = "decr_script"

	// ScriptKeyIncr 增值脚本的键名
	ScriptKeyIncr = "incr_script"

	// ScriptKeyHDecr 安全Hash减值脚本的键名
	ScriptKeyHDecr = "hdecr_script"

	// ScriptKeyHIncr 安全Hash增值脚本的键名
	ScriptKeyHIncr = "hincr_script"

	// ScriptKeyCheckExpire 检查键过期脚本的键名
	ScriptKeyCheckExpire = "check_key_expire_script"

	// ScriptKeyCheckValueAndDel 检查值并删除脚本的键名
	ScriptKeyCheckValueAndDel = "check_value_and_del_script"

	// ScriptKeyTest 测试脚本的键名
	ScriptKeyTest = "test_script"

	// ScriptKeyLock 分布式锁脚本的键名
	ScriptKeyLock = "lock_script"

	// ScriptKeyUnlock 分布式解锁脚本的键名
	ScriptKeyUnlock = "unlock_script"

	// ScriptKeyRenewLock 续期分布式锁脚本的键名
	ScriptKeyRenewLock = "renew_lock_script"

	// ScriptKeyMultiLock 多键锁脚本的键名
	ScriptKeyMultiLock = "multi_lock_script"

	// ScriptKeyMultiUnlock 多键解锁脚本的键名
	ScriptKeyMultiUnlock = "multi_unlock_script"
)

// Lua脚本内容定义

// DecrScript 安全减值脚本
// 参数: KEYS[1] = key, ARGV[1] = 减少的值
// 返回: 减少后的值，如果当前值小于要减少的值则返回当前值
const DecrScript = `
local cur = tonumber(redis.call('get', KEYS[1]) or 0)
local decr = tonumber(ARGV[1])
if cur >= decr then
    return redis.call('decrby', KEYS[1], decr)
else
    return cur
end`

// IncrScript 安全增值脚本
// 参数: KEYS[1] = key, ARGV[1] = 增加的值, ARGV[2] = 最大值
// 返回: 增加后的值，如果当前值大于等于最大值则返回当前值
const IncrScript = `
local cur = tonumber(redis.call('get', KEYS[1]) or 0)
local incr = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
if cur < max then
    return redis.call('incrby', KEYS[1], incr)
else
    return cur
end`

// HDecrScript 安全Hash减值脚本
// 参数: KEYS[1] = key, KEYS[2] = field, ARGV[1] = 减少的值
// 返回: 减少后的值，如果当前值小于要减少的值则返回当前值
const HDecrScript = `
local cur = tonumber(redis.call('hget', KEYS[1], KEYS[2]) or 0)
local decr = tonumber(ARGV[1])
if cur >= decr then
    return redis.call('hdecrby', KEYS[1], KEYS[2], decr)
else
    return cur
end`

// HIncrScript 安全Hash增值脚本
// 参数: KEYS[1] = key, KEYS[2] = field, ARGV[1] = 增加的值, ARGV[2] = 最大值
// 返回: 增加后的值，如果当前值大于等于最大值则返回当前值
const HIncrScript = `
local cur = tonumber(redis.call('hget', KEYS[1], KEYS[2]) or 0)
local incr = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
if cur < max then
    return redis.call('hincrby', KEYS[1], KEYS[2], incr)
else
    return cur
end`

// CheckKeyExpireScript 检查键存在并设置过期时间
// 参数: KEYS[1] = key, ARGV[1] = TTL秒数
// 返回: 1表示成功设置过期时间，0表示键不存在
const CheckKeyExpireScript = `
local key = KEYS[1]
local ttl = ARGV[1]

if redis.call('EXISTS', key) == 1 then
    return redis.call('EXPIRE', key, ttl)
else
    return 0
end`

// CheckValueAndDelScript 检查值匹配后删除键
// 参数: KEYS[1] = key, ARGV[1] = 期望的值
// 返回: 1表示值匹配并删除成功，0表示值不匹配
const CheckValueAndDelScript = `
local current_value = redis.call('GET', KEYS[1])
if current_value == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end`

// TestScript 测试脚本（加法运算）
// 参数: ARGV[1] = 第一个数, ARGV[2] = 第二个数
// 返回: 两数之和
const TestScript = `return ARGV[1] + ARGV[2]`

// LockScript 分布式锁脚本
// 参数: KEYS[1] = 锁的key, ARGV[1] = 锁的值(通常是UUID), ARGV[2] = 过期时间(毫秒)
// 返回: 1表示获取锁成功，0表示获取锁失败，-1表示参数错误
const LockScript = `
local key = KEYS[1]
local value = ARGV[1]
local ttl = tonumber(ARGV[2])

-- 检查参数
if not key or not value or not ttl or ttl <= 0 then
    return -1
end

-- 尝试获取锁，如果锁不存在则设置
local result = redis.call('SET', key, value, 'NX', 'PX', ttl)
if result then
    return 1
else
    return 0
end`

// UnlockScript 分布式解锁脚本
// 参数: KEYS[1] = 锁的key, ARGV[1] = 锁的值(通常是UUID)
// 返回: 1表示解锁成功，0表示解锁失败（锁不存在或值不匹配）
const UnlockScript = `
local key = KEYS[1]
local value = ARGV[1]

-- 检查参数
if not key or not value then
    return 0
end

-- 只有当锁的值匹配时才删除
if redis.call('GET', key) == value then
    redis.call('DEL', key)
    return 1
else
    return 0
end`

// RenewLockScript 续期分布式锁脚本
// 参数: KEYS[1] = 锁的key, ARGV[1] = 锁的值(通常是UUID), ARGV[2] = 新的过期时间(毫秒)
// 返回: 1表示续期成功，0表示续期失败（锁不存在或值不匹配），-1表示参数错误
const RenewLockScript = `
local key = KEYS[1]
local value = ARGV[1]
local ttl = tonumber(ARGV[2])

-- 检查参数
if not key or not value or not ttl or ttl <= 0 then
    return -1
end

-- 只有当锁的值匹配时才续期
if redis.call('GET', key) == value then
    redis.call('PEXPIRE', key, ttl)
    return 1
else
    return 0
end`

// MultiLockScript 多键锁脚本（原子性操作）
// 参数: KEYS = 多个锁的key, ARGV[1] = 锁的值(通常是UUID), ARGV[2] = 过期时间(毫秒)
// 返回: 1表示所有锁获取成功，0表示至少有一个锁获取失败，-1表示参数错误
const MultiLockScript = `
local value = ARGV[1]
local ttl = tonumber(ARGV[2])

-- 检查参数
if not value or not ttl or ttl <= 0 or #KEYS == 0 then
    return -1
end

-- 第一阶段：检查所有锁是否都可以获取
for i, key in ipairs(KEYS) do
    local current = redis.call('GET', key)
    if current and current ~= value then
        return 0  -- 有锁被其他客户端持有
    end
end

-- 第二阶段：获取所有锁（此时确保都能成功）
for i, key in ipairs(KEYS) do
    redis.call('SET', key, value, 'PX', ttl)
end

return 1`

// MultiUnlockScript 多键解锁脚本
// 参数: KEYS = 多个锁的key, ARGV[1] = 锁的值(通常是UUID)
// 返回: 成功解锁的锁数量
const MultiUnlockScript = `
local value = ARGV[1]
local unlocked = 0

-- 检查参数
if not value or #KEYS == 0 then
    return 0
end

-- 解锁所有匹配的锁
for i, key in ipairs(KEYS) do
    if redis.call('GET', key) == value then
        redis.call('DEL', key)
        unlocked = unlocked + 1
    end
end

return unlocked`

// RegisterAllScripts 注册所有Lua脚本到RedisManager
func RegisterAllScripts(rm *RedisManager) {
	rm.RegisterScript(ScriptKeyDecr, DecrScript)
	rm.RegisterScript(ScriptKeyIncr, IncrScript)
	rm.RegisterScript(ScriptKeyHDecr, HDecrScript)
	rm.RegisterScript(ScriptKeyHIncr, HIncrScript)
	rm.RegisterScript(ScriptKeyCheckExpire, CheckKeyExpireScript)
	rm.RegisterScript(ScriptKeyCheckValueAndDel, CheckValueAndDelScript)
	rm.RegisterScript(ScriptKeyTest, TestScript)
	rm.RegisterScript(ScriptKeyLock, LockScript)
	rm.RegisterScript(ScriptKeyUnlock, UnlockScript)
	rm.RegisterScript(ScriptKeyRenewLock, RenewLockScript)
	rm.RegisterScript(ScriptKeyMultiLock, MultiLockScript)
	rm.RegisterScript(ScriptKeyMultiUnlock, MultiUnlockScript)
}

func RegisterScripts(rm *RedisManager, scripts map[string]string) {
	for name, script := range scripts {
		rm.RegisterScript(name, script)
	}
}

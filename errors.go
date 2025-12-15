package redisx

import (
	"fmt"
)

// ErrorCode 错误代码
type ErrorCode int

const (
	// OK 成功
	OK ErrorCode = iota
	// INTERRUPTED 中断
	INTERRUPTED
	// TIMEOUT 超时
	TIMEOUT
	// BREAK 失败
	BREAK
	// REDIS_INNER_ERROR Redis内部错误
	REDIS_INNER_ERROR
	// CONNECTION_FAILED 连接失败
	CONNECTION_FAILED
	// KEY_NOT_FOUND 键不存在
	KEY_NOT_FOUND
	// INVALID_CONFIG 无效配置
	INVALID_CONFIG
	// INVALID_OPERATION 操作无效
	INVALID_OPERATION
	// CLUSTER_NOT_READY 集群未就绪
	CLUSTER_NOT_READY
	// HEALTH_CHECK_FAILED 健康检查失败
	HEALTH_CHECK_FAILED
)

func (e ErrorCode) String() string {
	names := map[ErrorCode]string{
		OK:                "OK",
		INTERRUPTED:       "INTERRUPTED",
		TIMEOUT:           "TIMEOUT",
		BREAK:             "BREAK",
		REDIS_INNER_ERROR: "REDIS_INNER_ERROR",
		CONNECTION_FAILED: "CONNECTION_FAILED",
		KEY_NOT_FOUND:     "KEY_NOT_FOUND",
		INVALID_CONFIG:    "INVALID_CONFIG",
	}
	return names[e]
}

// CacheResult Redis操作的结果
type CacheResult[T any] struct {
	Val     T
	ErrCode ErrorCode
	Err     error
}

// IsOK 返回操作是否成功
func (cr *CacheResult[T]) IsOK() bool {
	return cr.ErrCode == OK
}

func (cr *CacheResult[T]) String() string {
	return fmt.Sprintf("CacheResult{val=%v, errCode=%v}", cr.Val, cr.ErrCode)
}

// IsKeyNotFound 返回是否是键不存在错误
func (cr *CacheResult[T]) IsKeyNotFound() bool {
	return cr.ErrCode == KEY_NOT_FOUND
}

// NewCacheResult 创建一个成功的缓存结果
func NewCacheResult[T any](val T) CacheResult[T] {
	return CacheResult[T]{
		Val:     val,
		ErrCode: OK,
	}
}

// NewCacheError 创建一个错误的缓存结果
func NewCacheError[T any](errCode ErrorCode, err error) CacheResult[T] {
	var zero T
	return CacheResult[T]{
		Val:     zero,
		ErrCode: errCode,
		Err:     err,
	}
}

// RedisError Redis错误类型
type RedisError struct {
	Code    ErrorCode
	Message string
	Err     error
}

func (e *RedisError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("redis error [%s]: %s, caused by: %v", e.Code.String(), e.Message, e.Err)
	}
	return fmt.Sprintf("redis error [%s]: %s", e.Code.String(), e.Message)
}

func (e *RedisError) WithMessage(msg string) *RedisError {
	return &RedisError{
		Code:    e.Code,
		Message: msg,
		Err:     e.Err,
	}
}

func (e *RedisError) WithError(err error) *RedisError {
	return &RedisError{
		Code:    e.Code,
		Message: e.Message,
		Err:     err,
	}
}

// 预定义的错误
var (
	ErrInvalidConfig     = &RedisError{Code: INVALID_CONFIG, Message: "invalid config"}
	ErrConnectionFailed  = &RedisError{Code: CONNECTION_FAILED, Message: "connection failed"}
	ErrOperationTimeout  = &RedisError{Code: TIMEOUT, Message: "operation timeout"}
	ErrOperationFailed   = &RedisError{Code: REDIS_INNER_ERROR, Message: "operation failed"}
	ErrKeyNotFound       = &RedisError{Code: KEY_NOT_FOUND, Message: "key not found"}
	ErrInvalidOperation  = &RedisError{Code: INVALID_OPERATION, Message: "invalid operation"}
	ErrClusterNotReady   = &RedisError{Code: CLUSTER_NOT_READY, Message: "cluster not ready"}
	ErrHealthCheckFailed = &RedisError{Code: HEALTH_CHECK_FAILED, Message: "health check failed"}
)

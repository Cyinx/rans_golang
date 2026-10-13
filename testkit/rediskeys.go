package testkit

// 共享测试 Redis 的残键兜底：测试族 KeyPrefix 下存在设计性无 TTL 的键族
// ——{dir}:inc 纪元 Hash（纪元序列必须跨条目死亡存续）与 {reg}:nodes 注册
// 总表（枚举面，value=续命时间戳）——加上未到期的 TTL 键，测试结束即残留
// 在共享实例上并随轮次累积。清扫只在测试侧：生产 Directory.Close 只关客户端
// 绝不清键（红线）；测试夹具在构造点登记 t.Cleanup，LIFO 保证清扫在节点
// 停机与目录 Close 之后执行（届时本前缀已无写入方）。

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// sweepRedisKeys SCAN+DEL 掉 prefix 下全部键（分批，单键 DEL——与生产
// 键方案的 cluster 约束无涉，SCAN/DEL 是测试侧工具命令）。多地址走
// cluster 客户端（Scan 跨分片聚合、Del 按 key 路由），与 directory/redis
// 的 UniversalClient 装配同口径。
func sweepRedisKeys(ctx context.Context, addrs []string, password, prefix string) error {
	cli := goredis.NewUniversalClient(&goredis.UniversalOptions{Addrs: addrs, Password: password})
	defer cli.Close()
	var cursor uint64
	for {
		keys, next, err := cli.Scan(ctx, cursor, prefix+"*", 500).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := cli.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// sweepRedisTestKeys 组合出有界超时的清扫调用（NewCluster 的 t.Cleanup 用）。
func sweepRedisTestKeys(addrs []string, password, prefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return sweepRedisKeys(ctx, addrs, password, prefix)
}

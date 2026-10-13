package redis

import (
	"context"
	"errors"
	"strconv"
	"sync"

	"github.com/cyinx/rans_golang"
	goredis "github.com/redis/go-redis/v9"
)

// scanClient 是 SCAN 命令的最小收窄面（*goredis.Client 与
// *goredis.ClusterClient 的 cmdable 都满足）。
type scanClient interface {
	Scan(ctx context.Context, cursor uint64, match string, count int64) *goredis.ScanCmd
}

// scanNode 单节点上完成整轮 SCAN（游标归零），逐键回调 fn。
func scanNode(ctx context.Context, cli scanClient, pattern string, fn func(key string) error) error {
	iter := cli.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		if err := fn(iter.Val()); err != nil {
			return err
		}
	}
	return iter.Err()
}

// scanAllShards 全分片 SCAN：SCAN 是节点本地命令，通用 cmdable 实现在
// cluster 下被路由到单个节点——对全量快照即失明。cluster 形态经
// ForEachMaster 逐 master 节点 SCAN（fn 并发调用，收集侧加锁），单实例
// 形态退化为直接 SCAN（行为不变）。
func (d *Directory) scanAllShards(ctx context.Context, pattern string) ([]string, error) {
	var (
		mu   sync.Mutex
		keys []string
	)
	collect := func(key string) error {
		mu.Lock()
		keys = append(keys, key)
		mu.Unlock()
		return nil
	}
	if cc, ok := d.cli.(*goredis.ClusterClient); ok {
		if err := cc.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			return scanNode(ctx, node, pattern, collect)
		}); err != nil {
			return nil, err
		}
		return keys, nil
	}
	if err := scanNode(ctx, d.cli, pattern, collect); err != nil {
		return nil, err
	}
	return keys, nil
}

// entryAlive 读主键并判存活（键值 v3 的判活口径）：键在且值为合法 v3 且
// node≠0（墓碑 node=0 视同死）。Nil → false；网络错误向上传播。
func (d *Directory) entryAlive(ctx context.Context, key string) (bool, error) {
	val, err := d.cli.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return false, nil
		}
		return false, err
	}
	e, ok := parseEntry(val)
	return ok && e.Node != 0, nil
}

// UnregisterNode 成员注销（H13 收敛形态）：删除判活键 + 注册总表 field
// ——节点即时退出成员视图。实体条目不清扫：本地逐实体销毁已各自条件
// 注销，残留由死 owner 自愈承接（有效性判定 + CAS 接管）。
func (d *Directory) UnregisterNode(ctx context.Context, node rans.NodeID) error {
	pipe := d.cli.Pipeline()
	pipe.Del(ctx, d.nodeKey(node))
	pipe.HDel(ctx, d.rosterKey(), strconv.Itoa(int(node)))
	_, err := pipe.Exec(ctx)
	return err
}

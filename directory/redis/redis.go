package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	goredis "github.com/redis/go-redis/v9"
)

// defaultHeartbeatTTL 判活键（成员键）存活期缺省（实体主键无 TTL——键值
// v3 主键永不清删）。
const defaultHeartbeatTTL = 2 * time.Minute

// Options 配置 Redis 目录。Addrs 为多地址形态（Redis Cluster 种子节点或
// 哨兵地址列表；非空时优先于 Addr，底层走 goredis UniversalClient 自动
// 按 MOVED 重定向/按哨兵发现分片）；单地址（Addr 或 Addrs 长度 1）行为
// 与普通客户端完全一致。⚠️ Cluster 模式的 DB 必须为 0（Redis 侧约束）。
// 目录三 Lua 是单键操作（逐实体 hash tag，键方案见 keys.go）——任意
// slot 合法，cluster 下无需集中；registry 面为单键命令族，同兼容。
// KeyPrefix 供测试注入独立前缀做键隔离（默认空）。
type Options struct {
	Addr  string
	Addrs []string // 多地址：cluster/哨兵种子列表；非空优先于 Addr
	// MasterName 哨兵主名（非空即哨兵 failover 形态——Addrs 为哨兵地址
	// 列表）。票 16：此前无此面，哨兵部署不可达。
	MasterName string
	Password   string
	// Username ACL 用户名（托管 Redis 的 ACL 形态；空 = 默认用户）。票 16。
	Username string
	DB       int
	PoolSize int
	// TLSConfig 传输加密（托管 Redis 常要求；nil = 明文）。票 16。
	TLSConfig *tls.Config
	// DialTimeout/ReadTimeout/WriteTimeout 连接与读写超时（≤0 取 go-redis
	// 缺省）。票 16。
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// Cluster 显式声明集群形态：true 时单种子地址也构造 ClusterClient
	//（go-redis 的 Universal 规则是 len(Addrs)>1 才进 cluster——单种子
	// cluster 会得到普通客户端、不跟随 MOVED）。票 16。
	Cluster bool
	// HeartbeatTTL 节点成员键存活期（过期阈值）；<=0 取默认 2min。
	HeartbeatTTL time.Duration
	// TombstoneTTL 实体墓碑可选存活期（票 17，opt-in）：0 = 永久保留
	//（缺省，现语义——墓碑是纪元单调性的锚点）；>0 时注销写的无主墓碑
	// 带 PX 过期（Register/Transfer 覆写时自动清除 TTL）。开启前提：必
	// 须存在纪元下限来源（Store 高水位或 FencingFloor——node.New 校
	// 验），否则墓碑过期即纪元回退（review14 A2 修复面回退）。
	TombstoneTTL time.Duration
	// KeyPrefix 键前缀（测试隔离注入；注意实际键名 = 前缀 + keys.go 的
	// rans: 字面——缺省空前缀时即 rans:，非空前缀会叠成 <p>rans:…）。
	KeyPrefix string
}

// Directory 是目录的 Redis 实现，提供全部能力面：
// 基础 Directory（合并端口）+ NodeRegistry。
//
// 并发约定（本类型全部导出方法共同契约，方法注释不再重复——spec §5.8）：
// 阻塞至 Redis 应答或 ctx 结束（耗尽返回包装 ctx.Err() 的错误；goredis
// 另有拨号/读写超时兜底）；并发安全（共享客户端串行化于连接池）；可在
// 实体 Handle 内调用（调用方自备预算）。
// Close 关闭连接池（阻塞至池排空，幂等）。
//
// 告警出口经 log 门面（库包不持 logger 实例）。
type Directory struct {
	cli          goredis.UniversalClient // 单地址=普通客户端；多地址=cluster/哨兵
	prefix       string
	heartbeatTTL time.Duration
	tombstoneTTL time.Duration // 墓碑 PX（0 = 永久）；票 17
}

// 编译期断言：合并端口目录 + 节点成员面。
var (
	_ directory.Directory    = (*Directory)(nil)
	_ directory.NodeRegistry = (*Directory)(nil)
)

// New 构造 Redis 目录。连接惰性建立：失败延迟到首个命令返回 err。
func New(opt Options) *Directory {
	hbTTL := opt.HeartbeatTTL
	if hbTTL <= 0 {
		hbTTL = defaultHeartbeatTTL
	}
	addrs := opt.Addrs
	if len(addrs) == 0 {
		addrs = []string{opt.Addr}
	}
	// 形态选择（票 16 写清三规则）：MasterName 非空 → 哨兵 failover；
	// Cluster 显式置位或 len(Addrs)>1 → ClusterClient（单种子 cluster 的
	// MOVED 跟随前提）；否则单地址普通客户端。ContextTimeoutEnabled 恒开
	//——「阻塞至 Redis 应答或 ctx 结束」的并发约定由此真正生效（此前
	// go-redis 不看 ctx，只受自身超时+重试，最坏十余秒）。
	uo := &goredis.UniversalOptions{
		Addrs:        addrs,
		MasterName:   opt.MasterName,
		Username:     opt.Username,
		Password:     opt.Password,
		DB:           opt.DB,
		PoolSize:     opt.PoolSize,
		TLSConfig:    opt.TLSConfig,
		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,
		// 票 16：ctx 预算约束 Redis IO（阻塞面受 ctx 截断）。
		ContextTimeoutEnabled: true,
	}
	var cli goredis.UniversalClient
	switch {
	case opt.MasterName != "":
		cli = goredis.NewFailoverClient(uo.Failover())
	case opt.Cluster || len(addrs) > 1:
		cli = goredis.NewClusterClient(uo.Cluster())
	default:
		cli = goredis.NewClient(uo.Simple())
	}
	return &Directory{
		cli:          cli,
		prefix:       opt.KeyPrefix,
		heartbeatTTL: hbTTL,
		tombstoneTTL: opt.TombstoneTTL,
	}
}

// Close 关闭底层连接池（目录不再使用时调用；接口面不含此方法）。
func (d *Directory) Close() error { return d.cli.Close() }

// entryValue 是主键值 v3（值格式版本号 v=3，双端同版本）：归属三元
// {node, epoch, inc} 由三个 Lua 以 string.match 按字段名解析（数值全量
// 匹配，无子串边界问题）。墓碑同构：node=0 即无主（epoch 随置零，inc
// 保留——纪元单调跨墓碑存续）。
func entryValue(node rans.NodeID, epoch, inc uint64) string {
	return fmt.Sprintf(`{"node":%d,"epoch":%d,"inc":%d,"v":3}`, node, epoch, inc)
}

// tombstoneValue 是无主墓碑（UnregisterIfOwner 的写入形态；inc 保留）。
func tombstoneValue(inc uint64) string { return entryValue(0, 0, inc) }

// entryJSON 对应 entryValue 的存储格式（解析失败由读面按 miss 处理——
// 非本实现的值/格式漂移不该让调用方崩，注册可覆写）。
type entryJSON struct {
	Node        rans.NodeID `json:"node"`
	Epoch       uint64      `json:"epoch"`
	Incarnation uint64      `json:"inc"`
	V           int         `json:"v"`
}

// parseEntry 解析主键值；非合法 JSON → (zero, false)（读面视同 miss）。
func parseEntry(val string) (entryJSON, bool) {
	var e entryJSON
	if json.Unmarshal([]byte(val), &e) != nil {
		return entryJSON{}, false
	}
	return e, true
}

// Register 注册/收养/接管原子域（luaRegister 单键四分支，KEYS/ARGV 见
// scripts.go；directory.Directory 契约）。主键无值/墓碑 → 按墓碑残值+1
// 写自归属；现值归属即本实例（node+epoch，含 epoch=0 未接代际条目的收养
// 形态）→ inc+1 升写调用方纪元（返回值=落库值）；expected 与现值全等 →
// CAS 接管 inc+1；否则输并返回现归属。self.Incarnation 作纪元下限：
// 赢分支取号 = max(现值, 下限)+1（0 = 无下限）。
func (d *Directory) Register(ctx context.Context, id rans.ID, self directory.Owner, expected *directory.Owner) (bool, directory.Owner, error) {
	var en, ee, ei uint64
	if expected != nil {
		en, ee, ei = uint64(expected.Node), expected.Epoch, expected.Incarnation
	}
	res, err := scriptRegister.Run(ctx, d.cli,
		[]string{d.primaryKey(id)},
		[]interface{}{uint16(self.Node), self.Epoch, en, ee, ei, self.Incarnation},
	).Result()
	if err != nil {
		return false, directory.Owner{}, err
	}
	code, second, third, fourth, err := parseQuadReply(res, "register")
	if err != nil {
		return false, directory.Owner{}, err
	}
	if code == 1 {
		owner := directory.Owner{Node: self.Node, Epoch: self.Epoch, Incarnation: second}
		return true, owner, nil
	}
	// 输：现归属（node/epoch/inc）
	return false, directory.Owner{Node: rans.NodeID(second), Epoch: third, Incarnation: fourth}, nil
}

// UnregisterIfOwner 实例级条件注销（luaUnregister，KEYS/ARGV 见 scripts.go；
// directory.Directory 契约）：仅当条目归属恰为 owner（三元全等）时改写
// 无主墓碑（inc 保留）；不符（新实例已接管/换代/已墓碑）→ false，不误删。
func (d *Directory) UnregisterIfOwner(ctx context.Context, id rans.ID, owner directory.Owner) (bool, error) {
	// 墓碑 PX（票 17）：TTL>0 时 ARGV[4] 携毫秒（0 = 永久，Lua 分支同
	// 语义）；Register/Transfer 的 SET 覆写天然清除 TTL。
	ttlMS := int64(d.tombstoneTTL / time.Millisecond)
	res, err := scriptUnregister.Run(ctx, d.cli,
		[]string{d.primaryKey(id)},
		[]interface{}{uint16(owner.Node), owner.Epoch, owner.Incarnation, ttlMS},
	).Int64()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// UnregisterNode 成员注销（H13 收敛形态；directory.NodeRegistry 契约）：
// epoch≠0 时仅当键内代际 == epoch 才删（单键比较删除 Lua；不匹配 no-op
// ——迟到的旧代际停机不误删已让位者的键，守卫拒绝时总表 field 一并
// 保留——总表是超集，漂移由读面探活收敛）；0 = 无条件。实体条目不清扫：
// 残留由死 owner 自愈承接（有效性判定 + CAS 接管）。
func (d *Directory) UnregisterNode(ctx context.Context, node rans.NodeID, epoch uint64) error {
	if epoch != 0 {
		guarded, err := scriptUnregNode.Run(ctx, d.cli,
			[]string{d.nodeKey(node)},
			[]interface{}{epoch},
		).Int64()
		if err != nil {
			return err
		}
		if guarded != 1 {
			return nil // 守卫拒绝：键已换代/缺席——零副作用
		}
	}
	pipe := d.cli.Pipeline()
	pipe.Del(ctx, d.nodeKey(node))
	pipe.HDel(ctx, d.rosterKey(), strconv.Itoa(int(node)))
	_, err := pipe.Exec(ctx)
	return err
}

// Lookup 读取归属位置（实例级 Owner 三元组）。miss/墓碑/值解析失败 →
// (zero,false,nil)（解析失败视同 miss：非本实现的值/格式漂移不该让调用方
// 崩，注册可覆写；墓碑 = 无主，对路由不可见）。
func (d *Directory) Lookup(ctx context.Context, id rans.ID) (directory.Owner, bool, error) {
	val, err := d.cli.Get(ctx, d.primaryKey(id)).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return directory.Owner{}, false, nil
		}
		return directory.Owner{}, false, err
	}
	e, ok := parseEntry(val)
	if !ok || e.Node == 0 {
		return directory.Owner{}, false, nil
	}
	return directory.Owner{Node: e.Node, Epoch: e.Epoch, Incarnation: e.Incarnation}, true, nil
}

// Transfer 实例级换锁（luaTransfer，KEYS/ARGV 见 scripts.go；
// directory.Directory 契约）：仅当条目归属恰为 expected（三元全等）时，
// 原子写目标归属（node+epoch）+升纪元（现值+1）。返回值语义：
//
//	(false, 0, Owner{}, nil)           条目不存在
//	(false, 0, current, ErrNotOwner)   归属不符（含墓碑——无主条目不可让渡）
//	(true, newInc, 新归属, nil)        成功：新纪元严格大于旧值（per-id 同序列）
func (d *Directory) Transfer(ctx context.Context, id rans.ID, expected, target directory.Owner) (bool, uint64, directory.Owner, error) {
	res, err := scriptTransfer.Run(ctx, d.cli,
		[]string{d.primaryKey(id)},
		[]interface{}{uint16(expected.Node), expected.Epoch, expected.Incarnation, uint16(target.Node), target.Epoch},
	).Result()
	if err != nil {
		return false, 0, directory.Owner{}, err
	}
	code, second, third, fourth, err := parseQuadReply(res, "transfer")
	if err != nil {
		return false, 0, directory.Owner{}, err
	}
	switch code {
	case 1:
		owner := directory.Owner{Node: target.Node, Epoch: target.Epoch, Incarnation: second}
		return true, second, owner, nil
	case -1:
		return false, 0, directory.Owner{Node: rans.NodeID(second), Epoch: third, Incarnation: fourth}, directory.ErrNotOwner
	case 0:
		return false, 0, directory.Owner{}, nil // 条目不存在
	default:
		return false, 0, directory.Owner{}, fmt.Errorf("rans: transfer: unexpected script result %d", code)
	}
}

// parseQuadReply 解析单键 Lua 的四元组回复 {code, a, b, c}。
func parseQuadReply(res interface{}, what string) (int64, uint64, uint64, uint64, error) {
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 4 {
		return 0, 0, 0, 0, fmt.Errorf("rans: %s: script reply %T(%v), want 4-element array", what, res, res)
	}
	vals := make([]uint64, 4)
	for i := 0; i < 4; i++ {
		n, ok := arr[i].(int64)
		if !ok {
			return 0, 0, 0, 0, fmt.Errorf("rans: %s: script reply %v, want integer elements", what, arr)
		}
		vals[i] = uint64(n)
	}
	return int64(vals[0]), vals[1], vals[2], vals[3], nil
}

// MemberTTL 成员键（判活键）存活期（Options.HeartbeatTTL 的读面——
// Liveness 自围栏缺省推导的基准）。
func (d *Directory) MemberTTL() time.Duration { return d.heartbeatTTL }

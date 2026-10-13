// chat 演示：同一进程内拉起两个 node 节点，经回环 TCP 走完整线协议——
// 节点 2 经 OneWay 发言，Call 拉取节点 1 上托管的聊天实体历史。不依赖
// testkit（测试基建）：目录 = 共享 inmem + 节点地址表（NodeAddrResolver
// 能力接口——redis 适配器同款的公开探测面，node 装配据此派生地址解析），
// 装配全部走 node.New + Client() 统一调用入口。实体面全由 ransgen 生成
// （rans_gen.go 入库）：请求类型/序列化器/wrapper 分发/客户端代理。
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cyinx/rans_golang"
	"github.com/cyinx/rans_golang/directory"
	"github.com/cyinx/rans_golang/directory/inmem"
	"github.com/cyinx/rans_golang/node"
)

//go:generate go run github.com/cyinx/rans_golang/cmd/ransgen

// chatMsg Say 的载荷消息：字段带 rans:"order=N" tag 即进序列化面，
// 编解码器由 ransgen 生成注册（不再手写）。
type chatMsg struct {
	Text string `rans:"order=1"`
}

// HistoryReply History 的应答消息：[]string 字段按序列线型编码
// （WireSequence of String，规格 §7.3）。
type HistoryReply struct {
	Items []string `rans:"order=1"`
}

// Room 聊天实体（kind=chat）：Say 追加历史，History 返回全量。
// 用户实体不实现 rans.Entity——伴生 wrapper RoomPawnite 由生成器产出，
// 承担 Activate/Deactivate/Handle 分发（本实体无生命周期逻辑）。
//
//rans:kind=chat
type Room struct {
	msgs []string // 本地状态：无 order tag，不进消息面
}

// Say OneWay 形态（无返回值）：发言入箱即返回，无应答无回执。
func (r *Room) Say(ctx context.Context, m chatMsg) {
	r.msgs = append(r.msgs, m.Text)
}

// History (T, error) 形态：返回全量历史。
func (r *Room) History(ctx context.Context) (HistoryReply, error) {
	return HistoryReply{Items: r.msgs}, nil
}

// chatDir 双节点演示目录：注册/查找委托共享 inmem（同进程同实例——激活
// 仲裁与目录归属即刻互见），节点地址解析经 NodeAddrResolver 能力接口
// （node.resolver 对注入目录做能力探测，redis 适配器同款公开面；inmem
// 单节点形态缺省自解析，多节点必须提供跨节点解析）。地址表在两节点
// Start 后一次性回填（解析函数 New 期定死，读面经原子指针取最新表——
// testkit 同模式）。
type chatDir struct {
	directory.Directory
	addrs atomic.Pointer[map[rans.NodeID]string]
}

func newChatDir() *chatDir { return &chatDir{Directory: inmem.New()} }

// NodeAddrResolver 节点号→地址解析（能力接口，node 装配根探测消费；ctx
// 贯穿——表读取瞬时完成，ctx 仅随签名约定保留）。
func (d *chatDir) NodeAddrResolver() func(ctx context.Context, n rans.NodeID) (string, error) {
	return func(_ context.Context, n rans.NodeID) (string, error) {
		if m := d.addrs.Load(); m != nil {
			if addr, ok := (*m)[n]; ok {
				return addr, nil
			}
		}
		return "", fmt.Errorf("chat: node %d unknown", n)
	}
}

func (d *chatDir) set(m map[rans.NodeID]string) { d.addrs.Store(&m) }

func main() {
	dir := newChatDir()

	mkNode := func(id rans.NodeID) *node.Node {
		n, err := node.New(node.Config{
			NodeID: id, ClusterName: "chat-demo", ListenAddr: "127.0.0.1:0",
			// 演示无密钥运行（fail-closed 缺省须显式声明）；真实部署经
			// node.LoadHMACKeyFromEnv/File 配钥。
			Security:  node.SecurityConfig{Insecure: true},
			Kinds:     Kinds,
			Directory: node.DirectoryConfig{Injected: dir},
			// 形状维无需配置接线：生成物 init 把 RansShapes 登记进 codec
			// 全量形状集，codec.SchemaDigest() 随握手观测。
		})
		if err != nil {
			panic(err)
		}
		// 例外①（进程入口）：main 是进程根，节点根 ctx 由 Start 内部自建
		// ——§5.1 第 2 条管辖库代码，main 不在列。
		if err := n.Start(context.Background()); err != nil {
			panic(err)
		}
		return n
	}
	node1 := mkNode(1)
	node2 := mkNode(2)
	defer node1.Close()
	defer node2.Close()
	dir.set(map[rans.NodeID]string{1: node1.Addr(), 2: node2.Addr()})

	// 调用方必须传带 deadline 的 ctx（无 deadline 的 Call 在吞 panic 场景
	// 会无限阻塞——决策出处见 examples/counter）。
	ctx, cancel := context.WithTimeout(context.Background() /* 例外①（进程入口）：演示进程的显式预算 */, 10*time.Second)
	defer cancel()
	roomID, err := rans.NewID("chat", "room-1")
	if err != nil {
		panic(err)
	}

	// 节点 1 先激活实体（本地路径，目录自此指向节点 1）：统一调用面
	//（node.Client()）作生成 RoomClient 的唯一耦合字段。
	node1p := RoomClient{C: node1.Client()}
	if _, err := node1p.History(ctx, roomID); err != nil {
		panic(err)
	}
	// 跨节点调用：OneWay 远程路径——与后续 Call 请求帧同箱同分片（同目标
	// FIFO，单 worker 串行写出），帧序即写出序，输出确定。
	p := RoomClient{C: node2.Client()}
	for _, text := range []string{"hello", "world"} {
		if err := p.Say(ctx, roomID, chatMsg{Text: text}); err != nil {
			panic(err)
		}
		fmt.Printf("[node2] sent: %s\n", text)
	}
	// 跨节点 Call：目录命中节点 1，应答 HistoryReply 经生成序列化器还值。
	hist, err := p.History(ctx, roomID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[node2] history = %v\n", hist.Items)
}

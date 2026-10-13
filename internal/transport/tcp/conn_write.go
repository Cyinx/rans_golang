package tcp

import (
	"bufio"
	"context"
	"fmt"

	"github.com/cyinx/rans_golang/internal/protocol"
	"github.com/cyinx/rans_golang/internal/transport"
	"github.com/cyinx/rans_golang/internal/wire"
)

// enqueue 内部控制面出站入队（握手/心跳应答——绕过 Send 的 pre-auth
// 门禁，控制面帧本身即门禁放行对象）。走优先通道：读循环的应答帧不被
// 数据背压堵在队尾（对端停读积压本端数据队列时，Pong 必达是心跳判活
// 与读路径存活的前提）。控道满（64 帧控制面积压，写循环优先消费下
// 不可能持续）则阻塞让行退役/关闭——此时连接已不可用，阻塞等终态即可。
// 退役后的迟到控制面帧（Pong 等）直接丢弃：连接终局已由存活连接接管，
// 控制面帧无人写出（writeLoop 或已按退役收尾）。
func (c *Conn) enqueue(f outFrame) bool {
	select {
	case c.ctl <- f:
		return true
	case <-c.retired:
		if f.body != nil {
			c.pool.Put(f.body)
		}
		return false
	case <-c.closed:
		if f.body != nil {
			c.pool.Put(f.body)
		}
		return false
	}
}

// writeLoop 出站主循环：控制优先 chan + 数据有界 chan → bufio（64KiB）
// 批量写。控制帧优先预检（教训：Go select 双就绪随机选择，「控制信号
// vs 数据」的 select 必须控制信号先行预检）——每轮先非阻塞取尽控道，
// 再进主 select；控道帧写完立即回到预检，数据积压不得插队。冲刷策略：
// 写完一帧后两条队列已空则 Flush，仍有积压则继续合帧——channel 积压
// 自然承担批量发送语义。每次冲刷经 onFlush 上报本批帧数（观测面；帧数
// = 自上次冲刷起写出的全部帧，控制帧计入批量）。
// 退役分支（同节点顶替败者的零丢失收尾）：retired 关闭后排空两条队列
// 余量、退役通知帧殿后、冲刷、半关收尾——新帧此时已不可能入队（Retire
// 的互斥封队协议），「队列空」即真正终态，Send 已返回成功（入队）的帧
// 无一丢弃。
// 任一退出路径（写/冲刷错误或关闭）都经 defer 排空两条出站队列：滞留帧
// 的页归还池，保持 Get/Put 收支锚点（错误退出同样不留滞留页）。
func (c *Conn) writeLoop() {
	bw := bufio.NewWriterSize(c.nc, writeBufSize)
	defer close(c.retireDrained) // 已入队帧处置完毕（正常/退役/错误退出全覆盖）
	defer c.drainOut()
	batch := 0 // 本次冲刷周期内已写出的帧数
	for {
		select { // 控制帧优先预检：数据背压积压时控道帧不排队尾
		case f := <-c.ctl:
			var live bool
			if batch, live = c.writeQueued(bw, f, batch); !live {
				return
			}
			continue // 控道可能连发：预检循环到控道排空再回主 select
		default:
		}
		select {
		case f := <-c.ctl:
			var live bool
			if batch, live = c.writeQueued(bw, f, batch); !live {
				return
			}
		case f := <-c.out:
			var live bool
			if batch, live = c.writeQueued(bw, f, batch); !live {
				return
			}
		case <-c.retired:
			c.retireDrain(bw) // 成败皆退出（错误路径已 shutdown）
			return
		case <-c.closed:
			return
		}
	}
}

// writeQueued 写出一帧并在两条队列同时排空时冲刷（返回 live=false = 写
// 路径已终态，调用方立即退出）。控制帧计入本冲刷周期批量——帧数语义
// 不变：自上次冲刷起写出的全部帧。
func (c *Conn) writeQueued(bw *bufio.Writer, f outFrame, batch int) (int, bool) {
	if err := c.writeFrame(bw, f); err != nil {
		c.shutdown(err)
		return batch, false
	}
	batch++
	if len(c.out) == 0 && len(c.ctl) == 0 {
		if err := bw.Flush(); err != nil {
			c.shutdown(err)
			return batch, false
		}
		if c.onFlush != nil {
			c.onFlush(batch)
		}
		batch = 0
	}
	return batch, true
}

// retireDrain 退役排空（writeLoop 退役分支）：控道与数据道余量全部写出
// （Send 已返回成功的帧无一丢弃——零丢失的承重点），退役通知帧殿后
// （排空之后、FIN 之前：对端据此把本 socket 其副本的死亡按抑制语义收
// 场，不误报节点断连），冲刷后半关收尾。写失败即 shutdown，调用方
// 随后退出（错误与正常路径对 writeLoop 等价——都到终态）。
func (c *Conn) retireDrain(bw *bufio.Writer) {
	for {
		select {
		case f := <-c.ctl:
			if err := c.writeFrame(bw, f); err != nil {
				c.shutdown(err)
				return
			}
			continue
		default:
		}
		select {
		case f := <-c.out:
			if err := c.writeFrame(bw, f); err != nil {
				c.shutdown(err)
				return
			}
		default:
			if err := c.writeFrame(bw, outFrame{hdr: wire.Header{Type: protocol.MsgRetire}}); err != nil {
				c.shutdown(err)
				return
			}
			if err := bw.Flush(); err != nil {
				c.shutdown(err)
				return
			}
			c.retireFinish()
			return
		}
	}
}

// writeFrame 编码并写出一帧。EncodeFrame 返回时 body 已拷入整帧缓冲，
// 页即刻归还（不必等写出：bufio.Write 再拷入自身缓冲，页无引用）。
// 编码失败（超限）同样终结 body 所有权后返回错误——上层序列化本就
// 受帧上限约束，超限属调用方违约，连接关闭并在日志留痕。
func (c *Conn) writeFrame(bw *bufio.Writer, f outFrame) error {
	frame, err := wire.EncodeFrame(f.hdr.Encode(), f.body)
	if f.body != nil {
		c.pool.Put(f.body)
	}
	if err != nil {
		return err
	}
	_, err = bw.Write(frame)
	return err
}

// drainOut 关闭后排空两条出站队列：未写出的帧归还页，保持池收支平衡。
func (c *Conn) drainOut() {
	for {
		select {
		case f := <-c.out:
			if f.body != nil {
				c.pool.Put(f.body)
			}
			continue
		default:
		}
		select {
		case f := <-c.ctl:
			if f.body != nil {
				c.pool.Put(f.body)
			}
		default:
			return
		}
	}
}

// Send 实现 transport.Conn。出站侧 pre-auth 门禁：未就绪时仅控制面
// 消息可发，应用消息返回 transport.ErrNotReady。控制面帧入优先通道、
// 数据帧入有界队列；队列满则阻塞——直到腾出空间、连接关闭或 ctx 到期
// （返回包装 ctx.Err() 的错误，非永久阻塞）；已关连接返回错误；退役中
// 连接返回 ErrRetiring（同节点顶替败者——重试经 byNode 落到存活连接）。
// 队列有位时即刻入队，即便 ctx 已到期（ctx 只辖满队等待段）。body 所有权
// 随成功返回移交本连接（写循环写出后归还页池），失败返回时所有权不移交。
//
// 入队与 Retire 的互斥协议（零丢失承重）：入队决策在 retireMu 内与
// retiredFlag 串行——Retire 置标志后不可能再有新帧入队，writeLoop 的
// 退役排空据此以「队列空」为真正终态。满队阻塞持锁是良性的：writeLoop
// 排空（不取 retireMu）腾出空位即解阻塞，Retire 亦被让行，无环；ctx
// 到期同样解阻塞，等待有界。
func (c *Conn) Send(ctx context.Context, hdr wire.Header, body []byte) error {
	if !c.ready.Load() && !protocol.IsControlPlane(hdr.Type) {
		return transport.ErrNotReady
	}
	select {
	case <-c.closed: // 先行终态检查：已关连接不再接收帧
		return c.sendErr
	default:
	}
	ch := c.out
	if protocol.IsControlPlane(hdr.Type) {
		ch = c.ctl
	}
	c.retireMu.Lock()
	if c.retiredFlag {
		c.retireMu.Unlock()
		return ErrRetiring
	}
	select { // 快路径：有位即刻入队（ctx 已到期不拦可同步完成的发送）
	case ch <- outFrame{hdr: hdr, body: body}:
		c.retireMu.Unlock()
		return nil
	default:
	}
	select { // 慢路径：满队阻塞，ctx/关闭/退役终态任一解除
	case ch <- outFrame{hdr: hdr, body: body}:
		c.retireMu.Unlock()
		return nil
	case <-ctx.Done():
		c.retireMu.Unlock()
		return fmt.Errorf("rans: conn: send: %w", ctx.Err())
	case <-c.closed:
		c.retireMu.Unlock()
		return c.sendErr
	}
}

// TrySend 实现 transport.Conn 的非阻塞形态（ok 语义）：满即 false 不
// 阻塞，可能因竞争不生效——pre-auth 门禁拒绝、已关、退役、队满一律
// false，调用方据此丢弃或换路径；false 时所有权不移交。
func (c *Conn) TrySend(hdr wire.Header, body []byte) bool {
	if !c.ready.Load() && !protocol.IsControlPlane(hdr.Type) {
		return false
	}
	select {
	case <-c.closed:
		return false
	default:
	}
	ch := c.out
	if protocol.IsControlPlane(hdr.Type) {
		ch = c.ctl
	}
	c.retireMu.Lock()
	defer c.retireMu.Unlock()
	if c.retiredFlag {
		return false
	}
	select {
	case ch <- outFrame{hdr: hdr, body: body}:
		return true
	default:
		return false
	}
}

// pingBestEffort 尽力发一拍心跳 Ping（Manager 心跳环专用）：控道满则
// 跳过本拍——心跳环不可被背压阻塞，控制面优先通道 64 帧积压本身即
// 写路径告急的信号，留待下一拍（持续积压最终由 pong 超时踢除收口）。
func (c *Conn) pingBestEffort() {
	select {
	case c.ctl <- outFrame{hdr: wire.Header{Type: protocol.MsgPing}}:
	case <-c.closed:
	default:
	}
}

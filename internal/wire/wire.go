// Package wire 实现 Rans 线协议序列化内核（规格 §7）：帧与消息头、握手、
// 信封与页池。线编码原语（Writer/Reader/field-tag 对象）与值注册表在
// codec 包。
package wire

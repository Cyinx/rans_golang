package main

import (
	"bytes"
	"fmt"
	"go/format"
	"text/template"
)

// Render 以 tmpl 渲染生成模型，输出经 go/format.Source（gofmt）格式化——
// 幂等根基：模型定序（planGen 内 normalize）+ gofmt 归一，同一输入
// （无论发现时的遍历序）得到字节确定的生成物。
func Render(data any, tmpl *template.Template) ([]byte, error) {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("ransgen: execute template: %w", err)
	}
	out, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("ransgen: format generated source: %w\n--- raw output ---\n%s", err, buf.Bytes())
	}
	return out, nil
}

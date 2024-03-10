package slogclickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"log/slog"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	slogcommon "github.com/samber/slog-common"
)

type Option struct {
	// log level (default: debug)
	Level slog.Leveler

	// Kafka Writer
	DB       *sql.DB
	LogTable string
	Timeout  time.Duration // default: 60s

	// optional: customize Kafka event builder
	Converter Converter

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}

func (o Option) NewClickHouseHandler() slog.Handler {
	if o.Level == nil {
		o.Level = slog.LevelDebug
	}

	if o.DB == nil {
		panic("missing clickhouse db connection")
	}

	if o.LogTable == "" {
		panic("missing log table name")
	}

	if o.Timeout == 0 {
		o.Timeout = 60 * time.Second
	}

	if o.Converter == nil {
		o.Converter = DefaultConverter
	}

	return &ClickHouseHandler{
		option: o,
		attrs:  []slog.Attr{},
		groups: []string{},
	}
}

var _ slog.Handler = (*ClickHouseHandler)(nil)

type ClickHouseHandler struct {
	option Option
	attrs  []slog.Attr
	groups []string
}

func (h *ClickHouseHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.option.Level.Level()
}

func (h *ClickHouseHandler) Handle(ctx context.Context, record slog.Record) error {
	payload := h.option.Converter(h.option.AddSource, h.option.ReplaceAttr, h.attrs, h.groups, &record)

	return h.saveToDB(record.Time, record, payload)
}

func (h *ClickHouseHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ClickHouseHandler{
		option: h.option,
		attrs:  slogcommon.AppendAttrsToGroup(h.groups, h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *ClickHouseHandler) WithGroup(name string) slog.Handler {
	return &ClickHouseHandler{
		option: h.option,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}

func (h *ClickHouseHandler) saveToDB(timestamp time.Time, record slog.Record, payload map[string]any) error {
	level := record.Level.String()
	message := record.Message

	sql := `INSERT INTO ` + h.option.LogTable + ` (timestamp, level, message, attrs) VALUES (?, ?, ?, ?)`

	// 使用clickhpouse-go插入数据
	values, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	_, err = h.option.DB.Exec(sql, timestamp, level, message, values)

	return err
}

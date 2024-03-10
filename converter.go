package slogclickhouse

import (
	"net/http"

	"log/slog"

	slogcommon "github.com/samber/slog-common"
)

var SourceKey = "source"
var ContextKey = "extra"
var RequestKey = "request"
var ErrorKeys = []string{"error", "err"}
var RequestIgnoreHeaders = false

// Converter is a function that converts a log record to a map of fields.
type Converter func(addSource bool, replaceAttr func(groups []string, a slog.Attr) slog.Attr, loggerAttr []slog.Attr, groups []string, record *slog.Record) map[string]any

// DefaultConverter is the default converter used by the handler.
func DefaultConverter(addSource bool, replaceAttr func(groups []string, a slog.Attr) slog.Attr, loggerAttr []slog.Attr, groups []string, record *slog.Record) map[string]any {
	// aggregate all attributes
	attrs := slogcommon.AppendRecordAttrsToAttrs(loggerAttr, groups, record)

	// developer formatters
	if addSource {
		attrs = append(attrs, slogcommon.Source(SourceKey, record))
	}
	attrs = slogcommon.ReplaceAttrs(replaceAttr, []string{}, attrs...)

	// handler formatter
	extra := slogcommon.AttrsToMap(attrs...)

	payload := map[string]any{}

	for _, errorKey := range ErrorKeys {
		if v, ok := extra[errorKey]; ok {
			if err, ok := v.(error); ok {
				payload[errorKey] = slogcommon.FormatError(err)
				delete(extra, errorKey)
				break
			}
		}
	}

	if v, ok := extra[RequestKey]; ok {
		if req, ok := v.(*http.Request); ok {
			payload[RequestKey] = slogcommon.FormatRequest(req, RequestIgnoreHeaders)
			delete(extra, RequestKey)
		}
	}

	payload[ContextKey] = extra

	return payload
}

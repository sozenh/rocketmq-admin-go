package mqadmin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var numericKeyPattern = regexp.MustCompile(`([\{,])([0-9]+):`)

func decodeRouteData(body []byte) (*routeData, error) {
	route := &routeData{}
	if err := json.Unmarshal(body, route); err == nil {
		return route, nil
	}
	patched := []byte(numericKeyPattern.ReplaceAllString(string(body), `${1}"${2}":`))
	if err := json.Unmarshal(patched, route); err != nil {
		preview := string(patched)
		if len(preview) > 260 {
			preview = preview[:260]
		}
		return nil, fmt.Errorf("mqadmin: decode topic route failed: %w, body=%s", err, preview)
	}
	return route, nil
}

func decodeTopicStatsTable(body []byte) (TopicStatsTable, error) {
	table := TopicStatsTable{}
	if err := json.Unmarshal(body, &table); err == nil {
		if table.OffsetTable == nil {
			table.OffsetTable = map[string]any{}
		}
		return table, nil
	}
	patched := []byte(numericKeyPattern.ReplaceAllString(string(body), `${1}"${2}":`))
	if err := json.Unmarshal(patched, &table); err == nil {
		if table.OffsetTable == nil {
			table.OffsetTable = map[string]any{}
		}
		return table, nil
	}

	entries, err := parseFieldEntries(patched, "offsetTable")
	if err != nil {
		return TopicStatsTable{OffsetTable: map[string]any{"__raw__": string(patched)}}, nil
	}
	out := map[string]any{}
	for k, v := range entries {
		val := map[string]any{}
		if uErr := json.Unmarshal(v, &val); uErr != nil {
			out[k] = string(v)
		} else {
			out[k] = val
		}
	}
	return TopicStatsTable{OffsetTable: out}, nil
}

func decodeResetOffsetBody(body []byte) map[string]any {
	m := map[string]any{}
	if err := json.Unmarshal(body, &m); err == nil {
		return m
	}
	patched := []byte(numericKeyPattern.ReplaceAllString(string(body), `${1}"${2}":`))
	if err := json.Unmarshal(patched, &m); err == nil {
		return m
	}
	entries, err := parseFieldEntries(patched, "offsetTable")
	if err != nil {
		return map[string]any{"__raw__": string(patched)}
	}
	offsetTable := map[string]any{}
	for k, v := range entries {
		var n int64
		if uErr := json.Unmarshal(v, &n); uErr == nil {
			offsetTable[k] = n
			continue
		}
		offsetTable[k] = string(v)
	}
	return map[string]any{"offsetTable": offsetTable}
}

func parseFieldEntries(body []byte, field string) (map[string]json.RawMessage, error) {
	needle := `"` + field + `":`
	idx := strings.Index(string(body), needle)
	if idx < 0 {
		return nil, fmt.Errorf("field %s not found", field)
	}
	start := idx + len(needle)
	for start < len(body) && (body[start] == ' ' || body[start] == '\n' || body[start] == '\t' || body[start] == '\r') {
		start++
	}
	if start >= len(body) || body[start] != '{' {
		return nil, fmt.Errorf("field %s not object", field)
	}
	end, err := matchBracket(body, start, '{', '}')
	if err != nil {
		return nil, err
	}
	content := bytes.TrimSpace(body[start+1 : end])
	out := map[string]json.RawMessage{}

	for len(content) > 0 {
		content = bytes.TrimSpace(content)
		if len(content) == 0 {
			break
		}

		var key string
		var consumed int
		switch content[0] {
		case '{':
			kEnd, e := matchBracket(content, 0, '{', '}')
			if e != nil {
				return nil, e
			}
			key = string(content[:kEnd+1])
			consumed = kEnd + 1
		case '"':
			kEnd := 1
			for kEnd < len(content) {
				if content[kEnd] == '"' && content[kEnd-1] != '\\' {
					break
				}
				kEnd++
			}
			if kEnd >= len(content) {
				return nil, fmt.Errorf("invalid quoted key")
			}
			key = string(content[1:kEnd])
			consumed = kEnd + 1
		default:
			return nil, fmt.Errorf("invalid key start: %c", content[0])
		}

		rest := bytes.TrimSpace(content[consumed:])
		if len(rest) == 0 || rest[0] != ':' {
			return nil, fmt.Errorf("missing colon after key")
		}
		rest = bytes.TrimSpace(rest[1:])
		if len(rest) == 0 {
			return nil, fmt.Errorf("missing value")
		}

		valConsumed := 0
		switch rest[0] {
		case '{':
			vEnd, e := matchBracket(rest, 0, '{', '}')
			if e != nil {
				return nil, e
			}
			valConsumed = vEnd + 1
		case '[':
			vEnd, e := matchBracket(rest, 0, '[', ']')
			if e != nil {
				return nil, e
			}
			valConsumed = vEnd + 1
		default:
			for valConsumed < len(rest) && rest[valConsumed] != ',' {
				valConsumed++
			}
		}
		value := bytes.TrimSpace(rest[:valConsumed])
		out[key] = append([]byte(nil), value...)

		next := bytes.TrimSpace(rest[valConsumed:])
		if len(next) > 0 && next[0] == ',' {
			next = next[1:]
		}
		content = next
	}

	return out, nil
}

func matchBracket(data []byte, start int, open, close byte) (int, error) {
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(data); i++ {
		ch := data[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		if ch == '"' {
			inString = true
			continue
		}
		if ch == open {
			depth++
		} else if ch == close {
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("unbalanced brackets")
}

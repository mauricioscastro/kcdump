package yjq

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	gjs "github.com/goccy/go-json"
	"github.com/itchyny/gojq"
	"github.com/itchyny/json2yaml"
	"gopkg.in/yaml.v3"

	"github.com/mikefarah/yq/v4/pkg/yqlib"

	"gopkg.in/op/go-logging.v1"
)

type EvalFunc func(string, string, ...any) (string, error)

var (
	yamlPrefs = yqlib.YamlPreferences{
		Indent:                      2,
		ColorsEnabled:               false,
		LeadingContentPreProcessing: true,
		PrintDocSeparators:          true,
		UnwrapScalar:                true,
		EvaluateTogether:            true,
	}
	jsonPrefs = yqlib.JsonPreferences{
		Indent:        2,
		ColorsEnabled: false,
		UnwrapScalar:  true,
	}
	jsonPrefsCompact = yqlib.JsonPreferences{
		Indent:        -1,
		ColorsEnabled: false,
		UnwrapScalar:  true,
	}
)

func SilenceYqLogs() {
	bke := logging.NewLogBackend(io.Discard, "", 0)
	bkel := logging.AddModuleLevel(bke)
	yqlib.GetLogger().SetBackend(bkel)
}

func Y2JC(yaml string) (string, error) {
	yamlDecoder := yqlib.NewYamlDecoder(yamlPrefs)
	compactJsonEncoder := yqlib.NewJSONEncoder(jsonPrefsCompact)
	return yqEval(".", yaml, compactJsonEncoder, yamlDecoder)
}

func Y2JP(yaml string) (string, error) {
	yamlDecoder := yqlib.NewYamlDecoder(yamlPrefs)
	prettyJsonEncoder := yqlib.NewJSONEncoder(jsonPrefs)
	return yqEval(".", yaml, prettyJsonEncoder, yamlDecoder)
}

func J2JP(json string) (string, error) {
	prettyJsonEncoder := yqlib.NewJSONEncoder(jsonPrefs)
	return yqEval(".", json, prettyJsonEncoder, yqlib.NewJSONDecoder())
}

func J2Y(json string) (string, error) {
	var sb strings.Builder
	if err := json2yaml.Convert(&sb, strings.NewReader(json)); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func YqEval(expr string, yaml string, param ...any) (string, error) {
	yamlEncoder := yqlib.NewYamlEncoder(yamlPrefs)
	yamlDecoder := yqlib.NewYamlDecoder(yamlPrefs)
	return yqEval(expr, yaml, yamlEncoder, yamlDecoder, param...)
}

func YqEval2JC(expr string, yaml string, param ...any) (string, error) {
	yamlDecoder := yqlib.NewYamlDecoder(yamlPrefs)
	compactJsonEncoder := yqlib.NewJSONEncoder(jsonPrefsCompact)
	return yqEval(expr, yaml, compactJsonEncoder, yamlDecoder, param...)
}

func YqEvalJ2JC(expr string, json string, param ...any) (string, error) {
	compactJsonEncoder := yqlib.NewJSONEncoder(jsonPrefsCompact)
	return yqEval(expr, json, compactJsonEncoder, yqlib.NewJSONDecoder(), param...)
}

func YqEvalJ2JP(expr string, json string, param ...any) (string, error) {
	prettyJsonEncoder := yqlib.NewJSONEncoder(jsonPrefs)
	return yqEval(expr, json, prettyJsonEncoder, yqlib.NewJSONDecoder(), param...)
}

func YqEvalJ2Y(expr string, json string, param ...any) (string, error) {
	yamlEncoder := yqlib.NewYamlEncoder(yamlPrefs)
	return yqEval(expr, json, yamlEncoder, yqlib.NewJSONDecoder(), param...)
}

func yqEval(expr string, input string, encoder yqlib.Encoder, decoder yqlib.Decoder, param ...any) (string, error) {
	res, err := yqlib.NewStringEvaluator().Evaluate(fmt.Sprintf(expr, param...), input, encoder, decoder)
	if err == nil {
		res = strings.TrimSuffix(res, "\n")
	}
	return res, err
}

func Eval2Int(evalFunc EvalFunc, expr string, input string, param ...any) (int, error) {
	if l, e := evalFunc(expr, input, param...); e != nil {
		return 0, e
	} else if len(l) == 0 {
		return 0, nil
	} else {
		return strconv.Atoi(l)
	}
}

func Eval2List(evalFunc EvalFunc, expr string, input string, param ...any) ([]string, error) {
	l, e := evalFunc(expr, input, param...)
	return strings.Split(l, "\n"), e
}

func JqEval(expr string, input string, param ...any) (string, error) {
	return jq(expr, input, false, param...)
}

func JqEval2Y(expr string, input string, param ...any) (string, error) {
	return jq(expr, input, true, param...)
}

func EscapeJsonEncodedStrings(input string) (string, error) {
	escapeEncodedString = true
	defer func() { escapeEncodedString = false }()
	return jq(".", input, false)
}

func jq(expr string, json string, yamlOutput bool, param ...any) (string, error) {
	query, err := gojq.Parse(fmt.Sprintf(expr, param...))
	if err != nil {
		return "", err
	}
	code, err := gojq.Compile(query)
	if err != nil {
		return "", err
	}
	var jsonIn any
	if len(json) == 0 {
		jsonIn = nil
	} else {
		jsonBytes := bytes.TrimLeft([]byte(json), " \t\r\n")
		if len(jsonBytes) > 0 {
			if jsonBytes[0] == '[' {
				jsonIn = make([]any, 1)
			} else if jsonBytes[0] == '{' {
				jsonIn = make(map[string]interface{})
			}
		}
		err = gjs.Unmarshal(jsonBytes, &jsonIn)
		if err != nil {
			return "", fmt.Errorf("JqEval Unmarshal: %w", err)
		}
	}
	var out bytes.Buffer
	iter := code.Run(jsonIn)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return "", err
		}
		if out.Len() > 0 {
			out.WriteString("\n")
		}
		if reflect.TypeOf(v).Kind() == reflect.String {
			out.Write([]byte(v.(string)))
		} else {
			if yamlOutput {
				err = yamlEnc(v, &out)
			} else {
				err = jsonEnc(v, &out)
			}
		}
		if err != nil {
			return "", err
		}
	}
	return string(bytes.TrimRight(out.Bytes(), "\n")), nil
}

func jsonEnc(v any, w io.Writer) error {
	if r, e := Marshal(v); e != nil {
		return e
	} else {
		w.Write(r)
	}
	return nil
}

func yamlEnc(v any, w io.Writer) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	if e := enc.Encode(v); e != nil {
		return e
	}
	return enc.Close()
}

package util

import (
	"os"
	"strings"

	"github.com/mauricioscastro/hcreport/pkg/yjq"
	"github.com/rwtodd/Go.Sed/sed"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"gopkg.in/yaml.v3"
)

func ValidateJson(yamlInput string, jsonSchemaAsYaml string) error {
	var (
		input        map[string]interface{}
		schemaString string
		schema       *jsonschema.Schema
		err          error
	)
	if err = yaml.Unmarshal([]byte(yamlInput), &input); err != nil {
		return err
	}
	if schemaString, err = yjq.Y2JC(jsonSchemaAsYaml); err != nil {
		return err
	}
	compiler := jsonschema.NewCompiler()
	if err = compiler.AddResource("schema.json", strings.NewReader(schemaString)); err != nil {
		return err
	}
	if schema, err = compiler.Compile("schema.json"); err != nil {
		return err
	}
	if err = schema.Validate(input); err != nil {
		return err
	}
	return err
}

func GetEnv(k string, d string) string {
	if env := os.Getenv(k); env == "" {
		return d
	} else {
		return env
	}
}

func Sed(expr string, input string) (string, error) {
	r, e := sed.New(strings.NewReader(expr))
	if e != nil {
		return "", e
	}
	s, e := r.RunString(input)
	if e != nil {
		return "", e
	}
	return s, e
}

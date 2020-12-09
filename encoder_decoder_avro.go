package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	logger "github.com/disturb16/apilogger"
	"github.com/linkedin/goavro"
)

var (
	// TypeMap maps go types to avro types
	// k: go type, v: avro type
	typeMap = map[string]string{
		"string":  "string",
		"int":     "int",
		"int32":   "int",
		"int64":   "long",
		"float32": "float",
		"float64": "double",
		"[]uint8": "bytes"}
)

// AvroEncoderDecoder enhances the capabilities of the linkedin/goavro
// package - it helps transform avro-formatted kafka messages between
// struct and binary form in relation to an avro schema
type avroEncoderDecoder struct {
	schemaReg schemaRegistry
}

// NewAvroEncDec constructs and returns a new avro EncoderDecoder
func newAvroEncDec(s schemaRegistry) encoderDecoder {
	return &avroEncoderDecoder{schemaReg: s}
}

// GetCodec returns a Codec for the given topic, which can use the
// topic's schema to transform data from, and intended for, the topic
func (a avroEncoderDecoder) GetCodec(
	ctx context.Context, topic string) (c codec, e error) {

	lg := logger.New(ctx, "")

	schema, schemaID, e := a.schemaReg.GetSchemaByTopic(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	codec, e := goavro.NewCodec(schema)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return &avroCodec{
		codec:    codec,
		schema:   schema,
		schemaID: schemaID}, nil
}

// AvroCodec contains data structures necessary to
// transform data from, and intended for, the topic
type avroCodec struct {
	schemaID int
	schema   string
	codec    *goavro.Codec
}

// BinaryToNative converts a binary message to a native go struct
func (c avroCodec) BinaryToNative(
	ctx context.Context, b []byte, ptr interface{}) (e error) {

	lg := logger.New(ctx, "")

	rv := reflect.ValueOf(ptr)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		e = errors.New("pointer to struct required")
		lg.Error(logger.LogCatUncategorized, e)
		return
	}
	rv = rv.Elem()

	data, e := c.binaryToMap(ctx, b)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	fields, e := getFieldMap(ctx, rv)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	for k, v := range data {
		v, ok := v.(map[string]interface{})
		if !ok {
			e = errors.New("problem with message format")
			lg.Error(logger.LogCatUncategorized, e)
			return
		}

		fieldInfo := fields[k]
		mapKey := fieldInfo.AvroType
		mapVal := v[mapKey]

		f := rv.FieldByName(fieldInfo.Name)

		targetFieldType := f.Type()
		mapValType := reflect.TypeOf(mapVal)

		if targetFieldType != mapValType {
			e = fmt.Errorf("struct field [%s], currently type %s, should be type %s to match binary data",
				fieldInfo.Name, targetFieldType, mapValType)
			lg.Error(logger.LogCatUncategorized, e)
			return
		}

		if f.IsValid() && f.CanSet() {
			f.Set(reflect.ValueOf(mapVal))
		}
	}

	return
}

// NativeToBinary converts a native go struct to binary avro data
func (c avroCodec) NativeToBinary(
	ctx context.Context, s interface{}) (b []byte, e error) {

	lg := logger.New(ctx, "")

	rv := reflect.ValueOf(s)
	if rv.Kind() != reflect.Struct {
		e = errors.New("input is not struct")
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	fields, e := getFieldMap(ctx, rv)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	dataMap := make(map[string]interface{})
	for k, v := range fields {
		if v.GoType == "[]uint8" {
			v.Value = string(v.Value.([]byte))
		}
		dataMap[k] = map[string]interface{}{v.AvroType: v.Value}
	}

	b, e = c.codec.BinaryFromNative(nil, dataMap)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return
}

// GetSchemaID returns the schema ID for the codec's schema
func (c avroCodec) GetSchemaID() int {
	return c.schemaID
}

func (c avroCodec) binaryToMap(
	ctx context.Context, b []byte) (data map[string]interface{}, e error) {

	lg := logger.New(ctx, "")

	i, _, e := c.codec.NativeFromBinary(b)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	data, ok := i.(map[string]interface{})
	if !ok {
		e = errors.New("problem with message format")
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return
}

type field struct {
	Name     string
	Value    interface{}
	GoType   string
	AvroType string
	AvroTag  string
}

func newField(name string, val interface{}, goType string, avroType string, tag string) field {
	return field{Name: name, Value: val, GoType: goType, AvroType: avroType, AvroTag: tag}
}

func getFieldMap(ctx context.Context,
	rv reflect.Value) (m map[string]field, e error) {

	lg := logger.New(ctx, "")

	if rv.IsNil() {
		e = errors.New("reflect.Value is nil")
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	m = make(map[string]field)

	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	for i := 0; i < rv.NumField(); i++ {
		fName := rv.Type().Field(i).Name
		fType := rv.Field(i).Type().String()
		fTag := rv.Type().Field(i).Tag.Get("avro")
		fVal := rv.Field(i).Interface()

		m[fTag] = newField(fName, fVal, fType, typeMap[fType], fTag)
	}

	return
}

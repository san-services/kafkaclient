package kafkaclient

import (
	"context"
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

// NewAvroEncDec constructs and returns a new avro message EncoderDecoder
func newAvroEncDec(s schemaRegistry) EncoderDecoder {
	return &avroEncoderDecoder{schemaReg: s}
}

// Encode converts a native go struct to binary avro data
//
// Fields of the struct to be encoded should have avro
// tags matching schema field names, e.g.:
//
//  type Thing struct {
//		ID    int64  `avro:"ID"`
//		Name  string `avro:"NAME"`
//  }
//
//  matches
//
//  schema := `
//		{
//			"type": "record",
//          "fields": [
//	            {
//		            "name": "ID",
//		            "type": [
//			             "null",
//			             "long"
//		             ],
//		            "default": null
//	            },
//	            {
//		            "name": "NAME",
//		            "type": [
//			             "null",
//			             "string"
//		             ],
//		            "default": null
//	            },
//			 ]
//       }
//  `
//
// Also note the typemap above when creating structs to
// either encode as avro or unmarshall avro message data into.
//
func (ed avroEncoderDecoder) Encode(
	ctx context.Context, topic string, s interface{}) (b []byte, e error) {

	lg := logger.New(ctx, "")

	rv := reflect.ValueOf(s)
	if rv.Kind() != reflect.Struct {
		e = errStructRequired
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

	codec, e := ed.getTopicCodec(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	b, e = codec.BinaryFromNative(nil, dataMap)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return
}

// Decode converts a binary message to a native go struct
//
// Target struct fields should have avro tags matching schema field names, e.g.:
//
//  type Thing struct {
//		ID    int64  `avro:"ID"`
//		Name  string `avro:"NAME"`
//  }
//
//  matches
//
//  schema := `
//		{
//			"type": "record",
//          "fields": [
//	            {
//		            "name": "ID",
//		            "type": [
//			             "null",
//			             "long"
//		             ],
//		            "default": null
//	            },
//	            {
//		            "name": "NAME",
//		            "type": [
//			             "null",
//			             "string"
//		             ],
//		            "default": null
//	            },
//			 ]
//       }
//  `
//
// Also note the typemap above when creating structs to
// either encode as avro or unmarshall avro message data into.
//
func (ed avroEncoderDecoder) Decode(ctx context.Context,
	topic string, b []byte, target interface{}) (e error) {

	lg := logger.New(ctx, "")

	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		e = errPtrRequired
		lg.Error(logger.LogCatUncategorized, e)
		return
	}
	rv = rv.Elem()

	// convert binary message into a map of data
	data, e := ed.binaryToMap(ctx, topic, b)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	// get metadata for all fields in target struct
	fields, e := getFieldMap(ctx, rv)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	// range through data map and insert data into
	// target struct, field-by-field
	for k, v := range data {
		// each value in the map should be an inner map
		v, ok := v.(map[string]interface{})
		if !ok {
			e = errMessageFmt
			lg.Error(logger.LogCatUncategorized, e)
			return
		}

		// get field with tag name == key in the parent data map
		fieldInfo := fields[k]
		// get key for inner map
		mapKey := fieldInfo.AvroType
		// get field value using inner map key
		mapVal := v[mapKey]

		// get appropriate target field
		f := rv.FieldByName(fieldInfo.Name)

		// get field type and intended value type, check they match
		tarFieldType := f.Type()
		mapValType := reflect.TypeOf(mapVal)

		if tarFieldType != mapValType {
			e = errUnmarshallFieldType(fieldInfo.Name, tarFieldType, mapValType)
			lg.Error(logger.LogCatUncategorized, e)
			return
		}

		// set field value
		if f.IsValid() && f.CanSet() {
			f.Set(reflect.ValueOf(mapVal))
		}
	}

	return
}

func (ed avroEncoderDecoder) GetSchemaID(
	ctx context.Context, topic string) (id int, e error) {

	lg := logger.New(ctx, "")

	_, id, e = ed.schemaReg.GetSchemaByTopic(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	return
}

func (ed avroEncoderDecoder) binaryToMap(
	ctx context.Context, topic string, b []byte) (data map[string]interface{}, e error) {

	lg := logger.New(ctx, "")

	codec, e := ed.getTopicCodec(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
	}

	i, _, e := codec.NativeFromBinary(b)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	data, ok := i.(map[string]interface{})
	if !ok {
		e = errMessageFmt
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	return
}

func (ed avroEncoderDecoder) getTopicCodec(
	ctx context.Context, topic string) (c *goavro.Codec, e error) {

	lg := logger.New(ctx, "")

	schema, _, e := ed.schemaReg.GetSchemaByTopic(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
		return
	}

	c, e = goavro.NewCodec(schema)
	if e != nil {
		lg.Error(logger.LogCatUncategorized, e)
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

	// lg := logger.New(ctx, "")

	// if rv.Is() {
	// 	e = errFieldValNil
	// 	lg.Error(logger.LogCatUncategorized, e)
	// 	return
	// }

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

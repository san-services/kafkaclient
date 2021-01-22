package kafkaclient

import (
	"context"
	"reflect"
	"time"

	"github.com/linkedin/goavro/v2"
	cache "github.com/patrickmn/go-cache"
	logger "github.com/san-services/apilogger"
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
	cache     *cache.Cache
	cacheTime time.Duration
}

// NewAvroEncDec constructs and returns a new avro message EncoderDecoder
func newAvroEncDec(s schemaRegistry,
	c *cache.Cache, cacheTime time.Duration) EncoderDecoder {

	return &avroEncoderDecoder{
		schemaReg: s,
		cache:     c,
		cacheTime: cacheTime}
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
		lg.Error(logger.LogCatInputValidation, e)
		return
	}

	fields := getFieldMap(ctx, rv)
	if fields == nil || len(fields) == 0 {
		e = errNoFields
		lg.Error(logger.LogCatInputValidation, e)
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
		lg.Error(logger.LogCatKafkaEncode, e)
		return
	}

	b, e = codec.BinaryFromNative(nil, dataMap)
	if e != nil {
		lg.Error(logger.LogCatKafkaEncode, e)
		return
	}

	return
}

// Decode converts a binary message to a native go struct
//
// Target struct fields should have avro tags matching schema field names.
// Complex target fields, which are structs relating to another topic schema,
// should include a "topic" tag to help determine the schema required to decode them.
//
//  e.g.:
//
//  type Thing struct {
//		ID    int64  `avro:"ID"`
//		Name  string `avro:"NAME"`
//  }
//
//  matches the following schema:
//
//	{
//		"type": "record",
//      "fields": [
//	        {
//		        "name": "ID",
//		        "type": [
//			         "null",
//			         "long"
//		         ],
//		        "default": null
//	        },
//	        {
//		        "name": "NAME",
//		        "type": [
//			         "null",
//			         "string"
//		         ],
//		        "default": null
//	        },
//	    ]
//  }
//
//  and
//
//	type ThingRetry struct {
//		ErrorMessage     string  `avro:"ERROR_MESSAGE"`
//  	OriginalMessage  []byte  `avro:"ORIGINAL_MESSAGE"`
//  }
//
//	and
//
//	type ThingRetry struct {
//		ErrorMessage     string        `avro:"ERROR_MESSAGE"`
//  	OriginalMessage  TopicMessage  `avro:"ORIGINAL_MESSAGE" topic:"new_things"`
//  }
//
//  match the following schema:
//
//	{
//		"type": "record",
//      "fields": [
//	        {
//		        "name": "ERROR_MESSAGE",
//		        "type": [
//			         "null",
//			         "string"
//		         ],
//		        "default": null
//	        },
//	        {
//		        "name": "ORIGINAL_MESSAGE",
//		        "type": [
//			         "null",
//			         "bytes"
//		         ],
//		        "default": null
//	        },
//	    ]
//  }
//
func (ed avroEncoderDecoder) Decode(
	topic string, b []byte, target interface{}) (e error) {

	lg := logger.New(nil, "")
	var rv reflect.Value

	if _, ok := target.(reflect.Value); ok {
		rv = target.(reflect.Value)
	} else {
		rv = reflect.ValueOf(target)
	}

	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		e = errPtrRequired
		lg.Error(logger.LogCatInputValidation, e)
		return
	}
	rv = rv.Elem()

	// convert binary message into a map of data
	data, e := ed.binaryToMap(topic, b)
	if e != nil {
		lg.Error(logger.LogCatKafkaDecode, e)
		return
	}

	// get metadata for all fields in target struct
	fields := getFieldMap(nil, rv)

	// range through data map and insert data into
	// target struct, field-by-field
	for k, v := range data {
		fieldInfo := fields[k]
		if (fieldInfo == field{}) {
			e = errFieldInfo(k)
			lg.Error(logger.LogCatInputValidation, e)
			return
		}

		// get appropriate target field
		f := rv.FieldByName(fieldInfo.Name)
		// get field type
		tarFieldType := f.Type()

		e = ed.applyDataToField(fieldInfo, f, tarFieldType, v)
		if e != nil {
			lg.Error(logger.LogCatKafkaDecode, e)
			return
		}
	}

	return
}

func (ed avroEncoderDecoder) applyDataToField(fldInfo field,
	fld reflect.Value, fldType reflect.Type, dataVal interface{}) (e error) {

	lg := logger.New(nil, "")

	if dataVal == nil || fldType == nil {
		return
	}

	// find val to set
	valToSet := dataVal
	if v, ok := dataVal.(map[string]interface{}); ok {
		// handle primitive type/value
		// get key for inner map
		mapKey := fldInfo.AvroType
		// get field value using inner map key
		valToSet = v[mapKey]
	}

	if valToSet == nil {
		return
	}

	// handle complex type/value (recursive decode)
	if fldInfo.TopicTag != "" {
		b, ok := valToSet.([]byte)
		if ok {
			nestedMsg := reflect.New(fldType)
			e = ed.Decode(fldInfo.TopicTag, b, nestedMsg)
			if e != nil {
				lg.Error(logger.LogCatKafkaDecode, e)
				return
			}

			if fld.IsValid() && fld.CanSet() {
				fld.Set(nestedMsg.Elem())
			}
			return
		}
	}

	// get intended value type
	valType := reflect.TypeOf(valToSet)

	// check field type and intended value type match
	if fldType != valType {
		e = errUnmarshallFieldType(fldInfo.Name, fldType, valType)
		lg.Error(logger.LogCatKafkaDecode, e)
		return
	}

	// set field value
	if fld.IsValid() && fld.CanSet() {
		fld.Set(reflect.ValueOf(valToSet))
	}

	return
}

func (ed avroEncoderDecoder) GetSchemaID(
	ctx context.Context, topic string) (id int, e error) {

	lg := logger.New(ctx, "")

	_, id, e = ed.schemaReg.GetSchemaByTopic(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
	}

	return
}

func (ed avroEncoderDecoder) binaryToMap(
	topic string, b []byte) (data map[string]interface{}, e error) {

	lg := logger.New(nil, "")

	codec, e := ed.getTopicCodec(nil, topic)
	if e != nil {
		lg.Error(logger.LogCatKafkaDecode, e)
		return
	}

	// the first 5 bytes contain encoded metadata, so
	// we skip them in order for the decoding to work
	i, _, e := codec.NativeFromBinary(b[5:])
	if e != nil {
		lg.Error(logger.LogCatKafkaDecode, e)

		// if the schema was first registered by the service
		// messages might not contain encoded metadata
		i, _, e = codec.NativeFromBinary(b)
		if e != nil {
			lg.Error(logger.LogCatKafkaDecode, e)
			return
		}
	}

	data, ok := i.(map[string]interface{})
	if !ok {
		e = errMessageFmt
		lg.Error(logger.LogCatKafkaDecode, e)
		return
	}

	return
}

func (ed avroEncoderDecoder) getTopicCodec(
	ctx context.Context, topic string) (c *goavro.Codec, e error) {

	lg := logger.New(ctx, "")

	if ed.cache != nil {
		if codec, found := ed.cache.Get(topic); found {
			var ok bool
			c, ok = codec.(*goavro.Codec)
			if ok {
				return
			}

			// if there was an issue above, log error,
			// go on ahead and fetch schema from schema reg, not cache
			lg.Error(logger.LogCatCacheRead, errCacheItemType)
		}
	}

	schema, _, e := ed.schemaReg.GetSchemaByTopic(ctx, topic)
	if e != nil {
		lg.Error(logger.LogCatKafkaSchemaReg, e)
		return
	}

	c, e = goavro.NewCodec(schema)
	if e != nil {
		lg.Error(logger.LogCatKafkaDecode, e)
		return
	}

	if ed.cache != nil {
		ed.cache.Set(topic, c, ed.cacheTime)
	}
	return
}

type field struct {
	Name     string
	Value    interface{}
	GoType   string
	AvroType string
	AvroTag  string
	TopicTag string
}

func newField(name string, val interface{},
	goType string, avroType string, avroTag string,
	topicTag string) field {

	return field{
		Name: name, Value: val,
		GoType: goType, AvroType: avroType,
		AvroTag: avroTag, TopicTag: topicTag}
}

func getFieldMap(ctx context.Context,
	rv reflect.Value) (m map[string]field) {

	m = make(map[string]field)

	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	for i := 0; i < rv.NumField(); i++ {
		fName := rv.Type().Field(i).Name
		fType := rv.Field(i).Type().String()
		aTag := rv.Type().Field(i).Tag.Get("avro")
		tTag := rv.Type().Field(i).Tag.Get("topic")
		fVal := rv.Field(i).Interface()

		m[aTag] = newField(fName, fVal, fType, typeMap[fType], aTag, tTag)
	}

	return
}

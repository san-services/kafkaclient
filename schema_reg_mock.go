package kafkaclient

import "context"

var (
	testTopicName  = "test"
	retryTopicName = "test_retry_1"

	testTopicSchema = `{
		"type": "record",
		"name": "testSchema",
		"fields": [
			{
				"name": "ID",
				"type": [
					"null",
					"long"
				],
				"default": null
			},
			{
				"name": "NAME",
				"type": [
					"null",
					"string"
				],
				"default": null
			}
		]
	}`

	retryTopicSchema = `{
		"type": "record",
		"name": "retrySchema",
		"fields": [
			{
				"name": "ERROR_MESSAGE",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "ORIGINAL_TOPIC",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "ORIGINAL_MESSAGE",
				"type": [
					"null",
					"bytes"
				],
				"default": null
			}
		]
	}`
)

type mockSchemaReg struct {
}

func (s mockSchemaReg) GetSchemaByID(
	ctx context.Context, id int) (schemaID string, e error) {

	return
}

func (s mockSchemaReg) GetSchemaByTopic(
	ctx context.Context, topic string) (schema string, schemaID int, e error) {

	switch topic {
	case testTopicName:
		return testTopicSchema, 1, nil
	case retryTopicName:
		return retryTopicSchema, 2, nil
	default:
		return
	}
}

func (s mockSchemaReg) RegisterSchema(
	ctx context.Context, topic string) (schemaID int, e error) {

	return
}

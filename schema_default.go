package kafkaclient

const (
	// DefaultFailTopicSchema is a schema that
	// can be used to configure retry topics
	DefaultFailTopicSchema = `
	{
		"type": "record",
		"name": "DefaultRetryTopicSchema",
		"fields": [
			{
				"name": "error_message",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "original_topic",
				"type": [
					"null",
					"string"
				],
				"default": null
			},
			{
				"name": "original_partition",
				"type": [
					"null",
					"int"
				],
				"default": null
			},
			{
				"name": "original_offset",
				"type": [
					"null",
					"long"
				],
				"default": null
			},
			{
				"name": "original_message",
				"type": [
					"null",
					"bytes"
				],
				"default": null
			}
		]
	}
	`
)

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
				"name": "ERROR_MESSAGE",
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
	}
	`
)

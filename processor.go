package kafkaclient

import (
	"context"

	logger "github.com/disturb16/apilogger"
)

type ProcessorDependencies interface{}

func DefaultProcessor(ctx context.Context,
	dependencies ProcessorDependencies, msg ConsumerMessage) error {

	lg := logger.New(ctx, "")

	lg.Info(logger.LogCatUncategorized,
		infoEvent("message process with default processor",
			msg.Topic(), msg.Partition(), msg.Offset()))

	return nil
}

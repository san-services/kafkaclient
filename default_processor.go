package kafkaclient

import (
	"context"

	logger "github.com/disturb16/apilogger"
)

func DefaultProcessor(ctx context.Context, msg ConsumerMessage) error {
	lg := logger.New(ctx, "")

	lg.Info(logger.LogCatUncategorized,
		infoEvent("message process with default processor",
			msg.Topic(), msg.Partition(), msg.Offset()))

	return nil
}

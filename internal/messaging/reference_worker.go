package messaging

import "context"

func (w *Workers) references(pollCtx, workCtx context.Context) {
	for pollCtx.Err() == nil {
		ctx, cancel := context.WithTimeout(workCtx, w.cfg.ProcessTimeout)
		count, err := w.service.ResumePending(ctx)
		cancel()
		if err != nil {
			w.metrics.Retry("references")
			w.metrics.StorageError(err)
			w.log.Warn("reference retry deferred", "error", err)
		} else if count > 0 {
			w.log.Info("pending references revisited", "count", count)
		}
		if !wait(pollCtx, w.cfg.ReferencePoll) {
			return
		}
	}
}

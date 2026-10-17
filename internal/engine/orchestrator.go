package engine

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/c12s/gprunner/internal/persistence"
	"github.com/c12s/gprunner/internal/resolver"
	"github.com/c12s/gprunner/pkg/model"
)

const layerPrefix = "gp"

type Orchestrator struct {
	res         *resolver.BuildResolver
	concurrency int
	store       *persistence.Store
	imgDir      string
	mqAddr      string
	logger      *slog.Logger
}

func NewOrchestrator(r *resolver.BuildResolver, c int, l *slog.Logger, store *persistence.Store, imgDir, mqAddr string) *Orchestrator {
	return &Orchestrator{res: r, concurrency: c, logger: l, store: store, imgDir: imgDir, mqAddr: mqAddr}
}

func (o *Orchestrator) InstantiateChart(ctx context.Context, chart model.Chart) error {
	if err := o.clearStaleMachines(ctx, chart); err != nil {
		return err
	}

	topicMap, err := buildTopicMap(chart)
	if err != nil {
		o.logger.Error("error occurred while resolving topic map", "chart", chart.Metadata.Name)
		return err
	}

	layers := chart.ChartData
	imageMap, err := o.res.ResolveLayers(ctx, layers, topicMap)
	if err != nil {
		return fmt.Errorf("an error has occurred while resolving layers: %w", err)
	}

	MQAddr := o.mqAddr

	// TODO: add logging
	if err := o.RunAll(ctx, chart, topicMap, imageMap, MQAddr); err != nil {
		return err
	}

	if err := o.store.UpdateChartState(ctx, chart.Metadata.ID, persistence.ChartStarted); err != nil {
		o.logger.Error("error occurred while updating chart state", "chart", chart.Metadata.Name, "new_state", persistence.ChartStarted.String())
		return err
	}

	return nil
}

func (o *Orchestrator) KillChart(ctx context.Context, chart model.Chart) error {
	if err := o.store.UpdateChartState(ctx, chart.Metadata.ID, persistence.ChartStopping); err != nil {
		o.logger.Error("error occurred while updating chart state", "chart", chart.Metadata.Name, "new_state", persistence.ChartStopping.String())
		return err
	}

	if err := o.teardownChart(ctx, chart.Metadata.ID); err != nil {
		return fmt.Errorf("an error has occurred while killing chart %q: %w", chart.Metadata.Name, err)
	}

	if err := o.store.UpdateChartState(ctx, chart.Metadata.ID, persistence.ChartStopped); err != nil {
		o.logger.Error("error occurred while updating chart state", "chart", chart.Metadata.Name, "new_state", persistence.ChartStopped.String())
		return err
	}

	return nil
}

func (o *Orchestrator) clearStaleMachines(ctx context.Context, chart model.Chart) error {
	machines, err := chartMachines(ctx, chart.Metadata.ID)
	if err != nil {
		return err
	}

	for _, m := range machines {
		if m.Status == machineRunning {
			return fmt.Errorf("chart %q: %w", chart.Metadata.Name, ErrChartRunning)
		}
	}

	for _, m := range machines {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := removeMachine(ctx, m.Name); err != nil {
			return err
		}
		o.logger.Debug("stale layer removed", "machine", m.Name, "was", m.Status)
	}

	return nil
}

func (o *Orchestrator) teardownChart(ctx context.Context, chartID string) error {
	machines, err := chartMachines(ctx, chartID)
	if err != nil {
		return err
	}

	for _, m := range machines {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := removeMachine(ctx, m.Name); err != nil {
			return err
		}
		o.logger.Debug("layer removed", "machine", m.Name, "was", m.Status)
	}

	return nil
}

func (o *Orchestrator) abortStart(ctx context.Context, chart model.Chart, cause error) error {
	// the caller's ctx may be what failed, so the teardown gets its own
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), machineCmdTimeout*4)
	defer cancel()

	if err := o.store.UpdateChartState(ctx, chart.Metadata.ID, persistence.ChartStopping); err != nil {
		o.logger.Error("error occurred while updating chart state", "chart", chart.Metadata.Name, "new_state", persistence.ChartStopping.String(), "err", err)
		return cause
	}

	if err := o.teardownChart(ctx, chart.Metadata.ID); err != nil {
		o.logger.Error("tearing down chart after a failed start", "chart", chart.Metadata.Name, "err", err)
		return cause
	}

	if err := o.store.UpdateChartState(ctx, chart.Metadata.ID, persistence.ChartStopped); err != nil {
		o.logger.Error("error occurred while updating chart state", "chart", chart.Metadata.Name, "new_state", persistence.ChartStopped.String(), "err", err)
	}

	return cause
}

func buildTopicMap(chart model.Chart) (map[string]string, error) {
	topicMap := make(map[string]string)

	for _, e := range chart.ChartData.Events {
		if e.Metadata.Topic == "" {
			return nil, fmt.Errorf("event %q: %w", e.Metadata.Name, ErrEventTopicEmpty)
		}
		topicMap[e.Metadata.Name] = chart.Metadata.ID + "." + e.Metadata.Topic
	}

	return topicMap, nil
}

func resolveEventLinks(eventLinks []string, topicMap map[string]string) ([]string, error) {
	topics := make([]string, 0, len(eventLinks))

	for _, name := range eventLinks {
		topic, ok := topicMap[name]
		if !ok {
			return nil, fmt.Errorf("event link %q: %w", name, ErrEventUnknown)
		}
		topics = append(topics, topic)
	}

	return topics, nil
}

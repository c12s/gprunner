package engine

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/c12s/gprunner/internal/persistence"
	"github.com/c12s/gprunner/internal/resolver"
	"github.com/c12s/gprunner/pkg/model"
	"golang.org/x/sync/errgroup"
)

const (
	// rootfsMount tells vfscore to extract the initrd onto /. kraft adds this
	// itself when run from a project or package, but not for a bare kernel file,
	// where the guest would otherwise boot with an empty root. Doubled quotes
	// escape the inner ones for the CSV parser behind --kernel-arg.
	rootfsMount = `"vfs.fstab=[ ""initrd0:/:extract:::"" ]"`

	// dataSourceMountRoot is where linked data sources appear inside the guest,
	// one mount per data source named after it.
	dataSourceMountRoot = "/data"

	// topicsEnv carries a layer's topics into the guest, comma-separated.
	topicsEnv = "TOPICS"
)

type RunJob struct {
	contentKey  string
	MQAddr      string
	layerName   string
	machineName string
	ftrs        model.Features
	ctrl        model.Control
	links       model.Links
	datasources map[string]model.DataSource
	topics      []string
}

func (o *Orchestrator) RunAll(ctx context.Context, chart model.Chart, topicMap map[string]string, imageMap map[string]string, MQAddr string) error {
	jg, err := buildJobGroup(chart, topicMap, imageMap, MQAddr)
	if err != nil {
		return fmt.Errorf("building run jobs: %w", err)
	}

	if err := o.store.UpdateChartState(ctx, chart.Metadata.ID, persistence.ChartStarting); err != nil {
		o.logger.Error("error occurred while updating chart state", "chart", chart.Metadata.Name, "new_state", persistence.ChartStarting.String())
		return err
	}

	// procedures
	if err := o.runPhase(ctx, jg.pcdJobs); err != nil {
		return o.abortStart(ctx, chart, fmt.Errorf("starting stored procedures: %w", err))
	}

	// event triggers
	if err := o.runPhase(ctx, jg.etJobs); err != nil {
		return o.abortStart(ctx, chart, fmt.Errorf("starting event triggers: %w", err))

	}

	// events
	if err := o.runPhase(ctx, jg.eJobs); err != nil {
		return o.abortStart(ctx, chart, fmt.Errorf("starting events: %w", err))
	}

	return nil
}

func (o *Orchestrator) runPhase(ctx context.Context, jobs []RunJob) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(o.concurrency)

	for _, job := range jobs {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			if err := runLayer(o.imgDir, job); err != nil {
				return fmt.Errorf("layer %q: %w", job.layerName, err)
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func runLayer(
	imgDir string,
	job RunJob,
) error {
	binDir := filepath.Join(imgDir, job.contentKey, "unikraft", "bin")

	// Layers built from a Kraftfile without a rootfs have no initrd.
	initrd := filepath.Join(binDir, "initrd")
	if _, err := os.Stat(initrd); err != nil {
		initrd = ""
	}

	args, err := BuildArgs(filepath.Join(binDir, "kernel"), initrd, job.MQAddr, job.machineName, job.ctrl, job.ftrs, job.links, job.datasources, job.topics)
	if err != nil {
		return err
	}

	fmt.Printf("kraft %s\n", strings.Join(args, " "))

	cmd := exec.Command("kraft", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func BuildArgs(
	kernel, initrd, MQAddr, layerName string,
	ctrl model.Control, ftr model.Features,
	links model.Links,
	datasources map[string]model.DataSource,
	topics []string,
) ([]string, error) {
	args := []string{"run", "--no-prompt"}

	if MQAddr != "" {
		args = append(args, "--network", resolver.BrokerNetworkName)
	}
	if ctrl.Memory != "" {
		args = append(args, "--memory", ctrl.Memory)
	}
	if ctrl.RemoveOnStop {
		args = append(args, "--rm")
	}
	if ctrl.RunDetached {
		args = append(args, "--detach")
	}
	if ctrl.DisableVirtualization {
		args = append(args, "--disable-acceleration")
	}

	// for _, p := range ftr.Ports {
	// 	args = append(args, "--port", p)
	// }

	// chart-declared volumes first, then the linked data sources; both go
	// through the same flag so the guest sees no difference between them
	for _, v := range ftr.Volumes {
		args = append(args, "--volume", v)
	}
	mounts, err := dataSourceMounts(links, datasources)
	if err != nil {
		return nil, err
	}
	for _, m := range mounts {
		args = append(args, "--volume", m)
	}

	for _, e := range ftr.EnvVars {
		args = append(args, "--env", e)
	}
	// stored procedures are booted without a broker address
	if MQAddr != "" {
		args = append(args, "--env", "MQ_ADDR="+MQAddr)
	}
	// the finished, chart-prefixed topic names: the one an event publishes to,
	// or the ones a trigger subscribes to. The guest never builds a topic itself
	if len(topics) > 0 {
		args = append(args, "--env", topicsEnv+"="+strings.Join(topics, ","))
	}

	args = append(args, "--name", layerName)

	if initrd != "" {
		args = append(args, "--rootfs", initrd, "--kernel-arg", rootfsMount)
	}

	args = append(args, kernel)

	if ctrl.KernelArgs != "" {
		args = append(args, "--")
		args = append(args, strings.Fields(ctrl.KernelArgs)...)
	}

	return args, nil
}

func dataSourceMounts(links model.Links, datasources map[string]model.DataSource) ([]string, error) {
	var mounts []string

	for _, name := range links.HardLinks {
		ds, ok := datasources[name]
		if !ok {
			return nil, fmt.Errorf("hard link %q: %w", name, ErrDataSourceUnknown)
		}
		mounts = append(mounts, volumeSpec(ds))
	}

	for _, name := range links.SoftLinks {
		ds, ok := datasources[name]
		if !ok {
			return nil, fmt.Errorf("soft link %q: %w", name, ErrDataSourceUnknown)
		}
		if _, err := os.Stat(ds.Path); err != nil {
			continue // optional, and the resolver has already warned about it
		}
		mounts = append(mounts, volumeSpec(ds))
	}

	return mounts, nil
}

// volumeSpec maps a data source to host:guest. kraft volumes are 9pfs shares
// and can only carry directories, so a data source that points at a file
// shares the directory holding it; the guest then finds the file under
// /data/<name>/<basename>.
func volumeSpec(ds model.DataSource) string {
	host := ds.Path
	if info, err := os.Stat(host); err == nil && !info.IsDir() {
		host = filepath.Dir(host)
	}

	return host + ":" + filepath.Join(dataSourceMountRoot, ds.Name)
}

type JobGroup struct {
	pcdJobs []RunJob
	etJobs  []RunJob
	eJobs   []RunJob
}

func buildJobGroup(chart model.Chart, topicMap map[string]string, imageMap map[string]string, MQAddr string) (JobGroup, error) {
	layers := chart.ChartData
	jg := JobGroup{}

	for _, pcd := range layers.StoredProcedures {
		rj := RunJob{
			contentKey:  imageMap[pcd.Metadata.ID],
			MQAddr:      "",
			machineName: machineName(chart.Metadata.ID, pcd.Metadata.ID),
			ftrs:        pcd.Features,
			ctrl:        pcd.Control,
			links:       pcd.Links,
			datasources: layers.DataSources,
			topics:      nil,
			layerName:   pcd.Metadata.Name,
		}
		jg.pcdJobs = append(jg.pcdJobs, rj)
	}

	for _, et := range layers.EventTriggers {
		topics, err := resolveEventLinks(et.Links.EventLinks, topicMap)
		if err != nil {
			return JobGroup{}, fmt.Errorf("layer %q: %w", et.Metadata.Name, err)
		}
		rj := RunJob{
			contentKey:  imageMap[et.Metadata.ID],
			MQAddr:      MQAddr,
			machineName: machineName(chart.Metadata.ID, et.Metadata.ID),
			ftrs:        et.Features,
			ctrl:        et.Control,
			links:       et.Links,
			datasources: layers.DataSources,
			topics:      topics,
			layerName:   et.Metadata.Name,
		}
		jg.etJobs = append(jg.etJobs, rj)
	}

	for _, e := range layers.Events {
		rj := RunJob{
			contentKey:  imageMap[e.Metadata.ID],
			MQAddr:      MQAddr,
			machineName: machineName(chart.Metadata.ID, e.Metadata.ID),
			ftrs:        e.Features,
			ctrl:        e.Control,
			links:       model.Links{},
			datasources: layers.DataSources,
			topics:      []string{topicMap[e.Metadata.Name]},
			layerName:   e.Metadata.Name,
		}
		jg.eJobs = append(jg.eJobs, rj)
	}

	return jg, nil
}

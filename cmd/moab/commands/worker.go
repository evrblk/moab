package commands

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/evrblk/monstera"
	monstera_grpc "github.com/evrblk/monstera/transport/grpc"
	"github.com/evrblk/yellowstone-common/metrics"

	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/workers"
)

var workerCmdCfg struct {
	prometheusPort int
	nodes          monsteraNodesFlags
}

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Run Moab background worker",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Moab Worker...")

		// Metrics
		metricsSrv := metrics.NewMetricsServer(workerCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Node discovery + polling config provider.
		discovery, err := buildNodeDiscovery(workerCmdCfg.nodes)
		if err != nil {
			log.Fatal(err)
		}
		adminClient := monstera_grpc.NewAdminClient()
		provider := monstera.NewPollingClusterConfigProvider(discovery, adminClient, monstera.PollingOptions{})

		// Data plane + Monstera client
		transport := monstera_grpc.NewDataPlaneClient()
		monsteraClient := monstera.NewMonsteraClient(provider, transport, monstera.DefaultClientConfig())

		ctx, cancel := context.WithCancel(context.Background())
		if err := monsteraClient.Start(ctx); err != nil {
			log.Fatalf("failed to start monstera client: %v", err)
		}
		defer monsteraClient.Stop()
		defer adminClient.Close()

		// Moab client
		moabCoreApiClient := coreapis.NewMoabMonsteraStub(monsteraClient)

		// Moab workers
		moabQueuesCronWorker := workers.NewMoabQueuesCronWorker(moabCoreApiClient)
		moabQueuesCronWorker.Start()
		moabTasksGCWorker := workers.NewMoabTasksGCWorker(moabCoreApiClient)
		moabTasksGCWorker.Start()
		moabQueuesGCWorker := workers.NewMoabQueuesGCWorker(moabCoreApiClient)
		moabQueuesGCWorker.Start()

		wg := sync.WaitGroup{}
		wg.Add(1)
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				log.Println("Received SIGINT. Shutting down...")
				cancel()
				metricsSrv.Stop()
				moabQueuesCronWorker.Stop()
				moabTasksGCWorker.Stop()
				moabQueuesGCWorker.Stop()
			case <-ctx.Done():
			}
			wg.Done()
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		wg.Wait()
	},
}

func init() {
	runCmd.AddCommand(workerCmd)

	workerCmd.PersistentFlags().IntVarP(&workerCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	addMonsteraNodesFlags(workerCmd, &workerCmdCfg.nodes)
}

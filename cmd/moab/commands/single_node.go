package commands

import (
	"context"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/evrblk/yellowstone-common/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/queues"
	moab_v0 "github.com/evrblk/moab/pkg/server/v0"
	"github.com/evrblk/moab/pkg/tasks"
	"github.com/evrblk/moab/pkg/workers"
)

var singleNodeCmdCfg struct {
	gatewayListenAddr    string
	prometheusListenAddr string
	authKeysPath         string
	shardsCount          int
	dataDir              string
	log                  logFlags
}

var singleNodeCmd = &cobra.Command{
	Use:   "single-node",
	Short: "Run Moab in single-node mode",
	Run: func(cmd *cobra.Command, args []string) {
		baseLogger := setupLogger(singleNodeCmdCfg.log).With("service_name", "single-node")
		baseLogger.Info("Initializing Moab...")

		lis, err := net.Listen("tcp", singleNodeCmdCfg.gatewayListenAddr)
		if err != nil {
			baseLogger.Error("failed to listen", "error", err, "address", singleNodeCmdCfg.gatewayListenAddr)
			os.Exit(1)
		}

		// Metrics
		moab_v0.RegisterMetrics(prometheus.DefaultRegisterer)
		workers.RegisterMetrics(prometheus.DefaultRegisterer)
		metricsSrv := metrics.NewMetricsServer(singleNodeCmdCfg.prometheusListenAddr)
		metricsSrv.Start()

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(store.DefaultOptions(filepath.Join(singleNodeCmdCfg.dataDir, "cores")))
		if err != nil {
			baseLogger.Error("failed to create data store", "error", err)
			os.Exit(1)
		}

		// Node-local registry handing out a stable two-byte prefix per shard, so
		// every core namespaces its data in the shared store without collisions.
		replicaRegistry := honey.NewReplicaPrefixRegistry(dataStore)
		replicaPrefix := func(shardId string) []byte {
			prefix, err := replicaRegistry.GetOrAssignPrefix(shardId)
			if err != nil {
				baseLogger.Error("failed to assign replica prefix", "shard_id", shardId, "error", err)
				os.Exit(1)
			}
			return prefix
		}

		// Middleware
		monitoringMiddleware := middleware.NewMonitoringMiddleware("moab", baseLogger.With("component", "grpc"))
		monitoringMiddleware.Register(prometheus.DefaultRegisterer)

		unaryInterceptors := []grpc.UnaryServerInterceptor{monitoringMiddleware.Unary}
		if singleNodeCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, middleware.NewAuthenticationMiddleware(singleNodeCmdCfg.authKeysPath, "Moab").Unary)
		}

		// Moab single node client
		coresFactory := &coreapis.MoabNonclusteredApplicationCoresFactory{
			MoabQueuesCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.MoabQueuesCoreApi {
				return queues.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
			},
			MoabTasksCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.MoabTasksCoreApi {
				return tasks.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
			},
		}
		moabCoreApiClient := coreapis.NewMoabNonclusteredStub(singleNodeCmdCfg.shardsCount, coresFactory, baseLogger.With("component", "core"))

		// Moab workers
		moabQueuesCronWorker := workers.NewMoabQueuesCronWorker(moabCoreApiClient, baseLogger.With("component", "moab-queues-cron-worker"))
		moabQueuesCronWorker.Start()
		moabTasksGCWorker := workers.NewMoabTasksGCWorker(moabCoreApiClient, baseLogger.With("component", "moab-tasks-gc-worker"))
		moabTasksGCWorker.Start()
		moabQueuesGCWorker := workers.NewMoabQueuesGCWorker(moabCoreApiClient, baseLogger.With("component", "moab-queues-gc-worker"))
		moabQueuesGCWorker.Start()

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
		)

		ctx, cancel := context.WithCancel(context.Background())
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				baseLogger.Info("Received SIGINT. Shutting down...")
				cancel()
				moabQueuesCronWorker.Stop()
				moabTasksGCWorker.Stop()
				moabQueuesGCWorker.Stop()
				grpcServer.GracefulStop()
				metricsSrv.Stop()
			case <-ctx.Done():
			}
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		// Moab API Gateway
		moabApiGatewayServer := moab_v0.NewMoabApiServer(moabCoreApiClient)
		defer moabApiGatewayServer.Close()
		moabpb.RegisterMoabApiServer(grpcServer, moabApiGatewayServer)

		baseLogger.Info("Starting API Gateway Server...", "address", singleNodeCmdCfg.gatewayListenAddr)
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(singleNodeCmd)

	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.gatewayListenAddr, "gateway-listen-addr", "", ":8000", "API Gateway bind address")
	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.prometheusListenAddr, "prometheus-listen-addr", "", ":2112", "Prometheus metrics bind address")
	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.dataDir, "data-dir", "", "./data", "Base directory for data")
	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.shardsCount, "shards", "", 64, "Number of internal shards")
	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")

	addLogFlags(singleNodeCmd, &singleNodeCmdCfg.log)
}

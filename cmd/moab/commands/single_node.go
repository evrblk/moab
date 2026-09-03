package commands

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/yellowstone-common/honey"
	"github.com/evrblk/yellowstone-common/metrics"
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
	port           int
	prometheusPort int
	authKeysPath   string
	shardsCount    int
	dataDir        string
}

var singleNodeCmd = &cobra.Command{
	Use:   "single-node",
	Short: "Run Moab in single-node mode",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Moab...")

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", singleNodeCmdCfg.port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Metrics
		moab_v0.RegisterMetrics()
		metricsSrv := metrics.NewMetricsServer(singleNodeCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Create shared Badger store for application cores
		dataStore, err := store.NewBadgerStore(store.DefaultOptions(filepath.Join(singleNodeCmdCfg.dataDir, "cores")))
		if err != nil {
			log.Fatalf("failed to create data store: %v", err)
		}

		// Node-local registry handing out a stable two-byte prefix per shard, so
		// every core namespaces its data in the shared store without collisions.
		replicaRegistry := honey.NewReplicaPrefixRegistry(dataStore)
		replicaPrefix := func(shardId string) []byte {
			prefix, err := replicaRegistry.GetOrAssignPrefix(shardId)
			if err != nil {
				log.Fatalf("failed to assign replica prefix for shard %s: %v", shardId, err)
			}
			return prefix
		}

		// Middleware
		unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
		if singleNodeCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, moab_v0.NewAuthenticationMiddleware(singleNodeCmdCfg.authKeysPath).Unary)
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
		moabCoreApiClient := coreapis.NewMoabNonclusteredStub(singleNodeCmdCfg.shardsCount, coresFactory)

		// Moab workers
		moabQueuesCronWorker := workers.NewMoabQueuesCronWorker(moabCoreApiClient)
		moabQueuesCronWorker.Start()
		moabTasksGCWorker := workers.NewMoabTasksGCWorker(moabCoreApiClient)
		moabTasksGCWorker.Start()
		moabQueuesGCWorker := workers.NewMoabQueuesGCWorker(moabCoreApiClient)
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
				log.Println("Received SIGINT. Shutting down...")
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

		log.Println("Starting API Gateway Server...")
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(singleNodeCmd)

	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.port, "port", "", 0, "Server port")
	err := singleNodeCmd.MarkPersistentFlagRequired("port")
	if err != nil {
		panic(err)
	}

	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	singleNodeCmd.PersistentFlags().IntVarP(&singleNodeCmdCfg.shardsCount, "shards", "", 64, "Number of internal shards")

	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.dataDir, "data-dir", "", "", "Base directory for data")
	err = singleNodeCmd.MarkPersistentFlagRequired("data-dir")
	if err != nil {
		panic(err)
	}

	singleNodeCmd.PersistentFlags().StringVarP(&singleNodeCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")
}

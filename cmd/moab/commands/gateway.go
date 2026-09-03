package commands

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/evrblk/monstera"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/coreapis"
	moab_v0 "github.com/evrblk/moab/pkg/server/v0"
	monstera_grpc "github.com/evrblk/monstera/transport/grpc"
)

var gatewayCmdCfg struct {
	port           int
	prometheusPort int
	nodes          monsteraNodesFlags
	authKeysPath   string
}

var gatewayCmd = &cobra.Command{
	Use:   "gateway",
	Short: "Run Moab API Gateway",
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Initializing Moab API Gateway Server...")

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", gatewayCmdCfg.port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		// Metrics
		moab_v0.RegisterMetrics()
		metricsSrv := metrics.NewMetricsServer(gatewayCmdCfg.prometheusPort)
		metricsSrv.Start()

		// Node discovery + polling config provider: the gateway learns the cluster
		// config from the cluster itself and refreshes as the topology changes.
		discovery, err := buildNodeDiscovery(gatewayCmdCfg.nodes)
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

		// Middleware
		unaryInterceptors := make([]grpc.UnaryServerInterceptor, 0)
		if gatewayCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, moab_v0.NewAuthenticationMiddleware(gatewayCmdCfg.authKeysPath).Unary)
		}

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
		)

		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				log.Println("Received SIGINT. Shutting down...")
				cancel()
				grpcServer.GracefulStop()
				monsteraClient.Stop()
				adminClient.Close()
				metricsSrv.Stop()
			case <-ctx.Done():
			}
		}()
		defer func() {
			signal.Stop(c)
			cancel()
		}()

		// Moab API Gateway
		moabCoreApiClient := coreapis.NewMoabMonsteraStub(monsteraClient)
		moabApiGatewayServer := moab_v0.NewMoabApiServer(moabCoreApiClient)
		defer moabApiGatewayServer.Close()
		moabpb.RegisterMoabApiServer(grpcServer, moabApiGatewayServer)

		log.Println("Starting API Gateway Server...")
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(gatewayCmd)

	gatewayCmd.PersistentFlags().IntVarP(&gatewayCmdCfg.port, "port", "", 0, "Server port")
	err := gatewayCmd.MarkPersistentFlagRequired("port")
	if err != nil {
		panic(err)
	}

	gatewayCmd.PersistentFlags().IntVarP(&gatewayCmdCfg.prometheusPort, "prometheus-port", "", 2112, "Prometheus metrics port")

	addMonsteraNodesFlags(gatewayCmd, &gatewayCmdCfg.nodes)

	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")
}

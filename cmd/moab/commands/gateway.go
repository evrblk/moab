package commands

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/evrblk/monstera"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/evrblk/yellowstone-common/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/coreapis"
	moab_v0 "github.com/evrblk/moab/pkg/server/v0"
	monstera_grpc "github.com/evrblk/monstera/transport/grpc"
)

var gatewayCmdCfg struct {
	gatewayListenAddr    string
	prometheusListenAddr string
	nodes                monsteraNodesFlags
	authKeysPath         string
	log                  logFlags
}

var gatewayCmd = &cobra.Command{
	Use:   "gateway",
	Short: "Run Moab API Gateway",
	Run: func(cmd *cobra.Command, args []string) {
		baseLogger := setupLogger(gatewayCmdCfg.log).With("service_name", "gateway")
		baseLogger.Info("Initializing Moab API Gateway Server...")

		lis, err := net.Listen("tcp", gatewayCmdCfg.gatewayListenAddr)
		if err != nil {
			baseLogger.Error("failed to listen", "error", err, "address", gatewayCmdCfg.gatewayListenAddr)
			os.Exit(1)
		}

		// Metrics
		moab_v0.RegisterMetrics(prometheus.DefaultRegisterer)
		metricsSrv := metrics.NewMetricsServer(gatewayCmdCfg.prometheusListenAddr)
		metricsSrv.Start()

		// Node discovery + polling config provider: the gateway learns the cluster
		// config from the cluster itself and refreshes as the topology changes.
		discovery, err := buildNodeDiscovery(gatewayCmdCfg.nodes)
		if err != nil {
			baseLogger.Error(err.Error())
			os.Exit(1)
		}
		adminClient := monstera_grpc.NewAdminClient()
		provider := monstera.NewPollingClusterConfigProvider(discovery, adminClient, monstera.PollingOptions{})

		// Data plane + Monstera client
		transport := monstera_grpc.NewDataPlaneClient()
		monsteraClient := monstera.NewMonsteraClient(provider, transport, monstera.DefaultClientConfig())

		ctx, cancel := context.WithCancel(context.Background())
		if err := monsteraClient.Start(ctx); err != nil {
			baseLogger.Error("failed to start monstera client", "error", err)
			os.Exit(1)
		}

		// Middleware
		monitoringMiddleware := middleware.NewMonitoringMiddleware("moab", baseLogger.With("component", "grpc"))
		monitoringMiddleware.Register(prometheus.DefaultRegisterer)

		unaryInterceptors := []grpc.UnaryServerInterceptor{monitoringMiddleware.Unary}
		if gatewayCmdCfg.authKeysPath != "" {
			unaryInterceptors = append(unaryInterceptors, middleware.NewAuthenticationMiddleware(gatewayCmdCfg.authKeysPath, "Moab").Unary)
		}

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(unaryInterceptors...),
		)

		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
		go func() {
			select {
			case <-c:
				baseLogger.Info("Received SIGINT. Shutting down...")
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

		baseLogger.Info("Starting API Gateway Server...", "address", gatewayCmdCfg.gatewayListenAddr)
		grpcServer.Serve(lis)
	},
}

func init() {
	runCmd.AddCommand(gatewayCmd)

	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdCfg.gatewayListenAddr, "gateway-listen-addr", "", ":8000", "API Gateway bind address")
	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdCfg.prometheusListenAddr, "prometheus-listen-addr", "", ":2112", "Prometheus metrics bind address")
	gatewayCmd.PersistentFlags().StringVarP(&gatewayCmdCfg.authKeysPath, "auth-keys-path", "", "", "Path to the directory with auth keys. No authn if empty.")

	addMonsteraNodesFlags(gatewayCmd, &gatewayCmdCfg.nodes)
	addLogFlags(gatewayCmd, &gatewayCmdCfg.log)
}

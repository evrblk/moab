package v0

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/moab"
)

var (
	tasksEnqueuedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_tasks_enqueued_total",
		Help: "Moab tasks enqueued total",
	})
	tasksDequeuedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_tasks_dequeued_total",
		Help: "Moab tasks dequeued total",
	})
	tasksEnqueuedBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_tasks_enqueued_bytes_total",
		Help: "Moab tasks enqueued total size",
	})
	tasksDequeuedBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_bytes_dequeued_bytes_total",
		Help: "Moab tasks dequeued total size",
	})
)

type MoabApiServer struct {
	moabpb.UnimplementedMoabApiServer

	handler *MoabApiServerHandler
}

func (s *MoabApiServer) Close() {
	log.Println("Stopping MoabApiServer...")
	s.handler.Stop()
}

func (s *MoabApiServer) CreateQueue(ctx context.Context, request *moabpb.CreateQueueRequest) (*moabpb.CreateQueueResponse, error) {
	if err := ValidateCreateQueueRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateQueue(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) GetQueue(ctx context.Context, request *moabpb.GetQueueRequest) (*moabpb.GetQueueResponse, error) {
	if err := ValidateGetQueueRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetQueue(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) UpdateQueue(ctx context.Context, request *moabpb.UpdateQueueRequest) (*moabpb.UpdateQueueResponse, error) {
	if err := ValidateUpdateQueueRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateQueue(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) DeleteQueue(ctx context.Context, request *moabpb.DeleteQueueRequest) (*moabpb.DeleteQueueResponse, error) {
	if err := ValidateDeleteQueueRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteQueue(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) ListQueues(ctx context.Context, request *moabpb.ListQueuesRequest) (*moabpb.ListQueuesResponse, error) {
	if err := ValidateListQueuesRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListQueues(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) Enqueue(ctx context.Context, request *moabpb.EnqueueRequest) (*moabpb.EnqueueResponse, error) {
	if err := ValidateEnqueueRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	tasksEnqueuedTotal.Add(float64(len(request.Entries)))
	for _, entry := range request.Entries {
		tasksEnqueuedBytesTotal.Add(float64(len(entry.Payload)))
	}

	return s.handler.Enqueue(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) Dequeue(ctx context.Context, request *moabpb.DequeueRequest) (*moabpb.DequeueResponse, error) {
	if err := ValidateDequeueRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp, err := s.handler.Dequeue(ctx, request, 0, moab.DefaultServiceLimits)
	if err != nil {
		return nil, err
	}

	tasksDequeuedTotal.Add(float64(len(resp.Tasks)))
	for _, task := range resp.Tasks {
		tasksDequeuedBytesTotal.Add(float64(len(task.Payload)))
	}

	return resp, nil
}

func (s *MoabApiServer) ReportStatus(ctx context.Context, request *moabpb.ReportStatusRequest) (*moabpb.ReportStatusResponse, error) {
	if err := ValidateReportStatusRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ReportStatus(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) DeleteTasks(ctx context.Context, request *moabpb.DeleteTasksRequest) (*moabpb.DeleteTasksResponse, error) {
	if err := ValidateDeleteTasksRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteTasks(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) RestartTasks(ctx context.Context, request *moabpb.RestartTasksRequest) (*moabpb.RestartTasksResponse, error) {
	if err := ValidateRestartTasksRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.RestartTasks(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) CreateSchedule(ctx context.Context, request *moabpb.CreateScheduleRequest) (*moabpb.CreateScheduleResponse, error) {
	if err := ValidateCreateScheduleRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.CreateSchedule(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) GetSchedule(ctx context.Context, request *moabpb.GetScheduleRequest) (*moabpb.GetScheduleResponse, error) {
	if err := ValidateGetScheduleRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetSchedule(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) UpdateSchedule(ctx context.Context, request *moabpb.UpdateScheduleRequest) (*moabpb.UpdateScheduleResponse, error) {
	if err := ValidateUpdateScheduleRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.UpdateSchedule(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) DeleteSchedule(ctx context.Context, request *moabpb.DeleteScheduleRequest) (*moabpb.DeleteScheduleResponse, error) {
	if err := ValidateDeleteScheduleRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.DeleteSchedule(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) ListSchedules(ctx context.Context, request *moabpb.ListSchedulesRequest) (*moabpb.ListSchedulesResponse, error) {
	if err := ValidateListSchedulesRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.ListSchedules(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) PurgeQueue(ctx context.Context, request *moabpb.PurgeQueueRequest) (*moabpb.PurgeQueueResponse, error) {
	if err := ValidatePurgeQueueRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.PurgeQueue(ctx, request, 0, moab.DefaultServiceLimits)
}

func (s *MoabApiServer) GetTask(ctx context.Context, request *moabpb.GetTaskRequest) (*moabpb.GetTaskResponse, error) {
	if err := ValidateGetTaskRequest(request); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	return s.handler.GetTask(ctx, request, 0, moab.DefaultServiceLimits)
}

func NewMoabApiServer(moabCoreApiClient coreapis.MoabClientApi) *MoabApiServer {
	return &MoabApiServer{
		handler: NewMoabApiServerHandler(moabCoreApiClient),
	}
}

package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Never used
type RegisterWorkerArgs struct {
}

//Never used
type RegisterWorkerReply struct {
	WorkerId int
}

// Worker request
type RequestTaskArgs struct {
	TaskType int
}

// Master reply
type RequestTaskReply struct {
	ReplyTask          Task
	GotTask            bool
	MapTaskFinished    int
	ReduceTaskFinished int
	MapTaskNum         int
	ReduceTaskNum      int
	Seq                int
}

// Woker report
type ReportTaskArgs struct {
	ReportTask Task
}

// Master repky
type ReportTaskReply struct {
}

// Master request
type MasterReq struct {
	TaskType string
}

// Master response
type MasterResp struct {
	// files to be handled
	files []string
}

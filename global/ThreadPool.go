package global

// 定义一个任务接口
type Job interface {
	DoDataCheck()
	//Func()
}

// 定义一个任务队列或者队列池子
type JobQueue chan Job

// 定义一个工作结构体，里面包含任务池子
type Worker struct {
	JobChan JobQueue //每一个worker对象具有JobQueue（队列）属性。
}

// 定义一个工作池子，里面包含工作池子的大小，任务池子，以及工作池子的队列
type WorkerPool struct { //线程池：
	Workerlen   int           //线程池的大小
	JobQueue    JobQueue      //Job队列，接收外部的数据
	WorkerQueue chan JobQueue //worker队列：处理任务的Go程队列
}

// 定义一个函数，返回一个任务初始化结构体
func NewWorker() Worker {
	return Worker{JobChan: make(chan Job)}
}

// 定义一个方法，名字叫run，将任务队列里面的任务加到工作池子中，并执行
// 启动参与程序运行的Go程数量
func (w Worker) Run(wq chan JobQueue) {
	go func() {
		for {
			wq <- w.JobChan //处理任务的Go程队列数量有限，每运行1个，向队列中添加1个，队列剩余数量少1个 (JobChain入队列)
			select {
			case job := <-w.JobChan:
				job.DoDataCheck() //执行操作
				//job.Func()
			}
		}
	}()
}

// 初始化工作池子
func NewWorkerPool(workerlen int) *WorkerPool {
	return &WorkerPool{
		Workerlen:   workerlen,
		JobQueue:    make(JobQueue),
		WorkerQueue: make(chan JobQueue, workerlen),
	}
}

func (wp *WorkerPool) Run() {
	defer func() {
		if err := recover(); err != nil {
			// Error output removed - use logs instead
		}
	}()
	//初始化worker(多个Go程)
	for i := 0; i < wp.Workerlen; i++ {
		worker := NewWorker()
		worker.Run(wp.WorkerQueue) //开启每一个Go程
	}

	// 循环获取可用的worker,往worker中写job
	go func() {
		for {
			select {
			//将JobQueue中的数据存入WorkerQueue
			case job := <-wp.JobQueue: //线程池中有需要待处理的任务(数据来自于请求的任务) :读取JobQueue中的内容
				worker := <-wp.WorkerQueue //队列中有空闲的Go程   ：读取WorkerQueue中的内容,类型为：JobQueue
				worker <- job              //空闲的Go程执行任务  ：整个job入队列（channel） 类型为：传递的参数（Score结构体）
			}
		}
	}()
}
func (wp *WorkerPool) Close() {

}

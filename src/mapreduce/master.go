package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  mr.Workers = make(map[string]*WorkerInfo)
  mr.job_chan = make(chan DoJobArgs)
  mr.reply_chan = make(chan string)
  mr.map_stat = make([]string, mr.nMap)
  for w := range mr.map_stat {
    mr.map_stat[w] = ""
  }
  mr.reduce_stat = make([]string, mr.nReduce)
  for w := range mr.reduce_stat {
    mr.reduce_stat[w] = ""
  }
  go func() {
    for {
      worker := <- mr.registerChannel
      mr.Workers[worker] = &WorkerInfo{worker}
      go func() {
        for {
		  args := <- mr.job_chan
		  var reply DoJobReply
		  ok := call(worker, "Worker.DoJob", &args, &reply)
		  if ok == false {
			break
		  } else {
			mr.reply_chan <- fmt.Sprintf("%s %d", args.Operation, args.JobNumber)
		  }
        }
      } ()
    }
  } ()
  for {
    var args DoJobArgs
	var work_done bool
    for {
	  work_done = false
      var w int
      for w = range mr.map_stat {
        if mr.map_stat[w] == "" {
          break
        }
      }
      if mr.map_stat[w] == "" {
        args.File = mr.file
        args.Operation = Map
        args.JobNumber = w
        args.NumOtherPhase = mr.nReduce
		break
      }
      for w = range mr.map_stat {
        if mr.map_stat[w] != "*done*" {
          break
        }
      }
      if mr.map_stat[w] != "*done*" {
        args.File = mr.file
        args.Operation = Map
        args.JobNumber = w
        args.NumOtherPhase = mr.nReduce
		break
      }
      for w = range mr.reduce_stat {
        if mr.reduce_stat[w] == "" {
          break
        }
      }
      if mr.reduce_stat[w] == "" {
        args.File = mr.file
        args.Operation = Reduce
        args.JobNumber = w
        args.NumOtherPhase = mr.nMap
		break
      }
      for w = range mr.reduce_stat {
        if mr.reduce_stat[w] != "*done*" {
          break
        }
      }
      if mr.reduce_stat[w] != "*done*" {
        args.File = mr.file
        args.Operation = Reduce
        args.JobNumber = w
        args.NumOtherPhase = mr.nMap
		break
      }
	  work_done = true
	  break
	}
	if work_done {
	  break
	}
	select {
	case mr.job_chan <- args:
	case reply := <- mr.reply_chan:
	  var Operation string
	  var JobNumber int
	  if c,_ := fmt.Sscanf(reply, "%s %d", &Operation, &JobNumber); c == 2 {
		switch Operation {
		case Map:
		  mr.map_stat[JobNumber] = "*done*"
		case Reduce:
		  mr.reduce_stat[JobNumber] = "*done*"
		}
	  } else {
		fmt.Printf("Wrong format reply %s\n", reply)
	  }
	}
  }
  return mr.KillWorkers()
}

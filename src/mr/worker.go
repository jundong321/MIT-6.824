package mr

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "encoding/json"
import "io/ioutil"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func SendResult(outputs []string, id int, workType int) (HandoverAck, bool) {
	args := HandoverWork{}

	args.outputs = outputs
	args.id = id
	args.workType = workType

	reply := HandoverAck{}

	res := call("Coordinator.ResultHandler", &args, &reply)

	return reply, res
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()


	// while coordinator is alive.
	for work, res := CallForWork(); res == true; {
		switch work.workType {
		case -1:
			time.Sleep(time.Second)
		case 0:
			// map
			for _, filename := range work.inputs {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()

				outputs := make([]string, work.nReduce)
				for _, kv := range mapf(filename, string(content)) {
					mapId := ihash(kv.Key) % work.nReduce
					filename = "mr-" + string(work.id) + "-" + string(mapId)
					outputFile, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE, 0222)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}

					enc := json.NewEncoder(outputFile)
					err = enc.Encode(&kv)
					outputs[mapId] = filename

					outputFile.Close()
				}
				fmt.Printf("map produced outputs: %v\n", outputs)

				SendResult(outputs, work.id, work.workType)
			}
		case 1:
			// reduce
			intermediate := []KeyValue{}
			for _, filename := range work.inputs {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			oname := "mr-out-" + string(work.id)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			outputs := []string{oname}
			SendResult(outputs, work.id, work.workType)
		}
	}
}


func CallForWork() (Work, bool)  {
	args := AskForWork{}

	reply := Work{}

	res := call("Coordinator.WorkHandler", &args, &reply)

	return reply, res
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"

	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key   string
	Value string
	Name  string

	ClientId  int64
	RequestId int
}

type Notification struct {
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db                   map[string]string         // key-value database
	acknowledgedRequests map[int64]int             // client id -> last acknowledged request id
	notifyChs            map[int]chan Notification // index -> notify channel
}

func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int) bool {
	appliedRequestId, ok := kv.acknowledgedRequests[clientId]
	if ok == false || requestId > appliedRequestId {
		return false
	}
	return true
}

func (kv *KVServer) waitApplying(op Op, timeout time.Duration) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return true
	}

	var wrongLeader bool

	kv.mu.Lock()
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan Notification, 1)
	}
	ch := kv.notifyChs[index]
	kv.mu.Unlock()

	select {
	case notification := <-ch:
		if notification.ClientId != op.ClientId || notification.RequestId != op.RequestId {
			wrongLeader = true
		} else {
			wrongLeader = false
		}
	case <-time.After(timeout):
		kv.mu.Lock()
		if kv.isDuplicateRequest(op.ClientId, op.RequestId) {
			wrongLeader = false
		} else {
			wrongLeader = true
		}
		kv.mu.Unlock()
	}
	//DPrintf("kvserver %d got %s() RPC, insert op %+v at %d, reply WrongLeader = %v",kv.me, op.Name, op, index, wrongLeader)

	kv.mu.Lock()
	delete(kv.notifyChs, index)
	kv.mu.Unlock()
	return wrongLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Name:      "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = kv.waitApplying(op, 500*time.Millisecond)
	if !reply.WrongLeader {
		kv.mu.Lock()
		value, ok := kv.db[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = value
			return
		}
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Name:      args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = kv.waitApplying(op, 500*time.Millisecond)
}

func (kv *KVServer) applyloop() {
	for msg := range kv.applyCh {
		if msg.CommandValid == false {
			continue
		}

		op := msg.Command.(Op)
		//DPrintf("kvserver %d start applying command %s at index %d, request id %d, client id %d", kv.me, op.Name, msg.CommandIndex, op.RequestId, op.ClientId)
		kv.mu.Lock()
		if kv.isDuplicateRequest(op.ClientId, op.RequestId) {
			kv.mu.Unlock()
			continue
		}
		switch op.Name {
		case "Put":
			kv.db[op.Key] = op.Value
		case "Append":
			kv.db[op.Key] += op.Value
			// Get() does not need to modify db, skip
		}
		kv.acknowledgedRequests[op.ClientId] = op.RequestId

		if ch, ok := kv.notifyChs[msg.CommandIndex]; ok {
			notify := Notification{
				ClientId:  op.ClientId,
				RequestId: op.RequestId,
			}
			ch <- notify
		}

		kv.mu.Unlock()
		//DPrintf("kvserver %d applied command %s at index %d, request id %d, client id %d",kv.me, op.Name, msg.CommandIndex, op.RequestId, op.ClientId)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.acknowledgedRequests = make(map[int64]int)
	kv.notifyChs = make(map[int]chan Notification)

	go kv.applyloop()

	return kv
}

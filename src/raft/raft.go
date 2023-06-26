package raft

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("readPersist error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg

	status    int
	votecount int

	electWin  chan bool
	granted   chan bool
	heartbeat chan bool
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}

func (rf *Raft) GetLastindex() int {
	return len(rf.log) - 1
}

func (rf *Raft) GetLastTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
	LastlogIdx  int
	LogTerm     int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) isUptoDate(cIndex int, cTerm int) bool {
	term, index := rf.GetLastTerm(), rf.GetLastindex()
	if cTerm != term {
		return cTerm >= term
	}
	return cIndex >= index
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	defer rf.persist()

	defer rf.mu.Unlock()

	// Do not grant vote if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//log.Printf("worker [%d] refuse to vote for [%d] with term [%d] ------", rf.me, args.CandidateId, rf.currentTerm)
		return
	}
	// Convert to follower state if term > currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUptoDate(args.LastlogIdx, args.LogTerm) {
		rf.granted <- true
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		//log.Printf("Follower [%d] vote for Candidate [%d] with term [%d] ------", rf.me, args.CandidateId, rf.currentTerm)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return ok
	}

	if rf.status != Candidate || args.Term != rf.currentTerm {
		//log.Printf("Candidate [%d] wit term [%d] is not Candidate or term [%d] changed ------", rf.me, rf.currentTerm, args.Term)
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1

		rf.persist()

		return ok
	}
	if reply.VoteGranted {
		rf.votecount++
		if rf.votecount > len(rf.peers)/2 {
			rf.status = Leader
			rf.electWin <- true
			//log.Printf("Candidate [%d] become Leader with term [%d] ------", rf.me, rf.currentTerm)
		}
	}
	return ok
}

func (rf *Raft) sendAllRequestVote() {

	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastlogIdx = rf.GetLastindex()
	args.LogTerm = rf.GetLastTerm()
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.status == Candidate {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	//2(B)
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	NextTry int
}

func (rf *Raft) HasLogConflict(restlogs []LogEntry, appendlogs []LogEntry) bool {
	for i := 0; i < len(restlogs); i++ {
		if i >= len(appendlogs) {
			return true
		}
		if restlogs[i].Term != appendlogs[i].Term {
			return true
		}
	}
	return false
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		if rf.status == Leader {
			//log.Printf("Leader [%d] apply Command with term [%d] ------", rf.me, rf.log[i].Term)
		} else {
			//log.Printf("Follower [%d] apply Command with term [%d] ------", rf.me, rf.log[i].Term)
		}
		rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.log[i].Command, CommandValid: true}
	}

	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	defer rf.persist()

	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}

	rf.heartbeat <- true
	//TODO: part 2B
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.GetLastindex() {
		reply.Success = false
		reply.NextTry = rf.GetLastindex() + 1
		return
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		term := rf.log[args.PrevLogIndex].Term
		for reply.NextTry = args.PrevLogIndex - 1; reply.NextTry > 0 && rf.log[reply.NextTry].Term == term; reply.NextTry-- {
		}
		reply.NextTry++
		reply.Success = false
	} else {
		restlog := rf.log[args.PrevLogIndex+1:]
		rf.log = rf.log[:args.PrevLogIndex+1]
		if rf.HasLogConflict(restlog, args.Entries) || len(restlog) < len(args.Entries) {
			rf.log = append(rf.log, args.Entries...)
			//log.Printf("Follower [%d] append logs with term [%d]------", rf.me, rf.currentTerm)
		} else {
			rf.log = append(rf.log, restlog...)
		}
		reply.Success = true
		reply.NextTry = rf.GetLastindex() + 1
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > rf.GetLastindex() {
				rf.commitIndex = rf.GetLastindex()
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			go rf.applyLogs()
			//log.Printf("Follower [%d] apply logs with term [%d] ------", rf.me, rf.currentTerm)
		}
	}

	//log.Printf("Follower [%d] receive heartbeat from Leader [%d] with term [%d]", rf.me, args.LeaderId, rf.currentTerm)
	return
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return ok
	}
	if rf.status != Leader || args.Term != rf.currentTerm {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.votedFor = -1

		rf.persist()

		return ok
	}
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = reply.NextTry
	}
	//log.Printf("Leader [%d] receive heartbeat reply from worker [%d] with term [%d] ------", rf.me, server, rf.currentTerm)

	//log.Printf("Leader [%d] to decide whether to apply logs, now we have matchIndex array [%d] ------", rf.me, rf.matchIndex)

	for N := rf.GetLastindex(); N > rf.commitIndex; N-- {
		count := 1

		if rf.log[N].Term == rf.currentTerm {
			for i := range rf.peers {
				if rf.matchIndex[i] >= N {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLogs()
			break
		}

	}

	return ok
}

func (rf *Raft) SendAllAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//log.Printf("Leader [%d] send heartbeat with term [%d]", rf.me, rf.currentTerm)

	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			args := &AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.LeaderCommit = rf.commitIndex
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			if rf.nextIndex[i] <= rf.GetLastindex() {
				args.Entries = rf.log[rf.nextIndex[i]:]
			}
			go rf.SendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}

}

func (rf *Raft) runServer() {
	for {
		switch rf.status {
		case Leader:
			//log.Printf("Leader [%d] send heartbeat to all workers with term [%d] ------", rf.me, rf.currentTerm)
			rf.SendAllAppendEntries()
			time.Sleep(time.Millisecond * 120)

		case Follower:
			select {
			case <-rf.granted:
			case <-rf.heartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
				rf.status = Candidate
			}
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.votecount = 1

			rf.persist()

			rf.mu.Unlock()
			rf.sendAllRequestVote()

			select {
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
			case <-rf.electWin:
				rf.status = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.GetLastindex() + 1
					rf.matchIndex[i] = 0
				}
			case <-rf.heartbeat:
				rf.status = Follower
			}
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return 0, 0, false
	}

	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{term, command})
	//log.Printf("Leader [%d] receive command with term [%d] ------", rf.me, rf.currentTerm)

	rf.persist()

	return rf.GetLastindex(), term, true

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votecount = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = Follower
	rf.applyCh = applyCh
	rf.electWin = make(chan bool)
	rf.granted = make(chan bool)
	rf.heartbeat = make(chan bool)

	rf.log = append(rf.log, LogEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.persist()

	//log.Printf("worker [%d] start with term [%d] ------ ", rf.me, rf.currentTerm)
	go rf.runServer()
	return rf
}

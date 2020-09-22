package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"

// import crand "crypto/rand"
// import "math/big"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER = 1 + iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastContactTime time.Time

	// 'leader' 'candidate' 'follower'
	meState int
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.meState == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("....Check Vote: voted:%v  args.Term:%v vote:%v? %v@%v",
		rf.votedFor, args.Term, args.CandidateID, rf.me, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = 0
	reply.Term = MaxInt(args.Term, rf.currentTerm)

	// Reject it when term is less current
	if args.Term < rf.currentTerm {
		return
	}
	// args.Term >= rf.currentTerm
	if rf.votedFor == -1 {
		// If one server’s current term is smaller than the other’s,
		// then it updates its current term to the larger value
		reply.VoteGranted = 1
		rf.TransToFollower(args.Term)
		DPrintf("%v@%v vote for %v\n", rf.me, args.Term, args.CandidateID)
	}

}

func (rf *Raft) TransToFollower(term int) {
	rf.votedFor = -1
	rf.meState = FOLLOWER
	rf.lastContactTime = time.Now()
	rf.currentTerm = term
	go rf.checkStatus()
}

func (rf *Raft) TransToLeader() {
	rf.mu.Lock()
	rf.meState = LEADER
	lastLogEntry := rf.GetLastLogEntry()
	for id := range rf.nextIndex {
		rf.nextIndex[id] = lastLogEntry.Index + 1
	}
	for id := range rf.matchIndex {
		rf.matchIndex[id] = 0
	}
	go rf.sendHeartBeat()
	rf.mu.Unlock()
}

func (rf *Raft) GetLastLogEntry() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) GetLogEntry(i int) LogEntry {
	return rf.logs[i]
}

func (rf *Raft) LastLogIndex() int {
	return rf.logs[len(rf.logs)-1].Index
}

func (rf *Raft) TryApply() {
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		logEntry := rf.logs[rf.lastApplied]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
		DPrintf("%v now try to apply ............\n", rf.me)
		rf.applyCh <- applyMsg
	}
}

// Invoked by leader to replicate log entries
// also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	rf.TransToFollower(args.Term)
	rf.TryApply()

	prevLog := rf.GetLogEntry(args.PrevLogIndex)

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if len(rf.logs) <= args.PrevLogIndex {
		// leader will retry next time
		rf.logs = rf.logs[:prevLog.Index]
		return
	}

	// If an existing entry conflicts with a new one
	// (same index but different terms), delete the existing entry
	// and all that follow it (§5.3)
	if prevLog.Index != args.PrevLogIndex || prevLog.Term != args.PrevLogTerm {
		return
	}
	pos1 := args.PrevLogIndex + 1
	pos2 := 0
	for pos1 < len(rf.logs) && pos2 < len(args.Entries) {
		if rf.logs[pos1].Index == args.Entries[pos2].Index &&
			rf.logs[pos1].Term == args.Entries[pos2].Term {
			pos1++
			pos2++
		} else {
			break
		}
	}
	rf.logs = append(rf.logs[:pos1], args.Entries[pos2:]...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = MinInt(args.LeaderCommit, rf.LastLogIndex())
	}

	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.meState != LEADER {
		return -1, -1, false
	} else {
		lastEntry := rf.GetLastLogEntry()
		index := lastEntry.Index + 1
		newEntry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		rf.logs = append(rf.logs, newEntry)
		DPrintf("begin to agree: %v %v\n", rf.me, index)
		go rf.TryAgreement(command, index)
		return index, rf.currentTerm, true
	}
}

func (rf *Raft) TryAgreement(command interface{}, index int) {
	number := len(rf.peers) - 1
	results := make(chan int, number)
	appended := make([]bool, len(rf.peers))
	agreed := 0
	for {
		if rf.meState != LEADER || rf.killed() || agreed == len(rf.peers)-1 {
			return
		}
		for id := range rf.peers {
			if id == rf.me || appended[id] {
				continue
			}
			go func(id int) {
				nextIndex := rf.nextIndex[id]
				lastLogIndex := rf.GetLastLogEntry().Index
				prevLog := rf.GetLogEntry(nextIndex - 1)
				entries := rf.logs[nextIndex:]
				logArgs := AppendEntriesArgs{rf.currentTerm, rf.me,
					prevLog.Index, prevLog.Term, rf.commitIndex, entries}
				logReply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(id, &logArgs, &logReply)
				DPrintf("%v agree result: %v\n", id, logReply)
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if ok && logReply.Success {
					if logReply.Term > rf.currentTerm {
						rf.TransToFollower(logReply.Term)
						return
					}
					if !appended[id] {
						appended[id] = true
						DPrintf("%v set appended true", id)
						rf.nextIndex[id] = MaxInt(rf.nextIndex[id], lastLogIndex+1)
						rf.matchIndex[id] = MaxInt(rf.matchIndex[id], lastLogIndex)
					}
					results <- 1
				} else {
					rf.nextIndex[id]--
					results <- 0
				}
			}(id)
		}

		for i := 0; i < number; i++ {
			v := <-results
			if v == 1 {
				agreed++
			}
		}
		DPrintf("total aggreed: %v\n", agreed)
		if agreed > (number / 2) {
			rf.mu.Lock()
			rf.commitIndex = MaxInt(rf.commitIndex, index)
			DPrintf("%v now update commitIndex: %v\n", rf.me, rf.commitIndex)
			rf.mu.Unlock()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) checkStatus() {
	for {
		if rf.killed() || rf.meState == LEADER {
			break
		}
		//random time out
		ms := 800 + rand.Intn(200)
		timeout := time.Duration(ms) * time.Millisecond
		time.Sleep(timeout)
		diff := time.Now().Sub(rf.lastContactTime)
		if diff >= timeout {
			DPrintf("id(%v) with state(%v) start kickoff at term: %v\n",
				rf.me, rf.meState, rf.currentTerm)
			rf.kickOffElection()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for {
		if rf.killed() || rf.meState != LEADER {
			break
		}

		timeout := time.Duration(50 * time.Millisecond)
		time.Sleep(timeout)

		rf.TryApply()

		lastLogEntry := rf.GetLastLogEntry()
		heartArgs := AppendEntriesArgs{
			rf.currentTerm,
			rf.me,
			lastLogEntry.Index,
			lastLogEntry.Term,
			rf.commitIndex,
			[]LogEntry{}}

		for id := range rf.peers {
			if id == rf.me {
				continue
			}
			go func(id int) {
				heartReply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(id, &heartArgs, &heartReply)
				// If RPC request or response contains term T > currentTerm:
				// set currentTerm = T, convert to follower (§5.1)
				if ok {
					rf.mu.Lock()
					if heartReply.Term > rf.currentTerm {
						rf.TransToFollower(heartReply.Term)
					}
					rf.mu.Unlock()
				}
			}(id)
		}
	}
}

func (rf *Raft) kickOffElection() {
	for {
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		rf.meState = CANDIDATE
		rf.votedFor = -1
		rf.currentTerm++
		rf.mu.Unlock()

		votes := make(chan int, len(rf.peers)-1)
		//Sleep for a while
		ms := 150 + (rand.Int63() % 100)
		timeout := time.After(time.Duration(ms) * time.Millisecond)

		voteArgs := RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
		voteReply := RequestVoteReply{}
		for id := range rf.peers {
			if id == rf.me {
				continue
			}
			go func(id int) {
				ok := rf.sendRequestVote(id, &voteArgs, &voteReply)
				if ok {
					DPrintf("(%v@%v) %v => reply:%v\n", rf.me, rf.currentTerm, id, voteReply)
					if voteReply.VoteGranted == 1 {
						votes <- 1
					} else {
						rf.mu.Lock()
						if voteReply.Term > rf.currentTerm {
							rf.TransToFollower(voteReply.Term)
						}
						rf.mu.Unlock()
						votes <- 0
					}
				}
			}(id)
		}

		sended := 0
		granted := 0
		select {
		case v := <-votes:
			sended++
			if v == 1 {
				granted++
			}
		case <-timeout:
			DPrintf("timeout: %v@%v\n", rf.me, rf.currentTerm)
		}
		DPrintf("id: %v@%v granted:%v sended:%v\n", rf.me, rf.currentTerm, granted, sended)
		if sended > 0 && (granted > sended/2) {
			DPrintf("%v@%v become LEADER ....\n", rf.me, rf.currentTerm)
			rf.TransToLeader()

			return
		}
		if rf.meState == FOLLOWER {
			return
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastContactTime = time.Now()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logs = append(rf.logs, LogEntry{Index: 0, Term: 0})
	rf.applyCh = applyCh

	//When servers start up, they begin as followers
	rf.meState = FOLLOWER
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.TransToFollower(0)
	return rf
}

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
import crand "crypto/rand"
import "math/big"
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

type RfState int

const (
	FOLLOWER = 1 + iota
	CANDIDATE
	LEADER
)

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
	logs        interface{}

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastContactTime time.Time

	// 'leader' 'candidate' 'follower'
	meState int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("....Check Vote: voted:%v  args.Term:%v vote:%v? %v@%v", rf.votedFor, args.Term, args.CandidateID, rf.me, rf.currentTerm)
	reply.VoteGranted = 0
	reply.Term = MaxInt(args.Term, rf.currentTerm)

	// Reject it when term is less current
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term == rf.currentTerm && rf.meState == LEADER {
		return
	}

	rf.mu.Lock()
	if rf.votedFor == -1 {
		// If one server’s current term is smaller than the other’s,
		// then it updates its current term to the larger value
		reply.VoteGranted = 1
		rf.votedFor = args.CandidateID
		DPrintf("%v@%v vote for %v\n", rf.me, args.Term, args.CandidateID)
		rf.meState = FOLLOWER
	}
	rf.currentTerm = MaxInt(args.Term, rf.currentTerm)
	rf.mu.Unlock()

}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success int
}

// Invoked by leader to replicate log entries
// also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm && rf.meState != FOLLOWER {
		rf.meState = FOLLOWER
		rf.currentTerm = args.Term
		reply.Term = MaxInt(rf.currentTerm, args.Term)
	} else {
		rf.lastContactTime = time.Now()
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	go func() {
		for true {
			if rf.killed() {
				break
			}
			//random time out
			maxms := big.NewInt(60)
			ms, _ := crand.Int(crand.Reader, maxms)
			timeout := time.Duration(maxms.Int64()+ms.Int64()) * time.Millisecond
			select {
			case <-time.After(timeout):
				diff := time.Now().Sub(rf.lastContactTime)
				state := rf.meState
				if state == FOLLOWER && diff >= timeout {
					DPrintf("id(%v) with state(%v) start kickoff at term: %v\n", rf.me, rf.meState, rf.currentTerm)
					rf.kickOffElection()
				}
			}
		}
	}()
}

func (rf *Raft) sendHeartBeat() {
	go func() {
		for true {
			if rf.killed() {
				break
			}
			timeout := time.Duration(30 * time.Millisecond)
			select {
			case <-time.After(timeout):
				if rf.meState == LEADER {
					//DPrintf("leader: %v term: %v heartbeat.....\n", rf.me, rf.currentTerm)
					for id := range rf.peers {
						if id != rf.me {
							go func(id int) {
								heartArgs := AppendEntriesArgs{rf.currentTerm, rf.me}
								heartReply := AppendEntriesReply{}
								ok := rf.sendAppendEntries(id, &heartArgs, &heartReply)
								// If RPC request or response contains term T > currentTerm:
								// set currentTerm = T, convert to follower (§5.1)
								if ok {
									rf.mu.Lock()
									if heartReply.Term > rf.currentTerm {
										rf.currentTerm = heartReply.Term
										rf.meState = FOLLOWER
									}
									rf.mu.Unlock()
								}
							}(id)
						}
					}
				}
			}
		}
	}()
}

func (rf *Raft) kickOffElection() {
	for true {
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
		ms := 100 + (rand.Int63() % 100)
		timeout := time.After(time.Duration(ms) * time.Millisecond)

		for id, _ := range rf.peers {
			if id != rf.me {
				go func(id int) {
					rf.mu.Lock()
					voteArgs := RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
					rf.mu.Unlock()
					voteReply := RequestVoteReply{}
					ok := rf.sendRequestVote(id, &voteArgs, &voteReply)
					if ok {
						DPrintf("(%v@%v) %v => reply:%v\n", rf.me, rf.currentTerm, id, voteReply)
						if voteReply.VoteGranted == 1 {
							votes <- 1
						} else {
							rf.mu.Lock()
							if voteReply.Term > rf.currentTerm {
								rf.currentTerm = voteReply.Term
								rf.meState = FOLLOWER

							}
							rf.mu.Unlock()
							votes <- 0
						}
					} else {
						votes <- -1
					}
				}(id)
			}
		}

		sended := 0
		granted := 0
		select {
		case v := <-votes:
			if v == 1 || v == 0 {
				sended++
			}
			if v == 1 {
				granted++
			}
		case <-timeout:
			DPrintf("timeout: %v@%v\n", rf.me, rf.currentTerm)
		}
		DPrintf("id: %v@%v granted:%v sended:%v\n", rf.me, rf.currentTerm, granted, sended)
		if sended > 0 && (granted > sended/2) {
			DPrintf("%v@%v become LEADER ....\n", rf.me, rf.currentTerm)
			rf.mu.Lock()
			rf.meState = LEADER
			rf.mu.Unlock()
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

	//When servers start up, they begin as followers
	rf.meState = FOLLOWER
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.checkStatus()
	rf.sendHeartBeat()
	return rf
}

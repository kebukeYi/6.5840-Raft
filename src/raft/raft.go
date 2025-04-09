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

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"raftCourse/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// PartA
	role        Role
	currentTerm int
	votedFor    int
	leaderID    int

	// PartA used for election loop
	electionStart   time.Time
	electionTimeout time.Duration

	// PartB
	// log        []LogEntry
	nextIndex  []int
	matchIndex []int

	// PartC
	commitIndex int
	lastApplied int
	applyCond   *sync.Cond
	applyCh     chan ApplyMsg

	// PartD
	rlog        *RaftLog
	snapPending bool
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) GetStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}
func (rf *Raft) becomeFollowerLocked(term int) {
	if rf.currentTerm > term {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower currentTerm:%d, lower term:%d", rf.currentTerm, term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s -> Follower, For T%d->T%d", rf.role, rf.currentTerm, term)
	shouldPersist := term != rf.currentTerm
	if term > rf.currentTerm {
		rf.votedFor = -1
	} else {
		// becomeFollower() -> term == rf.currentTerm;
		// 说明在term任期内,自己没有获选leader, 只能从 candidate 变为 follower, 但是同一个任期内不能投票两次;
	}
	rf.role = Follower
	rf.currentTerm = term
	if shouldPersist {
		rf.persistLocked()
	}
}
func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}
	LOG(rf.me, rf.currentTerm, DVote, "%s -> Candidate, For T%d->T%d", rf.role, rf.currentTerm, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.reSetElectionBeat()
	rf.persistLocked()
}
func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "%s -> Leader, For T%d", rf.role, rf.currentTerm)
	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.rlog.size()
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) isMoreUpDate(index int, term int) bool {
	lastLogIndex := rf.rlog.size() - 1
	lastLogTerm := rf.rlog.at(lastLogIndex).Term
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d",
		lastLogIndex, lastLogTerm, index, term)
	if lastLogTerm != term {
		return lastLogTerm > term
	}
	return lastLogIndex > index
}

func (rf *Raft) getMajorityIndexLocked() int {
	temp := make([]int, len(rf.matchIndex))
	copy(temp, rf.matchIndex)
	sort.Ints(temp)
	//index1 := len(temp)/2 - 1
	index2 := (len(temp) - 1) / 2
	return temp[index2]
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	// 自己保存log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.GetState() // 内部也会尝试获得锁, 因此会导致死锁阻塞;
	if rf.role != Leader { // 直接进行比较即可
		return 0, 0, false
	}
	rf.rlog.append(LogEntry{
		Command:      command,
		CommandValid: true,
		Term:         rf.currentTerm,
	})
	rf.persistLocked()
	index := rf.rlog.size() - 1
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", index, rf.currentTerm)
	return index, rf.currentTerm, true
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// rf.log = append(rf.log, LogEntry{Term: InvalidTerm})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.rlog = NewRaftLog(InvalidIndex, InvalidTerm, nil, nil)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	go rf.applyTicker()

	return rf
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.role == role && rf.currentTerm == term)
}

func (rf *Raft) logString() string {
	var terms string
	prevTerm := rf.rlog.at(0).Term
	prevStart := 0
	for i := 0; i < rf.rlog.size(); i++ {
		if rf.rlog.at(i).Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d;", prevStart, i-1, prevTerm)
			prevTerm = rf.rlog.at(i).Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf(" [%d, %d]T%d;", prevStart, rf.rlog.size()-1, prevTerm)

	return terms
}

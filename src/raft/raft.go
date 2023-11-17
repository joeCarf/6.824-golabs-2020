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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       ServerStatus //当前节点的状态
	currentTerm int          //当前任期号
	votedFor    int          //投票给了谁

	heartbeatTimer *time.Timer //心跳包超时计时器
	electionTimer  *time.Timer //超时选举计时器
}

//raft中节点的状态
type ServerStatus int

const (
	FOLLOWER ServerStatus = iota
	CANDIDATE
	LEADER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
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
	Term        int //candidate的term
	CandidateId int //candidate的id
	//FIXME: 2B中需要添加额外的log相关
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //接受者的term, 用来让candidate可以更新自己的状态
	VoteGranted bool //是否投票给自己
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf(dVote, "T%d: S%d Vote refused for small term", rf.currentTerm, rf.me)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	//server rule: 只要遇到更新的term号，自己就要变为FOLLOWER，并且更新自己的Term
	if args.Term > rf.currentTerm {
		DPrintf(dVote, "T%d: S%d <- S%d receive forward term, convert to follower: [leaderTerm=%d > currentTerm=%d]", rf.currentTerm, rf.me, args.CandidateId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	//If votedFor is null or candidateId, vote for
	// 如果自己已经投过票了, 就退出
	//TODO: 为什么这里如果已经投给自己了也算, 感觉只有前边也可以通过
	if !(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		DPrintf(dVote, "T%d: S%d Has voted in this term %d", rf.currentTerm, rf.me, rf.currentTerm)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	//投票给他
	DPrintf(dVote, "T%d: S%d -> S%d Vote for candidate", rf.currentTerm, rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(rf.randomElectionTime())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	return
}

//==================AppendEntries RPC相关====================//
type AppendEntriesArgs struct {
	Term     int //leader的term
	LeaderId int //leader的id
}
type AppendEntriesReply struct {
	Term    int  //接收者的term, 让leader可以更新自己状态
	Success bool //AP操作是否成功
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果收到了落后的term, 回复false并让它更新
	if args.Term < rf.currentTerm {
		DPrintf(dAppend, "T%d: S%d <- S%d refused AppendEntries from leader for backward term: [leaderTerm=%d < currentTerm=%d]", rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	//server rule: 如果收到了更新的请求, convert to follower
	if args.Term > rf.currentTerm {
		DPrintf(dAppend, "T%d: S%d <- S%d receive forward term, convert to follower: [leaderTerm=%d > currentTerm=%d]", rf.currentTerm, rf.me, args.LeaderId, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	// 收到心跳包, 就更新选举计时器
	rf.electionTimer.Reset(rf.randomElectionTime())
	DPrintf(dAppend, "T%d: S%d <- S%d receive AppendEntries.", rf.currentTerm, rf.me, args.LeaderId)
	reply.Term, reply.Success = rf.currentTerm, true
	return
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

//====================处理函数===============================//
//
// leaderElection
//  @Description: 发起选举, 调用RequestVote RPC 请求选票
//  @receiver rf
//
func (rf *Raft) leaderElection() {
	//转candidate需要遵守的规则: Increment currentTerm, Vote for myself, Reset election timers
	rf.mu.Lock()
	DPrintf(dTicker, "T%d: S%d ElectionTimer timeout, leader election started", rf.currentTerm, rf.me)
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionTimer.Reset(rf.randomElectionTime())
	rf.mu.Unlock()
	// Send RequestVote RPC to all other servers
	getVotes := 1                       //得到的选票数, 默认自己有一张选票
	finishVotes := 1                    //已经完成投票的节点数
	var leMutex sync.Mutex              //用来互斥访问getVotes
	var condRV = sync.NewCond(&leMutex) //条件变量来检查
	//NOTE: 不可以用waitGroup让主线程等待所有rpc请求都完成, 因为可能出现部分节点crash, 只要有半数就行了
	var finWg sync.WaitGroup
	for peer := range rf.peers {
		if peer == rf.me {
			continue //如果是自己直接跳过
		}
		//异步发送请求
		finWg.Add(1)
		DPrintf(dVote, "T%d: S%d -> S%d send request for votes.", rf.currentTerm, rf.me, peer)
		go func(peer int) {
			defer finWg.Done()
			rf.mu.RLock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			reply := RequestVoteReply{}
			// 发送RPC之前一定要释放锁, 这种长时间的操作持锁很容易死锁
			rf.mu.RUnlock()
			ok := rf.sendRequestVote(peer, &args, &reply)
			//如果RPC发送失败了, 直接返回了
			if !ok {
				//DPrintf(dVote, "T%d: S%d received no reply from S%d", rf.currentTerm, rf.me, peer)
			}
			//NOTE: 加了一个过期rpc的处理, 比如发现args.term已经小于currentTerm了, 说明这是个过期的, 直接忽略就好, 因为RPC处理过程肯定能长, 因为节点是会crash的
			rf.mu.RLock()
			if args.Term < rf.currentTerm {
				DPrintf(dVote, "T%d: S%d <- S%d received expired rpc reply. [args.Term%d, currentTerm=%d]", rf.currentTerm, rf.me, peer, args.Term, rf.currentTerm)
				rf.mu.RUnlock()
				return
			}
			rf.mu.RUnlock()
			leMutex.Lock()
			if reply.VoteGranted == true {
				getVotes++ // 如果投票给你, 得票数++
			} else {
				rf.mu.Lock()
				// 如果不投给你, 需要检查是否需要更新状态, 需要转为follower
				if reply.Term > rf.currentTerm {
					DPrintf(dVote, "T%d: S%d <- S%d received newer term %d from servers", rf.currentTerm, rf.me, peer, reply.Term)
					rf.state = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					//Student Guide里面说: 只有三个时机需要重置选举时间超时
					//rf.electionTimer.Reset(rf.randomElectionTime())
				}
				rf.mu.Unlock()
			}
			finishVotes++
			condRV.Broadcast()
			leMutex.Unlock()
		}(peer)
	}
	leMutex.Lock()
	for getVotes <= len(rf.peers)/2 && finishVotes != len(rf.peers) {
		condRV.Wait()
	}
	var retVotes = getVotes
	leMutex.Unlock()
	// 首先需要保证自己还是candicate, 因为在选举过程中可能会更新状态
	rf.mu.Lock()
	if rf.state == CANDIDATE {
		//如果受到的选票过半, 说明可以转为leader
		if retVotes > len(rf.peers)/2 {
			DPrintf(dVote, "T%d: S%d received majority votes, convert to leader", rf.currentTerm, rf.me)
			rf.state = LEADER
			//NOTE: 这里不可以重置选票, 成为leader之后没有投票权
			//rf.votedFor = -1
			//发心跳包通知其他节点自己Leader的身份
			rf.heartbeatTimer.Reset(0 * time.Second)
			//go rf.broadcastHeartbeat() //发心跳包通知其他节点自己Leader的身份
		}
		//就算失败, 也不需要额外做什么, 因为一个任期内只能选举一次, 失败了就等下一次
	}
	rf.mu.Unlock()
	finWg.Wait()
}

//
// broadcastHeartbeat
//  @Description: 广播心跳
//  @receiver rf
//
func (rf *Raft) broadcastHeartbeat() {
	rf.mu.RLock()
	DPrintf(dTicker, "T%d: S%d HearBeatTimer timeout, broad heart beat started", rf.currentTerm, rf.me)
	rf.mu.RUnlock()
	var bhbWg sync.WaitGroup
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		bhbWg.Add(1)
		go func(peer int) {
			//FIXME: 为什么这里放到外面, 就会死循环, 按理说就要放外面才对
			//bhbWg.Add(1)
			defer bhbWg.Done()
			rf.mu.RLock()
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}
			rf.mu.RUnlock()
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				//DPrintf(dAppend, "T%d: S%d received no reply from S%d", rf.currentTerm, rf.me, peer)
				return
			}
			//NOTE: 加了一个过期rpc的处理, 比如发现args.term已经小于currentTerm了, 说明这是个过期的, 直接忽略就好, 因为RPC处理过程肯定能长, 因为节点是会crash的
			rf.mu.RLock()
			if args.Term < rf.currentTerm {
				DPrintf(dAppend, "T%d: S%d <- S%d received expired rpc reply. [args.Term%d, currentTerm=%d]", rf.currentTerm, rf.me, peer, args.Term, rf.currentTerm)
				rf.mu.RUnlock()
				return
			}
			rf.mu.RUnlock()
			//如果失败了, 说明对方的term更大, 需要更新状态为follower
			rf.mu.Lock()
			if reply.Success == false {
				if reply.Term > rf.currentTerm {
					DPrintf(dAppend, "T%d: S%d <- S%d received newer term %d from servers", rf.currentTerm, rf.me, peer, reply.Term)
					rf.state = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					//rf.electionTimer.Reset(rf.randomElectionTime())
				}
			}
			rf.mu.Unlock()
		}(peer)
	}
	// 等待RPC都结束了再退出, 因为有可能会转变状态
	bhbWg.Wait()
}

//====================重要的后台函数==========================//
//
// ticker
//  @Description: 后台进程, 监视raft节点的计时器, 触发对应的处理函数
//  @receiver rf
//
func (rf *Raft) ticker() {
	//需要保证退出机制, 就是rf节点退出的时候
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			//选举计时器超时, follower的规则: 选举计时器超时, 并且要没有收到其他的选票, 且自己没有投过票
			//总结一下就是能选举的条件是: 不是leader, 且在这个term中没投过票
			rf.mu.RLock()
			canElect := rf.state != LEADER
			rf.mu.RUnlock()
			if canElect {
				//NOTE: 这里需要用另一个线程去执行, 因为如果这个过程耗时较长(比如rpc连不到,要等rpc返回值), 会导致ticker阻塞;
				go rf.leaderElection()
			}
			rf.electionTimer.Reset(rf.randomElectionTime()) //只要触发了, 就自动重置一次
		case <-rf.heartbeatTimer.C:
			// 发送心跳包计时器超时, 需要发送心跳包, 同样只有leader可以发心跳包
			rf.mu.RLock()
			canBroadHB := rf.state == LEADER
			rf.mu.RUnlock()
			if canBroadHB {
				go rf.broadcastHeartbeat()
			}
			rf.heartbeatTimer.Reset(rf.stableHeartbeatTime())
		}
	}
}

//=================自定义功能函数=========================//
//
// RandomElectionTime
//  @Description: 生成选举超时计时器的随机数
//  @receiver rf
//  @return time.Duration
//
func (rf *Raft) randomElectionTime() time.Duration {
	ms := 600 + (rand.Int63() % 300)
	return time.Duration(ms) * time.Millisecond
}

//
// StableHeartbeatTime
//  @Description: 生成固定的心跳包间隔
//  @receiver rf
//  @return time.Duration
//
func (rf *Raft) stableHeartbeatTime() time.Duration {
	ms := 150
	return time.Duration(ms) * time.Millisecond
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
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(rf.stableHeartbeatTime()) //只有当了leader才能初始化这个心跳计时器
	rf.electionTimer = time.NewTimer(rf.randomElectionTime())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//开启后台监视进程
	go rf.ticker()

	return rf
}

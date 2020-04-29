// Core Raft implementation - Consensus Module.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DebugCM = 1

type LogEntry struct {
	Command interface{}
	Term    int
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

// ConsensusModule (CM) 实现的是Raft一致性集群中的一个节点
type ConsensusModule struct {
	// mu 用于保护对CM的并发访问.
	mu sync.Mutex

	// id 当前CM对应的服务器ID.
	id int

	// peerIds 罗列了集群中所有同伴的ID.
	peerIds []int

	// server 包含当前CM的服务器，用于向其它同伴发起RPC请求
	server *Server

	// 所有服务器都需要持久化存储的 Raft state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// 所有服务器中经常修改的 Raft state
	commitIndex        int
	lastApplied        int
	state              CMState
	electionResetEvent time.Time

	// 领导者中经常修改的 Raft state
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine.
/*
	NewConsensusModule方法使用给定的服务器ID、同伴ID列表peerIds以及服务器server来创建一个新的CM实例。
	ready channel用于告知CM所有的同伴都已经连接成功，可以安全启动状态机。
*/
func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		// The CM is quiescent until ready is signaled; then, it starts a countdown
		// for leader election.
		// 收到ready信号前，CM都是静默的；收到信号之后，就会开始选主倒计时
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return cm
}

// Report reports the state of this CM.
// Report方法会呈报当前CM的状态属性
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop stops this CM, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) for all goroutines to
// exit.
// Stop方法可以暂停当前CM，清除其状态。该方法很快结束，
// 但是所有的goroutine退出可能需要一点时间（取决于 选举等待时间）
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
	cm.dlog("becomes Dead")
}

// 如果DebugCM > 0，dlog 会记录一条调试信息.
func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] ", cm.id) + format
		log.Printf(format, args...)
	}
}

// 对照论文中的 figure 2
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC.
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("收到RequestVote请求: %+v [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlog("... 当前任期早于RequestVote任期，变为Follower")
		cm.becomeFollower(args.Term)
	}

	// 任期相同，且未投票或已投票给当前请求同伴，则返回赞成投票；否则，返回反对投票。
	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.dlog("... RequestVote 应答: %+v", reply)
	return nil
}

// 对照论文中的 figure 2
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.state == Dead {
		return nil
	}
	cm.dlog("收到AppendEntries请求: %+v", args)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlog("... 当前任期早于AppendEntries任期，变为Follower")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		// 如果当前状态不是追随者，则变为追随者
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.dlog("AppendEntries 应答: %+v", *reply)
	return nil
}

// electionTimeout generates a pseudo-random election timeout duration.
// electionTimeout方法生成一个伪随机的选举等待时长
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// 如果设置了 RAFT_FORCE_MORE_REELECTION, 会有意地经常返回硬编码的数字来进行压力测试。
	// 这会造成不同服务器之间的冲突，迫使集群出现更多的重新选举
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond
	}
}

// runElectionTimer implements an election timer. It should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the CM state changes from follower/candidate or the term changes.
/*
	runElectionTimer实现的是选举定时器。如果我们想在新一轮选举中作为候选人，就要启动这个定时器。
	该方法是阻塞的，需要在独立的goroutine中运行；它应该用作单次（一次性）选举定时器，
	因为一旦任期变化或者CM状态不是追随者/候选人，该方法就会退出。
*/
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()
	cm.dlog("选举定时器启动 (%v), 任期term=%d", timeoutDuration, termStarted)

	/*
	  循环会在以下条件结束：
	  1 - 发现不再需要选举定时器
	  2 - 选举定时器超时，CM变为候选人
	  对于追随者而言，定时器通常会在CM的整个生命周期中一直在后台运行。
	*/
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Candidate && cm.state != Follower {
			cm.dlog("选举定时器中的服务器状态 state=%s, 退出", cm.state)
			cm.mu.Unlock()
			return
		}

		if termStarted != cm.currentTerm {
			cm.dlog("选举定时器中的term从 %d 变为 %d, 退出", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		// 如果在等待时间内没有收到领导者的心跳，也没有为其它候选人投票，就发起新一轮选举。
 		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// startElection starts a new election with this CM as a candidate.
// Expects cm.mu to be locked.
// startElection方法会将该CM作为候选人发起新一轮选举，要求cm.mu被锁定
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id
	cm.dlog("变为候选人 (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived int32 = 1

	// Send RequestVote RPCs to all other servers concurrently.
	// 同时向其它所有服务器发送RequestVote请求
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateId: cm.id,
			}
			var reply RequestVoteReply

			cm.dlog("向服务器 %d 发送RequestVote请求 : %+v", peerId, args)
			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlog("收到RequestVote应答 : %+v", reply)

				// 状态不是候选人，退出选举（可能退化为追随者，也可能已经胜选成为领导者）
				if cm.state != Candidate {
					cm.dlog("等待RequestVote回复时, 状态变为 %v", cm.state)
					return
				}

				// 存在更高任期（新领导者），转换为追随者
				if reply.Term > savedCurrentTerm {
					cm.dlog("当前任期term早于RequestVote应答中的任期")
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(cm.peerIds)+1 {
							// 获得票数超过一半，选举获胜，成为最新的领导者
							cm.dlog("以 %d 票数胜选，成为Leader", votes)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// Run another election timer, in case this election is not successful.
	// 另行启动一个选举定时器，以防本次选举不成功
	go cm.runElectionTimer()
}

// becomeFollower makes cm a follower and resets its state.
// Expects cm.mu to be locked.
// becomeFollower方法将cm变为追随者并重置其状态。要求cm.mu锁定
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlog("变为Follower, 任期term=%d; 日志log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// startLeader switches cm into a leader state and begins process of heartbeats.
// Expects cm.mu to be locked.
// startLeader方法将CM转换为领导者，并启动心跳程序。要求cm.mu锁定
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlog("成为Leader; term=%d, log=%v", cm.currentTerm, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		// 只要当前服务器是领导者，就要周期性发送心跳
		for {
			cm.leaderSendHeartbeats()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

// leaderSendHeartbeats sends a round of heartbeats to all peers, collects their
// replies and adjusts cm's state.
// leaderSendHeartbeats方法向所有同伴发送心跳，收集各自的回复并调整cm的状态值
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderId: cm.id,
		}
		go func(peerId int) {
			cm.dlog("发送 AppendEntries 请求到服务器 %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				// 存在更高任期，转换为追随者
				if reply.Term > savedCurrentTerm {
					cm.dlog("当前任期term早于heartbeat应答中的任期")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}

// Core Raft implementation - Consensus Module.
//
// Eli Bendersky [https://eli.thegreenplace.net]
// This code is in the public domain.
package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DebugCM = 1

/*
	CommitEntry就是Raft向提交通道发送的数据。每一条提交的条目都会通知客户端，
	表明指令已满足一致性，可以应用到客户端的状态机上。
*/
type CommitEntry struct {
	// Command 是被提交的客户端指令
	Command interface{}

	// Index 是被提交的客户端指令对应的日志索引
	Index int

	// Term 是被提交的客户端指令对应的任期
	Term int
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

type LogEntry struct {
	Command interface{}
	Term    int
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

	// storage 用于持久化状态
	storage Storage

	// commitChan是CM用来传输已提交日志条目的通道。在创建时由客户端（使用Raft进行复制的服务）传入。
	commitChan chan<- CommitEntry

	// newCommitReadyChan是一个内部通知channel，负责向日志中提交新条目的goroutine
	// 使用它来告知这些条目可以在commitChan上发送。
	newCommitReadyChan chan struct{}

	// triggerAEChan是一个内部通知channel，在出现关注的更改时触发向追随者发送新的AE请求。
	triggerAEChan chan struct{}

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
// it's safe to start its state machine. commitChan is going to be used by the
// CM to send log entries that have been committed by the Raft cluster.
/*
	NewConsensusModule方法使用给定的服务器ID、同伴ID列表peerIds以及服务器server来创建一个新的CM实例。
	ready channel用于告知CM所有的同伴都已经连接成功，可以安全启动状态机。
*/
func NewConsensusModule(id int, peerIds []int, server *Server, storage Storage, ready <-chan interface{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIds = peerIds
	cm.server = server
	cm.storage = storage
	cm.commitChan = commitChan
	cm.newCommitReadyChan = make(chan struct{}, 16)
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if cm.storage.HasData() {
		cm.restoreFromStorage(cm.storage)
	}

	go func() {
		// The CM is dormant until ready is signaled; then, it starts a countdown
		// for leader election.
		// 收到ready信号前，CM都是静默的；收到信号之后，就会开始选主倒计时
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	go cm.commitChanSender()
	return cm
}

// Report reports the state of this CM.
// Report方法会呈报当前CM的状态属性
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Submit submits a new command to the CM. This function doesn't block; clients
// read the commit channel passed in the constructor to be notified of new
// committed entries. It returns true iff this CM is the leader - in which case
// the command is accepted. If false is returned, the client will have to find
// a different CM to submit this command to.
/*
	Submit方法会向CM呈递一条新的指令。这个函数是非阻塞的;
	客户端读取构造函数中传入的commit channel，以获得新提交条目的通知。
	如果当前CM是领导者返回true——表示指令被接受了。
	如果返回false，客户端会寻找新的服务器呈递该指令。
*/
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	cm.dlog("%v 收到新指令呈递: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		cm.persistToStorage()
		cm.dlog("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return true
	}

	cm.mu.Unlock()
	return false
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
	close(cm.newCommitReadyChan)
}

// restoreFromStorage restores the persistent stat of this CM from storage.
// It should be called during constructor, before any concurrency concerns.
// restoreFromStorage从存储中加载CM的持久化状态。该方法应该在构造时调用，避免任何的并发调用。
func (cm *ConsensusModule) restoreFromStorage(storage Storage) {
	if termData, found := cm.storage.Get("currentTerm"); found {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if votedData, found := cm.storage.Get("votedFor"); found {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}
	if logData, found := cm.storage.Get("log"); found {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

// persistToStorage saves all of CM's persistent state in cm.storage.
// Expects cm.mu to be locked.
// persistToStorage方法将CM中的所有持久化状态保存到cm.Storage中，要求cm.mu被锁定
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
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
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlog("收到RequestVote请求: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, cm.currentTerm, cm.votedFor, lastLogIndex, lastLogTerm)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlog("... 当前任期早于RequestVote任期，变为Follower")
		cm.becomeFollower(args.Term)
	}

	// 任期相同，未投票或已投票给当前请求同伴，且候选人的日志满足安全性要求， 则返回赞成投票；
	// 否则，返回反对投票。
	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
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

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	// 快速解决冲突优化
	ConflictIndex int
	ConflictTerm  int
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

		// 检查本地的日志在索引PrevLogIndex处是否包含任期与PrevLogTerm匹配的记录？
		// 注意在PrevLogIndex=-1的极端情况下，这里应该是true
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			// 找到插入点 —— 索引从PrevLogIndex+1开始的本地日志与RPC发送的新条目间出现任期不匹配的位置。
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			/*
				循环结束时：
				- logInsertIndex指向本地日志结尾，或者是与领导者发送日志间存在任期冲突的索引位置
				- newEntriesIndex指向请求条目的结尾，或者是与本地日志存在任期冲突的索引位置
			*/
			if newEntriesIndex < len(args.Entries) {
				cm.dlog("... 从索引 index %d 处开始插入日志 %v ", logInsertIndex, args.Entries[newEntriesIndex:])
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlog("... 现在日志内容为: %v", cm.log)
			}

			// Set commit index.
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlog("... 设置 commitIndex 为 %d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			// No match for PrevLogIndex/PrevLogTerm. Populate
			// ConflictIndex/ConflictTerm to help the leader bring us up to date
			// quickly.
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = len(cm.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex points within our log, but PrevLogTerm doesn't match
				// cm.log[PrevLogIndex].
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
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
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			cm.dlog("向服务器 %d 发送RequestVote请求: %+v", peerId, args)
			var reply RequestVoteReply
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

	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
	cm.dlog("成为Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	// This goroutine runs in the background and sends AEs to peers:
	// * Whenever something is sent on triggerAEChan
	// * ... Or every 50 ms, if no events occur on triggerAEChan
	/*
		该goroutine在后台运行并向同伴服务器发送AE请求：
		- triggerAEChan通道发送任何内容时
		- 如果triggerAEChan通道没有内容时，每50ms执行一次
	*/
	go func(heartbeatTimeout time.Duration) {
		// Immediately send AEs to peers.
		cm.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true

				// Reset timer to fire again after heartbeatTimeout.
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				// Reset timer for heartbeatTimeout.
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

// leaderSendAEs sends a round of AEs to all peers, collects their
// replies and adjusts cm's state.
func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			cm.mu.Lock()
			ni := cm.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = cm.log[prevLogIndex].Term
			}
			entries := cm.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     cm.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: cm.commitIndex,
			}
			cm.mu.Unlock()
			cm.dlog("发送 AppendEntries 请求到服务器 %v: ni=%d, args=%+v", peerId, ni, args)
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

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1

						savedCommitIndex := cm.commitIndex
						// 检查是否有可提交的指令
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds)+1 {
									cm.commitIndex = i
								}
							}
						}
						cm.dlog("从服务器 %d 接收到 AppendEntries 请求的成功应答: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, cm.nextIndex, cm.matchIndex, cm.commitIndex)
						// 有可提交指令，通知集群
						if cm.commitIndex != savedCommitIndex {
							cm.dlog("leader 设置 commitIndex := %d", cm.commitIndex)
							// Commit index changed: the leader considers new entries to be
							// committed. Send new entries on the commit channel to this
							// leader's clients, and notify followers by sending them AEs.
							// Commit index改变：表明领导者认为新指令可以被提交了。
							// 通过commit channel向领导者的客户端发送新指令。
							// 发送AE请求通知所有的追随者
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								cm.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								cm.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							cm.nextIndex[peerId] = reply.ConflictIndex
						}
						cm.dlog("从服务器 %d 接收到 AppendEntries 请求的失败应答: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked.
// lastLogIndexAndTerm方法返回服务器最新的日志索引及最新的日志条目对应的任期（如果没有日志返回-1）
// 要求cm.mu锁定
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// commitChanSender is responsible for sending committed entries on
// cm.commitChan. It watches newCommitReadyChan for notifications and calculates
// which new entries are ready to be sent. This method should run in a separate
// background goroutine; cm.commitChan may be buffered and will limit how fast
// the client consumes new committed entries. Returns when newCommitReadyChan is
// closed.
/*
	commitChanSender负责在cm.commitChan上发送已提交的日志条目。
	它会监听newCommitReadyChan的通知并检查哪些条目可以发送（给客户端）。
	该方法应该在单独的后台goroutine中运行；cm.commitChan可能会有缓冲来限制客户端消费已提交指令的速度。
	当newCommitReadyChan关闭时方法结束。
*/
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		// 查找需要执行哪些指令
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlog("commitChanSender 向客户端发送指令 entries=%v, 最近执行指令索引savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlog("commitChanSender 向客户端发送指令完成")
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

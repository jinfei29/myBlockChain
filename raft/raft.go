package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 定义日志结构体
type LogEntry struct {
	Term          int    `json:"term"`
	Message       string `json:"message"`
	RandomCommand string `json:"randomCommand"`
	Index         int    `json:"index"`
}


type Raft struct {
	node *NodeInfo // 声明raft节点类型
	vote int	// 本节点获得的投票数
	lock sync.Mutex 	// 线程锁
	me string 	// 节点编号
	currentTerm int 	// 当前任期
	votedFor string 	// 为哪个节点投票
	state int 	// 当前节点状态  0 follower  1 candidate  2 leader
	lastMessageTime int64 // 发送最后一条消息的时间
	lastHeartBeartTime int64 // 发送最后一条消息的时间
	currentLeader string // 当前节点的领导
	timeout int // 心跳超时时间(单位：秒)
	voteCh chan bool // 接收投票成功通道
	heartBeat chan bool  // 心跳信号
	logs []LogEntry // 日志条目
	logFile string // 本地日志文件
	applyCh chan LogEntry // 用于接收来自 Leader 节点的日志条目
	totalBroadcastTime time.Duration
}

type NodeInfo struct {
	ID   string
	Port string
}

type Message struct {
	Msg   string
	MsgID int
}

// 用于封装消息、命令和任期号
type MessageAndCommand struct {
	Msg     string
	Command string
	Term    int
}

func NewRaft(id, port string) *Raft {
	node := new(NodeInfo)
	node.ID = id
	node.Port = port

	rf := new(Raft)
	// 节点信息
	rf.node = node
	// 当前节点获得票数
	rf.setVote(0)
	// 编号
	rf.me = id
	// 给0  1  2三个节点投票，给谁都不投
	rf.setVoteFor("-1")
	// 0 follower
	rf.setStatus(0)
	// 最后一次心跳检测时间
	rf.lastHeartBeartTime = 0
	rf.timeout = heartBeatTimeout
	// 最初没有领导
	rf.setCurrentLeader("-1")
	// 设置任期
	rf.setTerm(0)
	// 投票通道
	rf.voteCh = make(chan bool)
	// 心跳通道
	rf.heartBeat = make(chan bool)
	// 日志初始化
	rf.logs = make([]LogEntry, 0)
	// 设置日志文件路径
	rf.logFile = fmt.Sprintf("node_%s_logs.txt", id)
	// 恢复日志
	rf.loadLogs()
	// 创建 applyCh 通道
	rf.applyCh = make(chan LogEntry)
	return rf
}

// 模拟命令执行
func (rf *Raft) applyLogs() {
	var x int
	for _, entry := range rf.logs {
		switch entry.RandomCommand {
		case "x=x+1":
			x++
		case "x=x*2":
			x *= 2
		case "x=x-1":
			x--
		case "x=x/2":
			x /= 2
		}
	}
	fmt.Printf("节点 %s 执行完所有日志条目，最终 x 的值为: %d\n", rf.me, x)
}

// 写入日志文件
func (rf *Raft) saveLogs() {
	data, err := json.Marshal(rf.logs)
	if err != nil {
		fmt.Printf("无法序列化日志: %v\n", err)
		return
	}
	err = ioutil.WriteFile(rf.logFile, data, 0644)
	if err != nil {
		fmt.Printf("无法写入日志文件: %v\n", err)
		return
	}
}

// 从日志文件恢复
func (rf *Raft) loadLogs() {
	data, err := ioutil.ReadFile(rf.logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		fmt.Printf("无法打开日志文件: %v\n", err)
		return
	}

	err = json.Unmarshal(data, &rf.logs)
	if err != nil {
		fmt.Printf("无法反序列化日志: %v\n", err)
		return
	}
}




// 修改节点为候选人状态
func (rf *Raft) becomeCandidate() bool {
	r := randRange(1500, 5000)
	time.Sleep(time.Duration(r) * time.Millisecond)
	if rf.state == 0 && rf.currentLeader == "-1" && rf.votedFor == "-1" {
		rf.setStatus(1)
		rf.setVoteFor(rf.me)
		rf.setTerm(rf.currentTerm + 1)
		rf.setCurrentLeader("-1")
		rf.voteAdd()
		fmt.Println("本节点已变更为候选人状态")
		fmt.Printf("当前得票数：%d\n", rf.vote)
		return true
	} else {
		return false
	}
}

// 进行选举
func (rf *Raft) election() bool {
	fmt.Println("开始进行领导者选举，向其他节点进行广播")
	start := time.Now() // 记录选举开始时间
	go rf.broadcast("Raft.Vote", rf.node, func(ok bool) {
		rf.voteCh <- ok
	})
	for {
		select {
		case <-time.After(time.Second * time.Duration(timeout)):
			fmt.Println("领导者选举超时，节点变更为追随者状态\n")
			rf.reDefault()
			return false
		case ok := <-rf.voteCh:
			if ok {
				rf.voteAdd()
				fmt.Printf("获得来自其他节点的投票，当前得票数：%d\n", rf.vote)
			}
			if rf.vote > raftCount/2 && rf.currentLeader == "-1" {
				end := time.Now()          // 记录选举结束时间
				duration := end.Sub(start) // 计算选举持续时间
				fmt.Println("获得超过网络节点二分之一的得票数，本节点被选举成为了leader，选举持续时间为: %v\n", duration)
				rf.setStatus(2)
				rf.setCurrentLeader(rf.me)
				fmt.Println("向其他节点进行广播...")
				go rf.broadcast("Raft.ConfirmationLeader", rf.node, func(ok bool) {
					fmt.Println(ok)
				})
				rf.heartBeat <- true
				return true
			}
		}
	}
}

// 心跳检测方法
func (rf *Raft) heartbeat() {
	if <-rf.heartBeat {
		for {
			fmt.Println("本节点开始发送心跳检测...")
			rf.broadcast("Raft.HeartbeatRe", rf.node, func(ok bool) {
				fmt.Println("收到回复:", ok)
			})
			rf.lastHeartBeartTime = millisecond()
			time.Sleep(time.Second * time.Duration(heartBeatTimes))
		}
	}
}

// 产生随机值
func randRange(min, max int64) int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min) + min
}

// 获取当前时间的毫秒数
func millisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// 设置任期
func (rf *Raft) setTerm(term int) {
	rf.lock.Lock()
	rf.currentTerm = term
	rf.lock.Unlock()
}

// 设置为谁投票
func (rf *Raft) setVoteFor(id string) {
	rf.lock.Lock()
	rf.votedFor = id
	rf.lock.Unlock()
}

// 设置当前领导者
func (rf *Raft) setCurrentLeader(leader string) {
	rf.lock.Lock()
	rf.currentLeader = leader
	rf.lock.Unlock()
}

// 设置当前状态
func (rf *Raft) setStatus(state int) {
	rf.lock.Lock()
	rf.state = state
	rf.lock.Unlock()
}

// 投票累加
func (rf *Raft) voteAdd() {
	rf.lock.Lock()
	rf.vote++
	rf.lock.Unlock()
}

// 设置投票数量
func (rf *Raft) setVote(num int) {
	rf.lock.Lock()
	rf.vote = num
	rf.lock.Unlock()
}

// 恢复默认设置
func (rf *Raft) reDefault() {
	rf.setVote(0)
	rf.setVoteFor("-1")
	rf.setStatus(0)
}

// 生成随机命令
func (rf *Raft) generateRandomCommand() string {
	switch rand.Intn(4) {
	case 0:
		return "x=x+1"
	case 1:
		return "x=x-1"
	case 2:
		return "x=x*2"
	case 3:
		return "x=x/2"
	}
	return ""
}

func (rf *Raft) GetLogs(node NodeInfo, logs *[]LogEntry) error {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	*logs = rf.logs
	return nil
}

func (rf *Raft) SyncLogs() {
	if rf.currentLeader == "-1" || rf.currentLeader == rf.me {
		return
	}
	// 获取 Leader 节点的日志
	var logs []LogEntry
	address := "127.0.0.1" + nodeTable[rf.currentLeader]
	rp, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		fmt.Printf("连接到节点 %s 失败: %v\n", rf.currentLeader, err)
		return
	}
	err = rp.Call("Raft.GetLogs", rf.node, &logs)
	if err != nil {
		fmt.Printf("获取 Leader 节点日志失败: %v\n", err)
		return
	}
	rp.Close()
	// 更新本地日志
	rf.lock.Lock()
	defer rf.lock.Unlock()
	rf.logs = logs
	rf.saveLogs()

}

// 追加日志并执行
func (rf *Raft) appendAndApplyLog(command string, message string) {
	entry := LogEntry{RandomCommand: command, Message: message, Term: rf.currentTerm, Index: len(rf.logs) + 1}
	rf.logs = append(rf.logs, entry)
	rf.saveLogs()
}

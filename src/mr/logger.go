package mr

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

/**
 * @Author: ygzhang
 * @Date: 2023/11/9 11:41
 * @Func:
 **/

type logTopic string

const (
	dMap    logTopic = "MAPP"
	dReduce logTopic = "RDCE"
	dError  logTopic = "ERRO"
	dLog    logTopic = "LOGS"
	dWorker logTopic = "WORK"
	dMaster logTopic = "MAST"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerboisty()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	//如果VERBOSE = 2, 将日志打印到文件里
	if debugVerbosity == 2 {
		file := "logs.txt"
		logFile, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0766)
		if err != nil {
			panic(err)
		}
		log.SetOutput(logFile)
	}
}

//从环境变量里读到日志level
func getVerboisty() int {
	v := os.Getenv("VERBOSE")
	level := 1
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalln("Invalid verbosity", v)
		}
	}
	return level
}

func DPrintf(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

//
// getGoroutineID
//  @Description: debug用, 打印出当前函数的协程id
//  @return int
//
func GetGoroutineID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Parse the 4707 out of "goroutine 4707 ["
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

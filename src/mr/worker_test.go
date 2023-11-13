package mr

import "testing"

/**
 * @Author: ygzhang
 * @Date: 2023/11/10 11:27
 * @Func:
 **/

func TestWorkMap(t *testing.T) {
	// 编写测试用例来测试 WorkMap 函数
	// 使用 t.Run() 函数来组织不同的测试用例
	t.Run("Test Case 1", func(t *testing.T) {
		// 编写测试用例1
		// 调用 WorkMap 函数并检查其返回值
		mapf := func(key, value string) []KeyValue {
			// 根据测试需要定义 mapf 函数的行为
			return []KeyValue{
				{"key1", "value1"},
				{"key2", "value2"},
			}
		}
		task := &Task{
			Type:     MapTask,
			Id:       1,
			Metadata: nil,
			NReduce:  10,
			Done:     false,
		}
		WorkMap(mapf, task)
	})
}

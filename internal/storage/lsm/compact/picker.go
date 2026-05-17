package compact

import version "mini-kv/internal/storage/lsm/sstable"

// Job 描述一次合并任务，包含输入文件和目标层级
type Job struct {
	Level  int                 // 当前执行合并的源层级，这里固定为 0
	Inputs []version.TableMeta // 参与合并的 SSTable 文件元数据
}

// Picker 负责从当前版本状态中选择需要合并的文件
// 目前仅实现最简单的 L0 触发策略：当 Level 0 文件数达到阈值时，选择全部 L0 文件
type Picker struct {
	L0Trigger int // 触发 L0 合并的文件数阈值，例如设为 4 表示 L0 有 4 个文件时触发
}

// Pick 根据给定的版本状态判断是否需要合并，并返回合并任务
// 若无需合并则返回 false
func (p Picker) Pick(state *version.State) (Job, bool) {
	// 空状态、未设置阈值或没有层级时，跳过
	if state == nil || p.L0Trigger <= 0 || len(state.Levels) == 0 {
		return Job{}, false
	}

	// 获取 Level 0 的所有文件
	l0 := state.Levels[0]

	// 文件数量未达触发阈值，不合并
	if len(l0) < p.L0Trigger {
		return Job{}, false
	}

	// 复制一份文件列表，避免外部修改影响内部状态
	inputs := make([]version.TableMeta, len(l0))
	for i := range l0 {
		inputs[i] = l0[i].Clone()
	}

	return Job{
		Level:  0,
		Inputs: inputs,
	}, true
}
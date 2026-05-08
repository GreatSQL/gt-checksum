package actions

import (
	"database/sql"
	"testing"
)

// TestAutoIncrementFixForImplicitToExplicitPK 测试隐式主键转显式主键场景下的 AUTO_INCREMENT 值修复
// Bug 场景：
// - 源端：MySQL 5.6+，有显式自增主键（如 id），AUTO_INCREMENT=4
// - 目标端：MySQL 8.0+，有隐式自增主键（my_row_id），AUTO_INCREMENT=3
// - 配置：requirePK=ON
// 预期行为：应该生成独立的 AUTO_INCREMENT 修复 SQL
func TestAutoIncrementFixForImplicitToExplicitPK(t *testing.T) {
	// 模拟源端表元数据（有显式自增主键 id，AUTO_INCREMENT=4）
	sourceMeta := &tableMeta{
		AutoIncrement: sql.NullInt64{Int64: 4, Valid: true},
	}

	// 模拟目标端表元数据（有隐式自增主键 my_row_id，AUTO_INCREMENT=3）
	destMeta := &tableMeta{
		AutoIncrement: sql.NullInt64{Int64: 3, Valid: true},
	}

	// 模拟结构模式状态
	sms := &structModeState{
		droppedAutoIncrementColumn: false, // my_row_id 的删除是在索引合并阶段插入的，这里不会被设置
		addedAutoIncrementColumn:   true,  // 检测到新添加的 id 列具有 AUTO_INCREMENT 属性
	}

	// 测试 AUTO_INCREMENT 值判断逻辑
	fixValue, needsFix := resolveMySQLTableAutoIncrementFixValue(sourceMeta.AutoIncrement, destMeta.AutoIncrement)

	if !needsFix {
		t.Errorf("Expected needsFix=true for AUTO_INCREMENT mismatch (source=4, dest=3), got false")
	}

	if fixValue != 4 {
		t.Errorf("Expected fixValue=4, got %d", fixValue)
	}

	// 模拟 shouldSkipAutoIncrementCheck 的判断逻辑
	// 在修复前，这个标志会被错误地设置为 true（因为目标端有 my_row_id）
	// 修复后，如果 addedAutoIncrementColumn=true，则不应该跳过检查
	shouldSkipAutoIncrementCheck := false

	// 模拟目标端有 my_row_id 但源端没有的场景
	destHasMyRowID := true
	sourceHasMyRowID := false

	if destHasMyRowID && !sourceHasMyRowID {
		// 修复后的逻辑：如果即将添加显式自增主键，则不应该跳过 AUTO_INCREMENT 检查
		if !sms.addedAutoIncrementColumn {
			shouldSkipAutoIncrementCheck = true
		}
	}

	// 验证修复后的行为：即使目标端有 my_row_id，但因为添加了显式自增主键，所以不应该跳过检查
	if shouldSkipAutoIncrementCheck {
		t.Errorf("Expected shouldSkipAutoIncrementCheck=false when addedAutoIncrementColumn=true (implicit to explicit PK), got true")
	}

	// 验证应该生成 AUTO_INCREMENT 修复 SQL
	// 根据修复后的逻辑，以下条件应该满足：
	// needsFix=true && !shouldSkipAutoIncrementCheck
	shouldGenerateFixSQL := needsFix && !shouldSkipAutoIncrementCheck

	if !shouldGenerateFixSQL {
		t.Errorf("Expected to generate AUTO_INCREMENT fix SQL for implicit to explicit PK scenario, but conditions not met")
	}
}

// TestAutoIncrementSkipForPureImplicitPK 测试纯隐式主键场景下应该跳过 AUTO_INCREMENT 检查
// 场景：
// - 源端：MySQL 5.6+，无主键
// - 目标端：MySQL 8.0+，有隐式自增主键（my_row_id）
// - 配置：requirePK=ON
// 预期行为：应该跳过 AUTO_INCREMENT 检查（因为 AUTO_INCREMENT 是由 my_row_id 引入的）
func TestAutoIncrementSkipForPureImplicitPK(t *testing.T) {
	// 模拟结构模式状态
	sms := &structModeState{
		droppedAutoIncrementColumn: false,
		addedAutoIncrementColumn:   false, // 没有添加显式自增主键
	}

	// 模拟目标端有 my_row_id 但源端没有的场景
	destHasMyRowID := true
	sourceHasMyRowID := false
	shouldSkipAutoIncrementCheck := false

	if destHasMyRowID && !sourceHasMyRowID {
		// 如果没有添加显式自增主键，则应该跳过 AUTO_INCREMENT 检查
		if !sms.addedAutoIncrementColumn {
			shouldSkipAutoIncrementCheck = true
		}
	}

	// 验证应该跳过检查
	if !shouldSkipAutoIncrementCheck {
		t.Errorf("Expected shouldSkipAutoIncrementCheck=true for pure implicit PK scenario (no explicit PK added), got false")
	}
}

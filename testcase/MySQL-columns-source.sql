-- =============================================================================
-- gt-checksum columns 功能回归测试 -- 源端初始化脚本
-- 适用于: scripts/regression-test-columns.sh
-- =============================================================================
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;
/*!80013 SET SESSION sql_require_primary_key=0 */;
/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;

DROP DATABASE IF EXISTS `gt_checksum_cols`;
CREATE DATABASE `gt_checksum_cols` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `gt_checksum_cols`;

-- ----------------------------------------------------------------------------
-- TC-01: cols-basic-ignore
-- 非选中列 ignored_col 在两端值不同，但选中列 name 完全一致
-- 预期：columns=id,name 时 Diffs=no → PASS
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `col_data`;
CREATE TABLE `col_data` (
    `id`          INT          NOT NULL,
    `name`        VARCHAR(50)  NOT NULL,
    `ignored_col` CHAR(1)      NOT NULL DEFAULT 'A',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `col_data` VALUES
    (1, 'Alice', 'A'),
    (2, 'Bob',   'A'),
    (3, 'Carol', 'A');

-- ----------------------------------------------------------------------------
-- TC-02: cols-selected-diff-fix
-- 选中列 amount 在两端不同（源端正确），修复后应收敛
-- 预期：Round1 Diffs=yes → repairDB 修复 → Round2 Diffs=no → PASS
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `order_data`;
CREATE TABLE `order_data` (
    `order_id` INT             NOT NULL,
    `amount`   DECIMAL(10,2)   NOT NULL,
    `note`     VARCHAR(100),
    PRIMARY KEY (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `order_data` VALUES
    (1001, 100.00, 'src-note'),
    (1002, 200.00, 'src-note2');

-- ----------------------------------------------------------------------------
-- TC-03: cols-source-only-advisory
-- 源端有 event_id=5 这一行，目标端没有（source-only 行）
-- 预期：生成 columns-advisory 文件（全注释），无可执行 SQL → PASS-ADVISORY
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `events`;
CREATE TABLE `events` (
    `event_id` INT    NOT NULL,
    `status`   CHAR(1) NOT NULL,
    PRIMARY KEY (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `events` VALUES
    (1, 'A'),
    (2, 'B'),
    (3, 'C'),
    (4, 'D'),
    (5, 'E');

-- ----------------------------------------------------------------------------
-- TC-04: cols-simple-syntax
-- 简单语法（格式一）：columns=score，单表
-- 非选中列 note 也不同，但不应被检出
-- 预期：Round1 Diffs=yes（score 差异）→ 修复 → Round2 Diffs=no → PASS
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `product`;
CREATE TABLE `product` (
    `prod_id` INT          NOT NULL,
    `score`   INT          NOT NULL,
    `note`    VARCHAR(100),
    PRIMARY KEY (`prod_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `product` VALUES
    (1, 90, 'src-note'),
    (2, 85, 'src-note');

-- ----------------------------------------------------------------------------
-- TC-05: cols-cross-table-mapping
-- 跨表列名映射：old_orders.src_total → new_orders.dst_total
-- 目标端 dst_total 值不同，修复后应收敛
-- 预期：Round1 Diffs=yes → 修复 → Round2 Diffs=no → PASS
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `old_orders`;
CREATE TABLE `old_orders` (
    `oid`       INT           NOT NULL,
    `src_total` DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `old_orders` VALUES
    (1, 500.00),
    (2, 750.00);

-- ----------------------------------------------------------------------------
-- TC-06: cols-no-pk-ddl-yes
-- 无主键/唯一键表，columns 模式下应记录 DDL-yes（跳过）
-- 配合 col_data 一起放在 tables 参数中（两张表），避免单表 os.Exit(1)
-- 预期：heap_data 行 Diffs=DDL-yes → FAIL-EXPECTED（设计行为）
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `heap_data`;
CREATE TABLE `heap_data` (
    `val` VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `heap_data` VALUES ('same-value');

-- ----------------------------------------------------------------------------
-- TC-07: cols-target-only-extra-rows
-- 目标端多出 item_id=99 行，源端无此行
-- extraRowsSyncToSource=ON → 生成 DELETE → 修复后收敛
-- 预期：Round1 Diffs=yes → 修复 → Round2 Diffs=no → PASS
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `inventory`;
CREATE TABLE `inventory` (
    `item_id` INT NOT NULL,
    `qty`     INT NOT NULL,
    PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `inventory` VALUES
    (10, 100),
    (20, 200);

SET FOREIGN_KEY_CHECKS=1;
COMMIT;

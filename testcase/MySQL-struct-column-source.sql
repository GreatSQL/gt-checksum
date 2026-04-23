-- =============================================================================
-- gt-checksum TableColumnNameCheck 结构检查回归 -- 源端初始化
-- 适用于: scripts/regression-test-struct-column.sh
-- 对应：docs/large-file-v3-dev-plan-cc-20260422.md Step 0 清单 ①-⑤、⑧、⑨
-- =============================================================================
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;
/*!80013 SET SESSION sql_require_primary_key=0 */;
/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;

DROP DATABASE IF EXISTS `gt_checksum_sc`;
CREATE DATABASE `gt_checksum_sc` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `gt_checksum_sc`;

-- ----------------------------------------------------------------------------
-- TC-ST-01: 双端存在、无差异
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_identical`;
CREATE TABLE `t_identical` (
    `id`   INT          NOT NULL,
    `name` VARCHAR(50)  NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO `t_identical` VALUES (1,'a'),(2,'b');

-- ----------------------------------------------------------------------------
-- TC-ST-02: 目标缺表（源端独有）
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_missing_dst`;
CREATE TABLE `t_missing_dst` (
    `id`  INT NOT NULL,
    `val` VARCHAR(32) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-03: 源缺表（源端不存在，目标存在 → 期望生成 DROP TABLE）
-- 源端显式 DROP 确保空缺
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_missing_src`;

-- ----------------------------------------------------------------------------
-- TC-ST-04: 列增 / 删 / 类型变更
-- 源端 3 列 (id/amount DECIMAL(12,2)/note)；目标端 2 列 (id/amount INT)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_col_diff`;
CREATE TABLE `t_col_diff` (
    `id`     INT            NOT NULL,
    `amount` DECIMAL(12,2)  NOT NULL DEFAULT 0.00,
    `note`   VARCHAR(100)   DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-05a / TC-ST-05b: 列名大小写差异
-- 源端 COL_A / col_b（混合大小写）；目标端 col_a / COL_B
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_case`;
CREATE TABLE `t_case` (
    `id`    INT         NOT NULL,
    `COL_A` VARCHAR(20) DEFAULT NULL,
    `col_b` VARCHAR(20) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-08: 不支持特性 advisory（generated column / CHECK 约束 / JSON 表达式）
-- 预期：struct 校验产出 advisory-only fixsql → PASS-ADVISORY
-- 注意：仅 MySQL 8.0+/MariaDB 10.5+ 可解析 generated / CHECK，但 fixture 用标准语法
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_advisory`;
CREATE TABLE `t_advisory` (
    `id`      INT          NOT NULL,
    `payload` JSON         DEFAULT NULL,
    `full_x`  VARCHAR(100) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(`payload`, '$.x'))) VIRTUAL,
    PRIMARY KEY (`id`),
    CONSTRAINT `ck_t_advisory_id` CHECK (`id` >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-09: columnPlan 列映射豁免 (data 预检)
-- 源端 3 列，目标端 4 列（多出 extra_col）。columns= 仅选取 id/val 两列
-- 预期：data 预检应豁免未映射列 → Diffs=no → PASS
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_plan`;
CREATE TABLE `t_plan` (
    `id`    INT         NOT NULL,
    `val`   VARCHAR(50) NOT NULL,
    `extra` VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO `t_plan` VALUES (1,'a','x'),(2,'b','y');

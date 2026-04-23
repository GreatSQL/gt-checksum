-- =============================================================================
-- gt-checksum TableColumnNameCheck 结构检查回归 -- 目标端初始化
-- 适用于: scripts/regression-test-struct-column.sh
-- =============================================================================
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;
/*!80013 SET SESSION sql_require_primary_key=0 */;
/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;

DROP DATABASE IF EXISTS `gt_checksum_sc`;
CREATE DATABASE `gt_checksum_sc` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `gt_checksum_sc`;

-- ----------------------------------------------------------------------------
-- TC-ST-01: 与源完全一致
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_identical`;
CREATE TABLE `t_identical` (
    `id`   INT          NOT NULL,
    `name` VARCHAR(50)  NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO `t_identical` VALUES (1,'a'),(2,'b');

-- ----------------------------------------------------------------------------
-- TC-ST-02: 目标缺表 → 不建表
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_missing_dst`;

-- ----------------------------------------------------------------------------
-- TC-ST-03: 源缺表 → 目标独有该表
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_missing_src`;
CREATE TABLE `t_missing_src` (
    `id`    INT          NOT NULL,
    `ghost` VARCHAR(32)  DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-04: 目标端仅 2 列，amount 类型从 DECIMAL 改为 INT
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_col_diff`;
CREATE TABLE `t_col_diff` (
    `id`     INT NOT NULL,
    `amount` INT NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-05a / 05b: 列名大小写翻转（col_a ↔ COL_A，COL_B ↔ col_b）
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_case`;
CREATE TABLE `t_case` (
    `id`    INT         NOT NULL,
    `col_a` VARCHAR(20) DEFAULT NULL,
    `COL_B` VARCHAR(20) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-08: 目标端无 generated column / CHECK
-- 预期：advisory-only fixsql
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_advisory`;
CREATE TABLE `t_advisory` (
    `id`      INT          NOT NULL,
    `payload` JSON         DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ST-09: 目标端 4 列，比源端多 extra_col
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `t_plan`;
CREATE TABLE `t_plan` (
    `id`        INT         NOT NULL,
    `val`       VARCHAR(50) NOT NULL,
    `extra`     VARCHAR(50) DEFAULT NULL,
    `extra_col` VARCHAR(50) DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO `t_plan` VALUES (1,'a','x','only-dst'),(2,'b','y','only-dst');

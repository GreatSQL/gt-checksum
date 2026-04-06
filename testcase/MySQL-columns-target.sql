-- =============================================================================
-- gt-checksum columns 功能回归测试 -- 目标端初始化脚本
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
-- ignored_col='X'（与源端 'A' 不同），name 完全相同
-- 预期：选中 id,name 时 Diffs=no
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `col_data`;
CREATE TABLE `col_data` (
    `id`          INT          NOT NULL,
    `name`        VARCHAR(50)  NOT NULL,
    `ignored_col` CHAR(1)      NOT NULL DEFAULT 'A',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `col_data` VALUES
    (1, 'Alice', 'X'),
    (2, 'Bob',   'X'),
    (3, 'Carol', 'X');

-- ----------------------------------------------------------------------------
-- TC-02: cols-selected-diff-fix
-- amount 不同（99.00/199.00 vs 源端 100.00/200.00），修复后收敛
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `order_data`;
CREATE TABLE `order_data` (
    `order_id` INT             NOT NULL,
    `amount`   DECIMAL(10,2)   NOT NULL,
    `note`     VARCHAR(100),
    PRIMARY KEY (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `order_data` VALUES
    (1001, 99.00,  'dst-note'),
    (1002, 199.00, 'dst-note2');

-- ----------------------------------------------------------------------------
-- TC-03: cols-source-only-advisory
-- 无 event_id=5 行（源端独有）→ advisory 文件
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
    (4, 'D');

-- ----------------------------------------------------------------------------
-- TC-04: cols-simple-syntax
-- score=80/75（差异），note 也不同但不在 columns 中
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `product`;
CREATE TABLE `product` (
    `prod_id` INT          NOT NULL,
    `score`   INT          NOT NULL,
    `note`    VARCHAR(100),
    PRIMARY KEY (`prod_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `product` VALUES
    (1, 80, 'dst-note'),
    (2, 75, 'dst-note');

-- ----------------------------------------------------------------------------
-- TC-05: cols-cross-table-mapping
-- 目标表名不同（new_orders），列名不同（dst_total），值也不同
-- 修复后应收敛
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `new_orders`;
CREATE TABLE `new_orders` (
    `oid`       INT           NOT NULL,
    `dst_total` DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `new_orders` VALUES
    (1, 400.00),
    (2, 600.00);

-- ----------------------------------------------------------------------------
-- TC-06: cols-no-pk-ddl-yes
-- heap_data 无主键（与源端结构相同），columns 模式下标记 DDL-yes
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `heap_data`;
CREATE TABLE `heap_data` (
    `val` VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `heap_data` VALUES ('same-value');

-- ----------------------------------------------------------------------------
-- TC-07: cols-target-only-extra-rows
-- 比源端多出 item_id=99 行
-- extraRowsSyncToSource=ON 后 DELETE → 修复后收敛
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `inventory`;
CREATE TABLE `inventory` (
    `item_id` INT NOT NULL,
    `qty`     INT NOT NULL,
    PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `inventory` VALUES
    (10, 100),
    (20, 200),
    (99, 999);

SET FOREIGN_KEY_CHECKS=1;
COMMIT;

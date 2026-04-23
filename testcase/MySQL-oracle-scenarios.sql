-- =============================================================================
-- gt-checksum Oracle→MySQL 场景回归 fixture（MySQL 目标端）
-- 适用于: scripts/regression-test-oracle.sh --with-scenarios
-- =============================================================================
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS=0;
/*!80013 SET SESSION sql_require_primary_key=0 */;
/*!80030 SET SESSION sql_generate_invisible_primary_key=0 */;

-- 复用 Oracle 源端 schema 名（大写），由于 MySQL 默认 lower_case_table_names=1 建库用小写
DROP DATABASE IF EXISTS `GT_CHECKSUM`;
DROP DATABASE IF EXISTS `gt_checksum`;
CREATE DATABASE `gt_checksum` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `gt_checksum`;

-- ----------------------------------------------------------------------------
-- TC-ORAST-01: 类型映射对应表（DECIMAL / VARCHAR / DATETIME）
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `sc_types`;
CREATE TABLE `sc_types` (
    `id`         INT            NOT NULL PRIMARY KEY,
    `amount`     DECIMAL(12,2)  DEFAULT 0,
    `name`       VARCHAR(100),
    `born_date`  DATETIME,
    `updated_at` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ORAST-02: 大小写敏感目标表（引号保留原样大小写）
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `ScCase`;
CREATE TABLE `ScCase` (
    `Id`       INT         NOT NULL PRIMARY KEY,
    `FullName` VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ORAST-03: 分区表
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `sc_part`;
CREATE TABLE `sc_part` (
    `id`  INT NOT NULL,
    `ymd` DATE NOT NULL,
    `val` VARCHAR(32),
    PRIMARY KEY (`id`, `ymd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (YEAR(`ymd`)) (
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION pmax  VALUES LESS THAN MAXVALUE
);

-- ----------------------------------------------------------------------------
-- TC-ORAST-04: 大字段
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `sc_largeobj`;
CREATE TABLE `sc_largeobj` (
    `id`    INT      NOT NULL PRIMARY KEY,
    `body`  LONGTEXT,
    `photo` LONGBLOB
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- TC-ORAST-05: 目标端缺表（刻意不建）→ struct 期望触发建表
-- DROP TABLE IF EXISTS `sc_missing`;
-- ----------------------------------------------------------------------------

-- ----------------------------------------------------------------------------
-- TC-ORAST-06: data 硬不兼容目标端（对应 RAW 源，MySQL 端用 VARBINARY）
-- 刻意保留结构差异，用于 data 预检断言
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS `sc_incompat`;
CREATE TABLE `sc_incompat` (
    `id`    INT            NOT NULL PRIMARY KEY,
    `token` VARBINARY(16)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

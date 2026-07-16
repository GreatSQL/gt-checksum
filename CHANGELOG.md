## 4.0.2

- 功能新增：新增 `hashAlgorithm` 参数，支持选择 `xxhash64`（默认）或 `md5` 哈希算法用于数据校验
- 性能优化：引入 XXHash64 算法替代硬编码 MD5，校验性能提升约 15-25 倍
- 测试完善：新增 checksum 模块单元测试，覆盖 XXHash64、MD5、CheckHash 等核心函数

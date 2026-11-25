# Ali MediaWorker migration case results

Release implementation: `68d0274`. This is `composite_verified`: 172 unaffected previous successes retain their actual revisions; four affected product cases and four new real inference cases passed.

All 180 cases meet expectations. Current eight-case mean: 160.27 seconds. Composite mean: 362.36 seconds.

DM01–DM04 ran before the implementation commit. Their original receipt keeps base `5b747d4` plus the working-tree digest; the full implementation/runner source digest matches `68d0274` exactly. Their negative terminal states are expected outcomes, not program failures.

[Design, verification and deployment](image-decompose-media-migration.md). Local detailed receipt: `.data/evidence/release/ali-media-68d0274.json`.

| Case | Title | Status | Meets expectation | Seconds | Evidence revision |
| --- | --- | --- | --- | ---: | --- |
| S01 | 图片中心裁切 | success | yes | 50.80 | 289455c |
| S02 | 图片等比缩小 | success | yes | 62.99 | 289455c |
| S03 | 图片顺时针旋转 | success | yes | 58.94 | 289455c |
| S04 | 图片水平镜像 | success | yes | 56.93 | 289455c |
| S05 | 图片亮度调整 | success | yes | 91.25 | 289455c |
| S06 | 图片灰度化 | success | yes | 66.01 | 289455c |
| S07 | 图片高斯模糊 | success | yes | 69.14 | 289455c |
| S08 | 图标抠透明背景 | success | yes | 115.37 | 289455c |
| S09 | SAM3人像分割 | success | yes | 248.52 | 289455c |
| S10 | 图片文字识别 | success | yes | 130.16 | 289455c |
| S11 | PNG转JPEG | success | yes | 70.95 | 289455c |
| S12 | WebP缩略图 | success | yes | 100.92 | 289455c |
| S13 | 音频精确截取 | success | yes | 76.94 | 289455c |
| S14 | 音量降低6dB | success | yes | 83.86 | 289455c |
| S15 | 音频首尾渐变 | success | yes | 83.79 | 289455c |
| S16 | 视频提取音频 | success | yes | 75.76 | 289455c |
| S17 | 视频精确剪切 | success | yes | 91.94 | 289455c |
| S18 | 视频等比缩小 | success | yes | 75.87 | 289455c |
| S19 | 视频顺时针旋转 | success | yes | 114.17 | 289455c |
| S20 | 两倍速保音高 | success | yes | 84.84 | 289455c |
| S21 | 移除视频音轨 | success | yes | 81.58 | 289455c |
| S22 | 按时刻抽帧 | success | yes | 122.88 | 289455c |
| S23 | 视频帧率转换 | success | yes | 87.61 | 289455c |
| S24 | 视频黑白效果 | success | yes | 115.05 | 289455c |
| S25 | 右上角透明图标 | success | yes | 158.21 | 289455c |
| S26 | 真实语音转写 | success | yes | 89.58 | 289455c |
| S27 | 真实文生图 | success | yes | 138.11 | 289455c |
| S28 | 真实文生视频 | success | yes | 357.29 | 289455c |
| S29 | 真实图生视频 | success | yes | 712.37 | 289455c |
| S30 | 字幕整理为文字 | success | yes | 66.44 | 289455c |
| S31 | 保留静音音轨 | success | yes | 92.73 | 289455c |
| C01 | 三段精剪与配乐 | success | yes | 403.66 | 289455c |
| C02 | 双片段交叉淡化 | success | yes | 270.88 | 289455c |
| C03 | 竖屏字幕与角标 | success | yes | 331.51 | 289455c |
| C04 | 抠图后视频合成 | success | yes | 359.16 | 289455c |
| C05 | 人像分割精修海报 | success | yes | 353.00 | 289455c |
| C06 | 跨图调色批量输出 | success | yes | 295.40 | 289455c |
| C07 | OCR驱动文字替换 | success | yes | 414.90 | 289455c |
| C08 | 透视校正打印卡片 | success | yes | 173.39 | 289455c |
| C09 | 真实图片分层与ORA | success | yes | 788.04 | 68d0274 |
| C10 | 异构素材竖版混剪 | success | yes | 337.36 | 289455c |
| C11 | 四宫格视频 | success | yes | 297.55 | 289455c |
| C12 | 图片动态幻灯片 | success | yes | 382.39 | 289455c |
| C13 | 分段变速与定格 | success | yes | 505.85 | 289455c |
| C14 | 倒放混剪 | success | yes | 404.30 | 289455c |
| C15 | 旁白配乐压低与字幕 | success | yes | 426.42 | 289455c |
| C16 | 降噪与双格式播客 | success | yes | 482.91 | 289455c |
| C17 | 中英字幕品牌短片 | success | yes | 248.00 | 289455c |
| C18 | 音画分离后重组 | success | yes | 251.85 | 289455c |
| C19 | 海报动画与透明角标 | success | yes | 275.87 | 289455c |
| C20 | 精修人像双背景对照 | success | yes | 381.55 | 289455c |
| C21 | 并行预处理后汇合 | success | yes | 345.63 | 289455c |
| C22 | 24片段长程批处理与暂停续跑 | success | yes | 577.79 | 289455c |
| C23 | 局部模糊遮挡与合成 | success | yes | 480.77 | 289455c |
| C24 | 静音替换配乐与封面 | success | yes | 221.73 | 289455c |
| C25 | 多画幅并行交付 | success | yes | 296.20 | 289455c |
| C26 | OCR定位后真实AI消除 | success | yes | 314.39 | 289455c |
| C27 | 文生图再编辑品牌海报 | success | yes | 279.24 | 289455c |
| C28 | 文生视频竖版包装 | success | yes | 625.97 | 289455c |
| C29 | 1080p长片与节点接管 | success | yes | 951.99 | 3006bd2 |
| C30 | 女孩图片到15秒海滩跑步完整视频 | success | yes | 716.29 | 3006bd2 |
| R01 | 待确认时改交付名称 | success | yes | 450.48 | 3006bd2 |
| R02 | 待确认时改尺寸与格式 | success | yes | 472.55 | 3006bd2 |
| R03 | 待确认时改素材选择与顺序 | success | yes | 933.74 | 3006bd2 |
| R04 | 部分完成时追加输出格式 | success | yes | 506.81 | 3006bd2 |
| R05 | 部分完成时重做一个分支参数 | success | yes | 506.84 | 3006bd2 |
| R06 | 部分完成时替换一个分支素材 | success | yes | 530.52 | 6adddb1 |
| R07 | 导出过程中更换封面 | success | yes | 642.09 | 3006bd2 |
| R08 | 导出过程中改变字幕样式 | success | yes | 1059.14 | 3006bd2 |
| R09 | 导出过程中改画幅和时长 | success | yes | 867.90 | 3006bd2 |
| R10 | 交付后追加缩略图 | success | yes | 808.65 | 6adddb1 |
| R11 | 验收时拒绝局部效果并重做 | success | yes | 1703.48 | 6adddb1 |
| R12 | 交付后改变整体交付目标 | success | yes | 1205.32 | 10b2c5e |
| R13 | 暂停讨论后停止旧版并重做 | success | yes | 919.79 | 6adddb1 |
| R14 | 导出完成附近取消并重做 | success | yes | 463.42 | 6adddb1 |
| R15 | 重做期间刷新与重新进入项目 | success | yes | 528.50 | 6adddb1 |
| R16 | 连续重做V1到V2到V3 | success | yes | 1501.40 | 90f51d5 |
| R17 | 真实浏览器执行中修改与重新授权 | success | yes | 648.93 | 5121adc |
| D01 | 真实 Manager 多轮扩图 | success | yes | 89.47 | 6adddb1 |
| D02 | 并行分支完成后触发汇合规划 | success | yes | 124.63 | 6adddb1 |
| D03 | 未封口禁止提前交付 | success | yes | 73.93 | 6adddb1 |
| D04 | Orchestrator 重启与提案去重 | success | yes | 86.25 | 6adddb1 |
| D05 | 两个 Orchestrator 竞争推进 | success | yes | 70.28 | 6adddb1 |
| D06 | 执行器退出后换节点恢复 | success | yes | 139.70 | 90f51d5 |
| D07 | Manager 执行中取消 | success | yes | 3.31 | 90f51d5 |
| D08 | 暂停边界不启动新任务 | success | yes | 157.97 | 90f51d5 |
| D09 | 外部调用超范围审批 | success | yes | 32.16 | 6adddb1 |
| D10 | 拒绝扩大授权后不调用供应商 | success | yes | 115.22 | 5121adc |
| D11 | 过期审批与重复命令拒绝 | success | yes | 107.86 | 5121adc |
| D12 | 跨项目读取与控制拒绝 | success | yes | 180.76 | 5121adc |
| D13 | 模型预算耗尽有界暂停 | success | yes | 115.90 | 5121adc |
| D14 | BOSS 内容校验与候选不放行 | success | yes | 161.84 | 5121adc |
| D15 | 授权内容指纹失效拒绝 | success | yes | 154.28 | 5121adc |
| D16 | 外部提交未知不重复生成 | success | yes | 154.25 | 5121adc |
| D17 | 观测服务中断不影响业务状态 | success | yes | 157.87 | 5121adc |
| D18 | API 切换与浏览器恢复 | success | yes | 50.73 | 90f51d5 |
| D19 | 独立部署与 main 数据隔离 | success | yes | 81.00 | 5121adc |
| D20 | 专业 Agent 权限与实际模型用量 | success | yes | 213.69 | 5121adc |
| D21 | 诊断后不可变修订并衔接下游 | success | yes | 179.09 | 5121adc |
| D22 | 单一Run身份与独立Control Job | success | yes | 170.21 | 5121adc |
| D23 | 业务排空与全Run派发屏障 | success | yes | 170.85 | 5121adc |
| D24 | 专业Agent在途输入排空 | success | yes | 194.20 | 5121adc |
| D25 | 规划期间连续输入分批处理 | success | yes | 204.76 | 5121adc |
| D26 | Wan在途输入保持原句柄 | success | yes | 456.98 | 5121adc |
| D27 | 外部提交未知时输入屏障 | success | yes | 0.82 | 5121adc |
| D28 | 质量反馈幂等与过期快照 | success | yes | 88.85 | 5121adc |
| D29 | 拒绝不能被人工批准且可补证据 | success | yes | 1.27 | 5121adc |
| D30 | 局部蒙版修复比例边界 | success | yes | 148.96 | 5121adc |
| D31 | 持续动作的真实时序证据 | success | yes | 346.14 | 5121adc |
| D32 | 输入与最终发布竞争 | success | yes | 151.31 | 5121adc |
| D33 | 交付后successor输入幂等 | success | yes | 196.69 | 5121adc |
| D34 | 同步外部请求在途输入 | success | yes | 74.94 | 5121adc |
| D35 | Provider配置切换后冻结路由恢复 | success | yes | 397.16 | 5121adc |
| D36 | 抠图候选边缘反馈修订 | success | yes | 72.12 | 5121adc |
| D37 | 双图构图反馈修订 | success | yes | 93.37 | 5121adc |
| D38 | Wan提交前参考图反馈修订 | success | yes | 627.55 | 5121adc |
| D39 | 合法替代失败节点与历史读取 | success | yes | 141.37 | 5121adc |
| D40 | 已验收音频下游Operation取证 | success | yes | 299.20 | 170c800 |
| D41 | 最终交付选择与多视频时序证据 | success | yes | 253.58 | 5121adc |
| D42 | Manager硬杀后同快照接管 | success | yes | 185.96 | 170c800 |
| D43 | Reviewer硬杀后证据与预算恢复 | success | yes | 157.80 | 95fcd7a |
| D44 | 业务Agent默认不可重试契约下的基础设施接管 | success | yes | 359.52 | 95fcd7a |
| D45 | 业务Agent连续两次硬杀与恢复预算分账 | success | yes | 166.30 | 90f51d5 |
| D46 | 异步Provider轮询硬杀后沿用原句柄 | success | yes | 1694.91 | 5ba1ce6 |
| D47 | 自然语言附加交付与Delivery叶节点完整选择 | success | yes | 1564.98 | 5ba1ce6 |
| D48 | 不可用附件在输入接收前原子拒绝 | success | yes | 1356.28 | 5ba1ce6 |
| D49 | 已交付Run首条successor输入携带原素材授权 | success | yes | 1293.17 | 5ba1ce6 |
| D50 | SeedDream批量调用次数授权允许 | success | yes | 1347.15 | 5ba1ce6 |
| D51 | SeedDream批量调用次数授权拒绝 | success | yes | 0.23 | 170c800 |
| P01 | 混合并行 JobGroup | success | yes | 73.06 | 170c800 |
| P02 | 整组授权允许 | success | yes | 0.31 | 170c800 |
| P03 | 整组授权拒绝 | success | yes | 0.27 | 170c800 |
| P04 | 多文件同角色集合 | success | yes | 104.95 | 170c800 |
| P05 | 多组完成合并续接 | success | yes | 9.33 | 90f51d5 |
| P06 | Job在途用户输入排序 | success | yes | 49.23 | 170c800 |
| P07 | 取消并替换子Harness | success | yes | 28.39 | 170c800 |
| P08 | 单Job停止 | success | yes | 21.34 | 170c800 |
| P09 | 整组停止 | success | yes | 12.33 | 170c800 |
| P10 | Thread全部停止 | success | yes | 4.47 | 170c800 |
| P11 | 最小Skill Profile | success | yes | 197.26 | 170c800 |
| P12 | 不兼容Skill组合预检 | success | yes | 0.22 | 170c800 |
| P13 | 节点硬杀后接管 | success | yes | 65.26 | 170c800 |
| P14 | 可选Reviewer不可用 | success | yes | 1.40 | 170c800 |
| P15 | 必需Reviewer阻止发布 | success | yes | 1.37 | 170c800 |
| P16 | 预算并发竞争 | success | yes | 0.93 | 170c800 |
| P17 | Provider接受响应丢失 | success | yes | 0.99 | 170c800 |
| P18 | Sandbox检查点接管 | success | yes | 10.23 | 170c800 |
| P19 | Artifact集合原子发布 | success | yes | 149.98 | 170c800 |
| P20 | Provider与子Harness混合扇出 | success | yes | 29.61 | 68d0274 |
| P22 | 紧凑类型化委派闭环 | success | yes | 111.66 | 170c800 |
| P23 | Primary自主并行决策 | success | yes | 311.72 | 170c800 |
| P24 | Child受限Operation续接 | success | yes | 289.09 | 170c800 |
| P25 | 并行品牌演示包 | success | yes | 615.96 | 170c800 |
| P26 | 并行多模态内容报告 | success | yes | 4448.93 | 170c800 |
| P27 | 混合并行任务节点恢复 | success | yes | 183.24 | 90f51d5 |
| K01 | Web与视觉Skill并行覆盖 | success | yes | 1248.08 | 95fcd7a |
| K02 | 品牌与演示Skill并行覆盖 | success | yes | 2952.87 | 170c800 |
| K03 | 数据与Office Skill并行覆盖 | success | yes | 3294.83 | 170c800 |
| K04 | 媒体与动画Skill并行覆盖 | success | yes | 3054.93 | 170c800 |
| K05 | 文档与沟通Skill并行覆盖 | success | yes | 1137.36 | 95fcd7a |
| K06 | Agent工程Skill并行覆盖 | success | yes | 549.18 | 90f51d5 |
| K07 | 视觉Operation并行流水线 | success | yes | 80.83 | 68d0274 |
| K08 | 音视频Operation并行流水线 | success | yes | 26.56 | 90f51d5 |
| K09 | 全Provider必跑矩阵 | success | yes | 454.63 | 90f51d5 |
| K10 | 视频Provider全模式矩阵 | success | yes | 344.36 | 90f51d5 |
| K11 | 3D Provider全模式矩阵 | success | yes | 86.13 | 90f51d5 |
| K12 | Skill并行续接与最终交付 | success | yes | 206.82 | 90f51d5 |
| K13 | Operation与Skill混合续接 | success | yes | 161.27 | 90f51d5 |
| K14 | 两阶段Provider不可变交接 | success | yes | 69.61 | 68d0274 |
| RC01 | 供应商配额耗尽后恢复 | success | yes | 9.36 | 90f51d5 |
| RC02 | 供应商容量等待中取消 | success | yes | 6.10 | 90f51d5 |
| RC03 | 供应商容量等待中超时 | success | yes | 7.95 | 90f51d5 |
| RC04 | 容量等待中 Worker 硬杀恢复 | success | yes | 12.99 | 90f51d5 |
| RC05 | 平台节点容量耗尽后恢复 | success | yes | 8.59 | 90f51d5 |
| RC06 | 创建响应未返回时 Worker 硬杀 | success | yes | 11.88 | 90f51d5 |
| RC07 | Worker 满载时恢复既有执行并释放配额 | success | yes | 23.04 | 90f51d5 |
| DM01 | 格式筛选与真实下载中断恢复 | success | yes | 155.05 | 5b747d4 + working tree |
| DM02 | MediaWorker SIGKILL 后接管原执行 | success | yes | 139.04 | 5b747d4 + working tree |
| DM03 | 真实执行取消与停止确认 | success | yes | 17.58 | 5b747d4 + working tree |
| DM04 | 真实创建响应丢失与防重复提交 | success | yes | 2.41 | 5b747d4 + working tree |

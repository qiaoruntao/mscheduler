[//]: # ([![Crates.io][crates-badge]][crates-url])
[![MIT licensed][mit-badge]][mit-url]
[![Build Status][actions-badge]][actions-url]

[//]: # ([crates-badge]: https://img.shields.io/crates/v/tokio.svg)

[//]: # ([crates-url]: https://crates.io/crates/tokio)

[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg

[mit-url]: https://github.com/qiaoruntao/mscheduler/blob/master/LICENSE

[actions-badge]: https://github.com/qiaoruntao/mscheduler/actions/workflows/ci.yml/badge.svg

[actions-url]: https://github.com/qiaoruntao/mscheduler/actions?query=branch%3Amaster

## 功能点
1. 任务的发布和执行
2. 多个worker同时运行任务
3. 任务失败后重试
4. worker ping超时后其他worker可以抢占任务
5. 

## 核心模型
Task
1. TaskState
   1. 整体任务状态参数
   2. Vec\<TaskWorkerState\>
2. TaskOption
3. \<T\>参数

任务中worker state的状态转换
1. INIT 刚发送
2. RUNNING 被worker占领并执行中
3. SUCCESS worker执行成功
4. FAIL worker执行失败

## 功能设计
### 任务发送
不变
### 任务消费

**如何发现任务**

1. 启动时主动查询一批任务
2. 通过change stream 订阅一批发生变化的任务
3. 任务列表空时主动查询一批任务

**查询什么任务**

1. INIT=》需要抢占
2. RUNNING=》等待超时后抢占
3. 其他worker执行失败,但是符合抢占条件=》走复杂判断逻辑

**按什么优先级查询任务**

目前没有特殊要求, 按任意key顺序查询

**如何判断任务是否可抢占**
1. 任务参数限制不可执行
   1. specific_worker_ids限制不可执行
2. 其他worker状态限制
   1. ping_expire_time没到, 不可抢占
   2. 其他worker info占据了并发执行数量
      1. max_unexpected_retries超出限制, 不执行. 约束: max_unexpected_retries\<failed worker info cnt
      2. concurrent_worker_cnt限制并行执行数量. 约束: concurrent_worker_cnt\<running worker cnt &&  concurrent_worker_cnt\<success worker info cnt

**抢占的流程**
1. 启动前使用change stream监听. 启动后查询一批任务, 查询条件: 任务参数允许worker执行 && 任务没有完全成功或失败(根据info cnt数量确定).
2. 查询后读取worker info状态, 用于判断抢占时间点
3. 遍历这些任务, 计算任务可抢占的时间点. 设置定时任务在这些时间点进行抢占. PriorityQueue实现, 有最大长度限制.
4. 如果可抢占任务为空, 那么重新查询一批任务直到没有数据返回. 之后完全可以通过change stream监听

**执行的流程**
1. 通过tokio::spawn隔离报错
2. 按option里的设置持续ping
3. 任务失败/成功后更新任务状态

**任务失败的报错**
没有特殊需求, 保持原样

**模块划分**
1. stream监听. 始终存在
2. 批量读取任务. 读取完成后可退出, 直到stream监听报错为止(存在时间gap, 无法判断是否存在新更新的任务). 所以这个由stream监听线程启动
3. 任务抢占. 监听和批量读取后通过该线程维护PriorityQueue, 并执行任务抢占
4. 任务执行. 任务抢占后spawn任务并执行. 执行过程中维护执行handler的状态. 任务完成后负责更新任务状态

## 测试用例
1. 一个worker, 持续运行, 

| Feature                    |      Bull       | Agenda |
|:---------------------------|:---------------:|:------:|
| Backend                    |      redis      | mongo  |
| Priorities                 |        ✓        |   ✓    |
| Concurrency                |        ✓        |   ✓    |
| Delayed jobs               |        ✓        |   ✓    |
| Global events              |        ✓        |   ✓    |
| Rate Limiter               |        ✓        |        |
| Pause/Resume               |        ✓        |   ✓    |
| Sandboxed worker           |        ✓        |   ✓    |
| Repeatable jobs            |        ✓        |   ✓    |
| Atomic ops                 |        ✓        |   ~    |
| Persistence                |        ✓        |   ✓    |
| UI                         |        ✓        |   ✓    |
| REST API                   |                 |   ✓    |
| Central (Scalable) Queue   |                 |   ✓    |
| Supports long running jobs |                 |   ✓    |
| Optimized for              | Jobs / Messages |  Jobs  |
| Gracefuly stop             |                 |        |
| Remote stop                |                 |        |
| Multiple worker            |                 |        |

## 流程

1. 发布任务
    1. 幂等

## worker状态

1. 执行中
   worker占用任务成功后立即进入执行中状态, 并设置下次刷新时间
   如果距离上次刷新时间超过worker_timeout_ms, 那么认为是超时状态
2. 失败
   worker主动设置执行状态为fail. 如果任务失败时worker不在列表中, 状态设置会失败
3. 成功
   worker主动设置执行状态为success. 如果任务成功时worker不在列表中, 状态设置会失败
4. 超时
   根据worker_timeout_ms推导得到的状态

## 任务状态

任务状态由当前worker列表决定

1. 未启动
   列表为空
2. 执行中
   列表中存在执行中worker
3. 成功
   列表中只有success的worker
4. 失败
   列表中只有fail的worker

## 提供功能

1. 发布任务(幂等)
   查找key对应的没有运行中的任务
   没有找到=>setOnInsert
   找到了=>set nothing, 按配置决定是否清除失败/成功的worker
2. 占用任务(多worker)
   查找条件: 判断是否被指定worker_id, 任务状态不是成功, 任务没有被当前worker处理过(不能在worker列表中), 可以接受更多的worker
   排序条件: priority最高, 优先被指定worker_id的
   没有找到=>不做操作
   找到了=>增加自己的worker对象, 并且过滤worker列表(清除超时的运行中worker)
3. 维持任务
   查找条件: key相同, worker id相同,
   找到了=> 更新对应的超时时间
   没找到=> 结束当前任务
4. 任务执行成功
   尝试更新任务状态为成功
5. 任务执行失败
   主动返回任务失败, 尝试更新任务状态为失败
6. 任务执行异常
   按option中设置重试, 并更新重试次数, 如果重试次数超过限制那么更新为任务失败. 重试过程只在本地发生, 所有重试次数失败后更新为失败状态

## 配置更新后的影响

1. specific_worker_ids变动
   如果更新后不允许当前worker执行, 那么结束任务
2. ping_interval_ms变动
   下次ping时生效
3. 其他参数变动
   不影响正在执行中的任务, 变动需要在重新占用任务时体现

## 具体实现

### 发布任务

直接发送就行

### 消费任务

核心问题: 下一次什么时候去占用任务

1. 启动时计算下一次时间next_try_time
2. 使用change_stream实时更新next_try_time

## TODO

-[ ] clean_success 暂不实现, maintenance
-[ ] clean_failed 暂不实现, maintenance
-[ ] detect compatibility of collection data
-[x] auto worker id
-[ ] 错误处理
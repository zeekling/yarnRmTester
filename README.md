
# 简介

Hadoop自带的hadoop-sls只能用于压测调度器，可在实际中影响ResourceManager性能的因素比较多，不能只看调度器。
当前项目可构造海量的Fake NM节点，用于模拟线上RM的巨大压力场景，进行优化。

# 架构

![pic](https://pan.zeekling.cn/zeekling/hadoop/fake/fake_01.png)

核心思想：
- Fake NM：构造大量的Fake NM。在Fake NM里面主要做Container的管理，不会真正的启动。防止占用大量资源。
- Fake AM: 构造的AM。只是一个对象，所有的AM由线程池管理，用于申请新的Container、控制整个作业的运行时长。
- SLSRunner: 压测模块，由于NM是Fake的，作业也是Fake的，只用于控制提交作业的数量。

# 运行

主要包含两个模块，Fake NM和SLSRunner。分别控制构造NM和压测RM。

## Fake NM 运行

当前模块主要是为RM构造大量的NM，建议在运行之前将集群内正常的NM停止掉。
当前模块的入口是SLSNodeManager，需要修改配置文件的路径，修改configPath为具体的路径即可。

配置文件主要包含：
- core-site.xml：从RM对应的集群里面获取
- hdfs-site.xml：从RM对应的集群里面获取
- yarn-site.xml：从RM对应的集群里面获取。但是下面参数需要按照模拟的实际情况修改：
  - yarn.scheduler.maximum-allocation-mb：模拟节点的内存。
  - yarn.scheduler.maximum-allocation-vcores：模拟节点的vcore。
- fake.properites：Fake NM的主要配置，含义如下：
  - yarn.fake.nodemanager.hostname：模拟节点的主机名，按照实际情况填写。
  - yarn.fake.nodemanager.rack： 模拟节点的Rack。
  - yarn.fake.nodemanger.rpc.port.begin：模拟NM节点的rpc端口范围的开始值。具体范围是起始值+模拟NM的id。
  - yarn.fake.nodemanger.http.port.begin： 模拟NM节点的http端口范围的开始值。具体范围是起始值+模拟NM的id。
  - yarn.fake.nodemanger.count： 模拟NM的数量。
  - yarn.fake.threadpool.size：处理心跳等的线程数目。
  - yarn.fake.job.token-servers： 获取hdfs token的详细信息。
  - yarn.fake.job.duration： Fake作业运行的时长，超过当前时长会将状态变为已完成。
  - yarn.fake.job.container.nums： 一个作业的普通Container数量。
  - yarn.fake.job.container.vcore：普通Container占用的vcore。
  - yarn.fake.job.container.memory-mb： 普通Container占用的内存大小。
  - yarn.fake.job.update.threadpool.size：作业状态更新的线程数目。

接下来就直接运行SLSNodeManager即可。

## SLSRunner

当前模块主要是运行压测任务。

当前模块的入口是SLSRunner，需要修改配置文件的路径，修改configPath为具体的路径即可。

配置文件主要包含：

- core-site.xml：从RM对应的集群里面获取
- hdfs-site.xml：从RM对应的集群里面获取
- yarn-site.xml：从RM对应的集群里面获取。
- fake.properites：压测作业相关的主要配置，含义如下：
   - yarn.fake.job.parallel： 提交作业的并行度。
   - yarn.fake.job.cycle.times： 循环次数。
   yarn.fake.job.queue： 作业提交的队列。

接下来就直接运行SLSRunner即可。



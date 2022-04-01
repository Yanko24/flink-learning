#### Flink常见的面试题

##### 1. Flink-On-Yarn常见的提交模式有哪些，分别有什么优缺点？

flink在yarn模式下运行，有三种任务提交模式，资源消耗各不相同。

###### 1. yarn-session

yarn-session这种方式需要先启动集群，然后在提交作业，接着会向yarn申请一块空间后，资源永远保持不变。如果资源满了，下一个就任务就无法提交，只能等到yarn中其中一个作业完成后，释放了资源，那下一个作业才会正常提交。所有作业共享资源，隔离性差，JVM负载瓶颈，main方法在客户端执行。适合执行时间短，频繁执行的短任务，集群中只有一个JobManager，另外Job被随机分配给TaskManager。

###### 2. yarn-per-job

每个作业单独启动集群，隔离性好，JVM负载均衡，main方法在客户端执行。在per-job模式下，每个Job都有一个JobManager，每个TaskManager只有单个Job。一个任务会对应一个Job，每提交一个作业会根据自身的情况，都会单独向yarn申请资源，独享Dispatcher和ResourceManager，按需接受资源申请，适合规模大长时间运行的作业。

###### 3. yarn-application

main方法在JobManager中执行，入口点位于ApplicationClusterEntryPoint，客户端只需要负责发起部署请求。这种方式减轻了客户端的压力，避免客户端资源成为瓶颈。

##### 2. 怎么做压力测试和监控？

- 产生的数据流的速度如果过快，而下游的算子消费不过来的话，会产生背压。背压的监控可以使用Flink Web UI来可视化监控Metrics，一旦报警就能知道。一般情况下可能由于sink这个操作符没有优化好，做一下优化就可以了。
- 设置watermark的最大延迟时间这个参数，如果设置的过大，可能会造成内存的压力。可以设置最大延迟时间小一些，然后把迟到的元素发送到测输出流中，晚一点更新结果。
- 还有就是滑动窗口的长度如果过大，而滑动距离很短的话，Flink的性能也会下降的厉害。可以通过分片的方法，将每个元素只存入一个“重叠窗口”，这样就可以减少窗口处理中状态的写入。

##### 3. Flink是通过什么机制实现的背压机制？

Flink在运行时主要由operators和streams两大构件组成。每个operator会消费中间状态的流，并在流上进行转换，然后生成新的流。对于Flink的网络机制一种形象的类比是，Flink使用了高效有界的分布式阻塞队列，就像Java通过的阻塞队列（BlockingQueue）一样。使用BlockingQueue的话，一个较慢的接受者会降低发送者的发送速率，因为一旦队列满了（有界队列）发送者会被阻塞。

在Flink中，这些分布式阻塞队列就是这些逻辑流，而队列容量通过缓冲池（LocalBufferPool）实现的。每个被生产和消费的流都会被分配一个缓冲池。缓冲池管理者一组缓冲（Buffer），缓冲在被消费后可以被回收循环利用。

##### 4. Flink的反压检测逻辑了解吗？

Flink-1.13版本之前，使用的堆栈采样式方式判断反压，在Flink-1.13版本开始使用基于任务Mailbox计时的方式判断反压。

##### 5. 为什么数据倾斜会造成反压？

首先，数据倾斜是由于不同的key对应的数据数量不同，导致不同的task所处理的数据量不同的问题。如果在Flink相同的Task的多个SubTask之间，个别SubTask接收到的数据量明显大于其他SubTask接收到的数据量，就会造成数据倾斜，这样就会导致任务执行过慢，从而引起反压情况。

Flink Web UI可以精确的看到每个SubTask处理了多少数据，即可判断出Flink任务是否存在数据倾斜。

##### 6. 反压有哪些危害呢？

反压如果不能得到正常的处理，可能会影响到checkpoint时长和state大小，甚至可能会导致资源耗尽甚至系统崩溃。

- 影响checkpoint时长：barrier不会越过普通数据，数据处理阻塞也会导致checkpoint barrier流经整个数据管道的时间变长，导致checkpoint总体时间（End to End Duration）变长。
- 影响state大小：barrier对齐时，接受到较快的输入管道的barrier后，它后面的数据会被缓存起来但不处理，直到较慢的输入管道的barrier也到达，这些被缓存的数据会被放入到state里面，导致checkpoint变大。

这两个影响对于生产环境的作业来说是十分危险的，因为checkpoint时保证数据一致性的关键，checkpoint时间变长可能导致checkpoint超时失败，而state大小同样可能拖慢checkpoint导致OOM（使用Heap-based StateBackend）或者物理内存使用超出容器资源（使用RocksDBStateBackend）的稳定性问题。

##### 7. Flink中的数据倾斜有哪些解决方法？

- keyBy后的聚合操作存在数据倾斜。使用LocalKeyBy的思想，在keyBy上游算子数据发送之前，首先在上游算子的本地对数据进行聚合后，再发送到下游，使下游接收到的数据量大大减少，从而使得keyBy之后的聚合操作不再是任务的瓶颈。
- keyBy之前发生数据倾斜。产生该情况可能是因为数据源的数据本身就不均匀，例如由于某些原因kafka的topic中某些partition的数据量较大，某些partition的数据量较少。解决方法：需要让Flink任务强制进行shuffle。使用shuffle，rebalance或rescale算子即可将数据均匀分配，从而解决数据倾斜的问题。
- keyBy后的窗口聚合操作存在数据倾斜。因为使用了窗口，变成了有界数据（攒批）的处理，窗口默认是触发时才会输出一条结果发送下游，所以可以使用两阶段聚合的方式。（第一阶段：key拼接随机数前缀或后缀，进行keyBy，开窗，聚合；第二阶段：按照原来的key及windowEnd做keyBy，聚合）

##### 8. 为什么使用Flink替代Spark？

使用Flink主要考虑的是Flink的低延迟、高吞吐量和对流式数据应用场景更好的支持。另外，Flink可以很好的处理乱序数据，而且可以保证exactly-once的状态一致性。

##### 9. 如果下级存储不支持事务，Flink是如果保证exactly-once的？

端到端的exactly-once对sink的要求比较高，具体的实现主要有幂等写入和事务性写入两种方式。幂等写入的场景依赖于业务逻辑，更常见的是用事务性写入。而事务性写入又有预写日志（WAL）和两阶段提交（2PC）两种方式。

如果外部系统不支持事务，那么可以使用预写日志的方式，把结果数据当成状态保存，然后在收到checkpoint完成的通知时，一次性写入sink系统。

##### 10. Flink的状态机制是什么？

###### 1. Raw State和ManagedState

按照由Flink管理还是用户管理，状态可以分为原始状态（Raw State）和托管状态（ManagedState）。

- 托管状态（ManagedState）：由Flink自行管理的state。
- 原始状态（Raw State）：由用户自行进行管理。

两者的主要区别是：Managed State由Flink Runtime管理，自动存储，自动恢复，在内存管理上有优化；而Raw State需要用户自己管理，需要自己序列化；Managed State支持已知的数据结构，比如Value，List，Map等；而Raw State只支持字节数组，所有的状态都要转换为二进制字节数组才可以。

###### 2. KeyedState和OperatorState

State按照是否有key划分为KeyedState和OperatorState两种。

KeyedState只能用在keyedStream的算子上，状态和特定的key绑定。keyedStream流上的每一个key对应一个state对象。若一个operator处理多个key，访问相应的的多个state，可对应多个state。keyedState保存在StateBackend中，通过RuntimeContext访问，实现Rich Function接口。支持多种数据结构：ValueState，ListState，ReducingState，AggregatingState，MapState。

OperatorState可以用于所有算子，但整个算子只对应一个state。OperatorState实现CheckpointedFunction或者ListCheckpointed（已经过时）接口。目前只支持ListState和BroadCastState数据结构。

###### 3. 存储

在Flink中，state一般被存储在StateBackend里面。总共包含三种方式：内存，文件，RocksDB等。

- MemoryStateBackend：运行时所需的state数据全部保存在TaskManager JVM堆内存上，执行检查点的时候，会把state的快照数据保存在JobManager进程的内存中。基于内存的StateBackend在生产环境下不建议使用，可以在本地开发调试。State存储在JobManager的内存中，受限于JobManager的内存大小；每个State默认5MB，可通过MemoryStateBackend构造函数调整；每个State不能超过Akka Frame大小。
- FSStateBackend：运行时所需的State数据全部保存在TaskManager的内存中，执行检查点的时候，会把State的快照数据保存在配置的文件系统中。适用于处理小状态，短窗口，或者小键值状态的有状态处理任务；State首先被存储在TaskManager的内存中；State大小不能超过TaskManager的内存；TaskManager异步将State数据写入到外部存储。
- RocksDBStateBackend：使用嵌入式的本地数据库RocksDB将流计算数据状态存储在本地磁盘中。在执行检查点的时候，再将整个RocksDB中保存的State数据全量或者增量持久化到配置的文件系统中。最适合用于处理大状态，长窗口，或者大键值状态的有状态处理任务；RocksDBStateBackend非常适合用于高可用方案；RocksDBStateBackend是目前唯一支持增量检查点的后端。增量检查点非常适用于超大状态的场景。

##### 11. 海量key怎么去重？

使用类似于Scala的set数据结构或者redis的set显然是不行的，因为可能有上亿个key，内存放不下。所以可以考虑使用布隆过滤器（Bloom Filter）来去重。

##### 12. Flink的checkpoint机制对比spark有什么不同和优势？

spark streaming的checkpoint仅仅是针对driver的故障恢复做了数据和元数据的checkpoint。而flink的checkpoint机制要复杂很多，它采用的是轻量级的分布式快照，实现了每个算子的快照，及流动中的数据的快照。

##### 13. 详细解释一下Flink的watermark机制？

Watermark的本质是Flink中衡量EventTime进展的一个机制，主要用来处理乱序的数据。

##### 14. Flink中的exactly-once语义如何实现的，状态如何存储的？

Flink依靠checkpoint机制来实现exactly-once语义，如果要实现端到端的exactly-once，还需要外部source和sink满足一定的条件。状态的存储通过状态后端来管理，Flink中可以配置不同的状态后端。

##### 15. Flink CEP编程中当状态没有到达的时候会将数据保存到哪里？

在流式处理中，CEP当然是要支持EventTime的，那么相对应的也要支持数据的迟到现象，也就是说watermark的处理逻辑。CEP对未匹配成功的事件序列的处理，和迟到数据是类似的。在Flink CEP的处理逻辑中，状态没有满足和迟到的数据，都会存储在一个Map数据结构中，也就是说，如果我们限定判断事件序列的时长为5分钟，那么内存中就会存储5分钟的数据，这也是对内存的极大损伤之一。

##### 16. Flink的三种时间语义是什么？应用场景是什么？

- Event Time：这是实际应用最常见的时间语义。
- Processing Time：没有事件时间的情况下，或者对实时要求超高的情况下。
- Ingestion Time：存在多个Source Operator的情况下，每个Source Operator可以使用自己本地系统始终指派Ingestion Time后。后续基于时间相关的各种操作，都会使用数据记录中的Ingestion Time。


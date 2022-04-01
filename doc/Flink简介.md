<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink简介](#flink%E7%AE%80%E4%BB%8B)
  - [1. 什么是Flink](#1-%E4%BB%80%E4%B9%88%E6%98%AFflink)
  - [2. Flink的特点](#2-flink%E7%9A%84%E7%89%B9%E7%82%B9)
    - [1. 事件驱动型](#1-%E4%BA%8B%E4%BB%B6%E9%A9%B1%E5%8A%A8%E5%9E%8B)
    - [2. 流与批的世界观](#2-%E6%B5%81%E4%B8%8E%E6%89%B9%E7%9A%84%E4%B8%96%E7%95%8C%E8%A7%82)
    - [3. 分层API](#3-%E5%88%86%E5%B1%82api)
  - [3. Flink和Spark](#3-flink%E5%92%8Cspark)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#### Flink简介

##### 1. 什么是Flink

```
Flink是为分布式、高性能、随时可用以及准确的流处理应用程序打造的开源流处理框架。
```

##### 2. Flink的特点

###### 1. 事件驱动型

```
事件驱动型应用是一类具有状态的应用，它从一个或者多个事件流提取数据，并根据到来的事件触发计算、状态更新或其他外部动作。
```

###### 2. 流与批的世界观

```
批处理的特点是有界、持久、大量，非常适合需要访问全套记录才能完成的计算工作，一般用于离线计算。
流处理的特点是无界、实时，无需针对整个数据集执行操作，而是对通过系统传输的每个数据项执行操作，一般用于实时统计。
```

有界数据流和无界数据流：

- 无界数据流有一个开始但是没有结束，它们不会在生成时终止并提供数据，必须连续处理无界流，也就是说必须在获取后立即处理event。对于无界数据流我们无法等待所有的数据都到达，因为输入是无界的，并且在任何时间点都不会完成。处理无界数据通常要求以特定顺序（例如事件发生的顺序）获取event，以便能够推断结果完整性。
- 有界数据流有明确定义的开始和结束，可以在执行任何计算之前通过获取所有数据来处理有界流，处理有界流不需要有序获取，因为可以始终对有界数据集进行排序，有界流的处理也被称为批处理。

###### 3. 分层API

![](images/Flink API.jpg)

##### 3. Flink和Spark

Spark和Flink一开始都有同一个梦想，都希望可以使用一个技术把流处理和批处理统一起来。Spark是以批处理为根本，并尝试在批处理之上支持流计算；Flink是以流处理为根本，在流处理上支持批处理。Spark和Flink的主要区别就是计算模型不同，Spark采用了微批处理模型，而Flink采用了基于操作符的连续流模型。


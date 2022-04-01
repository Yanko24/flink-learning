<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Flink快速上手](#flink%E5%BF%AB%E9%80%9F%E4%B8%8A%E6%89%8B)
  - [1. 引入Flink相关jar包](#1-%E5%BC%95%E5%85%A5flink%E7%9B%B8%E5%85%B3jar%E5%8C%85)
  - [2. 批处理WordCount](#2-%E6%89%B9%E5%A4%84%E7%90%86wordcount)
  - [3. 有界流处理WordCount](#3-%E6%9C%89%E7%95%8C%E6%B5%81%E5%A4%84%E7%90%86wordcount)
  - [4. 无界流处理WordCount](#4-%E6%97%A0%E7%95%8C%E6%B5%81%E5%A4%84%E7%90%86wordcount)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#### Flink快速上手

##### 1. 引入Flink相关jar包

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-java</artifactId>
    <version>1.12.2</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-scala_2.12</artifactId>
    <version>1.12.2</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java_2.12</artifactId>
    <version>1.12.2</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_2.12</artifactId>
    <version>1.12.2</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients_2.12</artifactId>
    <version>1.12.2</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime-web_2.12</artifactId>
	<version>1.12.2</version>
</dependency>
```

##### 2. 批处理WordCount

```java
public class Flink01_WordCount_Batch_Java {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.读取文件数据
        DataSource<String> input = env.readTextFile("input");

        // 3.压平
        FlatMapOperator<String, String> wordDS = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // 按照空格切分
                String[] words = value.split(" ");

                // 写出一个一个单词
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        // 4.讲单词转换为元组
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS =
                wordDS.map((MapFunction<String, Tuple2<String, Integer>>) value -> Tuple2.of(value, 1))
                        .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 5.分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDS.groupBy(0);

        // 6.聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        // 7.打印结果
        result.print();
    }
}
```

##### 3. 有界流处理WordCount

```scala
object Flink02_WordCount_Bounded_Scala {
  def main(args: Array[String]): Unit = {
    // 1.创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.读取文件
    val input: DataStream[String] = env.readTextFile("input")

    // 3.压平转换成元组
    val lineToTupleDS: DataStream[(String, Int)] = input.flatMap(_.split(" ")).map((_, 1))

    // 4.分组
    val keyedDS: KeyedStream[(String, Int), String] = lineToTupleDS.keyBy(_._1)

    // 5.聚合
    val result: DataStream[(String, Int)] = keyedDS.sum(1)

    // 6.打印
    result.print()

    // 7.提交
    env.execute()
  }
}
```

##### 4. 无界流处理WordCount

```scala
object Flink03_WordCount_UnBounded_Scala {
  def main(args: Array[String]): Unit = {
    // 1.创建流执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)

    // 2.读取socket数据
    val sockedStream: DataStream[String] = env.socketTextStream("hadoop01", 9999)

    // 3.压平并转换为元组
    val lineToTupleDS: DataStream[(String, Int)] = sockedStream.flatMap(_.split(" ")).map((_, 1))

    // 4.分组
    val keyedDS: KeyedStream[(String, Int), String] = lineToTupleDS.keyBy(_._1)

    // 5.聚合
    val result: DataStream[(String, Int)] = keyedDS.sum(1)

    // 6.打印测试
    result.print()

    // 7.提价
    env.execute()
  }
}
```


package com.yankee;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @date 2021/10/14 19:35
 */
public class ParseOggKafka {
    private static final Logger LOG = LoggerFactory.getLogger(ParseOggKafka.class);

    /**
     * kafka-source
     */
    private static final String SOURCE_SQL = "CREATE TABLE bill_change_topic (\n" +
            "\t`table` varchar,\n" +
            "\top_type varchar,\n" +
            "\top_ts varchar,\n" +
            "\tcurrent_ts varchar,\n" +
            "\tpos varchar,\n" +
            "\tprimary_keys array<varchar>,\n" +
            "\tafter row<\n" +
            "\tTRANDT varchar,\n" +
            "\tBILLSQ varchar,\n" +
            "\tTRANSQ varchar,\n" +
            "\tACCTBR varchar,\n" +
            "\tACCTID varchar,\n" +
            "\tACCTNO varchar,\n" +
            "\tSUBSAC varchar,\n" +
            "\tTRANTP varchar,\n" +
            "\tAMNTCD varchar,\n" +
            "\tCRCYCD varchar,\n" +
            "\tTRANAM decimal,\n" +
            "\tTRANBL decimal,\n" +
            "\tTRANBR varchar,\n" +
            "\tSMRYCD varchar,\n" +
            "\tTOACCT varchar,\n" +
            "\tTOSBAC varchar,\n" +
            "\tTOACNA varchar,\n" +
            "\tCQTPID varchar,\n" +
            "\tCHEQTP varchar,\n" +
            "\tCHEQNO varchar,\n" +
            "\tBKUSID varchar,\n" +
            "\tCKBKUS varchar,\n" +
            "\tCORRTG varchar,\n" +
            "\tDSCRTX varchar,\n" +
            "\tTIMSTP varchar,\n" +
            "\tSERVTP varchar>,\n" +
            "\tbefore row<\n" +
            "\tTRANDT varchar,\n" +
            "\tBILLSQ varchar,\n" +
            "\tTRANSQ varchar,\n" +
            "\tACCTBR varchar,\n" +
            "\tACCTID varchar,\n" +
            "\tACCTNO varchar,\n" +
            "\tSUBSAC varchar,\n" +
            "\tTRANTP varchar,\n" +
            "\tAMNTCD varchar,\n" +
            "\tCRCYCD varchar,\n" +
            "\tTRANAM varchar,\n" +
            "\tTRANBL decimal,\n" +
            "\tTRANBR varchar,\n" +
            "\tSMRYCD varchar,\n" +
            "\tTOACCT varchar,\n" +
            "\tTOSBAC varchar,\n" +
            "\tTOACNA varchar,\n" +
            "\tCQTPID varchar,\n" +
            "\tCHEQTP varchar,\n" +
            "\tCHEQNO varchar,\n" +
            "\tBKUSID varchar,\n" +
            "\tCKBKUS varchar,\n" +
            "\tCORRTG varchar,\n" +
            "\tDSCRTX varchar,\n" +
            "\tTIMSTP varchar,\n" +
            "\tSERVTP varchar>\n" +
            ") WITH (\n" +
            "\t'connector' = 'kafka',\n" +
            "\t'topic' = 'bill_change_topic',\n" +
            "\t'properties.bootstrap.servers' = 'master:9092,slave1:9092,salve2:9092',\n" +
            "\t'scan.startup.mode' = 'earliest-offset',\n" +
            "\t'properties.group.id' = 'test',\n" +
            "\t'format' = 'json'\n" +
            ")";

    /**
     * kafka-sink
     */
    private static final String SINK_SQL = "CREATE TABLE bill_change_custno_topic (\n" +
            "\tmsgparams row<\n" +
            "\tsceneCode varchar,\n" +
            "\tsilentSwitch varchar>,\n" +
            "\tdataparams row<\n" +
            "\tACCTNO varchar,\n" +
            "\tTOACCT varchar,\n" +
            "\tTRANAM decimal,\n" +
            "\tTIMSTP varchar,\n" +
            "\tDSCRTX varchar,\n" +
            "\tAMNTCD varchar,\n" +
            "\tCUSTNO varchar>\n" +
            ") WITH (\n" +
            "\t'connector' = 'kafka',\n" +
            "\t'topic' = 'bill_change_custno_topic',\n" +
            "\t'properties.bootstrap.servers' = 'master:9092,slave1:9092,slave2:9092',\n" +
            "\t'format' = 'json'\n" +
            ")";

    /**
     * print-sql
     */
    private static final String PRINT_SQL = "INSERT INTO\n" +
            "\tbill_change_custno_topic\n" +
            "SELECT\n" +
            "\t('模板', '0000') as msgparams,\n" +
            "\t(\n" +
            "\t\tbct.after.ACCTNO,\n" +
            "\t\tbct.after.TOACCT,\n" +
            "\t\tbct.after.TRANAM,\n" +
            "\t\tbct.after.TIMSTP,\n" +
            "\t\tbct.after.DSCRTX,\n" +
            "\t\tbct.after.AMNTCD,\n" +
            "\t\trs.CUSTNO\n" +
            "\t) as dataparams\n" +
            "FROM bill_change_topic bct\n" +
            "WHERE bct.op_type = 'U'";

    public static void main(String[] args) {
        try {
            // 获取流执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // 表环境配置
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();
            // 获取table执行环境
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

            // 注册kafka-source表
            tableEnv.executeSql(SOURCE_SQL);
            // 注册kafka-sink表
            tableEnv.executeSql(SINK_SQL);

            // 插入数据
            tableEnv.executeSql(PRINT_SQL);

            // 打印结果
            tableEnv.executeSql("SELECT * FROM bill_change_custno_topic").print();
        } catch (Exception e) {
            LOG.info("Program Error : ", e);
            System.exit(1);
        }
    }
}

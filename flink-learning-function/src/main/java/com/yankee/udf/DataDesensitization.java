package com.yankee.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class DataDesensitization extends ScalarFunction {
    /**
     * 根据数据长度对数据进行脱敏
     *
     * @param content 数字字符串内容
     * @param head    开头预留位数
     * @param tail    结尾预留位数
     * @return 脱敏后的字符串
     */
    public String eval(String content, Integer head, Integer tail) {
        // 判断是否为空
        if (StringUtils.isEmpty(content)) {
            return "";
        }
        // 获取要替换的长度
        int length = content.length() - head - tail;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append("*");
        }
        return content.replaceAll("^(\\d{" + head + "})\\d+(\\d{" + tail + "})$", "$1" + builder + "$2");
    }
}

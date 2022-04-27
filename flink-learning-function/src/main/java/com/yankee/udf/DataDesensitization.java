package com.yankee.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class DataDesensitization extends ScalarFunction {
    public String eval(String content) {
        // 判断是否为空
        if (StringUtils.isEmpty(content)) {
            return "";
        }
        // 获取要替换的长度
        int length = content.length() - 8;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            builder.append("*");
        }
        return content.replaceAll("^(\\d{4})\\d+(\\d{4})$", "$1" + builder + "$2");
    }
}

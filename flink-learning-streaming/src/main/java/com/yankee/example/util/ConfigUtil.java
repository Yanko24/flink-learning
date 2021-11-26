package com.yankee.example.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/26 14:03
 */
public class ConfigUtil {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtil.class);

    public ParameterTool loadConfig(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        String configFilePath = params.get("configFilePath");
        if (null == configFilePath) {
            log.error("未发现配置文件");
            System.exit(1);
        }
        ParameterTool parameterTool = null;
        try {
            parameterTool = ParameterTool.fromPropertiesFile(this.getClass().getClassLoader().getResourceAsStream("config/" + configFilePath + ".properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (null == parameterTool) {
            log.error("未发现入参");
            System.exit(1);
        }
        return parameterTool;
    }
}

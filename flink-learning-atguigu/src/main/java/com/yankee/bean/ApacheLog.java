package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2022/2/22 9:24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApacheLog {
    /**
     * IP地址
     */
    private String ip;

    /**
     * 用户ID
     */
    private String userId;

    /**
     * 时间戳
     */
    private Long ts;

    /**
     * 方法
     */
    private String method;

    /**
     * 页面访问url
     */
    private String url;
}

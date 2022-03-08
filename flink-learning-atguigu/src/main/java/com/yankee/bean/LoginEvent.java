package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 登陆ip
     */
    private String ip;

    /**
     * 事件类型（failer，success）
     */
    private String eventType;

    /**
     * 事件时间
     */
    private Long eventTime;
}

package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @since 2021/7/22
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {
    private Long orderId;

    private String eventType;

    private String txId;

    private Long eventTime;
}

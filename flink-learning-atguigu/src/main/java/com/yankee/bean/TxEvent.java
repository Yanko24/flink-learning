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
public class TxEvent {
    private String txId;

    private String payChannel;

    private Long eventTime;
}

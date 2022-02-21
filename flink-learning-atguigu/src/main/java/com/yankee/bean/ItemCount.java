package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2022/2/21 10:56
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount {
    /**
     * 商品
     */
    private Long item;

    /**
     * 窗口时间
     */
    private String time;

    /**
     * 数量
     */
    private Integer count;
}

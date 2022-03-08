package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2022/2/22 10:01
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlCount {
    /**
     * url
     */
    private String url;

    /**
     * 窗口结束时间
     */
    private Long windowEnd;

    /**
     * 统计数量
     */
    private Integer count;
}

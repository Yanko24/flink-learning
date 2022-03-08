package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdsClickCount {
    /**
     * 省份
     */
    private String province;

    /**
     * 窗口结束时间
     */
    private String windowEnd;

    /**
     * 统计count
     */
    private Integer count;
}

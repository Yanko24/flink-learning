package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/28 15:12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageViewCount {
    private String pv;

    private String time;

    private Integer count;
}

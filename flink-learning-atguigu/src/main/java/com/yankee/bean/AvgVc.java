package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/27 21:39
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AvgVc {
    private Integer vcSum;

    private Integer count;
}

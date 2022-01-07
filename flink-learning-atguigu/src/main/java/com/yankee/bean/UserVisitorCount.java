package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/29 11:24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserVisitorCount {
    private String uv;

    private String time;

    private Integer count;
}

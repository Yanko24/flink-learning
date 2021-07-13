package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description
 * @date 2021/6/7 16:31
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor_Java {
    private String id;
    private Long ts;
    private Integer vc;
}

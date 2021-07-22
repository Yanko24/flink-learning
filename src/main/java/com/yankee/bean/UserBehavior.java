package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description JavaBean
 * @since 2021/7/16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;

    private Long itemId;

    private Integer categoryId;

    private String behavior;

    private Long timestamp;
}

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
    /**
     * 用户Id
     */
    private Long userId;

    /**
     * itemId
     */
    private Long itemId;

    /**
     * 分类Id
     */
    private Integer categoryId;

    private String behavior;

    /**
     * 时间戳
     */
    private Long timestamp;
}

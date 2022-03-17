package com.yankee.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description TODO
 * @Date 2022/3/15 16:29
 * @Author yankee
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordToOne {
    /**
     * 单词
     */
    private String word;

    /**
     * 个数
     */
    private Integer cnt;
}



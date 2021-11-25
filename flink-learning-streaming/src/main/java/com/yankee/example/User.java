package com.yankee.example;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/11/25 13:25
 */
@Data
public class User implements Serializable {
    /**
     * 用户名
     */
    private String username;

    /**
     * 用户密码
     */
    private String password;

    /**
     * 年龄
     */
    private Integer age;

    /**
     * 用户地址
     */
    private String address;
}

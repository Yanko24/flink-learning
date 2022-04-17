package com.yankee.bean;

/**
 * @author Yankee
 * @version 1.0
 * @description TODO
 * @date 2021/12/29 11:24
 */
public class UserVisitorCount {
    private String uv;

    private String time;

    private Integer count;

    public UserVisitorCount() {}

    public UserVisitorCount(String uv, String time, Integer count) {
        this.uv = uv;
        this.time = time;
        this.count = count;
    }

    public String getUv() {
        return uv;
    }

    public void setUv(String uv) {
        this.uv = uv;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UserVisitorCount{" +
                "uv='" + uv + '\'' +
                ", time='" + time + '\'' +
                ", count=" + count +
                '}';
    }
}

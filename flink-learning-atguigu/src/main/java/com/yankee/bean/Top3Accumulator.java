package com.yankee.bean;

/**
 * @Description TODO
 * @Date 2022/3/18 09:48
 * @Author yankee
 */
public class Top3Accumulator {
    private Integer top1;

    private Integer top2;

    private Integer top3;

    public Top3Accumulator() {}

    public Top3Accumulator(Integer top1, Integer top2, Integer top3) {
        this.top1 = top1;
        this.top2 = top2;
        this.top3 = top3;
    }

    public Integer getTop1() {
        return top1;
    }

    public void setTop1(Integer top1) {
        this.top1 = top1;
    }

    public Integer getTop2() {
        return top2;
    }

    public void setTop2(Integer top2) {
        this.top2 = top2;
    }

    public Integer getTop3() {
        return top3;
    }

    public void setTop3(Integer top3) {
        this.top3 = top3;
    }
}

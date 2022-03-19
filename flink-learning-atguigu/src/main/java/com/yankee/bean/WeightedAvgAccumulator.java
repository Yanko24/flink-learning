package com.yankee.bean;

/**
 * @Description TODO
 * @Date 2022/3/18 09:31
 * @Author yankee
 */
public class WeightedAvgAccumulator {
    private int vcSum = 0;
    private int vcCount = 0;

    public WeightedAvgAccumulator() {
    }

    public WeightedAvgAccumulator(int vcSum, int vcCount) {
        this.vcSum = vcSum;
        this.vcCount = vcCount;
    }

    public int getVcCount() {
        return vcCount;
    }

    public void setVcCount(int vcCount) {
        this.vcCount = vcCount;
    }

    public int getVcSum() {
        return vcSum;
    }

    public void setVcSum(int vcSum) {
        this.vcSum = vcSum;
    }
}

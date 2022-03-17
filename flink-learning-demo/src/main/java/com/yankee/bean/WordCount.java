package com.yankee.bean;

/**
 * @Description TODO
 * @Date 2022/3/14 08:41
 * @Author yankee
 */
public class WordCount {
    /**
     * word单词
     */
    private String word;

    /**
     * 数量
     */
    private Integer count;

    public WordCount(String word, Integer count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}

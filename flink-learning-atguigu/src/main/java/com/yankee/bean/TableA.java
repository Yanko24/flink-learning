package com.yankee.bean;

/**
 * @Description TODO
 * @Date 2022/3/18 20:57
 * @Author yankee
 */
public class TableA {
    private String id;
    private String name;

    public TableA() {}

    public TableA(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

package com.yankee.bean;

/**
 * @Description TODO
 * @Date 2022/3/18 20:57
 * @Author yankee
 */
public class TableB {
    private String id;
    private Integer classId;

    public TableB() {}

    public TableB(String id, Integer classId) {
        this.id = id;
        this.classId = classId;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Integer getClassId() {
        return classId;
    }

    public void setClassId(Integer classId) {
        this.classId = classId;
    }
}

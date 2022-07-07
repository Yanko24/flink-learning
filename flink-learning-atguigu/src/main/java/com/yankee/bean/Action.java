package com.yankee.bean;

public class Action {
    private String userId;

    private String action;

    public Action() {
    }

    public Action(String userId, String action) {
        this.userId = userId;
        this.action = action;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return "Action{" +
                "userId='" + userId + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}

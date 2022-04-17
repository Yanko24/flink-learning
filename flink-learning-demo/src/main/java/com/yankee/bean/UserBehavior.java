package com.yankee.bean;

/**
 * @author Yankee
 * @program IntelliJ IDEA
 * @description JavaBean
 * @since 2021/7/16
 */
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

    /**
     * 用户行为
     */
    private String behavior;

    /**
     * 时间戳
     */
    private Long timestamp;

    public UserBehavior() {}

    public UserBehavior(
            Long userId,
            Long itemId,
            Integer categoryId,
            String behavior,
            Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}

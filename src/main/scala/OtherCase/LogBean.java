package OtherCase;

import java.util.Date;

public class LogBean {
    private Long logID;
    private Long userID;
    private Date time;
    private Long type;
    private Double consumed;

    public Long getLogID() {
        return logID;
    }

    public void setLogID(Long logID) {
        this.logID = logID;
    }

    public Long getUserID() {
        return userID;
    }

    public void setUserID(Long userID) {
        this.userID = userID;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Long getType() {
        return type;
    }

    public void setType(Long type) {
        this.type = type;
    }

    public Double getConsumed() {
        return consumed;
    }

    public void setConsumed(Double consumed) {
        this.consumed = consumed;
    }

    @Override
    public String toString() {
        return "LogBean{" +
                "logID=" + logID +
                ", userID=" + userID +
                ", time=" + time +
                ", type=" + type +
                ", consumed=" + consumed +
                '}';
    }
}

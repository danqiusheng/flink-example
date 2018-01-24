package streaming.model;

import java.text.SimpleDateFormat;

public class MyModel {
    private String code;
    private long createTime;
    private long data;

    public MyModel() {
    }



    public MyModel(String code, long data) {
        this.code = code;
        this.data = data;
    }

    public MyModel(String code, long createTime, long data) {
        this.code = code;
        this.createTime = createTime;
        this.data = data;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public String toString() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return "MyModel{" +
                "code='" + code + '\'' +
                ", createTime=" + format.format(createTime) +
                ", data=" + data +
                '}';
    }
}

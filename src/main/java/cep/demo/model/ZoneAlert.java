package cep.demo.model;

/**
 * Created by Administrator on 2018/5/15.
 *
 * @author danqiusheng
 */
public class ZoneAlert {

    private String zone;

    private String msg;

    private int count;


    public ZoneAlert(String zone, String msg, int count) {
        this.zone = zone;
        this.msg = msg;
        this.count = count;
    }

    public ZoneAlert() {
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ZoneAlert{" +
                "zone='" + zone + '\'' +
                ", msg='" + msg + '\'' +
                ", count=" + count +
                '}';
    }
}

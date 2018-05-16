package cep.demo.model;

/**
 * 当到达一定次数进行报警
 * Created by Administrator on 2018/5/16.
 */
public class ZoneWarn {

    private String zone;
    private String message;

    public ZoneWarn(String zone, String message) {
        this.zone = zone;
        this.message = message;
    }

    public ZoneWarn() {
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "ZoneWarn{" +
                "zone='" + zone + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}

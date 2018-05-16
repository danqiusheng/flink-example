package cep.demo.model;

/**
 * Created by Administrator on 2018/5/16.
 */
public class Zone {


    private String id;
    private Long catchtime;
    private String zone;

    private String type;

    private int count;


    public Zone(String id, Long catchtime, String zone, String type, int count) {
        this.id = id;
        this.catchtime = catchtime;
        this.zone = zone;
        this.type = type;
        this.count = count;
    }

    public Zone(String id, Long catchtime, String zone, int count) {
        this.id = id;
        this.catchtime = catchtime;
        this.zone = zone;
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Zone() {
    }

    public Zone(String id, Long catchtime, String zone) {
        this.id = id;
        this.catchtime = catchtime;
        this.zone = zone;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCatchtime() {
        return catchtime;
    }

    public void setCatchtime(Long catchtime) {
        this.catchtime = catchtime;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Zone{" +
                "id='" + id + '\'' +
                ", catchtime=" + catchtime +
                ", zone='" + zone + '\'' +
                ", type='" + type + '\'' +
                ", count=" + count +
                '}';
    }
}

package cep.demo.model;

import java.util.Date;

/**
 * Created by Administrator on 2018/5/15.
 */
public class People {
    private String id;
    private String name;
    private Long catchtime;

    public People() {
    }

    public People(String id, String name, Long catchtime) {
        this.id = id;
        this.name = name;
        this.catchtime = catchtime;
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

    public Long getCatchtime() {
        return catchtime;
    }

    public void setCatchtime(Long catchtime) {
        this.catchtime = catchtime;
    }

    @Override
    public String toString() {
        return "People{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", catchtime=" + catchtime +
                '}';
    }
}

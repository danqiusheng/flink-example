package streaming.model;

/**
 * 基本模型
 */
public class Model {
    private int objtype;
    private long createtime;

    private String color1;
    private String color2;


    private String license;
    public int getObjtype() {
        return objtype;
    }

    public void setObjtype(int objtype) {
        this.objtype = objtype;
    }

    public long getCreatetime() {
        return createtime;
    }

    public void setCreatetime(long createtime) {
        this.createtime = createtime;
    }

    public Model(int objtype, long createtime) {
        this.objtype = objtype;
        this.createtime = createtime;
    }

    public Model(int objtype, long createtime, String color1, String color2) {
        this.objtype = objtype;
        this.createtime = createtime;
        this.color1 = color1;
        this.color2 = color2;
    }

    public Model() {
    }

    @Override
    public String toString() {
        return "com.moa.Model{" +
                "objtype=" + objtype +
                ", createtime=" + createtime +
                ", color1='" + color1 + '\'' +
                ", color2='" + color2 + '\'' +
                '}';
    }

    public String getColor1() {
        return color1;
    }

    public void setColor1(String color1) {
        this.color1 = color1;
    }

    public String getColor2() {
        return color2;
    }

    public void setColor2(String color2) {
        this.color2 = color2;
    }

    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }
}

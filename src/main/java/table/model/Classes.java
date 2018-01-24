package table.model;

public class Classes {
    private Integer id;

    private String className;

    private long cTimestamp;

    public Classes() {
    }

    public long getcTimestamp() {
        return cTimestamp;
    }

    public void setcTimestamp(long cTimestamp) {
        this.cTimestamp = cTimestamp;
    }

    @Override
    public String toString() {
        return "Classes{" +
                "id='" + id + '\'' +
                ", className='" + className + '\'' +
                '}';
    }

    public Classes(Integer id, String className) {
        this.id = id;
        this.className = className;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }
}

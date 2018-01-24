package table.model;

public class Teacher {

    private String teacherId;
    private String name;

    private Integer classId;

    private long tTimestamp;

    public Teacher() {
    }


    @Override
    public String toString() {
        return "Teacher{" +
                "teacherId='" + teacherId + '\'' +
                ", name='" + name + '\'' +
                ", classId='" + classId + '\'' +
                '}';
    }

    public Teacher(String teacherId, String name, Integer classId) {
        this.teacherId = teacherId;
        this.name = name;
        this.classId = classId;
    }


    public String getTeacherId() {
        return teacherId;
    }

    public void setTeacherId(String teacherId) {
        this.teacherId = teacherId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getClassId() {
        return classId;
    }

    public void setClassId(Integer classId) {
        this.classId = classId;
    }

    public long gettTimestamp() {
        return tTimestamp;
    }

    public void settTimestamp(long tTimestamp) {
        this.tTimestamp = tTimestamp;
    }
}

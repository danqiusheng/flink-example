package streaming.model;

public class Student {
    private String name;
    private String classes;
    private Integer age;

    public Student(){

    }


    public Student(String name, String classes, Integer age){
        this.classes = classes;
        this.name = name;
        this.age = age;
    }

    public String getClasses() {
        return classes;
    }

    public void setClasses(String classes) {
        this.classes = classes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Teacher{" +
                "name='" + name + '\'' +
                ", classes='" + classes + '\'' +
                ", age=" + age +
                '}';
    }
}

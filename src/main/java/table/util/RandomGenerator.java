package table.util;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import streaming.model.StaEntity;
import table.model.Classes;
import table.model.Teacher;

import java.util.ArrayList;
import java.util.List;

public class RandomGenerator {


    // 提取水印
    public static class MyWatermark implements AssignerWithPeriodicWatermarks<StaEntity> {
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }

        public long extractTimestamp(StaEntity element, long previousElementTimestamp) {
            return element.getCreateTime();
        }
    }


    private static Integer[] arr = {1, 2, 3, 4};

    public static List<Teacher> generatorTeacher() {
        List<Teacher> data = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Teacher teacher = null;
            if (i % 2 == 0) {
                teacher = new Teacher("oo" + i, i+"老师", arr[i % 4]  );
            } else {
                teacher = new Teacher("oo" + i, i+"老师", arr[i % 4]  );
            }
            data.add(teacher);
        }
        return data;
    }

    public static List<Classes> generatorClasses() {
        List<Classes> data = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Classes classes = null;
            if (i % 2 == 0) {
                classes = new Classes(arr[i % 4] , "班级" + i );
            } else {
                classes = new Classes(arr[i % 4]  , "班级" + i);
            }
            data.add(classes);
        }
        return data;
    }


    public static List<StaEntity> generatorSource() {
        List<StaEntity> data = new ArrayList<>();
        long sum1 = 0;
        long sum2 = 0;
        for (int i = 0; i < 10; i++) {
            Double d = Math.random() * 100;
            StaEntity staEntity = null;
            if (i % 2 == 0) {
                sum1 += d.longValue();
                staEntity = new StaEntity("0001", (System.currentTimeMillis() + 1000 * i), d.longValue());
            } else {
                sum2 += d.longValue();
                staEntity = new StaEntity("0002", (System.currentTimeMillis() + 1000 * d.intValue()), d.longValue());
            }
            data.add(staEntity);
        }
        System.out.println("sum1=" + sum1 + "; sum2=" + sum2);
        return data;
    }
}

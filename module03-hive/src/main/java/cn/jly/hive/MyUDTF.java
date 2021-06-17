package cn.jly.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 进入一行，输出多行
 * user define table-generating function
 *
 * @author lanyangji
 * @date 2021/5/17 下午 2:11
 * @packageName cn.jly.hive
 * @className MyUDTF
 */
public class MyUDTF extends GenericUDTF {

    /**
     * 存储一行记录
     */
    private final ArrayList<Object> outList = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 定义输出的列名和类型
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> objectInspectors = new ArrayList<>();

        // 添加输入的列名和类型
        fieldNames.add("lineToWords");
        fieldNames.add("wordLength");

        objectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        objectInspectors.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, objectInspectors);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        // 校验参数个数
        if (objects.length != 2) {
            throw new HiveException("参数个数不为2");
        }

        // 获得原始数据
        String line = objects[0].toString();

        // 获得分隔符
        String separator = objects[1].toString();

        String[] words = line.split(separator);

        for (String word : words) {
            // 每次只记录一行，否则会重复
            outList.clear();

            outList.add(word);
            outList.add(word.length());

            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}

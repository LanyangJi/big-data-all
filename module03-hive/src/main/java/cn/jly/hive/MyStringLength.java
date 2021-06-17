package cn.jly.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义函数，继承自GenericUDF
 * 需求：计算字符串的长度
 *
 * @author lanyangji
 * @date 2021/5/17 下午 1:35
 * @packageName cn.jly.hive
 * @className MyStringLength
 */
public class MyStringLength extends GenericUDF {
    /**
     * @param objectInspectors 输入参数类型的鉴别器对象
     * @return 返回值类型的鉴别器对象
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 判断参数的个数
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("输入的参数个数不为1");
        }

        // 判断输入参数类型
        ObjectInspector firstParamInspector = objectInspectors[0];
        if (!firstParamInspector.getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentException("入参的类型不是原始类型");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     *
     * @param deferredObjects 输入的参数
     * @return 返回值
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects[0].get() == null) {
            return null;
        }
        System.out.println("结果为：");
        return deferredObjects[0].get().toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}

package cn.jly.dom4j;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.File;
import java.util.Map;

/**
 * 生成XML文件，使用Jdom2进行节点的拼装，写入xml文件，注意添加outStream.close();防止java占用文件不释放，导致内存溢出。
 * 实现java对象和xml的互转操作。
 * <p>
 * XML相关几个注解说明:
 * <p>
 * 注解@JacksonXmlRootElement 用于类名，是xml最外层的根节点。注解中有localName属性，该属性如果不设置，那么生成的XML最外面就是Clazz.
 * 注解@JacksonXmlCData 注解是为了生成<! [ CDATA [ text ] ]>
 * 注解@JacksonXmlProperty 注解通常可以不需要，若不用，生成xml标签名称就是实体类属性名称。但是如果你想要你的xml节点名字，首字母大写。比如例子中的TotalItems，那么必须加这个注解，并且注解的localName填上你想要的节点名字。最重要的是！实体类原来的属性totalIty必须首字母小写！否则会被识别成两个不同的属性。注解的isAttribute，确认是否为节点的属性，如上面BankType，效果见后续生成的XML文件。
 * 注解@JacksonXmlElementWrapper 一般用于list，list外层的标签。若不用的话，useWrapping =false
 * 注解@JacksonXmlTex 用实体类属性上，说明该属性是否为简单内容，如果是，那么生成xml时，不会生成对应标签名称。也就是只要值没有子标签。
 * 注解@JsonIgnore 忽略该实体类的属性，该注解是用于实体类转json的，但用于转xml一样有效。 业务逻辑生成xml文件
 *
 * @author jilanyang
 * @date 2021/6/25 0025 10:59
 * @packageName cn.jly.dom4j
 * @className XmlUtils
 */
public class XmlUtils {
    public static final XmlMapper XML_MAPPER = new XmlMapper();

    static {
        // 反序列化时，若实体类没有对应的属性，是否抛出JsonMappingException异常，false忽略掉
        XML_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 序列化是否绕根元素，true，则以类名为根元素
        XML_MAPPER.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
        // 忽略空属性
        XML_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        // XML标签名:使用骆驼命名的属性名
        XML_MAPPER.setPropertyNamingStrategy(PropertyNamingStrategies.UPPER_CAMEL_CASE);
        // 设置转换模式
        XML_MAPPER.enable(MapperFeature.USE_STD_BEAN_NAMING);
    }

    /**
     * xml文件转obj
     *
     * @param fileName
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T xmlFileToObj(String fileName, Class<T> clazz) throws Exception {
        return XML_MAPPER.readValue(new File(fileName), clazz);
    }

    /**
     * obj写入xml文件
     *
     * @param fileName
     * @param obj
     * @throws Exception
     */
    public static void objToXmlFile(String fileName, Object obj) throws Exception {
        XML_MAPPER.writeValue(new File(fileName), obj);
    }

    /**
     * xml str转java obj
     *
     * @param xmlStr
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T xmlStrToObj(String xmlStr, Class<T> clazz) throws Exception {
        return XML_MAPPER.readValue(xmlStr, clazz);
    }

    /**
     * obj转xml字符串
     *
     * @param obj
     * @param isFormat 是否以格式化方式输出
     * @return
     * @throws Exception
     */
    public static String objToXmlStr(Object obj, boolean isFormat) throws Exception {
        String res;
        if (isFormat) {
            res = XML_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } else {
            res = XML_MAPPER.writeValueAsString(obj);
        }

        return res;
    }

    /**
     * obj转map, bean需要配置注解@JacksonXmlProperty等
     *
     * @param obj
     * @return
     * @throws Exception
     */
    public static Map<?, ?> objToStrMap(Object obj) throws Exception {
        String xmlStr = XML_MAPPER.writeValueAsString(obj);
        return XML_MAPPER.readValue(xmlStr, Map.class);
    }

}

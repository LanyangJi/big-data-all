package cn.jly.dom4j;

import java.util.Arrays;
import java.util.HashMap;

/**
 * @author jilanyang
 * @date 2021/6/25 0025 10:38
 * @packageName cn.jly.dom4j
 * @className Test
 */
public class Test {
    public static void main(String[] args) throws Exception {
        HashMap<String, Object> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", 2);
        Model.InnerModel.InnerOfInnerModel innerOfInnerModel = Model.InnerModel.InnerOfInnerModel.builder().id(1001L).name("inner2").build();
        map.put("k3", innerOfInnerModel);

        Model.InnerModel innerModel = Model.InnerModel.builder().innerId(11L).innerName("innername").innerOfInnerModels(Arrays.asList(innerOfInnerModel))
                .innerMap(map).build();

        Model model = Model.builder().id(1L).name("model").innerModels(Arrays.asList(innerModel)).hobbies(Arrays.asList("tom", "amy"))
                .map(map).build();

        // obj -> xml file
        XmlUtils.objToXmlFile("demo.xml", model);

        // xml file -> obj
        Model res = XmlUtils.xmlFileToObj("demo.xml", Model.class);
        System.out.println(res);

        // obj -> xml str
        String str = XmlUtils.objToXmlStr(model, true);
        System.out.println(str);

        // xml str -> obj
        Model res2 = XmlUtils.xmlStrToObj(str, Model.class);
        System.out.println(res2);
    }
}

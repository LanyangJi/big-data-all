package cn.jly.dom4j;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * @author jilanyang
 * @date 2021/6/25 0025 11:25
 * @packageName cn.jly.dom4j
 * @className Model
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Model {
    private Long id;
    private String name;
    private List<String> hobbies;
    private Map<String, Object> map;
    private List<InnerModel> innerModels;


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class InnerModel {
        private String innerName;
        private Long innerId;
        private Map<String, Object> innerMap;
        private List<InnerOfInnerModel> innerOfInnerModels;

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        @Builder
        public static class InnerOfInnerModel {
            private String name;
            private Long id;
        }
    }
}

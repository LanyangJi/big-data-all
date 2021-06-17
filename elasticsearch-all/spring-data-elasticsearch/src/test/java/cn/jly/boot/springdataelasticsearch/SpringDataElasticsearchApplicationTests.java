package cn.jly.boot.springdataelasticsearch;

import cn.jly.boot.springdataelasticsearch.beans.Product;
import cn.jly.boot.springdataelasticsearch.dao.ProductDao;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;

import java.util.ArrayList;
import java.util.Optional;

@SpringBootTest
class SpringDataElasticsearchApplicationTests {

    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Autowired
    private ElasticsearchOperations elasticsearchOperations;

    @Autowired
    private ProductDao productDao;

    @Test
    void createIndex() {
        // 系统初始化会自动创建索引
        System.out.println("创建索引");
    }

    @Test
    void deleteIndex() {
        // 删除索引
        boolean isDeleted = elasticsearchRestTemplate.indexOps(Product.class).delete();
        System.out.println("isDeleted = " + isDeleted);
    }

    /**
     * ----------------------- 文档操作 --------------
     * 创建文档
     */
    @Test
    void createDoc() {
        Product product = new Product();
        product.setId(1002L);
        product.setTitle("华为mate40 Pro");
        product.setCategory("mobile phone");
        product.setPrice(8999D);
        product.setImages("/tmp/images/phones/1002.jpg");

        productDao.save(product);
        System.out.println("保存文档 product = " + product);
    }

    /**
     * 根据id查询文档
     */
    @Test
    void findById() {
        Optional<Product> optional = productDao.findById(1001L);
        if (optional.isPresent()) {
            Product product = optional.get();
            System.out.println("product = " + product);
        }
    }

    /**
     * 查询所有文档
     */
    @Test
    void findAll() {
        Iterable<Product> all = productDao.findAll();
        all.forEach(System.out::println);
    }

    /**
     * 删除文档
     */
    @Test
    void deleteDocById() {
        Product product = new Product();
        product.setId(1002L);
        productDao.delete(product);
    }

    /**
     * 批量新增文档
     */
    @Test
    void batchInsert() {
        ArrayList<Product> productList = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Product product = new Product();
            product.setId(2000L + i);
            product.setTitle("小米手机" + i);
            product.setCategory("mobile phone");
            product.setPrice(1999d + i);
            product.setImages("/tmp/images/phones/" + product.getId() + ".jpg");

            productList.add(product);
        }

        productDao.saveAll(productList);
    }

    /**
     * 分页查询文档
     */
    @Test
    void findByPage() {
        // 排序
        Sort sort = Sort.by(Sort.Direction.DESC, "id");
        // 分页
        PageRequest request = PageRequest.of(0, 5, sort);

        Page<Product> all = productDao.findAll(request);
        all.forEach(System.out::println);
    }

    /**
     * ------------------------ 文档搜索 -----------------------
     */
    @Test
    void termQuery() {
        IndexCoordinates index = IndexCoordinates.of("shopping");
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("title", "小米");
        // 过期了
        // productDao.search(query);

        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryBuilder).build();
        SearchHits<Product> hits = elasticsearchOperations.search(searchQuery, Product.class, index);
        for (SearchHit<Product> hit : hits) {
            System.out.println("hit = " + hit);
        }
    }

    /**
     * 分页查询
     */
    @Test
    void searchPage() {
        // 排序
        Sort sort = Sort.sort(Product.class).by(Product::getId).descending();
        // 分页
        PageRequest pageRequest = PageRequest.of(0, 5, sort);

        // 索引
        IndexCoordinates index = IndexCoordinates.of("shopping");
        // 查询体
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("title", "小米");
        NativeSearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryBuilder)
                .withPageable(pageRequest)
                .build();

        SearchHits<Product> hits = elasticsearchOperations.search(searchQuery, Product.class, index);
        for (SearchHit<Product> hit : hits) {
            System.out.println("hit = " + hit);
        }
    }
}

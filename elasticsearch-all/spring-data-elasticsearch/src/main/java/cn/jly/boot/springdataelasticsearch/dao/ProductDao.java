package cn.jly.boot.springdataelasticsearch.dao;

import cn.jly.boot.springdataelasticsearch.beans.Product;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * @author lanyangji
 * @date 2021/4/14 上午 10:51
 * @packageName cn.jly.boot.springdataelasticsearch.dao
 * @className ProductDao
 */
@Repository
public interface ProductDao extends ElasticsearchRepository<Product, Long> {
}

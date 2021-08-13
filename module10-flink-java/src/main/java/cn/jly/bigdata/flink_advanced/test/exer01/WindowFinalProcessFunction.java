package cn.jly.bigdata.flink_advanced.test.exer01;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.utils.CollectionUtils;

import javax.xml.bind.Element;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author jilanyang
 * @package cn.jly.bigdata.flink_advanced.test.exer01
 * @class WindowFinalProcessFunction
 * @date 2021/8/7 14:50
 */
public class WindowFinalProcessFunction extends ProcessWindowFunction<CategoryPojo, Object, String, TimeWindow> {
    private FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    // 通过测输出流输出总金额
    private OutputTag<Tuple2<String, Double>> outputTag;

    public WindowFinalProcessFunction(OutputTag<Tuple2<String, Double>> outputTag) {
        this.outputTag = outputTag;
    }

    /**
     * 当前的key就是当前1s的时间
     *
     * @param dateTime 当前统计的1s的时间,也是key
     * @param context
     * @param elements 按照时间分组的每1s的品类以及销售额的统计对象集合,简而言之就是截止到当前秒的各个分类的销售数据
     * @param out
     * @throws Exception
     */
    @Override
    public void process(String dateTime, Context context, Iterable<CategoryPojo> elements, Collector<Object> out) throws Exception {
        // 需求1：实时计算出当天零点截止到当前时间的销售总额 11月11日 00:00:00 ~ 23:59:59
        double sum = 0d;

        // 需求2：各个分类的销售总额top3
        // 注意：这里只需要top3,因此只需要排前3名就可以了，其他的不用管。
        // 所以这里使用小顶堆完成top3排序
        // 70
        // 80
        // 90
        // 如果进来一个比堆顶还要小的,那肯定比堆下面的还要小,直接忽略;
        // 如果进来一个堆顶大,把堆顶的先干掉,比如85,则直接把堆顶元素干掉,并把85加进去并继续按照小顶堆的规则排序：小的在上面,大的在下面
        // 创建一个小顶堆; 其实个人建议使用TreeSet更好用,创建一个treeSet，并传入比较器，直接addAll(elements)，然后取前三个即可，但是这种方法比较浪费空间
        PriorityQueue<CategoryPojo> queue = new PriorityQueue<>(
                3, // 初始容量3,这样就构成了一个容量为3的小顶堆
                // 正常的排序,就是小的在前,大的在后
                Comparator.comparingDouble(CategoryPojo::getTotalPrice)
        );

        for (CategoryPojo element : elements) {
            // 需求1：实时计算出当天零点截止到当前时间的销售总额 11月11日 00:00:00 ~ 23:59:59
            sum += element.getTotalPrice();

            // 需求2：各个分类的销售总额top3
            // 如果队列元素小于3,直接入队
            // !!!!!!!!!!! 注意.这段代码可以进行优化，大幅度减少代码量，但是会导致可读性变差，性能缺没有明显提高，没有必要
            if (queue.size() < 3) {
                queue.add(element); // 或者使用方法offer,也表示入队的意思      --------- !!!! hutool提供了这个过程的封装，参考类 BoundedPriorityQueue
            } else {
                // 否则,直接拿新的元素和堆顶元素比较：如果比堆顶元素小,那么肯定比堆中其他元素小,直接忽略新元素;
                // 如果新元素比堆顶元素大,直接用新元素替换掉堆顶元素,PriorityQueue会自定把新进来的元素按照规则重新排序
                if (element.getTotalPrice() > queue.peek().getTotalPrice()) {
                    queue.poll();
                    queue.add(element);
                }
            }
        }
        // 代码走到这里,queue里面存放的就是销售额top3,需要改为逆序输出
        List<String> top3List = queue.stream()
                .sorted((o1, o2) -> -Double.compare(o1.getTotalPrice(), o2.getTotalPrice()))
                .map(c -> "分类：" + c.getCategory() + "$销售额：" + c.getTotalPrice() + "$时间：" + c.getDateTime())
                .collect(Collectors.toList());
        out.collect(top3List);

        // 需求1的总金额用测输出流输出
        context.output(outputTag, Tuple2.of(dateTime, BigDecimal.valueOf(sum).setScale(2, RoundingMode.HALF_UP).doubleValue()));
    }
}

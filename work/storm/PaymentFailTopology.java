import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/** 支付失败实时统计拓扑。 */
public final class PaymentFailTopology {
  private PaymentFailTopology() {}

  /** 过滤支付失败事件并发射店铺 ID。 */
  public static class PaymentFailBolt extends BaseRichBolt {
    private OutputCollector collector;

    /**
     * 初始化 Bolt。
     *
     * @param config Storm 配置。
     * @param context 拓扑上下文。
     * @param collector Tuple 发射器。
     */
    @Override
    public void prepare(
        Map<String, Object> config, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    /**
     * 处理 Kafka 输入事件。
     *
     * @param tuple KafkaSpout 发射的 Tuple，value 格式为 event_time|shop_id|order_id|status|amount。
     */
    @Override
    public void execute(Tuple tuple) {
      String value = tuple.getStringByField("value");
      String[] parts = value.split("\\|");
      if (parts.length >= 5 && "PAY_FAILED".equals(parts[3])) {
        collector.emit(tuple, new Values(parts[1]));
      }
      collector.ack(tuple);
    }

    /**
     * 声明输出字段。
     *
     * @param declarer 输出字段声明器。
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("shop_id"));
    }
  }

  /** 按店铺统计支付失败次数。 */
  public static class ShopFailCountBolt extends BaseRichBolt {
    private final Map<String, Long> counters = new HashMap<>();
    private OutputCollector collector;

    /**
     * 初始化 Bolt。
     *
     * @param config Storm 配置。
     * @param context 拓扑上下文。
     * @param collector Tuple 发射器。
     */
    @Override
    public void prepare(
        Map<String, Object> config, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    /**
     * 累加店铺支付失败次数。
     *
     * @param tuple 上游 Bolt 发射的店铺 ID。
     */
    @Override
    public void execute(Tuple tuple) {
      String shopId = tuple.getStringByField("shop_id");
      long total = counters.getOrDefault(shopId, 0L) + 1L;
      counters.put(shopId, total);
      System.out.printf("shop_id=%s payment_fail_count=%d%n", shopId, total);
      collector.ack(tuple);
    }

    /**
     * 本 Bolt 只打印结果，不继续发射字段。
     *
     * @param declarer 输出字段声明器。
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
  }

  /**
   * 构建并提交拓扑。
   *
   * @param args args[0] 为 Kafka Broker 地址，args[1] 为主题名。
   * @throws Exception Storm 提交失败时抛出。
   */
  public static void main(String[] args) throws Exception {
    String bootstrapServers = args.length > 0 ? args[0] : "centos01:9092";
    String topic = args.length > 1 ? args[1] : "trade_events";

    KafkaSpoutConfig<String, String> kafkaConfig =
        KafkaSpoutConfig.builder(bootstrapServers, topic)
            .setGroupId("storm-payment-risk")
            .build();

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("trade-events", new KafkaSpout<>(kafkaConfig), 2);
    builder.setBolt("payment-fail-filter", new PaymentFailBolt(), 4)
        .shuffleGrouping("trade-events");
    builder.setBolt("shop-fail-counter", new ShopFailCountBolt(), 4)
        .fieldsGrouping("payment-fail-filter", new Fields("shop_id"));

    Config config = new Config();
    config.setNumWorkers(2);
    StormSubmitter.submitTopology("payment-fail-risk", config, builder.createTopology());
  }
}

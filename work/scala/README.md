# Scala 使用场景：会员运营标签规则

## 非常具体的场景

在“会员运营系统”中，`user-event-service` 每天产出用户行为事件，例如浏览品类、下单金额、所在城市。运营团队需要根据规则给用户打标签：

- 单笔消费超过 1000：`high_value`
- 城市为 Shanghai 或 Beijing：`core_city`
- 品类为 baby 或 beauty：`campaign_candidate`

Scala 适合表达这类规则：case class 定义事件结构，模式匹配表达分支，高阶函数处理集合。后续可以把同样逻辑放入 Spark Scala 作业中。

## 代码说明

`UserTagExample.scala` 是可直接运行的 Scala 示例：

```bash
scalac work/scala/UserTagExample.scala
scala UserTagExample
```

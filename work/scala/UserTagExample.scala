/** 会员运营标签规则示例。 */
object UserTagExample {
  /** 用户行为事件。
    *
    * @param userId 用户 ID。
    * @param city 用户所在城市。
    * @param category 浏览或购买品类。
    * @param amount 订单金额。
    */
  case class UserEvent(userId: String, city: String, category: String, amount: Double)

  /** 用户标签结果。
    *
    * @param userId 用户 ID。
    * @param tags 命中的标签列表。
    */
  case class UserTags(userId: String, tags: List[String])

  /** 根据单条行为事件生成标签。
    *
    * @param event 用户行为事件。
    * @return 用户标签结果。
    */
  def tag(event: UserEvent): UserTags = {
    val amountTag = event.amount match {
      case value if value >= 1000.0 => Some("high_value")
      case _ => None
    }

    val cityTag = event.city match {
      case "Shanghai" | "Beijing" => Some("core_city")
      case _ => None
    }

    val categoryTag = event.category match {
      case "baby" | "beauty" => Some("campaign_candidate")
      case _ => None
    }

    UserTags(event.userId, List(amountTag, cityTag, categoryTag).flatten)
  }

  /** 程序入口。
    *
    * @param args 命令行参数，本示例不使用。
    */
  def main(args: Array[String]): Unit = {
    val events = List(
      UserEvent("U1001", "Shanghai", "beauty", 1299.0),
      UserEvent("U1002", "Chengdu", "book", 89.0),
      UserEvent("U1003", "Beijing", "baby", 568.0)
    )

    events.map(tag).foreach(println)
  }
}

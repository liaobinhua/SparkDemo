package test1

/**
 * @author binhualiao
 *         Created Time: 2020/10/14 15:45 
 **/
class SortKey(var clickCount: Long, var orderCount: Long, var payCount: Long) extends Ordered[SortKey] with Serializable {
  override def compare(that: SortKey): Int = {
    if (this.clickCount - that.clickCount != 0) {
      if (this.clickCount - that.clickCount < 0) {
        -1
      } else {
        1
      }
    } else if (this.orderCount - that.orderCount != 0) {
      if (this.orderCount - that.orderCount < 0) {
        -1
      } else {
        1
      }
    } else {
      if (this.payCount - that.payCount < 0) {
        -1
      } else {
        1
      }
    }
  }
}

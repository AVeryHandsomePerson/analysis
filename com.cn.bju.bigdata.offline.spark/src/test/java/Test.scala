import Test.WeekDay.showAll

/**
 * @author ljh
 * @date 2021/3/30 16:17
 * @version 1.0
 */
object Test extends Enumeration {

  object WeekDay extends Enumeration{
    type WeekDay = Value  //声明枚举对外暴露的变量类型
    val Mon = Value("1")
    val Tue = Value("2")
    val Wed = Value("3")
    val Thu = Value("4")
    val Fri = Value("5")
    val Sat = Value("6")
    val Sun = Value("7")
    def checkExists(day:String) = this.values.exists(_.toString==day) //检测是否存在此枚举值
    def isWorkingDay(day:WeekDay) = ! ( day==Sat || day == Sun) //判断是否是工作日
    def showAll = this.values.foreach(println) // 打印所有的枚举值
  }

  def main(args: Array[String]): Unit = {


    val a = (1.02448885E7).formatted("%.5f")
    println(a)
  }
}

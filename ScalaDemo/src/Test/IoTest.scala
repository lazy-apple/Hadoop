package Test

import scala.io.Source

/**
  * @author LaZY(李志一) 
  * @create 2019-02-20 21:54 
  */
object IoTest {
  def main(args: Array[String]): Unit = {
//    val s = Source.fromFile("E:\\IDEA_workspace\\Hadoop\\ScalaDemo\\src\\file\\1.txt");
//    val lines = s.getLines();
//    for (line <- lines) {
//      println(line);
//    }

////正则与爬虫
//    import scala.io.Source
//    import java.util.regex.Pattern
//    val str = Source.fromFile("d:/scala/1.html").mkString
//    val p = Pattern.compile("<a\\s*href=\"([\u0000-\uffff&&[^\u005c\u0022]]*)\"")
//    val m = p.matcher(str)
//    while ( {
//      m.find
//    }) {
//      val s = m.group(1)
//      System.out.println(s)
//    }

//    //模式匹配
//    val x = '9';
//    x match {
//      case '+' => print("++++")
//      case '-' => print("----")
//      case _ if Character.isDigit(x)=> print("is number")
//      case _ => print("……")
//    }


//    //类型匹配
//        val x:Any = "8";
//        x match {
//          case b: Int => print("Int")
//          case a: String => print("String")
//          case _ => print("……")
//        }

    //匹配数组
        val arr = Array(0,1,2,3);
        arr match {
          case Array(0) => print("有0")
          case Array(a,b) => print("两个元素")
          case Array(0,_*) => print("以0开始")
          case _ => print("……")
        }
  }
}

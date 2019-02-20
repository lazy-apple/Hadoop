package Test

import scala.io.Source

/**
  * @author LaZY(李志一) 
  * @create 2019-02-20 21:54 
  */
object IoTest {
  def main(args: Array[String]): Unit = {
    val s = Source.fromFile("E:\\IDEA_workspace\\Hadoop\\ScalaDemo\\src\\file\\1.txt");
    val lines = s.getLines();
    for (line <- lines) {
      println(line);
    }
//正则与爬虫
    import scala.io.Source
    import java.util.regex.Pattern
    val str = Source.fromFile("d:/scala/1.html").mkString
    val p = Pattern.compile("<a\\s*href=\"([\u0000-\uffff&&[^\u005c\u0022]]*)\"")
    val m = p.matcher(str)
    while ( {
      m.find
    }) {
      val s = m.group(1)
      System.out.println(s)
    }
  }
}

import com.usecase.utils.LoadConf
import jdk.internal.net.http.frame.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.util

class TestApp extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()




  test("multiply zero by any other number is zero") {


  }

  test("multiply one by any other number is second one") {

  }

}

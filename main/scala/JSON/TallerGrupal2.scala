import com.cibo.evilplot.plot.{Bar, BarChart}
import play.api.libs.json._
import com.github.tototoshi.csv._
import java.io.File
import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.renderers.BarRenderer
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

object TallerGrupal2 extends App {

  val labeledByColor = new BarRenderer {

    val positive = RGB(241, 121, 6)
    val negative = RGB(226, 56, 140)

    def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
      val rect = Rect(extent)
      val value = category.values.head
      val color = if (value >= 0) positive else negative
      Align.center(rect filled color, Text(s"$value", size = 10)).group
    }
  }

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  def replacePattern(original: String): String = {
    var txtOr = original

    val pattern3: Regex = "(:\\s'\"(.*?)',)".r
    for (m <- pattern3.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }

    val pattern2: Regex = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
    for (m <- pattern2.findAllIn(txtOr)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }

    val pattern1: Regex = "(\\s\"(.*?)\",)".r
    for (m <- pattern1.findAllIn(txtOr)) {
      val textOriginal = m
      val replacementText = m.replace("'", "-u0027")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }


  val crew = data
    .map(row => row("crew"))
    .map(replacePattern)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Try(Json.parse(text)))
    .filter(_.isSuccess)
    //.size

  //Departamentos Megamind
  val megamindDeps = crew(196).map(jsonData => jsonData \\ "department").map(_.toList).get.distinct
  println("Departamentos de Megamind")
  println(megamindDeps)

  //Trabajos Megamind
  val megamindJobs = crew(196).map(jsonData => jsonData \\ "job").map(_.toList).get.distinct
  println("Jobs de Megamind")
  println(megamindJobs)

  //Departamentos totales
  val departamentos = crew.map(_.get).flatMap(jsonData => jsonData \\ "department").distinct
  val departamentos2 = crew.map(_.get).flatMap(jsonData => jsonData \\ "department")
  println("Departamentos Totales")
  println(departamentos)

  //Jobs Totales
  val jobs = crew.map(_.get).flatMap(jsonData => jsonData \\ "job").distinct
  println("Jobs Totales")
  println(jobs)

  val departamentosGroups = departamentos2.groupBy{
    case x => x
  }.map{
    case depar => (depar._1, depar._2.size)
  }.toList.sortBy(_._2).reverse

  //println(departamentosGroups)
  val barDeps = departamentosGroups.take(5).map(_._2.toDouble).toSeq
  val labels = departamentosGroups.take(5).map(_._1.toString()).toSeq

  BarChart
    .custom(barDeps.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
    .title("Top 5 Departamentos")
    .standard(xLabels = labels)
    .render()
    .write(new File("C:\\Users\\Daniel\\Demos/Top5Deps.png"))


  val gender = crew.map(_.get).flatMap(jsonData => jsonData \\ "gender")
  val gendergroups = gender.groupBy {
    case x => x
  }.map {
    case depar => (depar._1.toString, depar._2.size.toDouble)
  }.toList.sortBy(_._2).reverse.toSeq

  println("Genders: \n" + gendergroups)

  PieChart(gendergroups)
    .title("Crew Genders")
    .rightLegend()
    .render()
    .write(new File("C:\\Users\\Daniel\\Demos/PieChart.png"))

}

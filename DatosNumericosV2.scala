import java.io.File
import com.github.tototoshi.csv.CSVReader
import com.cibo.evilplot.plot.{BarChart, PieChart}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultElements, DefaultTheme}
import com.cibo.evilplot.plot.renderers.LegendRenderer

import scala.math._
import scala.math.BigDecimal.RoundingMode
object DatosNumericosV2 extends App {

  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  // ------------------------------------------------------------------------------------------------
  implicit val theme = DefaultTheme.copy(
    elements = DefaultElements.copy(categoricalXAxisLabelOrientation = 45)
  )

  // ----------------------------------Gráficos------------------------------------------------
  //TOP VOTE_AVERAGE
  val topVoted = data
    .map(x => (x("original_title"), x("vote_average").toDouble))
    .filter(_._1.nonEmpty)
    .sortBy(_._2)
    .reverse
    .take(10)

  BarChart(topVoted.map(_._2))
    .title("Top 10 películas mejor votadas")
    .xAxis(topVoted.map(_._1))
    .yAxis()
    .ybounds(5, 11) //Calificacion de 5 a 10
    .frame()
    .yLabel("Calificación")
    .render()
    .write(new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/TopVoted.png"))

  // ------------------------------------------------------------------------------------------------
  //TOP POPULARITY
  val popularity = data
    .map(x => (x("original_title"), x("popularity").toDouble))
    .sortBy(_._2)
    .reverse
    .take(7)

  BarChart(popularity.map(_._2))
    .title("Top 7 movies por mayor popularity")
    .xAxis(popularity.map(_._1))
    .yAxis()
    .frame()
    .yLabel("Popolaridad")
    .render()
    .write(new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/TopMovieByPopoularity.png"))

  // ------------------------------------------------------------------------------------------------
  //TOP RUNTIME
  val runtime = data
    .map(x => (x("original_title"), x("runtime")))
    .filter(_._2.contains("."))
    .sortBy(_._2.toDouble)
    .reverse
    .take(7)

  BarChart(runtime.map(_._2.toDouble))
    .title("Top 7 movies por mayor runtime")
    .xAxis(runtime.map(_._1))
    .yAxis()
    .frame()
    .yLabel("Valor")
    .render()
    .write(new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/TopMovieByRuntime.png"))

  // ------------------------------------------------------------------------------------------------
  //TOP VOTECOUNT
  val vote_count = data
    .map(x => (x("title"), x("vote_count")))
    .filter(_._2.nonEmpty)
    .sortBy(_._2.toInt)
    .reverse
    .take(7)

  BarChart(vote_count.map(_._2.toDouble))
    .title("Top 7 movies por mayor vote_count")
    .xAxis(vote_count.map(_._1))
    .yAxis()
    .frame()
    .yLabel("Valor")
    .render()
    .write(new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/TopMovieByVoteCount.png"))



}


import com.github.tototoshi.csv._

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.plot.renderers.BarRenderer
import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultFonts, DefaultTheme}

import scala.math.BigDecimal.RoundingMode


object DatosNumericos extends App{

  //LECTURA CSV
  val reader = CSVReader.open(new File("C:\\Users\\Daniel\\Downloads/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()
  reader.close()

  //FUNCIONES
  def average(valores: List[Long]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }

  def averageDouble(valores: List[Double]): Double = {
    val t = valores.foldLeft((0.0, 0))((acc, currVal) => (acc._1 + currVal, acc._2 + 1))
    t._1 / t._2
  }

  //-----------------------------------------------Columnas Numéricas-------------------------------------------------
  //Presupuestos
    //4803 valores
    //1037 ceros
  val budget = data.flatMap(row => row.get("budget"))
    .map(_.toLong)
    //.count(_ == 0)

  //Popularity
    //4803 valores
    //1 ceros
  val popularity = data.flatMap(row => row.get("popularity"))
      .map(_.toDouble)
    //.count(_ == 0.0)

  //Release_date
    //4802 valores
    //1 vacios
  val release_date = data.flatMap(elem => elem.get("release_date"))
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val releaseDateList = release_date
    .filter(_.nonEmpty)
    .map(text => LocalDate.parse(text, dateFormatter))

  //Revenue
    //1427 ceros
  val revenue = data.flatMap(row => row.get("revenue"))
     .map(_.toLong)
    //.count(_ == 0)

  //Runtime
    //2 vacios
    //1427 ceros
  val runtime = data.flatMap(row => row.get("runtime"))
    .filter(_.nonEmpty)
    .map(_.toDouble)

  //Vote_average
    //63 ceros
  val voteAverage = data.flatMap(row => row.get("vote_average"))
      .map(_.toDouble)
    //.count(_ == 0.0)

  //Vote_count
    //63 ceros
  val voteCount = data.flatMap(row => row.get("vote_count"))
      .map(_.toDouble)
    //.count(_ == 0.0)

  //------------------------PROMEDIOS-------------------------------
  val budgetAvg = average(budget)
  val revenueAvg = average(revenue)
  val popularityAvg = averageDouble(popularity)
  val runtimeAvg = averageDouble(runtime)
  val voteAverageAvg = averageDouble(voteAverage)
  val voteCountAvg = averageDouble(voteCount)

  //-------------------------------------------GRAFICAS-------------------------------------------------------------
  //-----------REVENUE-----------
  val cerosRevenue = data.flatMap(row => row.get("revenue"))
    .map(_.toLong)
    .count(_ == 0)
    .toDouble

  val underAvgRevenue = revenue.count(_ < revenueAvg)
  val aboveAvgRevenue = revenue.count(_ > revenueAvg)

  PieChart(Seq("Ceros" -> cerosRevenue, "Bajo el Promedio" -> underAvgRevenue, "Encima del Promedio" -> aboveAvgRevenue))
    .rightLegend()
    .title("Revenue")
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/Revenue.jpg"))

  //-----------BUDGET-----------
  val labeledByColor = new BarRenderer { //Renderizado Personalizado
    def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
      val rect = Rect(extent)
      val value = category.values.head
      val color =  RGB(0, 204, 204)
      Align.center(rect filled color, Text(s"$value%", size = 16)
      ).group
    }
  }
  val underAvgPres = BigDecimal((budget.count(_ < budgetAvg) / budget.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val aboveAvgPres = BigDecimal((budget.count(_ > budgetAvg) / budget.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val cerosPres = BigDecimal((budget.count(_ ==0) / budget.length.toDouble) * 100  )
    .setScale(2, RoundingMode.HALF_UP).toDouble


  val percentChange = Seq[Double](underAvgPres, aboveAvgPres, cerosPres)
  val labels = Seq("Menor", "Mayor", "Cero")
  BarChart
    .custom(percentChange.map(Bar.apply), spacing = Some(20),
      barRenderer = Some(labeledByColor)
    )
    .standard(xLabels = labels)
    .title("Porcentaje Presupuestos comparados al Promedio")
    .render()
    .write(new File
    ("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/Budgets.jpg"))

  //-----------RELEASE_DATE-----------
  val yearReleaseList = releaseDateList
    .map(_.getYear)
    .map(_.toDouble)

  Histogram(yearReleaseList)
    .title("Histograma Años de Lanzamiento")
    .frame()
    .xAxis()
    .yAxis()
    .xbounds(1916.0, 2018.0)
    .render()
    .write(new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/HistoYear.jpg"))

  //BOXPLOT
  /*
  val seqBudget = Seq[Double](presupuestos.filter(_ > 0).min.toDouble, budgetAvg, presupuestos.max.toDouble)
  val seqRevenue = Seq[Double](revenue.filter(_ > 0).min.toDouble, revenueAvg, revenue.max.toDouble)

  val seqPopularity = Seq[Double](popularity.filter(_ > 0).min, popularityAvg, popularity.max)
  val seqVoteCount= Seq[Double](voteCount.filter(_ > 0).min, voteCountAvg, voteCount.max)

  val seqRuntime = Seq[Double](runtime.filter(_ > 0).min, runtimeAvg, runtime.max)
  val seqVoteAverage = Seq[Double](voteAverage.filter(_ > 0).min, voteAverageAvg, voteAverage.max)

  val underAvgPres = BigDecimal((budget.count(_ < budgetAvg) / budget.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val aboveAvgPres = BigDecimal((budget.count(_ > budgetAvg) / budget.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val cerosPres = BigDecimal((budget.count(_ ==0) / budget.length.toDouble) * 100  )
    .setScale(2, RoundingMode.HALF_UP).toDouble

  val underAvgRev = BigDecimal((revenue.count(_ < revenueAvg) / revenue.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val aboveAvgRev= BigDecimal((revenue.count(_ > revenueAvg) / revenue.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val cerosRev = BigDecimal((revenue.count(_ == 0) / revenue.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble

  val underAvgVc = BigDecimal((voteCount.count(_ < voteCountAvg) / voteCount.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val aboveAvgVc = BigDecimal((voteCount.count(_ > voteCountAvg) / voteCount.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val cerosVc = BigDecimal((voteCount.count(_ == 0) / voteCount.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble

  val underAvgPop = BigDecimal((popularity.count(_ < popularityAvg) / popularity.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val aboveAvgPop = BigDecimal((popularity.count(_ > popularityAvg) / popularity.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val cerosPop = BigDecimal((popularity.count(_ == 0) / popularity.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble

  val underAvgRun = BigDecimal((runtime.count(_ < runtimeAvg) / runtime.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val aboveAvgRun = BigDecimal((runtime.count(_ > runtimeAvg) / runtime.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val cerosRun = BigDecimal((runtime.count(_ == 0) / runtime.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble

  val underAvgVA = BigDecimal((voteAverage.count(_ < voteAverageAvg) / voteAverage.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val aboveAvgVA = BigDecimal((voteAverage.count(_ > voteAverageAvg) / voteAverage.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble
  val cerosVA = BigDecimal((voteAverage.count(_ == 0) / voteAverage.length.toDouble) * 100)
    .setScale(2, RoundingMode.HALF_UP).toDouble

  val seqBudget = Seq[Double](cerosPres, underAvgPres, aboveAvgPres)
  val seqRevenue = Seq[Double](cerosRev, underAvgRev, aboveAvgRev)
  val seqVc = Seq[Double](cerosVc, underAvgVc, aboveAvgVc)
  val seqPopular = Seq[Double](cerosPop, underAvgPop, aboveAvgPop)
  val seqRuntime = Seq[Double](cerosRun, underAvgRun, aboveAvgRun)
  val seqVa= Seq[Double](cerosVA, underAvgVA, aboveAvgVA)

  val prueba = Seq[Seq[Double]](seqBudget, seqRevenue, seqVc, seqPopular, seqRuntime, seqVa)
  BoxPlot(prueba)
    .title("BoxPlot")
    .standard(xLabels = Seq[String]("Budget", "Revenue", "Vote_Count", "Popularity", "Runtime", "Vote_Average"))
    .render()
    .write(new File("C:\\Users\\Daniel\\Utpl\\3ER CICLO\\New/pruebaBoxPlot.png"))
   */

}

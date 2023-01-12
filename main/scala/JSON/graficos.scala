package JSON
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.github.tototoshi.csv._
import java.io.File

object graficos extends App {

  val reader = CSVReader.open(new File("C:\\Users\\SALA A\\Desktop\\ProyectoIntegrador-PrFuncional\\ejemplo\\src\\main/movie_dataset.csv"))
  val data: List[Map[String, String]] = reader.allWithHeaders()

  reader.close()

  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val realeaseDateList = data
    .map(row => row("release_date"))
    .filter(!_.equals(""))
    .map(text => LocalDate.parse(text, dateFormatter))


  val yearReleaseList = realeaseDateList
    .map(_.getYear)
    .map(_.toDouble)

  Histogram(yearReleaseList)
    .title("Años de lanzamiento")
    .xAxis()
    .yAxis()
    .xbounds(1916.0, 2018.0)
    .render()
    .write(new File("C:\\Users\\SALA A\\Desktop\\ProyectoIntegrador-PrFuncional\\ejemplo\\src\\main/histo.png"))
}

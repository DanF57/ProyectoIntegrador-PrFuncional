package JSON

import com.github.tototoshi.csv.CSVReader
import play.api.libs.json._

import java.io.File
object JsonMain extends  App {




    val reader = CSVReader.open(new File("/Users/PERSONAL/Desktop/movie_dataset.csv"))
    val data: List[Map[String, String]] = {
      reader.allWithHeaders()
    }
    reader.close()

    println("\nJSON")

    val parse = (columna : String) => {
      data.flatMap(x => x.get(columna)).map(Json.parse)
    }

    // production_companies JSON
    val pc11 = parse("production_companies")

    val production_companies = {
      val name = pc11.flatMap(_ \\ "name")
      val id = pc11.flatMap(_ \\ "id")
      name.zip(id)
    }

    val production_companiesAgrupar = production_companies.groupBy{
      case company => company
    }.map{
      case company => (company._1, company._2.size)
    }.toList.sortBy(_._2)

    val maxProduCom = production_companiesAgrupar.maxBy(_._2)._1._1


    val minProduCom = production_companiesAgrupar.minBy(_._2)._1._1


    //println(production_companiesGroupBy)

    println("\nLa production_company con mayor participación es: \t" + maxProduCom
     )

    println("\nLa production_company con menor participaciones es: \t" + minProduCom
      )

    println(production_companiesAgrupar)





  }


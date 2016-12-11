package controllers

/**
 * @author Leniel Macaferi
 * @company Leniel Macaferi's Consulting
 *
 * 12-2016
 */

import javax.inject._
import play.api._
import play.api.mvc._
import java.nio.file.{ Files, Path, Paths }
import play.api.libs.iteratee._
import java.io.File

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject() extends Controller {

  /**
   * Gets the CSV files' names for the directory passed as parameter.
   */
  def getFileNames(dir: File): List[String] =
    {
      val files = dir.listFiles

      val paths = files.map(file =>
        // Adds a space between each Capitalized word
        //http://stackoverflow.com/q/41082820/114029
        file.getName().flatMap(c => if (c.isUpper) Seq(' ', c) else Seq(c))).toList

      return paths.sorted // Alphabetically sorted
    }

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action {

    // CSV directory
    var dir = new File("../spark-backend/csv")

    var csvs = getFileNames(dir)

    //Ok.sendFile(new java.io.File("../spark-backend/csv/UsersGroupedByRegistrationMonthYear.csv"))
    //
    //var jSon = Json.obj("filePaths"->Json.arr(result))
    //     
    //Ok(jSon)

    Ok(views.html.index(csvs))
  }

  /**
   * Sends the CSV file to the user computer.
   */
  def getCsv(fileName: String) = Action {
    Ok.sendFile(new File("../spark-backend/csv/" + fileName.replace(" ", "")))
  }
}

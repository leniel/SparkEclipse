package controllers

import javax.inject._
import play.api._
import play.api.mvc._
//import java.io.File
import java.nio.file.{ Files, Path, Paths }
import play.api.libs.json.JsValue
import play.api.mvc._
import play.api.libs.iteratee._
import play.api.Play.current
import java.io.File
import play.api.libs.json.Json
import play.api.libs.json._
import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.Logger;
//import scala.util.matching.Regex

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject() extends Controller {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */

  def getFileNames(dir: File): List[String] =
    {
      val files = dir.listFiles

      val paths = files.map(file =>
        file.getName().flatMap(c => if (c.isUpper) Seq(' ', c) else Seq(c))).toList

      return paths
    }

  def index = Action {

    //Ok.sendFile(new java.io.File("../spark-backend/csv/UsersGroupedByRegistrationMonthYear.csv"))

    var dir = new File("../spark-backend/csv")

    var csvs = getFileNames(dir)

    //   var jSon = Json.obj("filePaths"->Json.arr(result))
    //     
    //   Ok(jSon)

    Ok(views.html.index(csvs))
  }

  def getCsv(fileName: String) = Action {
    Ok.sendFile(new File("../spark-backend/csv/" + fileName.replace(" ", "")))
  }
}

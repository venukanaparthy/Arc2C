package com.esri

import java.awt.Color
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO
import java.lang.Exception

import com.esri.arcgis.server.json.JSONObject
import com.esri.arcgis.system.{IObjectConstruct, IPropertySet, IRESTRequestHandler}

import scala.collection.JavaConversions._

import java.net.InetSocketAddress
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row


class Arc2CassandraSOI extends AbstractSOI with IObjectConstruct {

  val colorMapper = new ColorMapper()  
  var imagePNG: String = _
  var maxWidth: Double = _
  
  var port:Int = 9042
  var host:String = _
  var keyspace:String = _
  var table:String = _
  var cClient:CassandraClient = null
  var results:ResultSet = _
  var session:Session = null

  override def construct(propertySet: IPropertySet): Unit = {
    log.addMessage(3, 200, "Arc2CassandraSOI::construct")

    colorMapper.construct()
    
    host = propertySet.getProperty("host").asInstanceOf[String]
    port= propertySet.getProperty("port").asInstanceOf[String].toInt
    keyspace = propertySet.getProperty("keyspace").asInstanceOf[String]
    table = propertySet.getProperty("table").asInstanceOf[String]
    
    maxWidth = propertySet.getProperty("maxWidth").asInstanceOf[String].toDouble
    val json = new JSONObject(Map("Content-Type" -> "image/png"))
    imagePNG = json.toString()
    
    try {
    val cnxCfg = new CnxConfig(scala.collection.immutable.List(new InetSocketAddress(host, port)), keyspace, table)
    cClient = CassandraClient.DB
    cClient.connect(cnxCfg)
   
    //select pickup_latitude, pickup_longitude from trips where medallion = '8C1EED3FC560AC6AEEB3BD9E7C3753B5';
    session = cClient.getSession()       
    
    //execute count
    /*var results:ResultSet  = session.execute("select count(*) from vehicle_tracker.trips where medallion = '8C1EED3FC560AC6AEEB3BD9E7C3753B5'");
    var one:Row = results.one;
    val w:Int = one.getInt("count")
    
    log.addMessage(3, 200, s"Record Count : $w");*/
    
    /*val select:Statement = QueryBuilder.select().all().from(keyspace, table)
				.where(eq("medallion", "8C1EED3FC560AC6AEEB3BD9E7C3753B5"));*/
		//val results:ResultSet  = session.execute("select count(1) as cnt, pickup_longitude, pickup_latitude from vehicle_tracker.trips where medallion = '8C1EED3FC560AC6AEEB3BD9E7C3753B5'");
    } catch {
      case e: Exception =>  log.addMessage(3, 500, "Error:init cassandra connect")
      case _:Throwable => log.addMessage(3, 500, "Error:init cassandra connect generic error")
    }
  }

 
  def doExportImage(operationInput: String, responseProperties: Array[String]) = {
    val jsonInput = new JSONObject(operationInput)
    val sizeRE = "^(\\d+),(\\d+)$".r
    val (imgw, imgh) = jsonInput.getString("size") match {
      case sizeRE(wt, ht) => (wt.toInt, ht.toInt)
      case _ => (400, 400)
    }
    val (xmin, ymin, xmax, ymax) = jsonInput.getString("bbox") match {
      case text: String => {
        val tokens = text.split(',')
        (tokens(0).toDouble, tokens(1).toDouble, tokens(2).toDouble, tokens(3).toDouble)
      }
      case _ => (1.0, 1.0, -1.0, -1.0)
    }
    val xdel = xmax - xmin
    val ydel = ymax - ymin

    val fillw = (imgw * 110.0 / xdel).toInt
    val fillw2 = fillw / 2

    val fillh = (imgh * 130.0 / ydel).toInt
    val fillh2 = fillh / 2

    val bi = new BufferedImage(imgw, imgh, BufferedImage.TYPE_INT_ARGB)
    val g = bi.createGraphics()
    try {
      g.setBackground(Color.WHITE)
      if (xdel < maxWidth && xmin < xmax && ymin < ymax) {
        val minlon = WebMercator.xToLongitude(xmin)
        val maxlon = WebMercator.xToLongitude(xmax)
        val minlat = WebMercator.yToLatitude(ymin)
        val maxlat = WebMercator.yToLatitude(ymax)
        val dellon = maxlon - minlon
        val dellat = maxlat - minlat
        // log.addMessage(3, 200, s"$minlon,$minlat,$maxlon,$maxlat")
        val sb = new StringBuilder("POLYGON((")
        sb.append(minlon).append(' ').append(minlat).append(',')
          .append(maxlon).append(' ').append(minlat).append(',')
          .append(maxlon).append(' ').append(maxlat).append(',')
          .append(minlon).append(' ').append(maxlat).append(',')
          .append(minlon).append(' ').append(minlat).append("))")
        //preparedStatement.setString(1, sb.toString)
        //val resultSet = preparedStatement.executeQuery
        	                 
        try {
          //execute count
          var results:ResultSet  = session.execute("select count(*) from vehicle_tracker.trips where medallion = '8C1EED3FC560AC6AEEB3BD9E7C3753B5'");
          var one:Row = results.one;
          val w = one.getInt("count")
          
          //query by id
          results  = session.execute("select pickup_longitude, pickup_latitude from vehicle_tracker.trips where medallion = '8C1EED3FC560AC6AEEB3BD9E7C3753B5'");
          
          for (row <- results) {
            //val w = row.getInt("cnt") 
            val lon = row.getDouble("pickup_longitude")
            val lat = row.getDouble("pickup_latitude")
            val fx = (lon - minlon) / dellon
            val fy = 1.0 - (lat - minlat) / dellat
            val gx = (imgw * fx).toInt
            val gy = (imgh * fy).toInt
            g.setColor(colorMapper.getColor(w.min(255)))
            g.fillRect(gx - fillw2, gy - fillh2, fillw, fillh)          
          }                   
        } catch {
            case e: Exception =>   log.addMessage(3, 500, "Error:ExportImage")
            case _:Throwable => log.addMessage(3, 500, "Error:ExportImage generic error")
        } finally {
         // resultSet.close()
        }
        g.setColor(Color.GREEN)
      }
      else {
        g.setColor(Color.RED)
      }
      g.drawRect(0, 0, imgw - 1, imgh - 1)
    } finally {
      g.dispose()
    }

    responseProperties(0) = imagePNG

    val baos = new ByteArrayOutputStream(bi.getWidth * bi.getHeight)
    ImageIO.write(bi, "PNG", baos)
    baos.toByteArray
  }

  
  override def handleRESTRequest(capabilities: String,
                                 resourceName: String,
                                 operationName: String,
                                 operationInput: String,
                                 outputFormat: String,
                                 requestProperties: String,
                                 responseProperties: Array[String]
                                  ) = {

    log.addMessage(3, 200, s"r=$resourceName o=$operationName i=$operationInput f=$outputFormat")

    (operationName, outputFormat) match {      
      //case ("export", "image") => doExportImage(operationInput, responseProperties)
      case _ =>
        findRestRequestHandlerDelegate() match {
          case inst: IRESTRequestHandler => inst.handleRESTRequest(
            capabilities, resourceName, operationName, operationInput, outputFormat, requestProperties, responseProperties
          )
          case _ => null
        }
    }
  }

  override protected def preShutdown(): Unit = {
    log.addMessage(3, 200, "Arc2CassandraSOI::preShutdown")
    cClient.shutdown
  }
}

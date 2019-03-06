import groovy.sql.Sql
import com.branegy.dbmaster.model.*
import com.branegy.service.connection.api.ConnectionService
import com.branegy.dbmaster.connection.ConnectionProvider
import com.branegy.dbmaster.connection.JdbcDialect

def test_server    = p_database.split("\\.")[0]
def test_database  = p_database.split("\\.")[1]

connectionSrv = dbm.getService(ConnectionService.class)

RevEngineeringOptions options = new RevEngineeringOptions()
options.database = test_database
options.rawConfig =
//"+Table:*\n"+
"+View:*";
//"+Function:*\n"+
//"+Procedure:*\n"+
//"+Index:*\n"+
//"+Constraint:*\n"+
//"+Trigger:*\n"+
//"+ForeignKey:*\n"+
//"+SecurityObject:*\n"+
//"+Column:*\n"+
//"+Parameter:*\n"+
//"+ExtendedProperty:*"

connectionInfo = connectionSrv.findByName(test_server)

logger.info("Connecting to server ${test_server}")
dialect = ConnectionProvider.get().getDialect(connectionInfo)
dbm.closeResourceOnExit(dialect)

logger.info("Loading list of views from database ${test_database}")

model = dialect.getModel(test_server, options)

connection = dialect.getConnection()

def sql = new Sql(connection)

// model.views.sort { a,b -> a.name.compareToIgnoreCase b.name }

int errors = 0

println """<table cellspacing="0" class="simple-table" border="1">
               <tr style=\"background-color:#EEE\">
                   <td>View Name</td>
                   <td>Status</td>
                   <td>Rows</td>
                   <td>Execution Time (sec)</td>
               </tr>"""


def benchmark = { closure ->
  start = System.currentTimeMillis()
  closure.call()
  now = System.currentTimeMillis()
  now - start
}

model.views.each { view ->
   logger.info("Testing view ${view.name}")

   query = generateSql(view, dialect)
   try {
      logger.info("Query = ${query}")
      def rowsInView
      // return
      def duration = benchmark {
          def firstRow = sql.rows(query.toString())[0]
          rowsInView = firstRow==null ? -1 : firstRow.getAt(0)
      }

      println """<tr>
                    <td>${view.name}</td>
                    <td>No issues</td>
                    <td align=\"right\">${rowsInView}</td>
                    <td align=\"right\">${duration/1000.0}</td>
                 </tr>"""
      logger.info("View ${view.name} - No issues")
   } catch (Exception e) {
      e.printStackTrace()
      println """<tr>
                    <td><b>${view.name}</b></td>
                    <td><b>${e.getMessage()}</b></td>
                    <td align=\"right\">---</td>
                    <td align=\"right\">---</td></tr>"""
//      println "<li><b>View ${view.name} - ${e.getMessage()}</b></li>"
      logger.error("View ${view.name} - ${e.getMessage()}")
      errors+=1
   }
}

println "</table>"

println "<br/>Test completed. Total views tested ${model.views.size()}. Errors found - ${errors}"

logger.info("Test completed")


def generateSql(View view, JdbcDialect dialect) {
   def viewName = view.name
   def max_rows = 1

   switch (dialect.getDialectName().toLowerCase()){
   case "oracle":
       return  "select count(*) from \"${viewName}\"" // where ROWNUM <= ${max_rows}
   case "sqlserver":
       return  "select count(*) from [${view.schema}].[${view.simpleName}] with (NOLOCK)" // top ${max_rows} *
   case "mysql":
       return  "select count(*) from ${viewName}" // limit 0,${max_rows}
   default:
       return  "select count(*) from ${viewName}"
   }
}
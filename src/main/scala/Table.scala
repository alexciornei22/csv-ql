import util.Util.{Line, Row}

import scala.annotation.tailrec

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    val value = r.get(colName)

    if (value.isEmpty)
      None
    else
      Some(predicate(value.get))
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    if (f1.eval(r).isEmpty || f2.eval(r).isEmpty)
      None
    else
      Some(f1.eval(r).get && f2.eval(r).get)
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    if (f1.eval(r).isEmpty || f2.eval(r).isEmpty)
      None
    else
      Some(f1.eval(r).get || f2.eval(r).get)
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = ???
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = ???
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular
  private val rows : List[Row] = tabular.map(columnNames.zip(_).toMap)

  // 1.1
  override def toString: String = {
    def op(acc: String, elem: String): String = {
      acc match {
        case "" => elem
        case _ => acc + "," + elem
      }
    }

    columnNames.foldLeft("")(op) + "\n" +
      tabular.map(_.foldLeft("")(op))
        .foldLeft("")(_ + _ + "\n")
        .dropRight(1)
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    val selectedColumns = columnNames.filter(columns.contains(_))

    if (selectedColumns.isEmpty) None
    else Some(new Table(selectedColumns, rows.map(selectedColumns collect _)))
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    try {
      val filteredRows = rows.filter(cond.eval(_).get)

      if (filteredRows.isEmpty)
        None
      else
        Some(new Table(columnNames, filteredRows.map(_.values.toList)))
    } catch {
      case _: NoSuchElementException => None
    }
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table =
    new Table(
      columnNames ++ List(name),
      tabular.map(_ ++ List(defaultVal))
    )

  // 2.4.
  def merge(key: String, other: Table): Option[Table] =
    if (!columnNames.contains(key) || !other.getColumnNames.contains(key))
      None
    else {
      val mergedColumns = (columnNames ++ other.getColumnNames).distinct
      val mergedRows = (rows ++ other.rows).groupBy(_(key)).values

      def combineRows(rows: List[Row]): Row =
        if (rows.length == 1)
          rows.head
        else {
          val map1 = rows.head
          val map2 = rows(1)

          map2 ++ map1 map {case (k, v) => (k, map2.get(k) match {
            case Some(str) => if (str.equals(v)) v else v ++ ";" ++ str
            case _ => v
          })}
        }

      @tailrec
      def addNewCols(row: Row, cols: List[String]): Row =
        cols match {
          case Nil => row
          case col :: rest => addNewCols(row + (col -> ""), rest)
        }

      val combined = mergedRows.map(combineRows).map(row => {
        val cols = for (column <- mergedColumns if !row.contains(column)) yield column
        addNewCols(row, cols)
      })

      Some(new Table(mergedColumns, combined.map(mergedColumns collect _).toList))
    }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val lines = s.split("\n").toList

    val columns = lines.head.split(",").toList
    val tabular = lines.drop(1).map(_.split(",", - 1).toList)

    new Table(columns, tabular)
  }

}

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
  override def eval: Option[Table] =
    Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] =
    if (target.eval.isEmpty)
      None
    else
      target.eval.get.select(columns)
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] =
    if (target.eval.isEmpty)
      None
    else
      target.eval.get.filter(condition)
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] =
    if (target.eval.isEmpty)
      None
    else
      Some(target.eval.get.newCol(name, defaultVal))
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] =
    if (t1.eval.isEmpty || t2.eval.isEmpty)
      None
    else
      t1.eval.get.merge(key, t2.eval.get)

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
        .dropRight(1) // drop extra \n character
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    val selectedColumns = columnNames.filter(columns.contains(_))

    /*
     * Return a Table if we found all of the searched columns,
     * fail otherwise
     */
    if (!(selectedColumns.toSet equals columns.toSet)) None
    else Some(new Table(columns, rows.map(columns collect _)))
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    try {
      val filteredRows = rows.filter(cond.eval(_).get)

      if (filteredRows.isEmpty)
        None
      else
        Some(new Table(columnNames, filteredRows.map(columnNames collect _)))
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
      val groupedRows = (rows ++ other.rows).groupBy(_(key)).values

      /**
       * Merge a list of rows into a single row
       */
      def listToRow(rows: List[Row]): Row = {
        /**
         * Merge 2 rows together
         */
        def mergeRows(rowA: Row, rowB: Row): Row = {
          /**
           * Add a (key, value) pair to a row map,
           * according to the specified rules
           */
          def addToRow(row: Row, kv: (String, String)): Row = {
            kv match {
              case (key, value) =>
                if (!row.contains(key))
                  row + kv // key isn't in map
                else if (row(key).contains(value))
                  row // key with the same value already in map
                else {
                  // key with different value, must append with ';'
                  row + (key -> (row(key) + ";" + value))
                }
            }
          }

          rowB.foldLeft(rowA)(addToRow)
        }

        rows.foldLeft(Map.empty[String, String])(mergeRows)
      }

      /**
       * add new empty columns to row if no value is found
       * after merging
       */
      def addNewCols(row: Row): Row = {
        val newCols = mergedColumns.filterNot(row.contains)

        @tailrec
        def aux(newRow: Row, cols: Line): Row =
          cols match {
            case Nil => newRow
            case col :: rest => aux(newRow + (col -> ""), rest)
          }

        aux(row, newCols)
      }

      val mergedRows = groupedRows.map(listToRow).map(addNewCols)

      Some(new Table(mergedColumns, mergedRows.map(mergedColumns collect _).toList))
    }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val lines = s.split("\n").toList

    val columns = lines.head.split(",").toList
    val tabular = lines.drop(1)
      // limit set to -1 in order to include empty value
      // at end of line after ',' if it exists
      .map(_.split(",", -1).toList)

    new Table(columns, tabular)
  }
}

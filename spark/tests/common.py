from pyspark.sql.dataframe import DataFrame


def countNullsAcrossAllColumns(df: DataFrame) -> int:
    """Given a spark dataframe count the total number of cells with a null in them

    Args:
        df (DataFrame): [Spark dataframe]

    Returns:
        int: [Total number of cells with a null value]
    """
    # https://www.datasciencemadesimple.com/count-of-missing-nanna-and-null-values-in-pyspark/
    from pyspark.sql.functions import isnull, when, count, expr

    nullCountDf = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
    sumExpr = "+".join(nullCountDf.columns) + " as TOTAL"
    sumDf = nullCountDf.select(expr(sumExpr))
    return sumDf.collect()[0].TOTAL


def countWSAcrossAllStringColumns(df: DataFrame) -> int:
    """Given a spark dataframe count the Total number of cells with a BLANK or all SPACES value

    Args:
        df (DataFrame): [Spark dataframe]

    Returns:
        int: [Total number of cells with a BLANK or all SPACES value]
    """
    from pyspark.sql.functions import col, when, count, trim, expr

    stringCols = [cn for (cn, ct) in df.dtypes if ct == "string"]
    blanksCountdf = df.select(
        [count(when(trim(col(c)) == "", True)).alias(c) for c in stringCols]
    )
    sumExpr = "+".join(blanksCountdf.columns) + " as TOTAL"
    sumDf = blanksCountdf.select(expr(sumExpr))
    return sumDf.collect()[0].TOTAL

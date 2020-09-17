from pyspark.sql import SparkSession
from pyspark import SparkConf

standardNullReplacementMapPerStandardType = {
    "string": "-",
    "date": "1900-01-01",
    "timestamp": "1900-01-01 00:00:00",
    "double": "0.0",
    "int": "0",
    "boolean": "False",
}


def setSparkConfig(jars):
    """Set up config for spark session

    Args:
        jars ([str]): [semi-colon delimited list of jars as needed by spark at runtime]

    Returns:
        [SparkConf]: [spark session configuration object. Just the 'spark.jars' setting at this time]
    """
    conf = SparkConf()
    if (jars is not None):
        conf.set("spark.jars", jars)
    return conf


def getSpark(conf):
    """[Get a spark session for interacting with spark]

    Args:
        conf ([SparkConf]): [Spark configuration object]

    Returns:
        [SparkSession]: [A spark session manager]
    """
    return SparkSession.builder.appName("Test")\
        .config(conf=conf)\
        .getOrCreate()


def getUrl(db, user, secret):
    """[Connection URL for 'jdbc' and postgresql database]

    Args:
        db ([str]): [Database name]
        user ([str]): [User Id]
        secret ([str]): [Password]

    Returns:
        [str]: [jdbc url for postgresql]
    """
    return f"jdbc:postgresql://localhost/{db}?user={user}&password={secret}"


def getReader(sparkSession, url):
    """[Spark dataframe reader]

    Args:
        sparkSession ([SparkSession]): [Spark session]
        url ([str]): [Connection URL for 'jdbc' and postgresql database]

    Returns:
        [DataFrameReader]: [Spark dataframe reader]
    """
    return (
        sparkSession.read.format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", url)
    )


def getQueryDataFrame(sparkSession, url, query):
    """[Given a SQL query will return the results as a spark DataFrame]

    Args:
        sparkSession ([SparkSession]): [Spark session]
        url ([str]): [Connection URL for 'jdbc' and postgresql database]
        query ([str]): [SQL SELECT query]

    Returns:
        [type]: [description]
    """
    reader = getReader(sparkSession, url).option("dbtable", f"({query}) T")
    df = reader.load()
    df = replaceNulls(df)
    df = replaceBlanks(df)
    return df


def getStandardizedType(dfType):
    """[Normalise decimal as double]

    Args:
        dfType ([str]): [Dataframe column type]

    Returns:
        [str]: [Dataframe column type canonicalised]
    """
    asis = ["string", "date", "timestamp", "double", "int", "boolean"]
    if (dfType in asis):
        return dfType
    elif "decimal" in dfType:
        return "double"
    else:
        return "int"


def replaceNulls(df):
    """[Null replacement in Dataframe]

    Args:
        df ([DataFrame]): [Spark Dataframe]

    Returns:
        [DataFrame]: [Input dataframe transformed by replace nulls with the dtype specific default]
    """
    nullReplacementMap = {
        cn: standardNullReplacementMapPerStandardType.get(getStandardizedType(ct))
        for (cn, ct) in df.dtypes
    }
    return df.na.fill(nullReplacementMap)


def replaceBlanks(df):
    """[Blank or 'all' blanks string replacement in DataFrame]

    Args:
        df ([DataFrame]): [Spark DataFrame]

    Returns:
        [DataFrame]: [Input dataframe transformed by replacing BLANK strings with string specific default]
    """
    from pyspark.sql.functions import col, when, regexp_replace

    brv = standardNullReplacementMapPerStandardType.get("string", "")
    stringCols = [cn for (cn, ct) in df.dtypes if ct == "string"]
    for cn in stringCols:
        df = df.withColumn(cn, when(col(cn) == "", brv).otherwise(col(cn)))
        df = df.withColumn(cn, regexp_replace(col(cn), r"^\s+", brv))

    return df


def compareDfs(sparkSession, leftDf, rightDf, tolerance, keysLeft, keysRight, colExcludeList, joinType):
    """A side by side compare of dataframes. Each common column in the two dataframes appear side-by-side
       with an added extra column '_same'. If the two columns are the same (subject to tolerance) then the
       '_same' column is True. And addition column 'PASS' is created as the first column which is a boolean
       conditioned on all the '_same' columns'

     Columns in colExcludeList are ignored for comparison and evaluate to 'True' for comparison purposes.



    Args:
        sparkSession ([SparkSession]): [Spark session object]
        leftDf ([DataFrame]): [First dataframe for comparison]
        rightDf ([DataFrame]): [Second dataframe for comparison]
        tolerance ([double]): [Tolerance when comparing decimal types. A difference less than tolerance evaluates to True]
        keysLeft ([str]): [join key columns on left, comma delimited string]
        keysRight ([str]): [join key columns on right, comma delimited string]
        colExcludeList ([list(str)]): [List of str, columns not to be compared for differences]
        joinType ([str]): [The result dataframe will use this join type. See https://sparkbyexamples.com/pyspark/pyspark-join/]

    Returns:
        [DataFrame]: [Result of comparison]
    """
    from pyspark.sql.functions import col, abs, lit

    (leftSide_tag, rightSide_tag, boolCol_tag) = ("_left", "_right", "_same")
    joinConditionsAsList = makeDfJoinCondition(
        leftDf, rightDf, tolerance, keysLeft, keysRight
    )
    joinConditionAsString = joinCondAsString(joinConditionsAsList)
    joinConditionObject = compileAndEval(joinConditionAsString, leftDf, rightDf)
    (
        colsInBoth,
        colsInLeftOnly,
        colsInRightOnly,
        allLeftCols,
        allRightCols,
    ) = getDfColsInfo(leftDf, rightDf)

    newColNamesLeft = getNewColsNames(allLeftCols, leftSide_tag)
    newColNamesRight = getNewColsNames(allRightCols, rightSide_tag)

    colDictOldNameToNewNames = {}
    for item in colsInLeftOnly:
        colDictOldNameToNewNames[item] = (f"{item}{leftSide_tag}", None, None)
    for item in colsInRightOnly:
        colDictOldNameToNewNames[item] = (None, f"{item}{leftSide_tag}", None)

    newColNamesAll = list(newColNamesLeft)
    newColNamesAll.extend(newColNamesRight)

    # join the two dfs using joinType (suggest full_outer)
    df = (leftDf.join(rightDf, joinConditionObject, joinType)
          .toDF(*newColNamesAll)
          .select(*sorted(newColNamesAll)))

    # create new column "PASS" which initially will be true but will be re-evaluated based on individual columns
    df = df.withColumn("PASS", lit(True))

    # now add a new column to hold compare results for side by side diff
    # we can only do this for columns that exist both sides
    dictOfColDTypes = dict(df.dtypes)
    colsToCompare = list(colsInBoth)
    for colInBoth in colsToCompare:
        leftCol = f"{colInBoth}{leftSide_tag}"
        rightCol = f"{colInBoth}{rightSide_tag}"
        newBoolCol = f"{colInBoth}{boolCol_tag}"
        if (colInBoth in colExcludeList):
            df = df.withColumn(newBoolCol, lit(True))
        else:
            if leftCol in dictOfColDTypes:
                leftColType = dictOfColDTypes[leftCol]
                righColType = dictOfColDTypes[rightCol]
                if (isDoubleType(leftColType, righColType)):
                    df = df.withColumn(newBoolCol, abs(col(leftCol) - col(rightCol)) < tolerance)
                else:
                    df = df.withColumn(newBoolCol, col(leftCol) == col(rightCol))

        colDictOldNameToNewNames[colInBoth] = (leftCol, rightCol, newBoolCol)
        df = df.withColumn("PASS", col("PASS") & col(newBoolCol))
        newColNamesAll.append(newBoolCol)

        # - return data in its natural order list (ie the order as it existed in original dataframes)
    naturalOrderList = ["PASS"]
    for item in colsInBoth:
        tup = colDictOldNameToNewNames.get(item)
        naturalOrderList.extend(list(tup))
    for item in colsInLeftOnly:
        (a, b, c) = colDictOldNameToNewNames.get(item)
        naturalOrderList.append(a)
    for item in colsInRightOnly:
        (a, b, c) = colDictOldNameToNewNames.get(item)
        naturalOrderList.append(b)
    df = df.select(*naturalOrderList)

    return df


def getNewColsNames(columnNameList, tag):
    """Modify column name list by adding a tag at the end

    Args:
        columnNameList ([list(str)]): [A list of column names that have to be transformed to new names]
        tag ([str]): [A tag to add at the tail of the column name to generate a new name]

    Returns:
        [list(str)]: [The list of modified column names]
    """
    return [f"{x}{tag}" for x in columnNameList]


def joinCondAsString(joinConditionAsList):
    """Takes a list of join conditions and returns it as a single code block

    Args:
        joinConditionAsList ([list(str)]): [List of join conditions]

    Returns:
        [str]: [Code string representing the join conditions]
    """
    conditions = [
        x
        for x in joinConditionAsList
        if x != "" and x != "None" and x is not None
    ]

    code = ",".join(conditions)
    code = f"[{code}]"
    return code


def compileAndEval(codeStr, leftDf, rightDf):
    """[Take code string and calls 'eval' on it]

    Args:
        codeStr ([str]): [Code string that needs to be evaluated]
        leftDf ([DataFrame]): [leftDf and rightDf are required to provide local context for eval]
        rightDf ([DataFrame]): [leftDf and rightDf are required to provide local context for eval]

    Returns:
        [object]: [Result from 'eval()' on input code string]
    """
    # leftDf and rightDf are required to provide local context for eval
    compiledCode = compile(codeStr, "<string>", "eval")
    return eval(compiledCode)


def getDfColsInfo(leftDf, rightDf):
    """[Information on Dataframe columns]

    Args:
        leftDf ([DataFrame]): [left Dataframe]
        rightDf ([DataFrame]): [right Dataframe]

    Returns:
        [tuple(list)]: [5 element tuple. Each tuple item is a list.
                       1: is columns in both dataframe (common) as list[str]
                       2: is columns in left dataframe only as list[str]
                       3: is columns in right dataframe only as list[str]
                       4: all columns in left as list[str]
                       5: all columns in right as list[str]
                       ]
    """
    # order is important
    colsInBoth = [x for x in leftDf.columns if x in rightDf.columns]
    colsInLeftOnly = [x for x in leftDf.columns if x not in rightDf.columns]
    colsInRightOnly = [x for x in rightDf.columns if x not in leftDf.columns]
    allLeftCols = list(leftDf.columns)
    allRightCols = list(rightDf.columns)
    return (colsInBoth, colsInLeftOnly, colsInRightOnly, allLeftCols, allRightCols)


def makeDfJoinCondition(leftDf, rightDf, tolerance, keysLeft, keysRight):
    """[Builds join condition between left and right dataframe given the keys on each side and tolerance.]

    Args:
        leftDf ([DataFrame]): [Left side dataframe]
        rightDf ([DataFrame]): [Right side dataframe]
        tolerance ([double]): [A double for tolerance checking double columns]
        keysLeft ([str]): [A comma delimited list of keys on left dataframe]
        keysRight ([str]): [A comma delimited list of keys on right dataframe]

    Returns:
        [list(str)]: [A list of join conditions between dataframes]
    """
    leftTypesMap = dict(leftDf.dtypes)
    rightTypesMap = dict(rightDf.dtypes)
    leftKeysList = [x.strip() for x in keysLeft.split(",")]
    rightKeysList = [x.strip() for x in keysRight.split(",")]
    keyByKey = zip(leftKeysList, rightKeysList)
    return [
        makeDfConditionWithTolerance(
            lname, leftTypesMap[lname], rname, rightTypesMap[rname], tolerance
        )
        for (lname, rname) in keyByKey
    ]


def makeDfConditionWithTolerance(leftColName, leftColType, rightColName, rightColType, tolerance):
    """[Build join condition based on column type - if a double type we need to take the tolerance into account]

    Args:
        leftColName ([str]): [Left side dataframe column name]
        leftColType ([str]): [Left side dataframe column type - canonicalised]
        rightColName ([str]): [Right side dataframe column name]
        rightColType ([str]): [Right side dataframe column type - canonicalised]
        tolerance ([double]): [A double indicating a tolerance to use when comparing double column types]

    Returns:
        [str]: [A join as string]
    """
    if (isDoubleType(leftColType, rightColType)):
        return makeDoubleJoinCond(leftColName, rightColName, tolerance)
    else:
        return makeNormalJoinCond(leftColName, rightColName)


def isDoubleType(leftColType, rightColType):
    """[Check if a column is a double type]

    Args:
        leftColType ([str]): [dataframe column type]
        rightColType ([str]): [dataframe column type]

    Returns:
        [bool]: [True if the column type is a double]
    """
    listDoubleType = ["double", "decimal"]
    return leftColType in listDoubleType or rightColType in listDoubleType


def makeDoubleJoinCond(leftColName, rightColName, tolerance):
    """[Make a join condition between two columns, taking into account tolerance]

    Args:
        leftColName ([str]): [Left side dataframe column name]
        rightColName ([str]): [Right side dataframe column name]
        tolerance ([double]): [A double indicating a tolerance to use when comparing double column types]

    Returns:
        [str]: [A join as string, but taking into account the tolerance]
    """
    innerTolerance = tolerance + 0.000001
    return (f"(leftDf['{leftColName}'] - rightDf['{rightColName}']).between({-innerTolerance},{innerTolerance})")


def makeNormalJoinCond(leftColName, rightColName):
    """[Make a join condition between two columns, taking into account tolerance]

    Args:
        leftColName ([str]): [Left side dataframe column name]
        rightColName ([str]): [Right side dataframe column name]

    Returns:
        [str]: [A join as string]
    """

    return(f"leftDf['{leftColName}'] == rightDf['{rightColName}']")

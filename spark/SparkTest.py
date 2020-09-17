import SparkDFCompare as dfc
from dotenv import load_dotenv
import os
load_dotenv(verbose=True)

# Run with pyspark or spark-submit
# --pyspark
# C:\MyInstalled\spark-2.4.5-bin-hadoop2.7\spark-2.4.5-bin-hadoop2.7\bin\pyspark --jars C:\MyWork\GIT\python\spark\postgresql-42.2.14.jar #noqa: E501
# import SparkTest
# -- spark-submit
# C:\MyInstalled\spark-2.4.5-bin-hadoop2.7\spark-2.4.5-bin-hadoop2.7\bin\spark-submit --jars C:\MyWork\GIT\python\spark\postgresql-42.2.14.jar SparkTest.py #noqa: E501
sparkConfig = dfc.setSparkConfig(jars=os.getenv("JARS"))
sparkSession = dfc.getSpark(sparkConfig)
sparkSession.sparkContext.setLogLevel("ERROR")

url = dfc.getUrl(db=os.getenv("POSTGRES_DB"), user=os.getenv("POSTGRES_USER"), secret=os.getenv("POSTGRES_SECRET"))

# Get base data
baseSql = "SELECT * FROM tleft"
dfBaseline = dfc.getQueryDataFrame(sparkSession, url, baseSql)

# Get test data
testSql = "SELECT * FROM tright"
dfTest = dfc.getQueryDataFrame(sparkSession, url, testSql)

# To a side by side compare
# c1_left, c1_right, c1_same, \
# c2_left, c2_right, c2_same, \
# c_inbaseonly, c_intargetonly
dfResult = dfc.compareDfs(
    sparkSession,
    dfBaseline,
    dfTest,
    tolerance=0.1,
    keysLeft="bsr",
    keysRight="bsr",
    colExcludeList=["n1", "n2", "n3", "n4", "n5", "tx"],
    joinType="full_outer",
)

# now write to csv or parquet

sparkSession.stop()

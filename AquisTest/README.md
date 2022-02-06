

AcquisNaive.py
A first implementation (not really naive)
* But called naive because - just single threaded, and does not stream the file from the give URL (I captured a copy for repeatable testing)
* It does read the local file in a 'streaming' way
* Most early learnings out of the way here - such as fixing bad json
* RUNTIME: ~2secs

AquisSpark.py
* Uses the same above capture file and using pure Spark APIS
* Reads into a dataframe
  * Fixes the json string
  * Separates out MSG#8 and MSG#12
  * Performs aggregations on MSG#12 data
  * Performs join with MSG#8 data to produce final out via a dataframe write
  * Also captures the DDL for the schema (for later use, see resources/)
  * Also (for demo) writes msg8 to a postgres db
* RUNTIME: ~60secs

AquisAsyncIO.py
* Actually does a streaming read from the URL
* uses asyncio tasks to process the streaming data
* RUNTIME: ~45secs

AquisThread.py
* Also does a streaming read from the URL
* Uses threads to process the streaming data
* RUNTIME: ~140secs

AquisCommon.py
* A collection of common methods that are
* in use across the different files

resources/msgType_8.schema,msgType_12.schema
* Schema definitions of the data in above message types
* Obtained via spark api (.toDDL())
* Intention is to use in a 'spark structured streaming' demo [_if I get the time_]

output_sample/
* One file per generating source file

Comments
* used Python 3.7 and PyCharm IDE
* Linting/Formatting courtesy of "black"
* Only uses std python libraries, with exception of **pyspark** and **python-dotenv**
to illustrate the use of Spark API
* The output only has records for securities actually traded (ie have msg#12)
* There is 'bad' json that is repaired on the fly during processing
* Not all 'traded' securities have a corresponding security static data record (msg#8). This is illustrated in the output.

* Closest to the requirement use case is probably **AquisAsyncIO.py**
* The **Spark** version is there to illustrate the possibility of its usage and its
'power' - and would scale with compute resource.

**Still to do - (At time of writing Sunday 16:30)**
* DocStrings
* GitActions
* Spark streaming (if I have the time..)
* Scale out using securityId modulus as partition key (may not get time, but mentioning here due to consideration I need to give it)

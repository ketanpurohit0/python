
AcquisNaive.py
A first implementation (not really naive)
* But called native because - just single threaded, and does not stream the file from the give URL (I captured a copy for repeatable testing)
* It does read the local file in a 'streaming' way
* Most early learnings out of the way here - such as fixing bad json
* RUNTIME: ~2secs

AquisSpark.py
* Uses the same above capture file and using pure Spark APIS
* Reads into a dataframe
  * Fixes the json string
  * Separates out MSG#8 and MSG#12
  * Performs aggregations on MSG#12 data
  * Produces final out via a dataframe write
* RUNTIME: ~60secs

AquisAsyncIO.py
* Actually does a streaming read from the URL
* uses asyncio tasks to process the streaming data
* RUNTIME: ~45secs

AquisThread.py
* Also does a streaming read from the URL
* Uses threads to process the streaming data
* RUNTIME: ~140secs

Comments
* Closest to the requirement use case is probably **AquisAsyncIO.py**
* The **Spark** version is there to illustrate the possibility of its usage and its
'power' - and would scale with the compute resource.

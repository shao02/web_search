Source code for Web Search Engines at NYU in Fall, 2014.

[Final Project Writeup](https://docs.google.com/a/nyu.edu/document/d/13lkKyrSOUQHyZIybLLXnUD_LLSWqyq_obYmvZkNniyw/edit?usp=sharing)

Compile:
--------
Install Maven then compile everything with `mvn clean package`.

Section 2 -- Mine:
------------------
`java -jar target/search-engine-.1-SNAPSHOT-jar-with-dependencies.jar --mode=mining --options=conf/engine.conf`

The pagerank scores are output to the 'data/pagerank' folder (created if does not exist), and written as serialized objects. The numviews data is output to the 'data/log' folder. It is also written as a serialized object.

These are deserialized and loaded at serve time. 

Section 2.1 Pagerank discussion:
--------------------------------
Lambda 0.9 and two iterations were chosen since this setting gave the highest Spearman score (see Section 3). So for this data set (corpus + log), a relatively low weighting for the 'random surfing' influence, coupled with the impact of the link network's propagation effects, led to a seemingly better prediction of the browsing behavior as recorded by the click log.

That said, having a lower lambda value may sometimes be better. Other factors, like the user's changing interests in a given session session and short attention span may be accounted for more accurately by giving randomness a heavier weight. Additionally, research shows that choosing a lambda to close to 1 may lead to large fluxuation between iterations of the pagerank algorithm.

A dynamic search engine can therefore have an adaptive lambda that reacts to the behavior recorded in the click logs. 


Index:
------
`time java -Xmx512m -jar target/search-engine-.1-SNAPSHOT-jar-with-dependencies.jar --mode=index --options=conf/engine.conf`

Serve:
------
`java -Xmx512m -jar target/search-engine-.1-SNAPSHOT-jar-with-dependencies.jar --mode=serve --port=25812 --options=conf/engine.conf`

`curl "localhost:25812/search?query=george+herbert+walker+bush&ranker=comprehensive&format=text"`

Or visit `localhost:25812/search?query=%22web+search%22+google&ranker=comprehensive&format=html` in a browser.

Section 3 -- Spearman:
----------------------
`java -cp target/classes edu.nyu.cs.cs2580.Spearman [path to pageranks_lambda09_twoIterations] [path to numviews.serialized]`

Results with different pagerank settings:
lambda(0.1)  iterations(1)   score: 0.458579
lambda(0.1)  iterations(2)   score: 0.458583
lambda(0.9)  iterations(1)   score: 0.458549
lambda(0.9)  iterations(2)   score: 0.458745

Section 4.1 -- PRF:
-------------------
`java -Xmx512m -jar target/search-engine-.1-SNAPSHOT-jar-with-dependencies.jar --mode=serve --port=25812 --options=conf/engine.conf`

`curl "localhost:25812/prf?query=google&ranker=comprehensive&numdocs=10&numterms=20"`

Section 4.2 -- Bhattacharyya:
-----------------------------
`./test_bhattacharyya.bsh`

Crawler:
java -classpath crawler4j-3.6-SNAPSHOT-jar-with-dependencies.jar edu.uci.ics.crawler4j.examples.multiple.CrawlerControllerFinalProject <location>

Design:
=======
First, we create temporary indices for groups of D documents. Then, those temporary indices are merged into
a single large index with no repeats. Then that large index is written out to smaller indices split by token
with ~1 MB per file. During serving, these indices are loaded on demand and cached in memory. 

Compression is done at the last step -- when we are writing the ~1 MB final index files. This simplifies
the compression logic a lot, since the Indexer doesn't have to think about how to write to the file -- just how
to split its posting list into bytes during indexing and split the bytes into a posting list during serving.

For homework 2, we wrote all of our posting lists as strings. But, for homework 3, we converted our Indexers to
write and read raw bytes, since it ends up taking less space. Our final index (no document to frequencies) is
87 MB. This isn't ideal. But, it's still pretty good compression from a 1.1 GB corpus. We could improve the
compression by changing the format of the posting lists to "[list of doc ids] [list of number of occurrences]
[list of occurrences]" since that would allow for better delta encoding. But, that's not needed.

We didn't compress our data for PRF calculation. That could be another optimization. But, it wasn't necessary.

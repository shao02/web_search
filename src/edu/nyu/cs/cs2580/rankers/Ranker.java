package edu.nyu.cs.cs2580.rankers;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.QueryHandler;
import edu.nyu.cs.cs2580.helper.SpellingException;
import edu.nyu.cs.cs2580.index.Indexer;
import edu.nyu.cs.cs2580.models.QueryPhrase;
import edu.nyu.cs.cs2580.models.ScoredDocument;

import java.util.Vector;

/**
 * This is the abstract Ranker class for all concrete Ranker implementations.
 *
 * Use {@link Ranker.Factory} to create your concrete Ranker implementation. Do
 * NOT change the interface in this class!
 *
 * 2013-02-16: The instructor's code went through substantial refactoring
 * between HW1 and HW2, students are expected to refactor code accordingly.
 * Refactoring is a common necessity in real world and part of the learning
 * experience.
 *
 * @author congyu
 * @author fdiaz
 */
public abstract class Ranker {
    // Options to configure each concrete Ranker.
    protected Options _options;
    // CGI arguments user provide through the URL.
    protected QueryHandler.CgiArguments _arguments;

    // The Indexer via which documents are retrieved, see {@code IndexerFullScan}
    // for a concrete implementation. N.B. Be careful about thread safety here.
    protected Indexer _indexer;

    /**
     * Constructor: the construction of the Ranker requires an Indexer.
     */
    protected Ranker(Options options, QueryHandler.CgiArguments arguments, Indexer indexer) {
        _options = options;
        _arguments = arguments;
        _indexer = indexer;
    }

    /**
     * Processes one query.
     * @param query the parsed user query
     * @param numResults number of results to return
     * @return Up to {@code numResults} scored documents in ranked order
     */
    public abstract Vector<ScoredDocument> runQuery(QueryPhrase query, int numResults) throws SpellingException;

    /**
     * All Rankers must be created through this factory class based on the
     * provided {@code arguments}.
     */
    public static class Factory {
        public static Ranker getRankerByArguments(QueryHandler.CgiArguments arguments,
                                                  Options options, Indexer indexer) {
            switch (arguments._rankerType) {
                default:
                    return new RankerComprehensive(options, arguments, indexer);
            }
        }
    }
}

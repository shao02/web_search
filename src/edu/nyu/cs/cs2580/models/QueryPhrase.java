package edu.nyu.cs.cs2580.models;

import edu.nyu.cs.cs2580.helper.HtmlParser;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * @CS2580: implement this class for HW2 to handle phrase. If the raw query is
 * ["new york city"], the presence of the phrase "new york city" must be
 * recorded here and be used in indexing and ranking.
 */
public class QueryPhrase {
    public String _query = null;
    public Vector<String> _tokens = new Vector<String>();
    private Map<String, String> raw = new HashMap<String, String>();

    public QueryPhrase(String query) {
        _query = query;
        processQuery();
    }

    public void processQuery() {
        _tokens = new Vector<String>();
        if (_query == null) return;
        char[] chars = _query.toCharArray();
        StringBuilder currentToken = new StringBuilder();
        boolean inQuotes = false;
        for (char c : chars) {
            if (c == '+' || Character.isSpaceChar(c)) {
                if (inQuotes) currentToken.append(' ');
                else {
                    String token = currentToken.toString().trim();
                    if (!token.isEmpty()) _tokens.add(currentToken.toString());
                    currentToken = new StringBuilder();
                }
            }
            else if (c == '"') {
                if (inQuotes) {
                    String token = currentToken.toString().trim();
                    if (!token.isEmpty()) _tokens.add(token);
                    currentToken = new StringBuilder();
                }
                inQuotes = !inQuotes;
            }
            else {
                currentToken.append(c);
            }
        }
        String lastToken = currentToken.toString().trim();
        if (!lastToken.isEmpty()) _tokens.add(lastToken);
        
        Vector<String> cleanTokens = new Vector<String>();
        for (String token : _tokens) {
            String stemmed = HtmlParser.clean(token);
            cleanTokens.add(stemmed);
            String[] rawParts = token.split("\\s+");
            String[] stemParts = stemmed.split("\\s+");
            for (int i = 0; i < rawParts.length; i++) {
                raw.put(stemParts[i], rawParts[i]);
            }
        }
        _tokens = cleanTokens;
    }

    public String toString() {
        return _query.replaceAll("\\+", " ");
    }

    public String getRaw(String stemmed) {
        return raw.get(stemmed);
    }

}

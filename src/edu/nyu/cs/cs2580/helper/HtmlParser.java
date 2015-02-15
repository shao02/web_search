package edu.nyu.cs.cs2580.helper;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.util.Locale;

public class HtmlParser {

    String body, title;

    public HtmlParser(File f) {
        try {
            Document doc = Jsoup.parse(f, "UTF-8");
            body = doc.getElementsByTag("body").text().toLowerCase(Locale.ROOT);
            body = clean(body);
            // See http://stackoverflow.com/questions/1611979/remove-all-non-word-characters-from-a-string-in-java-leaving-accented-charact

            title = doc.getElementsByTag("title").text();
        } catch (Exception e) {
            // Ignore.
        }
    }

    public String getBody() { return body; }

    public String getTitle() { return title; }
	
	public static String stem(String raw)throws Exception{
		StringBuilder bodyBuilder = new StringBuilder();
        PorterStemming s = new PorterStemming();
        String[] body = raw.split(" ");

        for (String token : body) {
            String lower = token.toLowerCase();
            s.add(lower.toCharArray(), lower.length());
            s.stem();
            bodyBuilder.append(s.toString()).append(" ");
        }

        return bodyBuilder.toString();
	}
  
	public static String clean(String input) {
        input = input.toLowerCase(Locale.ROOT);
        input = input.replaceAll("[^\\p{L}\\p{Nd}]+", " ");
        try {
            input = stem(input);
        } catch (Exception e) {
            // Ignore.
        }
        return input.trim();
    }
	
}
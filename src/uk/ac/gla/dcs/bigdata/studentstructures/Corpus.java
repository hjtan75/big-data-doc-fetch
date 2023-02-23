package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.HashMap;

public class Corpus {
    HashMap<String, Lexicon> posting;
}

class Lexicon{
    int n_docs;
    int n_fre;
    HashMap<String, Integer> posting;
}

// doc_id = 5
// lex = test.get('a'); //return lexicon
// termFreqCurrentDoc = lex.posting.get('doc_id');
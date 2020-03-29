import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io._
//import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.index.IndexReader 
//import org.apache.lucene.store.RAMDirectory 
import org.apache.lucene.store.NIOFSDirectory 
import org.apache.lucene.index.IndexWriter 
import org.apache.lucene.document.TextField 
import org.apache.lucene.document.FieldType
import org.apache.lucene.document.Document 
import org.apache.lucene.index.DirectoryReader 
import org.apache.lucene.index.IndexWriterConfig 
import org.apache.lucene.document.Field 
import scala.io
import org.apache.lucene.search.IndexSearcher 
import java.io.BufferedReader
import java.io.FileReader
import org.apache.lucene.search.TermQuery 
import org.apache.lucene.index.Term 

import org.apache.spark.broadcast.Broadcast
import sys.process._




object CreateLuceneData {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CreateLuceneData")
    val sc = new SparkContext(conf)




val path1     = args(0)
val path2     = args(1)
val lucenePath= args(2)




val lang1 = path1.split("\\.").last
val lang2 = path2.split("\\.").last


def wordCount (text: RDD[String]) : RDD[(String, Int)] = {
    val wordFreqSentences = text.flatMap(line => line.split(" ") ).map(word => (word, 1 )).reduceByKey(_ + _)
    return wordFreqSentences
}



// Save data in lucene format
val luceneData = lucenePath+"/data"
val rm = "rm -r "+luceneData !
val mkdir = "mkdir "+luceneData !
val fBr = new BufferedReader(new FileReader(path1));
val tBr = new BufferedReader(new FileReader(path2));



val analyzer = new WhitespaceAnalyzer()


val directory = new NIOFSDirectory(java.nio.file.Paths.get(luceneData));
val writer = new IndexWriter(directory,new IndexWriterConfig(analyzer))

var completed = false;
while (!completed) {
	val line1 = fBr.readLine();
	val line2 = tBr.readLine();
	if (line1 == null || line2 == null){
	    completed=true
	}else{
		val doc = new Document()
		val fieldtype = new FieldType();
		doc.add( new TextField(lang1, line1, Field.Store.YES)  )
		doc.add( new TextField(lang2, line2, Field.Store.YES)  )
		val lineSize = line1.split(" ").size + line2.split(" ").size;
		doc.add( new TextField("size", lineSize.toString, Field.Store.YES)  )
		writer.addDocument(doc)
	}
}
writer.close()


val reader = DirectoryReader.open(directory);
val searcher = new IndexSearcher(reader);



//Save word counts
val wordCount1 = wordCount(sc.textFile(path1))
val wordCount2 = wordCount(sc.textFile(path2))

wordCount1.saveAsObjectFile(lucenePath+"/wordCount_"+lang1)
wordCount2.saveAsObjectFile(lucenePath+"/wordCount_"+lang2)



//Save text file with statistics (NUM_LINES)
val NUM_LINES=sc.textFile(path1).count()
val statsWriter = new PrintWriter(new File(lucenePath+"/statisticsFile" ))
statsWriter.write("NUM_LINES="+NUM_LINES)
statsWriter.close()


}}
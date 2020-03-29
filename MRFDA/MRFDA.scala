
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io._
//import com.typesafe.config.{ Config, ConfigFactory }
import com.typesafe.config.ConfigFactory 

import org.apache.lucene.index.IndexReader 
import org.apache.lucene.store.NIOFSDirectory 
import org.apache.lucene.document.Document 
import org.apache.lucene.index.DirectoryReader 
import scala.io
import org.apache.lucene.search.IndexSearcher 
import org.apache.lucene.search.TermQuery 
import org.apache.lucene.index.Term 
//import org.apache.lucene.analysis.util.CharArraySet
import org.apache.spark.broadcast.Broadcast
import sys.process._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
//import scala.util.Random
import scala.concurrent.Await
import scala.concurrent.duration._
import org.apache.lucene.search.PhraseQuery
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.Sort




object MRFDA {
  def main(args: Array[String]) {

    val conf = new SparkConf().
    	setAppName("MRFDA").
    	set("spark.executor.heartbeatInterval", "300")
    val sc = new SparkContext(conf)





/*

//Bilingual

val config          = "/home/alberto/Archivos/Proyectos/DCU/scala/MRFDA/config"
val phrasePairsPath = "/home/alberto/Archivos/Proyectos/DCU/scala/MRFDA/data/ngram_set"
val outputPath      = "/home/alberto/Archivos/Proyectos/DCU/scala/MRFDA/data"



*/

//CLUSTER:
val config          = args(0)
val phrasePairsPath = args(1)
val outputPath      = args(2)


val phrasePairs =  sc.textFile(phrasePairsPath )


//load from config
val configFile = ConfigFactory.parseFile(new File(config))
val lucenePath    =configFile.getString("lucenePath")
val langFrom    =configFile.getString("langFrom")
val langTo    =configFile.getString("langTo")
val numSelectedData  =configFile.getInt("numSelectedData")





val directory = new NIOFSDirectory(java.nio.file.Paths.get(lucenePath));
val reader = DirectoryReader.open(directory);
val searcher = new IndexSearcher(reader);




def getDoc(idx: Int): Future[org.apache.lucene.document.Document] = Future {
	searcher.doc(idx)
}




val base = 0.5
def log_base(x:Double)=math.log(x)/math.log(base)

//change the exponent so we can use e as base:  base^a == e^x
def chnge_exp(x:Double) = x*math.log(base)



// Get phrases (n-grams) or phrase pairs
val php = phrasePairs.map(x=>x.split("\\|\\|\\|")).map{case(x)=>{
	if (x.size==1)
		(x(0).trim,"")
	else
		(x(0).trim,x(1).trim)
}}.
filter(_._1!=".").filter(_._1!=",").
collect().distinct





def getQueryPresent(ngramFrom:String,ngramTo:String) : BooleanQuery = {
	val builderFrom = new PhraseQuery.Builder();
	ngramFrom.split(" ").foreach(e=>builderFrom.add( new Term(langFrom, e)))
	val pqFrom = builderFrom.build();
	val builderTo = new PhraseQuery.Builder();
	ngramTo.split(" ").foreach(e=>builderTo.add( new Term(langTo, e)))
	val pqTo = builderTo.build();
	val bq = new BooleanQuery.Builder(); 
	bq.add(pqFrom, org.apache.lucene.search.BooleanClause.Occur.MUST  )
	if (ngramTo.size>0){
		bq.add(pqTo, org.apache.lucene.search.BooleanClause.Occur.MUST  )
	}
	bq.build()
}



def getCommonPhrasesIdx(ngramFrom:String,ngramTo:String): Future[Array[Int]] =Future{
	val bqb = getQueryPresent(ngramFrom,ngramTo)
	val numRes = math.max(1,searcher.search(bqb, 1, new Sort, false ,false).totalHits)
	searcher.search(bqb, numRes, new Sort, false ,false).scoreDocs.map(_.doc)
}




// Get the sentences where the phrase is contained
//[(f1,[id1,id2,id8]),(f2,[id5,id3,id8])...]
val featureListIdsRdd = sc.parallelize(
	//php.map(feat=> (feat,getCommonPhrasesIdx(feat._1,feat._2) ) ).map(x=>(x._1,Await.result(x._2, Duration.Inf))) //Old scala version
	php.map(feat=> (feat,getCommonPhrasesIdx(feat._1,feat._2) ) ).map(x=>(x._1,Await.result(x._2, DurationInt(Int.MaxValue).seconds)))
)
featureListIdsRdd.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)


//create a map with the length of the sentences
val sizeMap = sc.broadcast(
	featureListIdsRdd.flatMap(_._2).distinct.collect().
	map(idx=> (idx, (getDoc(idx).map(_.get("size").toInt)) ) ).
	map(x=> (x._1,Await.result(x._2, DurationInt(Int.MaxValue).seconds))).
	toMap
)



def getSizes(sizeMapL:Broadcast[scala.collection.immutable.Map[Int,Int]] = sizeMap): RDD[((String, String), Array[(Int, Int)])] ={
	featureListIdsRdd.map{case(feat,listId)=>{
		(feat,
			listId.map(id=>
			(id,sizeMapL.value.getOrElse(id,Int.MaxValue))) )
	}}
}

//[(f1,[(id1,size1),(id2,size2),(id8,size8)]),...]
val listIdsSizeByFeat = getSizes()

listIdsSizeByFeat.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)






val idNormSizeMap = sc.broadcast(
	listIdsSizeByFeat.flatMap(_._2).countByValue().toArray.map{case((id,size),numFeat)=>{(id,size/numFeat.toDouble)}}.toMap
)


//Criterion order is length(s)/num_features

def toNormLength(idNormSizeMapP:Broadcast[scala.collection.immutable.Map[Int,Double]] =idNormSizeMap ) :
	RDD[((String, String), Array[(Int, Double)])] ={
	return listIdsSizeByFeat.
	map{case(feat,arIdSizeN)=>{(feat,arIdSizeN.map(x=>(x._1,idNormSizeMapP.value.getOrElse(x._1,Double.MaxValue)))) }}
}

val listIdsSizeByFeatNorm = toNormLength()



//get positions (and change the base)
def toPosition(numSentences: Int ) :  RDD[(Int, Array[ Double])] = {
listIdsSizeByFeatNorm.
flatMap{case(feat,arIdSizeN)=>{
arIdSizeN.sortBy(_._2).take(numSentences).zipWithIndex.map{case((id,siz),pos)=>{(id,pos-math.log(siz)/math.log(0.5))}}
}}.
map(x=>(x._1,Array(x._2)))
}

val scoresOfId = toPosition(numSelectedData).reduceByKey(_++_)





/*

val scoresOfId = listIdsSizeByFeat.
map{case(feat,arIdSizeN)=>{(feat,arIdSizeN.map(x=>(x._1,idNormSizeMap.value.getOrElse(x._1,Double.MaxValue)))) }}. 
flatMap{case(feat,arIdSizeN)=>{
arIdSizeN.sortBy(_._2).take(numSelectedData).zipWithIndex.map{case((id,siz),pos)=>{(id,pos-log_base(siz))}}
}}.
map(x=>(x._1,Array(x._2))).
reduceByKey(_++_)
*/



/*

Given [1,4,7,3] it computes 
log( [0.5^1,0.5^4,0.5^7,0.5^3] )
*/
def sentenceValue(exponents:Array[Double]) : Double ={
//get the exponents in base e
val expBaseE = exponents.map(x=>chnge_exp(x))
//apply log-Sum-Exp
val lse_factor = expBaseE.max
return lse_factor+math.log(expBaseE.map(x=>math.exp(x-lse_factor)).sum)
}

/*
Compute exponents and sort
*/
def getTopSentences(scoresOfIdL:RDD[(Int, Array[Double])], numSent: Int ): RDD[(Int,Double)] ={
val valuesOfId = scoresOfIdL.map{case(id,exponents)=>{
(id,sentenceValue(exponents) )
}}.
sortBy(-_._2).
zipWithIndex.
filter(_._2< numSent ).
map(_._1)
return valuesOfId
}




val selectedLines = getTopSentences(scoresOfId,numSelectedData)


val trainingData = sc.parallelize(
selectedLines.collect().map{case(idx,scr)=> 
(getDoc(idx).map(x=>(x.get(langFrom)+"\t"+x.get(langTo),scr.toString))) }.
map(x=> Await.result(x, DurationInt(Int.MaxValue).seconds)) 
)





import sys.process._
def saveRDD (rdd:RDD[String],outputPath:String) : Unit = {
	val outputFilename="output"
	val tmpOutputPath = outputPath+"/tmp"
	val foo1 = ("rm -r "+tmpOutputPath !)
	rdd.repartition(1).saveAsTextFile(tmpOutputPath)
	val cmnd = "ls "+tmpOutputPath
	val filename = (cmnd #| "grep part"!!).replace("\n","")
	val mcCmd = "mv "+tmpOutputPath+"/"+filename+"  "+outputPath+"/"+outputFilename
	val foo2 = (mcCmd !)
	val foo3 = ("rm -r "+tmpOutputPath ! )
}





saveRDD(trainingData.map(x=>x._1+"\t"+x._2),outputPath+"/data_out")
val mv1 = ("mv "+outputPath+"/data_out/output "+outputPath+"/traindata")!
val rm1 = ("rm -r "+outputPath+"/data_out")!



}}







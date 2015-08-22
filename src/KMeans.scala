import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object RecSystem{
def main(args: Array[String]) {
val UserId = "20"
val UserRatingFile = sc.textFile("/ratings.dat")
val UserRatingArray = UserRatingFile.toArray
val RatingList = new scala.collection.mutable.MutableList[String]
for(i <- 0 until UserRatingArray.length) {
val rateLine = UserRatingArray(i).split("::")
if(rateLine(0) == UserId && rateLine(2) == "3") {
RatingList += rateLine(1)
}
}
val SimilarityMatrixFile = sc.textFile("/output/indicator-matrix/part-00000")
val MatrixArray = SimilarityMatrixFile.toArray
val ListOfMovies = new scala.collection.mutable.HashMap[String,List[String]].withDefaultValue(Nil)
for(i <- 0 until MatrixArray.length){
val MatrixArrayLine = MatrixArray(i).split("\\s+")
val UserRatingKey = MatrixArrayLine(0)
if(MatrixArrayLine.length > 1){
val ListOfMoviesSM = MatrixArrayLine(1).split(",")
for( j <- 0 until ListOfMoviesSM.length){
val ListOfMoviesSMSplit = ListOfMoviesSM(j).split(":")
val SimilarityMatrixMovieID = ListOfMoviesSMSplit(0)
ListOfMovies(UserRatingKey) ::= SimilarityMatrixMovieID
}
}
}
val MovieFile = sc.textFile("/dxd132630/movies.dat")
val MovieHashMap = new scala.collection.mutable.HashMap[String,String]
val MovieFileToArray=MovieFile.toArray
for(i <- 0 until MovieFileToArray.length){
val MovieArrayLine = MovieFileToArray(i).split(":") 
MovieHashMap(MovieArrayLine(0)) = MovieArrayLine(1)
}
for(i <- 0 until RatingList.length){
val RatingListID = RatingList(i)
if(ListOfMovies.contains(RatingListID)){
println(RatingList(i)+":"+MovieHashMap(RatingListID))
for(i <- 0 until ListOfMovies(RatingListID).length){
if(i !=0){
print(","+ListOfMovies(RatingListID)(i)+":"+MovieHashMap(ListOfMovies(RatingListID)(i)))
}
else
print(ListOfMovies(RatingListID)(i)+":"+MovieHashMap(ListOfMovies(RatingListID)(i)))
}
println("")
println("")
}
}
}
}
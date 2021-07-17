def indiscernibility(dec_string_rows : Array[Array[String]]): Array[Array[Int]] = {
	var b = dec_string_rows.zipWithIndex.map{ case(x, i) => (x.toSeq.hashCode, i)};
	return b.groupBy(_._1).map(p => p._2.map(_._2)).toArray;
}

def allCombinations(columns : List[Int]) : Array[Array[Int]] = {
     	return (1 to columns.length).flatMap(i => columns.toArray.combinations(i)).toArray;
}

var dataset = spark.read.format("csv").option("header", "true").load("C:/Users/Sai Lakshmi Keerthana/Desktop/FinalTitanic.csv");
println("The old dataset is");
dataset.show();
var decision_attributes_names = Array("Survived");
var decision_attributes = dataset.select(decision_attributes_names.head, decision_attributes_names.tail: _*);
var conditional_attributes_names= dataset.columns.filterNot(decision_attributes.columns.toSet);
var conditional_attributes =  dataset.select(conditional_attributes_names.head, conditional_attributes_names.tail: _*);
conditional_attributes.show();
var cond = sc.broadcast(conditional_attributes.collect);
var no_of_partitions = 2;
var iterations = 20;
var dec_ind = decision_attributes_names.map(x => dataset.schema.fieldIndex(x));
var dec_rows = dataset.collect().map(x => dec_ind.map(y => x(y)))
var dec_string_rows = dec_rows.map(x => x.map(y => y.toString))
var decision_indiscernibility = indiscernibility(dec_string_rows);
var red = for (i<-0 until iterations)  yield{
	var ran = List.range(0, conditional_attributes.columns.length);
	var shuffled_list = scala.util.Random.shuffle(ran);
	var partitioned_list = (0 until no_of_partitions).map{ i => shuffled_list.drop(i).sliding(1, no_of_partitions).flatten.toList }.toList;
	var partitions = sc.parallelize(partitioned_list, no_of_partitions);
	var dependencies = partitions.mapPartitions{ x => x.map{
		y => allCombinations(y).map{ 
			z => (z, indiscernibility(cond.value.map{
				l => z.map(m => l(m).toString)
			}).map(p => decision_indiscernibility.map(q =>{ if (q.toList.intersect(p.toList).size == p.toList.size) p.toList.size else 0  }).reduce(_+_)).reduce(_+_), z.size);
		}
	}}
	var max_dependencies = dependencies.mapPartitions(x => x.map( y => (y.filter(_._2  == y.map(z => z._2).max))))
	var min_size = max_dependencies.mapPartitions(x => x.map( y => y.filter(_._3 == y.map(z => z._3).min)))
	var reducts_rdd = min_size.mapPartitions(x => x.map(y => y.map(z => z._1)))
	var reducts_par = reducts_rdd.mapPartitions(x => x.flatMap(y => y.flatMap(z => z.toList)))
	var reducts_iter = reducts_par.toDF.collect.flatMap(x => x.toSeq);
	reducts_iter;
}
var finalReducts = red(0);
for( i <- 1 to red.length-1){
     finalReducts = finalReducts.intersect(red(i));
}
var reduct_set = finalReducts.map(x => conditional_attributes.columns(x.asInstanceOf[Int]));

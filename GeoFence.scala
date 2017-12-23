/**




nb. Longitude = East-West
    Latitude = North-South
Syria:
westlimit=35.7166; southlimit=32.3111; eastlimit=42.3763; northlimit=37.3187
Canada:
westlimit=-141.0; southlimit=41.68; eastlimit=-52.62; northlimit=59.96
Toronto:
westlimit=-79.799207; southlimit=43.401253; eastlimit=-78.801023; northlimit=43.901488

lon > westlimit & lon < eastlimit
lat > southlimit & lat < northlimit



Silverdale Schema:
tweetid,    timestamp,  screen_name,    place_name, latitude,   longitude,  text,   language,   URL
0           1           2               3           4           5           6        7          8
 */




package com.oculusinfo

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.io.Source
import scala.util.Try



def isValid(tweet: String) : Boolean = {
    val x = tweet.split("\t")
    if (x.size != 9) false else (x(4).size > 0 & x(5).size > 0)
}


def inCanada(tweet: String) : Boolean = {
  var (west, south, east, north) = (-141.0, 41.68, -52.62, 59.96)
  return inGeoBounds(tweet, west, south, east, north)
}

def inToronto(tweet: String) : Boolean = {
  var (west, south, east, north) = (-79.799207, 43.401253, -78.801023, 43.901488)
  return inGeoBounds(tweet, west, south, east, north)
}

def inSyria(tweet: String) : Boolean = {
  var (west, south, east, north) = (35.7166, 32.3111, 42.3763, 37.3187)
  return inGeoBounds(tweet, west, south, east, north)
}

def inGeoBounds(tweet: String, west: Double, south: Double, east: Double, north: Double ) : Boolean = {
  val col = tweet.split("\t")
  val lat = col(4).toDouble
  val lon = col(5).toDouble
  return lon > west & lon < east & lat > south & lat < north
}


def withinGeoBounds(tweet: String, bounds: Array[Double]) : Boolean = {
    var Array(west, south, east, north) = bounds
    val col = tweet.split("\t")
    val lat = col(4).toDouble
    val lon = col(5).toDouble
    return lon > west & lon < east & lat > south & lat < north
}


def readGeoData(geofile: String) = {
    val loc2geo = Source.fromFile(geofile).getLines().toList.filter(!_.startsWith("#")).map(_.split("\t")).map(x => (x(0), x(1).split(" ").map(_.toDouble))).toMap
    loc2geo
}





val files = sc.textFile("xdata/data/twitter/silverdale/new_collect_201[4-5][0-1][1-2]/*")
val valid = files.filter(isValid(_)).cache

val syria_filter = valid.filter(inSyria(_)).map(x => x.split("\t")(2)).groupBy(x => x).map(x => (x._1, ("syria", x._2.size))).cache
val toronto_filter = valid.filter(inToronto(_)).map(x => x.split("\t")(2)).groupBy(x => x).map(x => (x._1, ("toronto", x._2.size))).cache
val canada_filter = valid.filter(inCanada(_)).map(x => x.split("\t")(2)).groupBy(x => x).map(x => (x._1, ("canada", x._2.size))).cache
val canada_filter = hasGeo.filter(inCanada(_)).map(x => (x.split("\t")(2), "canada")).map(x => ((x._1, x._2), 1)).reduceByKey(_ + _).map(x => (x._1._1, (x._1._2, x._2)) ).cache
val can_syria = syria_filter.cogroup(canada_filter).filter(x => x._2._1.size > 0 & x._2._2.size > 0)

val st = syria_filter.cogroup(toronto_filter).filter(x => x._2._1.size > 0 & x._2._2.size > 0).cache


val result = st.collect
for (line <- result) {
    val usr = line._1
    val v1 = line._2._1.toList(0)
    val v2 = line._2._2.toList(0)
    println(usr + "\t" + v1._1 + "," + v1._2 + "\t" + v2._1 + "," + v2._2 )
}



val files = sc.textFile("xdata/data/twitter/silverdale/*")
val valid = files.filter(isValid(_)).cache
val geofile = "/home/chagerman/GeoBoundsData.tsv"
val bounds = readGeoData(geofile)

val syria_filter = valid.filter( withinGeoBounds(_, bounds("USA_exclude_Canada")) ).map(x => x.split("\t")(2))   .groupBy(x => x).map(x => (x._1, ("syria", x._2.size)) ).cache
val france_filter = valid.filter( withinGeoBounds(_, bounds("USA_exclude_Canada")) ).map(x => x.split("\t")(2))   .groupBy(x => x).map(x => (x._1, ("france", x._2.size)) ).cache
val nz_filter = valid.filter( withinGeoBounds(_, bounds("USA_exclude_Canada")) ).map(x => x.split("\t")(2))   .groupBy(x => x).map(x => (x._1, ("nz", x._2.size)) ).cache
val usa_filter = valid.filter( withinGeoBounds(_, bounds("USA_exclude_Canada")) ).map(x => x.split("\t")(2))   .groupBy(x => x).map(x => (x._1, ("usa", x._2.size)) ).cache

val france_overlap = syria_filter.cogroup(france_filter).filter(x => x._2._1.size > 0 & x._2._2.size > 0).cache
val nz_overlap = syria_filter.cogroup(nz_filter).filter(x => x._2._1.size > 0 & x._2._2.size > 0).cache
val usa_overlap = syria_filter.cogroup(usa_filter).filter(x => x._2._1.size > 0 & x._2._2.size > 0).cache

val result = nz_overlap.collect





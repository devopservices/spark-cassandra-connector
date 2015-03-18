package com.datastax.driver.core

import java.math.{BigDecimal, BigInteger}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

class RowMock(columnSizes: Option[Int]*) extends Row {
  val bufs = columnSizes.map {
    case Some(size) => ByteBuffer.allocate(size)
    case _ => null
  }.toArray

  val defs = new ColumnDefinitions(columnSizes.map(i => new ColumnDefinitions.Definition("ks", "tab", s"c$i", DataType.text())).toArray)

  override def getColumnDefinitions: ColumnDefinitions = defs

  override def getUUID(i: Int): UUID = ???

  override def getUUID(s: String): UUID = ???

  override def getVarint(i: Int): BigInteger = ???

  override def getVarint(s: String): BigInteger = ???

  override def getInet(i: Int): InetAddress = ???

  override def getInet(s: String): InetAddress = ???

  override def getList[T](i: Int, aClass: Class[T]): util.List[T] = ???

  override def getList[T](s: String, aClass: Class[T]): util.List[T] = ???

  override def getDouble(i: Int): Double = ???

  override def getDouble(s: String): Double = ???

  override def getBytesUnsafe(i: Int): ByteBuffer = bufs(i)

  override def getBytesUnsafe(s: String): ByteBuffer = getBytesUnsafe(defs.getIndexOf(s))

  override def getFloat(i: Int): Float = ???

  override def getFloat(s: String): Float = ???

  override def getLong(i: Int): Long = ???

  override def getLong(s: String): Long = ???

  override def getBool(i: Int): Boolean = ???

  override def getBool(s: String): Boolean = ???

  override def getMap[K, V](i: Int, aClass: Class[K], aClass1: Class[V]): util.Map[K, V] = ???

  override def getMap[K, V](s: String, aClass: Class[K], aClass1: Class[V]): util.Map[K, V] = ???

  override def getDecimal(i: Int): BigDecimal = ???

  override def getDecimal(s: String): BigDecimal = ???

  override def isNull(i: Int): Boolean = bufs(i) == null

  override def isNull(s: String): Boolean = isNull(defs.getIndexOf(s))

  override def getSet[T](i: Int, aClass: Class[T]): util.Set[T] = ???

  override def getSet[T](s: String, aClass: Class[T]): util.Set[T] = ???

  override def getDate(i: Int): Date = ???

  override def getDate(s: String): Date = ???

  override def getInt(i: Int): Int = ???

  override def getInt(s: String): Int = ???

  override def getBytes(i: Int): ByteBuffer = ???

  override def getBytes(s: String): ByteBuffer = ???

  override def getString(i: Int): String = ???

  override def getString(s: String): String = ???

  override def getTupleValue(s: String): TupleValue = ???

  override def getUDTValue(s: String): UDTValue = ???

  override def getTupleValue(i: Int): TupleValue = ???

  override def getUDTValue(i: Int): UDTValue = ???
}

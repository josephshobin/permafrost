//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.permafrost.hdfs

import scala.util.control.Exception

import org.apache.hadoop.fs.{FileSystem, Path}

import scalaz._, Scalaz._

/** Utility functions for working with Hive partitions on HDFS.*/
object Hive {
  def safeStringToInt(str: String): Option[Int] =
    Exception.catching(classOf[NumberFormatException]) opt str.toInt

  /** Compare two numeric paths. */
  def safeCompare(f: (Int, Int) => Boolean)(a: Option[Path], b: Path): Option[Path] =
    (a.flatMap(x => safeStringToInt(x.getName)), safeStringToInt(b.getName)) match {
      case (Some(x), Some(y)) if f(x, y) => a
      case (Some(_), Some(_))            => Some(b)
      case (Some(_), None)               => a
      case (None, Some(_))               => Some(b)
      case _                             => None
  }

  def partition(path: Path, f: (Int, Int) => Boolean): Hdfs[Option[Path]] = for {
    fs    <- Hdfs.filesystem
    files <- Hdfs.value(fs.globStatus(new Path(path, "*")).toList.map(_.getPath))
    dirs  <- files.filterM(Hdfs.isDirectory)
  } yield {
    dirs.foldLeft(Option.empty[Path])(safeCompare(f))
  }

  /** Find smallest numeric partition. */
  def minPartitionM(path: Path): Hdfs[Option[Path]] = partition(path, (_ < _))

  /** Find largest numeric partition. */
  def maxPartitionM(path: Path): Hdfs[Option[Path]] = partition(path, (_ > _))
}

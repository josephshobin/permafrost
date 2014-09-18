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

import scala.util.control.NonFatal
import scala.collection.JavaConverters._

import java.io.File

import scalaz._, Scalaz._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ChecksumFileSystem, FileSystem, Path, FSDataInputStream, FSDataOutputStream}

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.apache.avro.util.Utf8

import au.com.cba.omnia.permafrost.io.Streams

/**
 * A data-type that represents a HDFS operation.
 *
 * HDFS operations use a hadoop configuration as context,
 * and produce a (potentially failing) result.
 */
case class Hdfs[A](run: Configuration => Result[A]) {
  /** Map across successful Hdfs operations. */
  def map[B](f: A => B): Hdfs[B] =
    andThen(f andThen Result.ok)

  /** Bind through successful Hdfs operations. */
  def flatMap[B](f: A => Hdfs[B]): Hdfs[B] =
    Hdfs(c => run(c) match {
      case Error(e) => Error(e)
      case Ok(a)    => f(a).run(c)
    })

  /** Chain an unsafe operation through a successful Hdfs operation. */
  def safeMap[B](f: A => B): Hdfs[B] =
    flatMap(a => Hdfs.value(f(a)))

  /** Chain a context free result (i.e. requires no configuration) to this Hdfs operation. */
  def andThen[B](f: A => Result[B]): Hdfs[B] =
    flatMap(a => Hdfs(_ => f(a)))

  /** Convert this to a safe Hdfs operation that converts any exceptions to failed results. */
  def safe: Hdfs[A] =
    Hdfs(c => try { run(c) } catch { case NonFatal(t) => Result.exception(t) })

  /** Set the error message in a failure case. Useful for providing contextual information without having to inspect result. */
  def setMessage(message: String) =
    Hdfs(c => run(c).setMessage(message))

  /**
    * Runs the first Hdfs operation. If it fails, runs the second operation. Useful for chaining optional operations.
    *
    * Throws away any error from the first operation.
    */
  def or(other: => Hdfs[A]): Hdfs[A] =
    Hdfs(c => run(c).fold(Result.ok, _ => other.run(c)))

  /** Alias for `or`. Provides nice syntax: `Hdfs.create("bad path") ||| Hdfs.create("good path")` */
  def |||(other: => Hdfs[A]): Hdfs[A] =
    or(other)
}

object Hdfs {
  /** Build a HDFS operation from a result. */
  def result[A](v: Result[A]): Hdfs[A] =
    Hdfs(_ => v)

  /** Build a failed HDFS operation from the specified message. */
  def fail[A](message: String): Hdfs[A] =
    result(Result.fail(message))

  /** Build a failed HDFS operation from the specified exception. */
  def exception[A](t: Throwable): Hdfs[A] =
    result(Result.exception(t))

  /** Build a failed HDFS operation from the specified exception and message. */
  def error[A](message: String, t: Throwable): Hdfs[A] =
    result(Result.error(message, t))

  /**
    * Fails if condition is not met
    *
    * Provided instead of [[scalaz.MonadPlus]] typeclass, as Hdfs does not
    * quite meet the required laws.
    */
  def guard(ok: Boolean, message: String): Hdfs[Unit] =
    result(Result.guard(ok, message))

  /**
    * Fails if condition is met
    *
    * Provided instead of [[scalaz.MonadPlus]] typeclass, as Hdfs does not
    * quite meet the required laws.
    */
  def prevent(fail: Boolean, message: String): Hdfs[Unit] =
    result(Result.prevent(fail, message))

  /**
    * Ensures a Hdfs operation returning a boolean success flag fails if unsuccessfull
    *
    * Provided instead of [[scalaz.MonadPlus]] typeclass, as Hdfs does not
    * quite meet the required laws.
    */
  def mandatory(action: Hdfs[Boolean], message: String): Hdfs[Unit] =
    action flatMap (guard(_, message))

  /**
    * Ensures a Hdfs operation returning a boolean success flag fails if succeesfull
    *
    * Provided instead of [[scalaz.MonadPlus]] typeclass, as Hdfs does not
    * quite meet the required laws.
    */
  def forbidden(action: Hdfs[Boolean], message: String): Hdfs[Unit] =
    action flatMap (prevent(_, message))

  /** Build a HDFS operation from a function. The resultant HDFS operation will not throw an exception. */
  def hdfs[A](f: Configuration => A): Hdfs[A] =
    Hdfs(c => Result.safe(f(c)))

  /** Build a HDFS operation from a value. The resultant HDFS operation will not throw an exception. */
  def value[A](v: => A): Hdfs[A] =
    hdfs(_ => v)

  /** Get the HDFS FileSystem for the current configuration. */
  def filesystem: Hdfs[FileSystem] =
    hdfs(c => FileSystem.get(c))

  /** Produce a value based upon a HDFS FileSystem. */
  def withFilesystem[A](f: FileSystem => A): Hdfs[A] =
    filesystem.safeMap(fs => f(fs))

  /** List all files matching `globPattern` under `dir` path. */
  def files(dir: Path, globPattern: String = "*") = for {
    fs    <- filesystem
    isDir <- isDirectory(dir)
    _     <- fail(s"'$dir' must be a directory!").unlessM(isDir)
    files <- glob(new Path(dir, globPattern))
  } yield files

  /** Get all files/directories from `globPattern`. */
  def glob(globPattern: Path) = for {
    fs <- filesystem
    files <- value { fs.globStatus(globPattern).toList.map(_.getPath) }
  } yield files

  /** Check the specified `path` exists on HDFS. */
  def exists(path: Path) =
    withFilesystem(_.exists(path))

  /** Check the specified `path` does _not_ exist on HDFS. */
  def notExists(path: Path) =
    exists(path).map(!_)

  /** Create file on HDFS with specified `path`. */
  def create(path: Path): Hdfs[FSDataOutputStream] =
    withFilesystem(_.create(path))

  /** Create directory on HDFS with specified `path`. */
  def mkdirs(path: Path): Hdfs[Boolean] =
    withFilesystem(_.mkdirs(path)).setMessage(s"Could not create dir $path")

  /** Check the specified `path` exists on HDFS and is a directory. */
  def isDirectory(path: Path) =
    withFilesystem(_.isDirectory(path))

  /** Check the specified `path` exists on HDFS and is a file. */
  def isFile(path: Path) =
    withFilesystem(_.isFile(path))

  /** Open file with specified path. */
  def open(path: Path): Hdfs[FSDataInputStream] =
    withFilesystem(_.open(path))

  /** Read contents of file at specified path to a string. */
  def read(path: Path, encoding: String = "UTF-8"): Hdfs[String] =
    open(path).safeMap(in => Streams.read(in, encoding))

  /** Write the string `content` to file at `path` on HDFS. */
  def write(path: Path, content: String, encoding: String = "UTF-8") =
    create(path).safeMap(out => Streams.write(out, content, encoding))

  /** Move file at `src` to `dest` on HDFS. */
  def move(src: Path, dest: Path): Hdfs[Path] =
    filesystem.safeMap(_.rename(src, dest)).as(dest).setMessage(s"Could not move $src to $dest")

  /** Downloads file at `hdfsPath` from HDFS to `localPath` on the local filesystem */
  def copyToLocalFile(hdfsPath: Path, localPath: File): Hdfs[File] =
    filesystem.safeMap(_ match {
      case cfs : ChecksumFileSystem => {
        val dest = new Path(localPath.getAbsolutePath)
        val checksumFile = cfs.getChecksumFile(dest)
        cfs.copyToLocalFile(hdfsPath, dest)
        if (cfs.exists(checksumFile)) cfs.delete(checksumFile, false)
      }
      case fs =>
        fs.copyToLocalFile(hdfsPath, new Path(localPath.getAbsolutePath))
    })
      .as(localPath)
      .setMessage(s"Could not download $hdfsPath to $localPath")

  /** Copy file from the local filesystem at `localPath` to `hdfsPath` on HDFS*/
  def copyFromLocalFile(localPath: File, hdfsPath: Path): Hdfs[Path] =
    filesystem.safeMap(_.copyFromLocalFile(new Path(localPath.getAbsolutePath), hdfsPath))
      .as(hdfsPath)
      .setMessage(s"Could not copy $localPath to $hdfsPath")

  /** Read lines of a file into a list. */
  def lines(path: Path, encoding: String = "UTF-8"): Hdfs[List[String]] =
    read(path).map(_.lines.toList)

  /** Copies all of the files on HDFS to the local filesystem. These files will be deleted on exit. */
  def copyToTempLocal(paths: List[Path]): Hdfs[List[(Path, File)]] = paths.map(hdfsPath => {
      val tmpFile = File.createTempFile("hdfs_", ".hdfs")
      tmpFile.deleteOnExit()
      copyToLocalFile(hdfsPath, tmpFile).map(f => hdfsPath -> f)
    }).sequence

  /** Convenience for constructing `Path` types from strings. */
  def path(path: String): Path =
    new Path(path)

  /** Read avro records from a file at specified path */
  def readAvro[A](path: Path)(implicit mf: Manifest[A]): Hdfs[List[A]] = for {
    fsIn    <- hdfs(c => new FsInput(path, c))
    freader <- value(new DataFileReader[A](fsIn, new SpecificDatumReader[A](mf.runtimeClass.asInstanceOf[Class[A]])))
    objs    <- value(freader.asInstanceOf[java.lang.Iterable[A]].asScala.toList).map(convertUtf8s) // strings are read as utf8's
    _        = freader.close()
    _        = fsIn.close()
  } yield objs

  /** Write avro records to a file at specified path */
  def writeAvro[A](path: Path, records: List[A], schema: Schema)(implicit mf: Manifest[A]): Hdfs[Unit] = for {
    out     <- create(path)
    fwriter <- value(new DataFileWriter[A](new SpecificDatumWriter[A](mf.runtimeClass.asInstanceOf[Class[A]])))
    _       <- value(fwriter.create(schema, out))
    _       <- records.map(r => value(fwriter.append(r))).sequence
    _        = fwriter.close()
  } yield ()

  /** When you tell Avro to read a String, it gives you a Utf8. This is a convienence
      function to convert the Utf8 back to a String */
  def convertUtf8s[A](records: List[A]): List[A] =
    records.map(r => if(r.isInstanceOf[Utf8]) r.toString.asInstanceOf[A] else r)

  implicit def HdfsMonad: Monad[Hdfs] = new Monad[Hdfs] {
    def point[A](v: => A) = result(Result.ok(v))
    def bind[A, B](a: Hdfs[A])(f: A => Hdfs[B]) = a flatMap f
  }
}

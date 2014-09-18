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

package au.com.cba.omnia.permafrost.test

import java.io.File

import org.specs2.specification.{Fragments, AfterExample}

/**
 * A base specification class for scalding/hdfs related testing.
 *
 * This specification will provide access to a unique path, via:
 * {{{
 *  testid     // for string form
 *  testpath   // for hdfs path form
 *  testfile   // for file form
 * }}}
 *
 * Arbitrary paths for use in properties are also provided and
 * will be generated under the unique test id.
 *
 * All files created will be cleaned up on completion of the
 * test.
 *
 * This base specification also include matchers for hdfs operations
 * and scalding jobs.
 */
abstract class HdfsTest extends Spec
  with HdfsMatchers
  with UniqueContext
  with ConfigurationContext
  with AfterExample {

  def after =
    clean(testfile)

  override def map(fs: => Fragments) =
    isolated ^ fs

  def clean(dir: File): Unit = {
    if (dir.isDirectory)
      dir.listFiles match {
        case null => ()
        case fs   => fs.foreach(clean)
      }
    dir.delete()
  }
}

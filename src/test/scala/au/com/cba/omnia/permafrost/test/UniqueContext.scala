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

import au.com.cba.omnia.permafrost.hdfs.HdfsString._

trait UniqueContext {
  lazy val uniqueId = s"${UniqueContext.jvm}.${UniqueContext.Ids.getAndIncrement}"

  def testid =
    s"target/hdfs/$uniqueId"

  def testpath =
    testid.toPath

  def testfile =
    new File(testid)

}

object UniqueContext {
  val Ids = new java.util.concurrent.atomic.AtomicInteger(0)

  def jvm =
    new java.rmi.dgc.VMID().toString.replace(':', '.')
}

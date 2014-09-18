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

import scalaz._, Scalaz._

trait HdfsImplicits {
  implicit def validation2Hdfs[A](v: Validation[String, A]): HdfsValidation[A] =
    HdfsValidation(v)
}

object HdfsImplicits extends HdfsImplicits

case class HdfsValidation[A](v: Validation[String, A]) {
  def toHdfs = v match {
    case Failure(e) => Hdfs.fail(e)
    case Success(m) => Hdfs.value(m)
  }
}

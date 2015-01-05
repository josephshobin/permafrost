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

import org.apache.hadoop.conf.Configuration

import org.scalacheck.{Arbitrary, Gen}

import au.com.cba.omnia.omnitool.{Result, Ok, Error}
import au.com.cba.omnia.omnitool.test.Arbitraries._

import au.com.cba.omnia.permafrost.hdfs.Hdfs

/**
  * Arbitraries for permafrost types
  *
  * Does not yet replace arbitrary instances defined in other objects
  */
object Arbitraries {
  implicit def HdfsIntArbitrary: Arbitrary[Hdfs[Int]] =
    Arbitrary(Arbitrary.arbitrary[Configuration => Result[Int]] map (Hdfs(_)))

  implicit def HdfsBooleanArbitrary: Arbitrary[Hdfs[Boolean]] =
    Arbitrary(Arbitrary.arbitrary[Configuration => Result[Boolean]] map (Hdfs(_)))
}

case class Identifier(value: String)

object Identifier {
  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(Gen.identifier map (Identifier.apply))
}

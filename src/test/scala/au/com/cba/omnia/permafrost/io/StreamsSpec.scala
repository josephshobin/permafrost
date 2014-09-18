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

package au.com.cba.omnia.permafrost.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import au.com.cba.omnia.permafrost.test.Spec

class StreamsSpec extends Spec { def is = s2"""
Java Stream Utilitities
=======================

Streams should:
  read all content                                $read
  write all content                               $write
  be symmetric in read and write                  $symmetric

"""

  def read = prop((s: String) => {
    val in = new ByteArrayInputStream(s.getBytes("UTF-8"))
    Streams.read(in, "UTF-8") must_== s
  })

  def write = prop((s: String) => {
    val out = new ByteArrayOutputStream()
    Streams.write(out, s)
    out.toByteArray must_== s.getBytes("UTF-8")
  })

  def symmetric = prop((s: String) => {
    val out = new ByteArrayOutputStream()
    Streams.write(out, s, "UTF-8")
    val in = new ByteArrayInputStream(out.toByteArray)
    Streams.read(in, "UTF-8") must_== s
  })
}

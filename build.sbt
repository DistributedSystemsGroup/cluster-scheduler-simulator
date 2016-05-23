// Copyright (c) 2013, Regents of the University of California
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// Redistributions of source code must retain the above copyright notice, this
// list of conditions and the following disclaimer.  Redistributions in binary
// form must reproduce the above copyright notice, this list of conditions and the
// following disclaimer in the documentation and/or other materials provided with
// the distribution.  Neither the name of the University of California, Berkeley
// nor the names of its contributors may be used to endorse or promote products
// derived from this software without specific prior written permission.  THIS
// SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

name := "Cluster Scheduler Simulator"

version := "0.1"

scalaVersion := "2.10.4"

organization := "edu.berkeley.cs"

mainClass := Some("Simulation")

scalacOptions += "-deprecation"

// Add a dependency on commons-math for poisson random number generator
libraryDependencies += "org.apache.commons" % "commons-math" % "2.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.6.1"

libraryDependencies += "log4j" % "log4j" % "1.2.14"



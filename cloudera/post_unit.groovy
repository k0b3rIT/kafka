// (c) Copyright 2022 Cloudera, Inc.

import hudson.matrix.MatrixBuild
import hudson.model.FreeStyleBuild
import hudson.model.Result
import java.util.concurrent.TimeUnit

//org.jvnet.hudson.plugins.groovypostbuild.GroovyPostbuildRecorder.BadgeManager manager
if (manager.build instanceof MatrixBuild || manager.build instanceof FreeStyleBuild) {
  def buildUrl = manager.build.absoluteUrl
  def gerritPort = manager.build.buildVariables.get("GERRIT_PORT")
  def gerritHost = manager.build.buildVariables.get("GERRIT_HOST")
  def commit = manager.build.buildVariables.get("GERRIT_PATCHSET_REVISION")

  manager.listener.logger.println("Updating gerrit change")
  manager.listener.logger.println("params: [buildUrl=$buildUrl, gerritHost=$gerritHost gerritPort=$gerritPort commit=$commit]")

  def message = "Unit tests execution failed! ($buildUrl)"
  def vote = "-1"
  if (manager.build.result.isBetterOrEqualTo(Result.UNSTABLE)) {
    vote = "+1"
    message = "Unit tests execution was successful! ($buildUrl)"
  }
  def label = "Unit-Tests=$vote"

  def cmd = "ssh -o StrictHostKeyChecking=no -p $gerritPort jenkins@$gerritHost gerrit review --message '$message' --label '$label' '$commit'".execute()
  def stdout = new StringBuffer(), stderr = new StringBuffer()
  cmd.consumeProcessOutput(stdout, stderr)
  def successful = cmd.waitFor(5, TimeUnit.MINUTES)
  if (!successful) {
    manager.listener.logger.println("Process execution timed out")
  }
  if ("$stdout".length() > 0) {
    manager.listener.logger.println("stdout: $stdout")
  }
  if ("$stderr".length() > 0) {
    manager.listener.logger.println("stderr: $stderr")
  }
}

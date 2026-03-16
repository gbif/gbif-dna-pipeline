package org.gbif.dna.spark.jobs;

public enum ExitCode {
  OK(0),
  INVALID_ARGS(1),
  JOB_FAILED(2),
  ILLEGAL_STATE(3);

  private final int code;

  ExitCode(int code) {
    this.code = code;
  }

  public int code() {
    return code;
  }
}
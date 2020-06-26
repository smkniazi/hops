package org.apache.hadoop.fs;

public enum CloudProvider {
  AWS("AWS"),
  AZURE("AZURE");

  private String name;

  CloudProvider(String name) { name = name;}
}

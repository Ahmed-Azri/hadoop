package org.apache.hadoop.io;

import java.io.IOException;

public interface Quantifiable {
  int getContentSize() throws IOException;
}

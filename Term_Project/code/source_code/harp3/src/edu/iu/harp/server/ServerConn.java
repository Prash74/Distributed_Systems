/*
 * Copyright 2013-2016 Indiana University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.iu.harp.server;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

/**
 * The connection object
 * 
 * @author zhangbj
 *
 */
public class ServerConn {

  private InputStream in;
  private Socket socket;

  /**
   * Connection as a server
   * 
   * @param out
   * @param in
   * @param socket
   */
  ServerConn(InputStream in, Socket socket) {
    this.socket = socket;
    this.in = in;
  }

  InputStream getInputDtream() {
    return this.in;
  }

  void close() {
    if (in != null || socket != null) {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
      }
      try {
        if (socket != null) {
          socket.close();
        }
      } catch (IOException e) {
      }
      in = null;
      socket = null;
    }
  }
}

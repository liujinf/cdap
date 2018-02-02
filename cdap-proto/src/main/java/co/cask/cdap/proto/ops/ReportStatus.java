/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.proto.ops;

/**
 * Represents the status of a program operation status report generation in an HTTP response.
 */
public class ReportStatus {
  private final String reportId;
  private final long startTs;
  private final ReportGenerationStatus status;

  public ReportStatus(String reportId, long startTs, ReportGenerationStatus status) {
    this.reportId = reportId;
    this.startTs = startTs;
    this.status = status;
  }

  public String getReportId() {
    return reportId;
  }

  public long getStartTs() {
    return startTs;
  }

  public co.cask.cdap.proto.ops.ReportGenerationStatus getStatus() {
    return status;
  }
}
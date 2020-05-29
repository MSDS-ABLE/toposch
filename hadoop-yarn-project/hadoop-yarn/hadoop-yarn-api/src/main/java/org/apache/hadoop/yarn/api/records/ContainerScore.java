package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.yarn.util.Records;
/**
 * Created by Junqing xiao 18.4.16
 */
public abstract class ContainerScore {

  public static ContainerScore newInstance(ContainerId id,double score) {
    ContainerScore containerScore = Records.newRecord(ContainerScore.class);
    containerScore.setId(id);
    containerScore.setScore(score);
    return containerScore;
  }

  public abstract ContainerId getId();
  public abstract void setId(ContainerId id);
  public abstract double getScore();
  public abstract void setScore(double score);
}

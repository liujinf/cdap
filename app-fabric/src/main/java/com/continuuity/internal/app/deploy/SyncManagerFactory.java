/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.stream.StreamConsumerFactory;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

/**
 *
 */
public final class SyncManagerFactory implements ManagerFactory {
  private final CConfiguration configuration;
  private final PipelineFactory<?> pFactory;
  private final LocationFactory lFactory;
  private final StoreFactory sFactory;
  private final StreamConsumerFactory streamConsumerFactory;
  private final QueueAdmin queueAdmin;
  private final DiscoveryServiceClient discoveryServiceClient;

  @Inject
  public SyncManagerFactory(CConfiguration configuration, PipelineFactory<?> pFactory,
                            LocationFactory lFactory, StoreFactory sFactory,
                            StreamConsumerFactory streamConsumerFactory, QueueAdmin queueAdmin,
                            DiscoveryServiceClient discoveryServiceClient) {
    this.configuration = configuration;
    this.pFactory = pFactory;
    this.lFactory = lFactory;
    this.sFactory = sFactory;
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public Manager<?, ?> create(ProgramTerminator programTerminator) {
    return new LocalManager(configuration, pFactory, lFactory, sFactory, programTerminator,
                            streamConsumerFactory, queueAdmin, discoveryServiceClient);
  }
}

package org.omnaest.datagrid;

import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.omnaest.utils.JSONHelper;
import org.omnaest.utils.StreamUtils;
import org.omnaest.utils.repository.CoreElementRepository;
import org.omnaest.utils.repository.ElementRepository;
import org.omnaest.utils.repository.IndexElementRepository;

public class DataGridUtils
{
    public static interface DataGrid extends AutoCloseable
    {
        public <D> IndexElementRepository<D> newIndexRepository(String name, Class<D> type);

        public <I, D> ElementRepository<I, D> newRepository(String name, Class<D> type, Function<Long, I> idSupplier);
    }

    public static DataGrid newLocalInstance()
    {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();
        dataStorageConfiguration.setWalMode(WALMode.FSYNC);
        Ignite ignite = Ignition.start(new IgniteConfiguration().setClientMode(false)
                                                                .setDataStorageConfiguration(dataStorageConfiguration)
                                                                .setPeerClassLoadingEnabled(false));
        return new DataGrid()
        {
            @Override
            public <D> IndexElementRepository<D> newIndexRepository(String name, Class<D> type)
            {
                return IndexElementRepository.of(this.newRepository(name, type, (id) ->
                {
                    return id;
                }));
            }

            @Override
            public <I, D> ElementRepository<I, D> newRepository(String name, Class<D> type, Function<Long, I> idSupplier)
            {
                IgniteAtomicLong atomicLong = ignite.atomicLong(name, 0l, true);
                CacheConfiguration<I, String> cacheConfiguration = new CacheConfiguration<>();
                cacheConfiguration.setName(name);
                cacheConfiguration.setCacheMode(CacheMode.LOCAL);
                cacheConfiguration.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
                IgniteCache<I, String> cache = ignite.getOrCreateCache(cacheConfiguration);
                @SuppressWarnings("resource")
                CoreElementRepository<I, D> coreElementRepository = new CoreElementRepository<I, D>()
                {
                    @Override
                    public void put(I id, D element)
                    {
                        cache.put(id, JSONHelper.prettyPrint(element));
                    }

                    @Override
                    public void remove(I id)
                    {
                        cache.remove(id);
                    }

                    @Override
                    public D get(I id)
                    {
                        return JSONHelper.readFromString(cache.get(id), type);
                    }

                    @Override
                    public long size()
                    {
                        return cache.sizeLong();
                    }

                    @Override
                    public Stream<I> ids()
                    {
                        return StreamUtils.fromIterable(cache)
                                          .map(entry -> entry.getKey());
                    }

                    @Override
                    public CoreElementRepository<I, D> clear()
                    {
                        cache.clear();
                        return this;
                    }

                    @Override
                    public void close()
                    {
                        CoreElementRepository.super.close();
                        cache.close();
                        atomicLong.close();
                    }
                };
                return coreElementRepository.toElementRepository(() -> idSupplier.apply(atomicLong.getAndIncrement()));
            }

            @Override
            public void close()
            {
                ignite.close();
            }
        };
    }
}

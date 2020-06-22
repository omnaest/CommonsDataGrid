package org.omnaest.datagrid;

import java.util.function.Function;
import java.util.stream.Stream;

import javax.cache.Cache;

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
import org.omnaest.utils.ComparatorUtils;
import org.omnaest.utils.EnumUtils;
import org.omnaest.utils.JSONHelper;
import org.omnaest.utils.JSONHelper.JsonStringConverter;
import org.omnaest.utils.StreamUtils;
import org.omnaest.utils.optional.NullOptional;
import org.omnaest.utils.repository.CoreElementRepository;
import org.omnaest.utils.repository.ElementRepository;
import org.omnaest.utils.repository.IndexElementRepository;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataGridUtils
{
    public static interface DataGrid extends AutoCloseable
    {
        public <D> IndexElementRepository<D> newIndexRepository(String name, Class<D> type, Class<?>... genericParameterTypes);

        public <I, D> ElementRepository<I, D> newRepository(String name, Function<Long, I> idSupplier, Class<D> type, Class<?>... genericParameterTypes);
    }

    private static class Data
    {
        @JsonProperty
        private String value;

        @JsonProperty
        private long index;

        public Data(String value, long index)
        {
            super();
            this.value = value;
            this.index = index;
        }

        protected Data()
        {
            super();
        }

        public String getValue()
        {
            return this.value;
        }

        public long getIndex()
        {
            return this.index;
        }

        @Override
        public String toString()
        {
            return "Data [value=" + this.value + ", index=" + this.index + "]";
        }

    }

    public static DataGrid newLocalInstance()
    {
        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();
        dataStorageConfiguration.setWalMode(WALMode.FSYNC);
        dataStorageConfiguration.setWalSegmentSize(16 * 1024 * 1024);
        dataStorageConfiguration.getDefaultDataRegionConfiguration()
                                .setPersistenceEnabled(true);
        Ignite ignite = Ignition.start(new IgniteConfiguration().setClientMode(false)
                                                                .setAutoActivationEnabled(true)
                                                                .setConsistentId("testid")
                                                                .setDataStorageConfiguration(dataStorageConfiguration)
                                                                .setPeerClassLoadingEnabled(false));
        ignite.cluster()
              .active(true);

        return new DataGrid()
        {
            @Override
            public <D> IndexElementRepository<D> newIndexRepository(String name, Class<D> type, Class<?>... genericParameterTypes)
            {
                return IndexElementRepository.of(this.newRepository(name, (id) -> id, type, genericParameterTypes));
            }

            @Override
            public <I, D> ElementRepository<I, D> newRepository(String name, Function<Long, I> idSupplier, Class<D> type, Class<?>... genericParameterTypes)
            {
                IgniteAtomicLong indexIdCounter = ignite.atomicLong(name + ".index.id", 0l, true);
                IgniteAtomicLong dataIdCounter = ignite.atomicLong(name + ".data.id", 0l, true);

                CacheConfiguration<I, Data> cacheConfiguration = new CacheConfiguration<>();
                cacheConfiguration.setName(name);
                cacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
                cacheConfiguration.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

                IgniteCache<I, Data> cache = ignite.getOrCreateCache(cacheConfiguration);
                //                ignite.cluster()
                //                      .enableWal(name);

                JsonStringConverter<D> converter = JSONHelper.converter(type, genericParameterTypes)
                                                             .withExceptionHandler(e ->
                                                             {
                                                                 throw new IllegalStateException(e);
                                                             });

                @SuppressWarnings("resource")
                CoreElementRepository<I, D> coreElementRepository = new CoreElementRepository<I, D>()
                {
                    @Override
                    public void put(I id, D element)
                    {
                        cache.put(id, new Data(converter.serializer()
                                                        .apply(element),
                                               dataIdCounter.getAndIncrement()));
                    }

                    @Override
                    public void remove(I id)
                    {
                        cache.remove(id);
                    }

                    @Override
                    public NullOptional<D> get(I id)
                    {
                        return NullOptional.ofNullable(cache.getEntry(id))
                                           .mapToNullable(entry -> entry.getValue())
                                           .mapToNullable(data -> converter.deserializer()
                                                                           .apply(data.getValue()));
                    }

                    @Override
                    public long size()
                    {
                        return cache.sizeLong();
                    }

                    @Override
                    public Stream<I> ids(IdOrder idOrder)
                    {
                        return EnumUtils.decideOn(idOrder)
                                        .ifEqualTo(IdOrder.FROM_NEWEST_TO_OLDEST, () -> StreamUtils.fromIterable(cache)
                                                                                                   .sorted(ComparatorUtils.builder()
                                                                                                                          .of(Cache.Entry.class)
                                                                                                                          .with(entry -> ((Data) entry.getValue()).getIndex())
                                                                                                                          .reverse()
                                                                                                                          .build())
                                                                                                   .map(entry -> entry.getKey()))
                                        .orIfEqualTo(IdOrder.FROM_OLDEST_TO_NEWEST, () -> StreamUtils.fromIterable(cache)
                                                                                                     .sorted(ComparatorUtils.builder()
                                                                                                                            .of(Cache.Entry.class)
                                                                                                                            .with(entry -> ((Data) entry.getValue()).getIndex())
                                                                                                                            .build())
                                                                                                     .map(entry -> entry.getKey()))
                                        .orIfEqualTo(IdOrder.ARBITRARY, () -> StreamUtils.fromIterable(cache)
                                                                                         .map(entry -> entry.getKey()))
                                        .orElseThrow(() -> new IllegalArgumentException("Unsupported IdOrder value: " + idOrder));
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
                        indexIdCounter.close();
                        dataIdCounter.close();
                    }
                };
                return coreElementRepository.toElementRepository(() -> idSupplier.apply(indexIdCounter.getAndIncrement()));
            }

            @Override
            public void close()
            {
                ignite.cluster()
                      .active(false);
                ignite.close();
            }
        };
    }
}

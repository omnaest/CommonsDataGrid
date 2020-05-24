package org.omnaest.datagrid;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.omnaest.utils.JSONHelper;
import org.omnaest.utils.StreamUtils;
import org.omnaest.utils.repository.IndexElementRepository;

public class DataGridUtils
{
    public static interface DataGrid extends AutoCloseable
    {
        public <D> IndexElementRepository<D> newIndexRepository(String name, Class<D> type);
    }

    public static DataGrid newLocalInstance()
    {
        Ignite ignite = Ignition.start(new IgniteConfiguration().setClientMode(false)
                                                                .setPeerClassLoadingEnabled(false));
        return new DataGrid()
        {
            @Override
            public <D> IndexElementRepository<D> newIndexRepository(String name, Class<D> type)
            {
                IgniteCache<Long, String> cache = ignite.getOrCreateCache(name);
                return new IndexElementRepository<D>()
                {
                    private AtomicLong idCounter = new AtomicLong();

                    @Override
                    public Long add(D element)
                    {
                        long id = this.idCounter.getAndIncrement();
                        cache.put(id, JSONHelper.prettyPrint(element));
                        return (long) id;
                    }

                    @Override
                    public void put(Long id, D element)
                    {
                        cache.put(id, JSONHelper.prettyPrint(element));
                    }

                    @Override
                    public void remove(Long id)
                    {
                        cache.remove(id);
                    }

                    @Override
                    public D get(Long id)
                    {
                        return JSONHelper.readFromString(cache.get(id), type);
                    }

                    @Override
                    public long size()
                    {
                        return cache.sizeLong();
                    }

                    @Override
                    public Stream<Long> ids()
                    {
                        return StreamUtils.fromIterable(cache)
                                          .map(entry -> entry.getKey()
                                                             .longValue());
                    }

                    @Override
                    public IndexElementRepository<D> clear()
                    {
                        cache.clear();
                        return this;
                    }
                };
            }

            @Override
            public void close()
            {
                ignite.close();
            }
        };
    }
}

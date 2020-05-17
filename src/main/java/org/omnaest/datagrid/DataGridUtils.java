package org.omnaest.datagrid;

import java.util.concurrent.atomic.AtomicInteger;
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
        public <D> IndexElementRepository<D> newRepository(Class<D> type);
    }

    public static DataGrid newLocalInstance()
    {
        Ignite ignite = Ignition.start(new IgniteConfiguration().setClientMode(false)
                                                                .setPeerClassLoadingEnabled(false));
        return new DataGrid()
        {
            @Override
            public <D> IndexElementRepository<D> newRepository(Class<D> type)
            {
                // Create an IgniteCache and put some values in it.
                IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");

                return new IndexElementRepository<D>()
                {
                    private AtomicInteger idCounter = new AtomicInteger();

                    @Override
                    public Long add(D element)
                    {
                        int id = this.idCounter.getAndIncrement();
                        cache.put(id, JSONHelper.prettyPrint(element));
                        return (long) id;
                    }

                    @Override
                    public void put(Long id, D element)
                    {
                        cache.put(id.intValue(), JSONHelper.prettyPrint(element));
                    }

                    @Override
                    public void remove(Long id)
                    {
                        cache.remove(id.intValue());
                    }

                    @Override
                    public D get(Long id)
                    {
                        return JSONHelper.readFromString(cache.get(id.intValue()), type);
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

package org.omnaest.datagrid;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import org.omnaest.datagrid.DataGridUtils.DataGrid;
import org.omnaest.utils.MapUtils;
import org.omnaest.utils.repository.IndexElementRepository;

public class DataGridUtilsTest
{

    @SuppressWarnings("rawtypes")
    @Test
    public void testNewLocalInstance() throws Exception
    {
        try (DataGrid dataGrid = DataGridUtils.newLocalInstance())
        {
            IndexElementRepository<Map> repository = dataGrid.newIndexRepository("test", Map.class);

            Long id1 = repository.add(MapUtils.builder()
                                              .put("key1", "value1")
                                              .put("key2", "value2")
                                              .build());
            Long id2 = repository.add(MapUtils.builder()
                                              .put("key3", "value3")
                                              .put("key4", "value4")
                                              .build());
            repository.put(3l, MapUtils.builder()
                                       .put("key5", "value5")
                                       .put("key6", "value6")
                                       .build());

            assertEquals(3, repository.size());
            assertEquals(Arrays.asList(id1, id2, 3l)
                               .stream()
                               .collect(Collectors.toSet()),
                         repository.ids()
                                   .collect(Collectors.toSet()));
            assertEquals(MapUtils.builder()
                                 .put("key1", "value1")
                                 .put("key2", "value2")
                                 .build(),
                         repository.get(id1));
            assertEquals(MapUtils.builder()
                                 .put("key3", "value3")
                                 .put("key4", "value4")
                                 .build(),
                         repository.get(id2));
            assertEquals(MapUtils.builder()
                                 .put("key5", "value5")
                                 .put("key6", "value6")
                                 .build(),
                         repository.get(3l));

        }
    }

}

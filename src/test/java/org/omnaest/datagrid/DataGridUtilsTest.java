package org.omnaest.datagrid;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
            try (IndexElementRepository<Map> repository = dataGrid.newIndexRepository("test", Map.class, String.class, String.class))
            {
                System.out.println("----");
                repository.ids()
                          .forEach(id ->
                          {
                              System.out.println("ID: " + id);
                          });
                repository.entries()
                          .forEach(entry ->
                          {
                              System.out.println(entry);
                          });

                System.out.println("----");

                repository.clear();

                Long id1 = repository.add(MapUtils.builder()
                                                  .put("key1", "value1")
                                                  .put("key2", "value2")
                                                  .build());
                System.out.println(id1);
                Long id2 = repository.add(MapUtils.builder()
                                                  .put("key3", "value3")
                                                  .put("key4", "value4")
                                                  .build());
                System.out.println(id2);

                Long id3 = 3l;
                repository.put(id3, MapUtils.builder()
                                            .put("key5", "value5")
                                            .put("key6", "value6")
                                            .build());

                System.out.println(repository.ids()
                                             .collect(Collectors.toList()));

                System.out.println("----");

                assertEquals(3, repository.size());
                assertEquals(Arrays.asList(id1, id2, id3)
                                   .stream()
                                   .collect(Collectors.toSet()),
                             repository.ids()
                                       .collect(Collectors.toSet()));
                assertEquals(MapUtils.builder()
                                     .put("key1", "value1")
                                     .put("key2", "value2")
                                     .build(),
                             repository.getValue(id1));
                assertEquals(MapUtils.builder()
                                     .put("key3", "value3")
                                     .put("key4", "value4")
                                     .build(),
                             repository.getValue(id2));
                assertEquals(MapUtils.builder()
                                     .put("key5", "value5")
                                     .put("key6", "value6")
                                     .build(),
                             repository.getValue(id3));

                System.out.println("----");
                repository.clear();
                IntStream.range(0, 1000)
                         .parallel()
                         .forEach(ii ->
                         {
                             repository.add(MapUtils.builder()
                                                    .put("key" + ii, "value" + ii)
                                                    .build());
                             System.out.println(ii);
                         });
                assertEquals(1000, repository.size());
            }

        }
    }

}

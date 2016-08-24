using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Vibrant.Tsdb.Ats.Serialization;

namespace Vibrant.Tsdb.Ats
{
   public class AtsDynamicStorage<TKey, TEntry> : IDynamicStorage<TKey, TEntry>, IDynamicStorageSelector<TKey, TEntry>, IDisposable
     where TEntry : IAtsEntry, new()
   {
      public const int DefaultReadParallelism = 50;
      public const int DefaultWriteParallelism = 50;

      private readonly StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>[] _defaultSelection;
      private object _sync = new object();
      private string _tableName;
      private CloudStorageAccount _account;
      private CloudTableClient _client;
      private Task<CloudTable> _table;
      private IPartitionProvider<TKey> _partitioningProvider;
      private IKeyConverter<TKey> _keyConverter;
      private IConcurrencyControl _cc;
      private EntryEqualityComparer<TKey, TEntry> _comparer;

      public AtsDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency, IPartitionProvider<TKey> partitioningProvider, IKeyConverter<TKey> keyConverter )
      {
         _cc = concurrency;
         _tableName = tableName;
         _account = CloudStorageAccount.Parse( connectionString );
         _client = _account.CreateCloudTableClient();
         _partitioningProvider = partitioningProvider;
         _comparer = new EntryEqualityComparer<TKey, TEntry>();
         _keyConverter = keyConverter;
         _defaultSelection = new[] { new StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>( this ) };

         _client.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;
      }

      public AtsDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency, IPartitionProvider<TKey> partitioningProvider )
         : this( tableName, connectionString, concurrency, partitioningProvider, DefaultKeyConverter<TKey>.Current )
      {
      }

      public AtsDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency )
         : this( tableName, connectionString, concurrency, new YearlyPartitioningProvider<TKey>() )
      {
      }

      public AtsDynamicStorage( string tableName, string connectionString )
         : this( tableName, connectionString, new ConcurrencyControl( DefaultReadParallelism, DefaultWriteParallelism ), new YearlyPartitioningProvider<TKey>() )
      {
      }

      public IEnumerable<StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>> GetStorage( TKey id, DateTime? from, DateTime? to )
      {
         return _defaultSelection;
      }

      public IDynamicStorage<TKey, TEntry> GetStorage( TKey key, TEntry entry )
      {
         return this;
      }

      public Task DeleteAsync( IEnumerable<TKey> ids )
      {
         return DeleteAllInternal( ids );
      }

      public Task DeleteAsync( IEnumerable<TKey> ids, DateTime to )
      {
         return DeleteUntilInternal( ids, to );
      }

      public Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         return DeleteRangeInternal( ids, from, to );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         return ReadAllInternal( ids, sort );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         return ReadUntilInternal( ids, to, sort );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         return ReadRangeInternal( ids, from, to, sort );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids, int count )
      {
         return ReadLatestInternal( ids, count );
      }

      public Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         return WriteInternal( series );
      }

      public Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken )
      {
         return ReadRangeSegmentedInternal( id, from, to, segmentSize, (ContinuationToken)continuationToken );
      }

      private async Task<int> DeleteAllInternal( IEnumerable<TKey> ids )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteAllInternal( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return tasks.Sum( x => x.Result );
      }

      private async Task<int> DeleteAllInternal( TKey id )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreatePartitionFilter( id ) );

         var count = await DeleteInternal( id, fullQuery ).ConfigureAwait( false );

         return count;
      }

      private async Task<int> DeleteUntilInternal( IEnumerable<TKey> ids, DateTime to )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteUntilInternal( id, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return tasks.Sum( x => x.Result );
      }

      private async Task<int> DeleteUntilInternal( TKey id, DateTime to )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreateBeforeFilter( id, to ) );

         var count = await DeleteInternal( id, fullQuery ).ConfigureAwait( false );

         return count;
      }

      private async Task<int> DeleteRangeInternal( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteRangeInternal( id, from, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return tasks.Sum( x => x.Result );
      }

      private async Task<int> DeleteRangeInternal( TKey id, DateTime from, DateTime to )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreateGeneralFilter( id, from, to ) );

         var count = await DeleteInternal( id, fullQuery ).ConfigureAwait( false );

         return count;
      }

      private async Task<MultiReadResult<TKey, TEntry>> ReadLatestInternal( IEnumerable<TKey> ids, int count )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadLatestInternal( id, Sort.Descending, count ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TKey, TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TKey, TEntry>> ReadLatestInternal( TKey id, Sort sort, int count )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreatePartitionFilter( id ) );

         var entries = await ReadInternal( id, fullQuery, sort, count ).ConfigureAwait( false );

         return new ReadResult<TKey, TEntry>( id, sort, entries );
      }

      private async Task<MultiReadResult<TKey, TEntry>> ReadAllInternal( IEnumerable<TKey> ids, Sort sort )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadAllInternal( id, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TKey, TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TKey, TEntry>> ReadAllInternal( TKey id, Sort sort )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreatePartitionFilter( id ) );

         var entries = await ReadInternal( id, fullQuery, sort, null ).ConfigureAwait( false );

         return new ReadResult<TKey, TEntry>( id, sort, entries );
      }

      private async Task<MultiReadResult<TKey, TEntry>> ReadUntilInternal( IEnumerable<TKey> ids, DateTime to, Sort sort )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadUntilInternal( id, to, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TKey, TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TKey, TEntry>> ReadUntilInternal( TKey id, DateTime to, Sort sort )
      {
         var query = new TableQuery<TsdbTableEntity>()
            .Where( CreateBeforeFilter( id, to ) );

         var entries = await ReadInternal( id, query, sort, null ).ConfigureAwait( false );

         return new ReadResult<TKey, TEntry>( id, sort, entries );
      }

      private async Task<MultiReadResult<TKey, TEntry>> ReadRangeInternal( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadRangeInternal( id, from, to, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TKey, TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TKey, TEntry>> ReadRangeInternal( TKey id, DateTime from, DateTime to, Sort sort )
      {
         // there's twos ways to accomplish this take:
         //  1. Super fast (iterate partitions and create query for each)
         //  2. Normal (use single query)

         if( _partitioningProvider is IIterablePartitionProvider<TKey> )
         {
            // use method 1 (Super fast)
            var tasks = new List<Task<List<TEntry>>>();
            var iterable = (IIterablePartitionProvider<TKey>)_partitioningProvider;
            foreach( var partitionRange in iterable.IteratePartitions( id, from, to ) )
            {
               var specificQuery = new TableQuery<TsdbTableEntity>()
                  .Where( CreateSpecificPartitionFilter( id, from, to, partitionRange ) );

               tasks.Add( ReadInternal( id, specificQuery, sort, null ) );
            }

            await Task.WhenAll( tasks ).ConfigureAwait( false );

            var queryResults = tasks.Select( x => x.Result );
            if( sort == Sort.Ascending )
            {
               queryResults = queryResults.Reverse();
            }

            // combine the results and return it
            return new ReadResult<TKey, TEntry>( id, sort, queryResults.SelectMany( x => x ).ToList() );
         }
         else
         {
            // use method 2 (Normal)
            var generalQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreateGeneralFilter( id, from, to ) );

            var entries = await ReadInternal( id, generalQuery, sort, null ).ConfigureAwait( false );

            return new ReadResult<TKey, TEntry>( id, sort, entries );
         }
      }

      private Task<SegmentedReadResult<TKey, TEntry>> ReadRangeSegmentedInternal( TKey id, DateTime? from, DateTime? to, int segmentSize, ContinuationToken continuationToken )
      {
         to = continuationToken?.To ?? to;

         string filter = null;
         if( from.HasValue && to.HasValue )
         {
            filter = CreateGeneralFilter( id, from.Value, to.Value );
         }
         else if( !from.HasValue && to.HasValue )
         {
            filter = CreateBeforeFilter( id, to.Value );
         }
         else if( from.HasValue && !to.HasValue )
         {
            filter = CreateAfterFilter( id, from.Value );
         }
         else
         {
            filter = CreatePartitionFilter( id );
         }

         var generalQuery = new TableQuery<TsdbTableEntity>()
            .Where( filter );

         return ReadSegmentedInternal( id, generalQuery, segmentSize );
      }

      private Task<SegmentedReadResult<TKey, TEntry>> ReadRangeSegmentedInternal( TKey id, int segmentSize, ContinuationToken continuationToken )
      {
         DateTime? to = continuationToken?.To;

         if( to.HasValue )
         {
            var generalQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreateBeforeFilter( id, to.Value ) );

            return ReadSegmentedInternal( id, generalQuery, segmentSize );
         }
         else
         {
            var generalQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreatePartitionFilter( id ) );

            return ReadSegmentedInternal( id, generalQuery, segmentSize );
         }
      }

      private async Task<List<TEntry>> ReadInternal( TKey id, TableQuery<TsdbTableEntity> query, Sort sort, int? take )
      {
         var table = await GetTable().ConfigureAwait( false );

         List<TEntry> results = new List<TEntry>();

         int taken = 0;
         TableContinuationToken token = null;
         do
         {
            using( await _cc.ReadAsync().ConfigureAwait( false ) )
            {
               var filteredQuery = query;
               if( take.HasValue )
               {
                  int toTake = take.Value - taken;
                  filteredQuery = filteredQuery.Take( toTake );
               }

               var rows = await table.ExecuteQuerySegmentedAsync( filteredQuery, token ).ConfigureAwait( false );
               var entries = Convert( rows );
               results.AddRange( entries );
               token = rows.ContinuationToken;

               taken += rows.Results.Count;
            }
         }
         while( token != null && ( !take.HasValue || taken < take ) );

         if( sort == Sort.Ascending )
         {
            results.Reverse();
         }

         return results;
      }

      private async Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedInternal( TKey id, TableQuery<TsdbTableEntity> query, int segmentSize )
      {
         var table = await GetTable().ConfigureAwait( false );

         List<TEntry> results = new List<TEntry>( segmentSize );
         List<TsdbTableEntity> allRows = new List<TsdbTableEntity>( segmentSize );

         bool isLastFull = true;
         TableContinuationToken token = null;
         DateTime? to = null;
         int read = 0;
         do
         {
            using( await _cc.ReadAsync().ConfigureAwait( false ) )
            {
               var rows = await table.ExecuteQuerySegmentedAsync( query, token ).ConfigureAwait( false );
               var entries = Convert( rows );

               isLastFull = rows.Results.Count == 1000;

               // add required items, and no more
               if( segmentSize >= read + rows.Results.Count )
               {
                  read += rows.Results.Count;
                  results.AddRange( entries );
                  allRows.AddRange( rows );
               }
               else
               {
                  var take = segmentSize - read;

                  foreach( var entry in entries.Take( take ) )
                  {
                     read++;
                     results.Add( entry );
                  }
                  foreach( var row in rows.Take( take ) )
                  {
                     allRows.Add( row );
                  }
               }

               token = rows.ContinuationToken;
               if( read == segmentSize ) // short circuit
               {
                  token = null;
               }
            }
         }
         while( token != null );

         // calculate continuation token
         if( isLastFull )
         {
            to = results[ results.Count - 1 ].GetTimestamp();
         }
         else
         {
            to = null;
         }

         return new SegmentedReadResult<TKey, TEntry>( id, Sort.Descending, new ContinuationToken( isLastFull, to ), results, () => DeleteInternal( id, allRows ) );
      }

      private async Task<int> DeleteInternal( TKey id, List<TsdbTableEntity> entities )
      {
         var table = await GetTable().ConfigureAwait( false );

         int count = 0;

         TableContinuationToken token = null;
         do
         {
            // iterate by partition and 100s
            var tasks = new List<Task<int>>();
            foreach( var kvp in IterateByPartition( entities ) )
            {
               var partitionKey = kvp.Key;
               var items = kvp.Value;

               // schedule parallel execution
               tasks.Add( DeleteInternalLocked( items ) );
            }
            await Task.WhenAll( tasks ).ConfigureAwait( false );

            count += tasks.Sum( x => x.Result );
         }
         while( token != null );

         return count;
      }

      private async Task<int> DeleteInternal( TKey id, TableQuery<TsdbTableEntity> query )
      {
         var table = await GetTable().ConfigureAwait( false );

         int count = 0;

         TableContinuationToken token = null;
         do
         {
            TableQuerySegment<TsdbTableEntity> rows;

            using( await _cc.ReadAsync().ConfigureAwait( false ) )
            {
               rows = await table.ExecuteQuerySegmentedAsync( query, token ).ConfigureAwait( false );
               foreach( var row in rows )
               {
                  row.ETag = "*";
               }
               token = rows.ContinuationToken;
            }

            // iterate by partition and 100s

            var tasks = new List<Task<int>>();
            foreach( var kvp in IterateByPartition( rows ) )
            {
               var partitionKey = kvp.Key;
               var items = kvp.Value;

               // schedule parallel execution
               tasks.Add( DeleteInternalLocked( items ) );
            }
            await Task.WhenAll( tasks ).ConfigureAwait( false );

            count += tasks.Sum( x => x.Result );
         }
         while( token != null );

         return count;
      }

      private async Task<int> DeleteInternalLocked( List<TsdbTableEntity> entries )
      {
         int count = 0;

         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            var table = await GetTable().ConfigureAwait( false );
            if( entries.Count == 1 )
            {
               var operation = TableOperation.Delete( entries[ 0 ] );
               await table.ExecuteAsync( operation ).ConfigureAwait( false );
            }
            else
            {
               var operation = new TableBatchOperation();
               foreach( var entity in entries )
               {
                  count++;
                  operation.Delete( entity );
               }
               await table.ExecuteBatchAsync( operation ).ConfigureAwait( false );
            }
         }

         return count;
      }

      private async Task WriteInternal( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         List<Task> tasks = new List<Task>();

         foreach( var kvp in IterateByPartition( series ) )
         {
            var partitionKey = kvp.Key;
            var items = kvp.Value;

            // schedule parallel execution
            tasks.Add( WriteInternalLocked( partitionKey, items ) );
         }

         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      private async Task WriteInternalLocked( string partitionKey, List<TEntry> entries )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            var table = await GetTable().ConfigureAwait( false );
            if( entries.Count == 1 )
            {
               var operation = TableOperation.InsertOrReplace( Convert( entries, partitionKey ).First() );
               await table.ExecuteAsync( operation ).ConfigureAwait( false );
            }
            else
            {
               var operation = new TableBatchOperation();
               foreach( var entity in Convert( entries, partitionKey ) )
               {
                  operation.InsertOrReplace( entity );
               }
               await table.ExecuteBatchAsync( operation ).ConfigureAwait( false );
            }
         }
      }

      private IEnumerable<TsdbTableEntity> Convert( IEnumerable<TEntry> entries, string partitionKey )
      {
         var stream = new MemoryStream();
         using( var writer = AtsSerializer.CreateWriter( stream ) )
         {
            foreach( var entry in entries )
            {
               yield return Convert( writer, stream, entry, partitionKey );
               stream.Seek( 0, SeekOrigin.Begin );
               stream.SetLength( 0 );
            }
         }
      }

      private TsdbTableEntity Convert( BinaryWriter writer, MemoryStream stream, TEntry entry, string partitionKey )
      {
         var entity = new TsdbTableEntity();
         entity.RowKey = AtsKeyCalculator.CalculateRowKey( entry.GetTimestamp() );
         entity.PartitionKey = partitionKey;
         AtsSerializer.SerializeEntry<TKey, TEntry>( writer, entry );
         entity.P0 = stream.ToArray();
         return entity;
      }

      private IEnumerable<TEntry> Convert( IEnumerable<TsdbTableEntity> entities )
      {
         foreach( var entity in entities )
         {
            var stream = new MemoryStream( entity.P0 );
            using( var reader = AtsSerializer.CreateReader( stream ) )
            {
               yield return Convert( reader, entity );
            }
         }
      }

      private TEntry Convert( BinaryReader reader, TsdbTableEntity entity )
      {
         return AtsSerializer.DeserializeEntry<TKey, TEntry>( reader );
      }

      private IEnumerable<KeyValuePair<string, List<TEntry>>> IterateByPartition( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         Dictionary<string, List<TEntry>> lookup = new Dictionary<string, List<TEntry>>();

         var hashkeys = new HashSet<EntryKey<TKey>>();
         foreach( var serie in series )
         {
            var key = serie.GetKey();
            var id = _keyConverter.Convert( key );

            foreach( var entry in serie.GetEntries() )
            {
               var timestamp = entry.GetTimestamp();
               var hashkey = new EntryKey<TKey>( key, timestamp );

               if( !hashkeys.Contains( hashkey ) )
               {
                  var pk = AtsKeyCalculator.CalculatePartitionKey( id, key, timestamp, _partitioningProvider );
                  List<TEntry> items;
                  if( !lookup.TryGetValue( pk, out items ) )
                  {
                     items = new List<TEntry>();
                     lookup.Add( pk, items );
                  }
                  items.Add( entry );
                  if( items.Count == 100 )
                  {
                     lookup.Remove( pk );
                     yield return new KeyValuePair<string, List<TEntry>>( pk, items );
                  }

                  hashkeys.Add( hashkey );
               }
            }
         }

         foreach( var kvp in lookup )
         {
            yield return kvp;
         }
      }

      private IEnumerable<KeyValuePair<string, List<TsdbTableEntity>>> IterateByPartition( IEnumerable<TsdbTableEntity> entities )
      {
         Dictionary<string, List<TsdbTableEntity>> lookup = new Dictionary<string, List<TsdbTableEntity>>();

         foreach( var entity in entities )
         {
            var pk = entity.PartitionKey;

            List<TsdbTableEntity> items;
            if( !lookup.TryGetValue( pk, out items ) )
            {
               items = new List<TsdbTableEntity>();
               lookup.Add( pk, items );
            }

            items.Add( entity );
            if( items.Count == 100 )
            {
               lookup.Remove( pk );
               yield return new KeyValuePair<string, List<TsdbTableEntity>>( pk, items );
            }
         }

         foreach( var kvp in lookup )
         {
            yield return kvp;
         }
      }

      private async Task<CloudTable> GetTableLocked()
      {
         var table = _client.GetTableReference( _tableName );
         await table.CreateIfNotExistsAsync().ConfigureAwait( false );
         return table;
      }

      private Task<CloudTable> GetTable()
      {
         lock( _sync )
         {
            if( _table == null || _table.IsFaulted || _table.IsCanceled )
            {
               _table = GetTableLocked();
            }
            return _table;
         }
      }

      private string CreateSpecificPartitionFilter( TKey key, DateTime from, DateTime to, string partitionKeyRange )
      {
         var id = _keyConverter.Convert( key );
         var fromRowKey = AtsKeyCalculator.CalculateRowKey( from );
         var toRowKey = AtsKeyCalculator.CalculateRowKey( to );
         var partitionKey = AtsKeyCalculator.CalculatePartitionKey( id, partitionKeyRange );

         return TableQuery.CombineFilters(
               TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.Equal, partitionKey ),
            TableOperators.And,
               TableQuery.CombineFilters(
                  TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.LessThanOrEqual, fromRowKey ),
                  TableOperators.And,
                  TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.GreaterThan, toRowKey ) ) );
      }

      private string CreateGeneralFilter( TKey key, DateTime from, DateTime to )
      {
         var id = _keyConverter.Convert( key );
         var fromRowKey = AtsKeyCalculator.CalculateRowKey( from );
         var toRowKey = AtsKeyCalculator.CalculateRowKey( to );
         var fromPartitionKey = AtsKeyCalculator.CalculatePartitionKey( id, key, from, _partitioningProvider );
         var toPartitionKey = AtsKeyCalculator.CalculatePartitionKey( id, key, to.AddTicks( -1 ), _partitioningProvider ); // -1 tick because it is an approximation value and we use gte operation

         return TableQuery.CombineFilters(
               TableQuery.CombineFilters(
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.LessThanOrEqual, fromPartitionKey ),
                  TableOperators.And,
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.GreaterThanOrEqual, toPartitionKey ) ),
            TableOperators.And,
               TableQuery.CombineFilters(
                  TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.LessThanOrEqual, fromRowKey ),
                  TableOperators.And,
                  TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.GreaterThan, toRowKey ) ) );
      }

      private string CreateAfterFilter( TKey key, DateTime from )
      {
         var id = _keyConverter.Convert( key );
         var fromRowKey = AtsKeyCalculator.CalculateRowKey( from );
         var fromPartitionKey = AtsKeyCalculator.CalculatePartitionKey( id, key, from, _partitioningProvider ); // 7125
         var toPartitionKey = AtsKeyCalculator.CalculateMaxPartitionKey( id, key, _partitioningProvider ); // 0000

         return TableQuery.CombineFilters(
               TableQuery.CombineFilters(
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.LessThanOrEqual, fromPartitionKey ),
                  TableOperators.And,
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.GreaterThanOrEqual, toPartitionKey ) ),
            TableOperators.And,
               TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.LessThanOrEqual, fromRowKey ) );
      }

      private string CreateBeforeFilter( TKey key, DateTime to )
      {
         var id = _keyConverter.Convert( key );
         var toRowKey = AtsKeyCalculator.CalculateRowKey( to );
         var toPartitionKey = AtsKeyCalculator.CalculatePartitionKey( id, key, to, _partitioningProvider ); // 7125
         var fromPartitionKey = AtsKeyCalculator.CalculateMinPartitionKey( id, key, _partitioningProvider ); // 9999

         return TableQuery.CombineFilters(
               TableQuery.CombineFilters(
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.LessThanOrEqual, fromPartitionKey ),
                  TableOperators.And,
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.GreaterThanOrEqual, toPartitionKey ) ),
            TableOperators.And,
               TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.GreaterThan, toRowKey ) );
      }

      private string CreatePartitionFilter( TKey key )
      {
         var id = _keyConverter.Convert( key );
         var fromPartitionKey = AtsKeyCalculator.CalculateMaxPartitionKey( id, key, _partitioningProvider ); // 0000
         var toPartitionKey = AtsKeyCalculator.CalculateMinPartitionKey( id, key, _partitioningProvider ); // 9999

         return TableQuery.CombineFilters(
            TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.GreaterThanOrEqual, fromPartitionKey ),
            TableOperators.And,
            TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.LessThanOrEqual, toPartitionKey ) );
      }

      #region IDisposable Support

      private bool _disposed = false; // To detect redundant calls

      protected virtual void Dispose( bool disposing )
      {
         if( !_disposed )
         {
            if( disposing )
            {

            }

            _disposed = true;
         }
      }

      // This code added to correctly implement the disposable pattern.
      public void Dispose()
      {
         Dispose( true );
      }

      #endregion
   }
}

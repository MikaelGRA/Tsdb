using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.CosmosDB;
using Microsoft.Azure.CosmosDB.Table;
using Microsoft.Azure.Storage;
using Vibrant.Tsdb.CosmosTables.Helpers;
using Vibrant.Tsdb.CosmosTables.Serialization;
using Vibrant.Tsdb.Helpers;

namespace Vibrant.Tsdb.CosmosTables
{
   /// <summary>
   /// Implementation of IVolumeStorage that uses Azure Table Storage
   /// as its backend. 
   /// </summary>
   public class CosmosTablesVolumeStorage<TKey, TEntry> : IStorage<TKey, TEntry>, IStorageSelector<TKey, TEntry>, IDisposable
      where TEntry : ICosmosTablesEntry, new()
   {
      public const int DefaultReadParallelism = 15 * 3;
      public const int DefaultWriteParallelism = 30 * 3;
      public const int DefaultThroughputUnits = 1000;

      private readonly StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>[] _defaultSelection;
      private object _sync = new object();
      private string _tableName;
      private CloudStorageAccount _account;
      private CloudTableClient _client;
      private Task<CloudTable> _table;
      private IPartitionProvider<TKey> _partitioningProvider;
      private IKeyConverter<TKey> _keyConverter;
      private IConcurrencyControl _cc;
      private int _throughputUnits;

      public CosmosTablesVolumeStorage( string tableName, string connectionString, int throughputUnits, IConcurrencyControl concurrency, IPartitionProvider<TKey> provider, IKeyConverter<TKey> keyConverter )
      {
         _cc = concurrency;
         _tableName = tableName;
         _account = CloudStorageAccount.Parse( connectionString );
         var policy = new TableConnectionPolicy
         {
            EnableEndpointDiscovery = true,
            MaxConnectionLimit = 1000,
            MaxRetryAttemptsOnThrottledRequests = 5,
            MaxRetryWaitTimeInSeconds = 10,
            UseDirectMode = true,
            UseTcpProtocol = true
         };
         _client = _account.CreateCloudTableClient( policy );
         _partitioningProvider = provider;
         _keyConverter = keyConverter;
         _defaultSelection = new[] { new StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>( this ) };
         _client.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;
         _throughputUnits = throughputUnits;
      }

      public CosmosTablesVolumeStorage( string tableName, string connectionString, int throughputUnits, IConcurrencyControl concurrency, IPartitionProvider<TKey> provider )
         : this( tableName, connectionString, throughputUnits, concurrency, provider, DefaultKeyConverter<TKey>.Current )
      {
      }

      public CosmosTablesVolumeStorage( string tableName, string connectionString, IConcurrencyControl concurrency )
         : this( tableName, connectionString, DefaultThroughputUnits, concurrency, new YearlyPartitioningProvider<TKey>() )
      {
      }

      public CosmosTablesVolumeStorage( string tableName, string connectionString )
         : this( tableName, connectionString, DefaultThroughputUnits, new ConcurrencyControl( DefaultReadParallelism, DefaultWriteParallelism ), new YearlyPartitioningProvider<TKey>() )
      {
      }

      #region Public

      public IEnumerable<StorageSelection<TKey, TEntry, IStorage<TKey, TEntry>>> GetStorage( TKey id, DateTime? from, DateTime? to )
      {
         return _defaultSelection;
      }

      public IStorage<TKey, TEntry> GetStorage( TKey key, TEntry entry )
      {
         return this;
      }

      /// <summary>
      /// Writes the specified entries.
      /// </summary>
      /// <param name="items">The entries to be written.</param>
      /// <returns></returns>
      public async Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         List<Task> tasks = new List<Task>();

         // split all entries by their id
         foreach( var entry in IterateByPartition( series ) )
         {
            var id = entry.Key;
            var from = entry.From;
            var to = entry.To.AddTicks( 1 ); // must be inclusive on the last measure point because we may be overriding it
            var entries = entry.Entries;

            tasks.Add( StoreForId( id, entries, from, to ) );
         }

         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteForId( id, from, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime to )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteForId( id, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteAllForId( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids, int count )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadLatestForId( id, count ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return tasks.Select( x => x.Result ).Combine();
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadForId( id, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<TKey, TEntry>( tasks.ToDictionary( x => x.Result.Key, x => x.Result ) );
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadForId( id, to, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<TKey, TEntry>( tasks.ToDictionary( x => x.Result.Key, x => x.Result ) );
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         var tasks = new List<Task<ReadResult<TKey, TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadForId( id, from, to, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );
         return new MultiReadResult<TKey, TEntry>( tasks.ToDictionary( x => x.Result.Key, x => x.Result ) );
      }

      #endregion

      private Task<ReadResult<TKey, TEntry>> ReadLatestForId( TKey id, int count )
      {
         return RetrieveLatestForId( id, count );
      }

      private async Task<ReadResult<TKey, TEntry>> ReadForId( TKey id, Sort sort )
      {
         var results = await RetrieveAllForId( id, sort ).ConfigureAwait( false );

         return new ReadResult<TKey, TEntry>(
            id,
            sort,
            results.SelectMany( x => x.GetEntries<TKey, TEntry>( sort ) )
               .ToList() );
      }

      private async Task<ReadResult<TKey, TEntry>> ReadForId( TKey id, DateTime from, DateTime to, Sort sort )
      {
         var results = await RetrieveRangeForId( id, from, to, sort ).ConfigureAwait( false );

         return new ReadResult<TKey, TEntry>(
            id,
            sort,
            results.SelectMany( x => x.GetEntries<TKey, TEntry>( sort ) )
               .Where( x => x.GetTimestamp() >= from && x.GetTimestamp() < to )
               .ToList() );
      }

      private async Task<ReadResult<TKey, TEntry>> ReadForId( TKey id, DateTime to, Sort sort )
      {
         var results = await RetrieveBeforeForId( id, to, sort ).ConfigureAwait( false );

         return new ReadResult<TKey, TEntry>(
            id,
            sort,
            results.SelectMany( x => x.GetEntries<TKey, TEntry>( sort ) )
               .Where( x => x.GetTimestamp() < to )
               .ToList() );
      }

      private async Task<int> DeleteForId( TKey id, DateTime from, DateTime to )
      {
         var retrievals = await RetrieveRangeForId( id, from, to, Sort.Descending ).ConfigureAwait( false );

         var oldEntities = retrievals.ToDictionary( x => x.RowKey );
         var oldEntries = retrievals.SelectMany( x => x.GetEntries<TKey, TEntry>( Sort.Descending ) ).ToList();

         // remove items between from and to
         int count = oldEntries.RemoveAll( x => x.GetTimestamp() >= from && x.GetTimestamp() < to );

         // create new entities
         var newEntities = CreateTableEntitiesFor( id, oldEntries ).ToDictionary( x => x.RowKey );

         var operations = CreateCosmosTablesOperations( newEntities, oldEntities );

         await ExecuteCosmosTablesOperatioons( operations ).ConfigureAwait( false );

         return count;
      }

      private async Task<int> DeleteForId( TKey id, DateTime to )
      {
         var retrievals = await RetrieveBeforeForId( id, to, Sort.Descending ).ConfigureAwait( false );

         var oldEntities = retrievals.ToDictionary( x => x.RowKey );
         var oldEntries = retrievals.SelectMany( x => x.GetEntries<TKey, TEntry>( Sort.Descending ) ).ToList();

         // remove items between from and to
         int count = oldEntries.RemoveAll( x => x.GetTimestamp() < to );

         // create new entities
         var newEntities = CreateTableEntitiesFor( id, oldEntries ).ToDictionary( x => x.RowKey );

         var operations = CreateCosmosTablesOperations( newEntities, oldEntities );

         await ExecuteCosmosTablesOperatioons( operations ).ConfigureAwait( false );

         return count;
      }

      private async Task<int> DeleteAllForId( TKey id )
      {
         var retrievals = await RetrieveAllForId( id, Sort.Descending ).ConfigureAwait( false );

         var oldEntities = retrievals.ToDictionary( x => x.RowKey );
         var oldEntries = retrievals.SelectMany( x => x.GetEntries<TKey, TEntry>( Sort.Descending ) ).ToList();

         // remove items between from and to
         int count = oldEntries.Count;
         oldEntries.Clear();

         // create new entities
         var newEntities = CreateTableEntitiesFor( id, oldEntries ).ToDictionary( x => x.RowKey );

         var operations = CreateCosmosTablesOperations( newEntities, oldEntities );

         await ExecuteCosmosTablesOperatioons( operations ).ConfigureAwait( false );

         return count;
      }

      private async Task StoreForId( TKey id, List<TEntry> newEntries, DateTime from, DateTime to )
      {
         // retrieve existing entries for this period
         var retrievals = await RetrieveRangeForId( id, from, to, Sort.Descending ).ConfigureAwait( false );
         var oldEntities = retrievals.ToDictionary( x => x.RowKey );

         // merge results
         var oldEntries = retrievals.SelectMany( x => x.GetEntries<TKey, TEntry>( Sort.Descending ) ).ToList();
         var mergedEntries = MergeSort.Sort(
            collections: new IEnumerable<TEntry>[] { newEntries, oldEntries },
            comparer: EntryComparer.GetComparer<TKey, TEntry>( Sort.Descending ),
            resolveConflict: x => x.First() ); // prioritize the item from the first collection (new one)

         // create new entities
         var newEntities = CreateTableEntitiesFor( id, mergedEntries ).ToDictionary( x => x.RowKey );

         var operations = CreateCosmosTablesOperations( newEntities, oldEntities );

         await ExecuteCosmosTablesOperatioons( operations ).ConfigureAwait( false );
      }

      private List<CosmosTablesOperation> CreateCosmosTablesOperations( IDictionary<string, TsdbTableEntity> newEntities, IDictionary<string, TsdbTableEntity> oldEntities )
      {
         List<CosmosTablesOperation> operations = new List<CosmosTablesOperation>();
         foreach( var createdTableEntity in newEntities )
         {
            TsdbTableEntity previousEntity;
            if( !oldEntities.TryGetValue( createdTableEntity.Key, out previousEntity ) )
            {
               operations.Add( new CosmosTablesOperation( createdTableEntity.Value, CosmosTablesOperationType.Insert ) );
            }
            else
            {
               operations.Add( new CosmosTablesOperation( createdTableEntity.Value, CosmosTablesOperationType.Replace ) );
            }
         }

         foreach( var oldTableEntity in oldEntities )
         {
            TsdbTableEntity newEntity;
            if( !newEntities.TryGetValue( oldTableEntity.Key, out newEntity ) )
            {
               operations.Add( new CosmosTablesOperation( oldTableEntity.Value, CosmosTablesOperationType.Delete ) );
            }
         }
         return operations;
      }

      private async Task ExecuteCosmosTablesOperatioons( List<CosmosTablesOperation> operations )
      {
         if( operations.Count == 1 )
         {
            var ats = operations[ 0 ];
            TableOperation operation = null;
            switch( ats.OperationType )
            {
               case CosmosTablesOperationType.Insert:
                  operation = TableOperation.InsertOrReplace( ats.Row );
                  break;
               case CosmosTablesOperationType.Replace:
                  operation = TableOperation.InsertOrReplace( ats.Row );
                  break;
               case CosmosTablesOperationType.Delete:
                  operation = TableOperation.Delete( ats.Row );
                  break;
               default:
                  break;
            }

            if( operation != null )
            {
               await ExecuteOperation( operation ).ConfigureAwait( false );
            }
         }
         else
         {
            List<Task> tasks = new List<Task>();

            foreach( var partitionOperations in operations.GroupBy( x => x.Row.PartitionKey ) )
            {
               var batch = new TableBatchOperation();
               foreach( var operation in partitionOperations )
               {
                  switch( operation.OperationType )
                  {
                     case CosmosTablesOperationType.Insert:
                        batch.InsertOrReplace( operation.Row );
                        break;
                     case CosmosTablesOperationType.Replace:
                        batch.InsertOrReplace( operation.Row );
                        break;
                     case CosmosTablesOperationType.Delete:
                        batch.Delete( operation.Row );
                        break;
                     default:
                        break;
                  }

                  // only 40, because the request itself can actually become too big... :)
                  if( batch.Count == 40 )
                  {
                     tasks.Add( ExecuteBatchOperation( batch ) );

                     batch = new TableBatchOperation();
                  }
               }
               if( batch.Count != 0 )
               {
                  tasks.Add( ExecuteBatchOperation( batch ) );
               }
            }

            await Task.WhenAll( tasks ).ConfigureAwait( false );
         }
      }

      private async Task ExecuteBatchOperation( TableBatchOperation operation )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            var table = await GetTable();
            await table.ExecuteBatchAsync( operation ).ConfigureAwait( false );
         }
      }

      private async Task ExecuteOperation( TableOperation operation )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            var table = await GetTable();
            await table.ExecuteAsync( operation ).ConfigureAwait( false );
         }
      }

      private List<TsdbTableEntity> CreateTableEntitiesFor( TKey key, List<TEntry> entries )
      {
         List<TsdbTableEntity> tableEntities = new List<TsdbTableEntity>();
         var id = _keyConverter.Convert( key );

         var results = CosmosTablesSerializer.Serialize<TKey, TEntry>( entries, TsdbTableEntity.MaxByteCapacity );
         foreach( var result in results )
         {
            var entity = new TsdbTableEntity();
            entity.SetData( result.Data );
            entity.RowKey = CosmosTablesKeyCalculator.CalculateRowKey( result.From );
            entity.PartitionKey = CosmosTablesKeyCalculator.CalculatePartitionKey( id, key, result.From, _partitioningProvider );

            tableEntities.Add( entity );
         }

         return tableEntities;
      }

      private async Task<List<TsdbTableEntity>> RetrieveAllForId( TKey id, Sort sort )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            var fullQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreatePartitionFilter( id ) );

            var query = await PerformQuery( fullQuery, true, sort ).ConfigureAwait( false );

            if( sort == Sort.Ascending )
            {
               query.Reverse();
            }

            return query;
         }
      }

      private async Task<ReadResult<TKey, TEntry>> RetrieveLatestForId( TKey id, int count )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            var fullQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreatePartitionFilter( id ) )
               .Take( 1 );

            var entries = await PerformLatestQuery( fullQuery, Sort.Descending, count ).ConfigureAwait( false );

            return new ReadResult<TKey, TEntry>( id, Sort.Descending, entries );
         }
      }

      private async Task<List<TsdbTableEntity>> RetrieveRangeForId( TKey id, DateTime from, DateTime to, Sort sort )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            var generalQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreateGeneralFilter( id, from, to ) );

            var firstQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreateFirstFilter( id, from ) )
               .Take( 1 );

            var generalQueryTask = PerformQuery( generalQuery, true, sort );
            var firstQueryTask = PerformQuery( firstQuery, false, sort );

            await Task.WhenAll( generalQueryTask, firstQueryTask ).ConfigureAwait( false );

            firstQueryTask.Result.AddRange( generalQueryTask.Result );

            if( sort == Sort.Ascending )
            {
               firstQueryTask.Result.Reverse();
            }

            return firstQueryTask.Result;
         }
      }

      private async Task<List<TsdbTableEntity>> RetrieveBeforeForId( TKey id, DateTime to, Sort sort )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            var generalQuery = new TableQuery<TsdbTableEntity>()
               .Where( CreateBeforeFilter( id, to ) );

            var generalQueryResult = await PerformQuery( generalQuery, true, sort );
            
            if( sort == Sort.Ascending )
            {
               generalQueryResult.Reverse();
            }

            return generalQueryResult;
         }
      }

      private IEnumerable<EntrySplitResult<TKey, TEntry>> IterateByPartition( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         Dictionary<string, EntrySplitResult<TKey, TEntry>> lookup = new Dictionary<string, EntrySplitResult<TKey, TEntry>>();

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
                  var pk = CosmosTablesKeyCalculator.CalculatePartitionKey( id, key, timestamp, _partitioningProvider );
                  EntrySplitResult<TKey, TEntry> items;
                  if( !lookup.TryGetValue( pk, out items ) )
                  {
                     items = new EntrySplitResult<TKey, TEntry>( key, pk );
                     lookup.Add( pk, items );
                  }
                  items.Insert( entry );

                  hashkeys.Add( hashkey );
               }
            }
         }

         foreach( var result in lookup )
         {
            result.Value.Sort( Sort.Descending );
            yield return result.Value;
         }
      }

      private async Task<List<TEntry>> PerformLatestQuery( TableQuery<TsdbTableEntity> query, Sort sort, int take )
      {
         List<TEntry> results = new List<TEntry>();

         int taken = 0;
         TableContinuationToken token = null;
         do
         {
            var table = await GetTable().ConfigureAwait( false );
            var rows = await table.ExecuteQuerySegmentedAsync( query, token ).ConfigureAwait( false );
            var entries = rows.FirstOrDefault()?.GetEntries<TKey, TEntry>( sort );
            if( entries != null )
            {
               var leftToTake = take - taken;
               int toTake = entries.Length > leftToTake ? leftToTake : entries.Length;
               results.AddRange( entries.Take( toTake ) );
               taken += toTake;
            }
            else
            {
               break; // fail safe, should not be needed
            }
            
            token = rows.ContinuationToken;
         }
         while( token != null && taken < take );

         return results;
      }

      private async Task<List<TsdbTableEntity>> PerformQuery( TableQuery<TsdbTableEntity> query, bool takeAll, Sort sort )
      {
         List<TsdbTableEntity> results = new List<TsdbTableEntity>();

         TableContinuationToken token = null;
         do
         {
            var table = await GetTable().ConfigureAwait( false );
            var rows = await table.ExecuteQuerySegmentedAsync( query, takeAll ? token : null ).ConfigureAwait( false );
            results.AddRange( rows );
            token = rows.ContinuationToken;
         }
         while( token != null && takeAll );

         return results;
      }

      private async Task<CloudTable> GetTableLocked()
      {
         var table = _client.GetTableReference( _tableName );
         await table.CreateIfNotExistsAsync( IndexingMode.None, _throughputUnits, default( CancellationToken ) ).ConfigureAwait( false );
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

      private string CreateGeneralFilter( TKey key, DateTime from, DateTime to )
      {
         var id = _keyConverter.Convert( key );
         var fromRowKey = CosmosTablesKeyCalculator.CalculateRowKey( from );
         var toRowKey = CosmosTablesKeyCalculator.CalculateRowKey( to );
         var fromPartitionKey = CosmosTablesKeyCalculator.CalculatePartitionKey( id, key, from, _partitioningProvider );
         var toPartitionKey = CosmosTablesKeyCalculator.CalculatePartitionKey( id, key, to.AddTicks( -1 ), _partitioningProvider ); // -1 tick because it is an approximation value and we use gte operation

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

      private string CreateBeforeFilter( TKey key, DateTime to )
      {
         var id = _keyConverter.Convert( key );
         var toRowKey = CosmosTablesKeyCalculator.CalculateRowKey( to );
         var toPartitionKey = CosmosTablesKeyCalculator.CalculatePartitionKey( id, key, to, _partitioningProvider ); // 7125
         var fromPartitionKey = CosmosTablesKeyCalculator.CalculateMinPartitionKey( id, key, _partitioningProvider ); // 9999

         return TableQuery.CombineFilters(
               TableQuery.CombineFilters(
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.LessThanOrEqual, fromPartitionKey ),
                  TableOperators.And,
                  TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.GreaterThanOrEqual, toPartitionKey ) ),
            TableOperators.And,
               TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.GreaterThan, toRowKey ) );
      }

      private string CreateFirstFilter( TKey key, DateTime from )
      {
         var id = _keyConverter.Convert( key );
         var fromRowKey = CosmosTablesKeyCalculator.CalculateRowKey( from );
         var fromPartitionKey = CosmosTablesKeyCalculator.CalculatePartitionKey( id, key, from, _partitioningProvider ); // 7125
         var toPartitionKey = CosmosTablesKeyCalculator.CalculateMinPartitionKey( id, key, _partitioningProvider ); // 9999

         return TableQuery.CombineFilters(
            TableQuery.CombineFilters(
               TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.GreaterThanOrEqual, fromPartitionKey ),
               TableOperators.And,
               TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.LessThanOrEqual, toPartitionKey ) ),
            TableOperators.And,
            TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.GreaterThan, fromRowKey ) );
      }

      private string CreatePartitionFilter( TKey key )
      {
         var id = _keyConverter.Convert( key );
         var fromPartitionKey = CosmosTablesKeyCalculator.CalculateMaxPartitionKey( id, key, _partitioningProvider ); // 0000
         var toPartitionKey = CosmosTablesKeyCalculator.CalculateMinPartitionKey( id, key, _partitioningProvider ); // 9999

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

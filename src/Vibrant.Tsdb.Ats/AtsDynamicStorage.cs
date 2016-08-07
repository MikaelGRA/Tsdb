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
   public class AtsDynamicStorage<TEntry> : IDynamicStorage<TEntry>, IDynamicStorageSelector<TEntry>, IDisposable
     where TEntry : IAtsEntry, new()
   {
      private object _sync = new object();
      private SemaphoreSlim _read;
      private SemaphoreSlim _write;
      private string _tableName;
      private CloudStorageAccount _account;
      private CloudTableClient _client;
      private Task<CloudTable> _table;
      private IPartitionProvider _partitioningProvider;

      public AtsDynamicStorage( string tableName, string connectionString, int readParallelism, int writeParallelism, IPartitionProvider partitioningProvider )
      {
         _read = new SemaphoreSlim( readParallelism );
         _write = new SemaphoreSlim( writeParallelism );
         _tableName = tableName;
         _account = CloudStorageAccount.Parse( connectionString );
         _client = _account.CreateCloudTableClient();
         _partitioningProvider = partitioningProvider;
      }

      public AtsDynamicStorage( string tableName, string connectionString, int readParallelism, int writeParallelism )
         : this( tableName, connectionString, readParallelism, writeParallelism, new YearlyPartitioningProvider() )
      {
      }

      public AtsDynamicStorage( string tableName, string connectionString )
         : this( tableName, connectionString, 25, 25, new YearlyPartitioningProvider() )
      {
      }

      public IDynamicStorage<TEntry> GetStorage( string id )
      {
         return this;
      }

      public Task Delete( IEnumerable<string> ids )
      {
         return DeleteAllInternal( ids );
      }

      public Task Delete( IEnumerable<string> ids, DateTime to )
      {
         return DeleteUntilInternal( ids, to );
      }

      public Task Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         return DeleteRangeInternal( ids, from, to );
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         return ReadAllInternal( ids, sort );
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending )
      {
         return ReadUntilInternal( ids, to, sort );
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         return ReadRangeInternal( ids, from, to, sort );
      }

      public Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         return ReadLatestInternal( ids );
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         return WriteInternal( items );
      }

      public Task<SegmentedReadResult<TEntry>> Read( string id, DateTime to, int segmentSize, object continuationToken )
      {
         return ReadRangeSegmentedInternal( id, to, segmentSize, (ContinuationToken)continuationToken );
      }

      public Task<SegmentedReadResult<TEntry>> Read( string id, int segmentSize, object continuationToken )
      {
         return ReadRangeSegmentedInternal( id, segmentSize, (ContinuationToken)continuationToken );
      }

      private async Task<int> DeleteAllInternal( IEnumerable<string> ids )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteAllInternal( id ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return tasks.Sum( x => x.Result );
      }

      private async Task<int> DeleteAllInternal( string id )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreatePartitionFilter( id ) );

         var count = await DeleteInternal( id, fullQuery ).ConfigureAwait( false );

         return count;
      }

      private async Task<int> DeleteUntilInternal( IEnumerable<string> ids, DateTime to )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteUntilInternal( id, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return tasks.Sum( x => x.Result );
      }

      private async Task<int> DeleteUntilInternal( string id, DateTime to )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreateBeforeFilter( id, to ) );

         var count = await DeleteInternal( id, fullQuery ).ConfigureAwait( false );

         return count;
      }

      private async Task<int> DeleteRangeInternal( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         var tasks = new List<Task<int>>();
         foreach( var id in ids )
         {
            tasks.Add( DeleteRangeInternal( id, from, to ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return tasks.Sum( x => x.Result );
      }

      private async Task<int> DeleteRangeInternal( string id, DateTime from, DateTime to )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreateGeneralFilter( id, from, to ) );

         var count = await DeleteInternal( id, fullQuery ).ConfigureAwait( false );

         return count;
      }

      private async Task<MultiReadResult<TEntry>> ReadLatestInternal( IEnumerable<string> ids )
      {
         var tasks = new List<Task<ReadResult<TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadLatestInternal( id, Sort.Descending ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TEntry>> ReadLatestInternal( string id, Sort sort )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreatePartitionFilter( id ) )
            .Take( 1 );

         var entries = await ReadInternal( id, fullQuery, false, sort ).ConfigureAwait( false );

         return new ReadResult<TEntry>( id, sort, entries );
      }

      private async Task<MultiReadResult<TEntry>> ReadAllInternal( IEnumerable<string> ids, Sort sort )
      {
         var tasks = new List<Task<ReadResult<TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadAllInternal( id, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TEntry>> ReadAllInternal( string id, Sort sort )
      {
         var fullQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreatePartitionFilter( id ) );

         var entries = await ReadInternal( id, fullQuery, true, sort ).ConfigureAwait( false );

         return new ReadResult<TEntry>( id, sort, entries );
      }

      private async Task<MultiReadResult<TEntry>> ReadUntilInternal( IEnumerable<string> ids, DateTime to, Sort sort )
      {
         var tasks = new List<Task<ReadResult<TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadUntilInternal( id, to, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TEntry>> ReadUntilInternal( string id, DateTime to, Sort sort )
      {
         var query = new TableQuery<TsdbTableEntity>()
            .Where( CreateBeforeFilter( id, to ) );

         var entries = await ReadInternal( id, query, true, sort ).ConfigureAwait( false );

         return new ReadResult<TEntry>( id, sort, entries );
      }

      private async Task<MultiReadResult<TEntry>> ReadRangeInternal( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort )
      {
         var tasks = new List<Task<ReadResult<TEntry>>>();
         foreach( var id in ids )
         {
            tasks.Add( ReadRangeInternal( id, from, to, sort ) );
         }
         await Task.WhenAll( tasks ).ConfigureAwait( false );

         return new MultiReadResult<TEntry>( tasks.Select( x => x.Result ) );
      }

      private async Task<ReadResult<TEntry>> ReadRangeInternal( string id, DateTime from, DateTime to, Sort sort )
      {
         var generalQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreateGeneralFilter( id, from, to ) );

         var entries = await ReadInternal( id, generalQuery, true, sort ).ConfigureAwait( false );

         return new ReadResult<TEntry>( id, sort, entries );
      }

      private Task<SegmentedReadResult<TEntry>> ReadRangeSegmentedInternal( string id, DateTime to, int segmentSize, ContinuationToken continuationToken )
      {
         to = continuationToken?.To ?? to;

         var generalQuery = new TableQuery<TsdbTableEntity>()
            .Where( CreateBeforeFilter( id, to ) );

         return ReadSegmentedInternal( id, generalQuery, segmentSize );
      }

      private Task<SegmentedReadResult<TEntry>> ReadRangeSegmentedInternal( string id, int segmentSize, ContinuationToken continuationToken )
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

      private async Task<List<TEntry>> ReadInternal( string id, TableQuery<TsdbTableEntity> query, bool takeAll, Sort sort )
      {
         var table = await GetTable().ConfigureAwait( false );

         List<TEntry> results = new List<TEntry>();

         TableContinuationToken token = null;
         do
         {
            await _read.WaitAsync().ConfigureAwait( false );
            try
            {
               var rows = await table.ExecuteQuerySegmentedAsync( query, takeAll ? token : null ).ConfigureAwait( false );
               var entries = Convert( rows, id );
               results.AddRange( entries );
               token = rows.ContinuationToken;
            }
            finally
            {
               _read.Release();
            }
         }
         while( token != null && takeAll );

         if( sort == Sort.Ascending )
         {
            results.Reverse();
         }

         return results;
      }

      private async Task<SegmentedReadResult<TEntry>> ReadSegmentedInternal( string id, TableQuery<TsdbTableEntity> query, int segmentSize )
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
            await _read.WaitAsync().ConfigureAwait( false );
            try
            {
               var rows = await table.ExecuteQuerySegmentedAsync( query, token ).ConfigureAwait( false );
               var entries = Convert( rows, id );

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
            finally
            {
               _read.Release();
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

         return new SegmentedReadResult<TEntry>( id, Sort.Descending, new ContinuationToken( isLastFull, to ), results, () => DeleteInternal( id, allRows ) );
      }

      private async Task<int> DeleteInternal( string id, List<TsdbTableEntity> entities )
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

      private async Task<int> DeleteInternal( string id, TableQuery<TsdbTableEntity> query )
      {
         var table = await GetTable().ConfigureAwait( false );

         int count = 0;

         TableContinuationToken token = null;
         do
         {
            TableQuerySegment<TsdbTableEntity> rows;

            await _read.WaitAsync().ConfigureAwait( false );
            try
            {
               rows = await table.ExecuteQuerySegmentedAsync( query, token ).ConfigureAwait( false );
               foreach( var row in rows )
               {
                  row.ETag = "*";
               }
               token = rows.ContinuationToken;
            }
            finally
            {
               _read.Release();
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

      private async Task<int> DeleteInternalLocked( IEnumerable<TsdbTableEntity> entries )
      {
         int count = 0;

         await _write.WaitAsync().ConfigureAwait( false );
         try
         {
            var operation = new TableBatchOperation();
            foreach( var entity in entries )
            {
               count++;
               operation.Delete( entity );
            }
            var table = await GetTable().ConfigureAwait( false );
            await table.ExecuteBatchAsync( operation ).ConfigureAwait( false );
         }
         finally
         {
            _write.Release();
         }

         return count;
      }

      private async Task WriteInternal( IEnumerable<TEntry> entries )
      {
         List<Task> tasks = new List<Task>();

         foreach( var kvp in IterateByPartition( entries ) )
         {
            var partitionKey = kvp.Key;
            var items = kvp.Value;

            // schedule parallel execution
            tasks.Add( WriteInternalLocked( partitionKey, items ) );
         }

         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      private async Task WriteInternalLocked( string partitionKey, IEnumerable<TEntry> entries )
      {
         await _write.WaitAsync().ConfigureAwait( false );
         try
         {
            var operation = new TableBatchOperation();
            foreach( var entity in Convert( entries, partitionKey ) )
            {
               operation.InsertOrReplace( entity );
            }
            var table = await GetTable().ConfigureAwait( false );
            await table.ExecuteBatchAsync( operation ).ConfigureAwait( false );
         }
         finally
         {
            _write.Release();
         }
      }

      private IEnumerable<TsdbTableEntity> Convert( IEnumerable<TEntry> entries, string partitionKey = null )
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

      private TsdbTableEntity Convert( BinaryWriter writer, MemoryStream stream, TEntry entry, string partitionKey = null )
      {
         var entity = new TsdbTableEntity();
         entity.RowKey = AtsKeyCalculator.CalculateRowKey( entry.GetTimestamp() );
         entity.PartitionKey = partitionKey ?? AtsKeyCalculator.CalculatePartitionKey( entry, _partitioningProvider );
         AtsSerializer.SerializeEntry( writer, entry );
         entity.P0 = stream.ToArray();
         return entity;
      }

      private IEnumerable<TEntry> Convert( IEnumerable<TsdbTableEntity> entities, string id )
      {
         foreach( var entity in entities )
         {
            var stream = new MemoryStream( entity.P0 );
            using( var reader = AtsSerializer.CreateReader( stream ) )
            {
               yield return Convert( reader, entity, id );
            }
         }
      }

      private TEntry Convert( BinaryReader reader, TsdbTableEntity entity, string id )
      {
         return AtsSerializer.DeserializeEntry<TEntry>( reader, id );
      }

      private IEnumerable<KeyValuePair<string, List<TEntry>>> IterateByPartition( IEnumerable<TEntry> entries )
      {
         Dictionary<string, List<TEntry>> lookup = new Dictionary<string, List<TEntry>>();

         foreach( var entry in entries )
         {
            var pk = AtsKeyCalculator.CalculatePartitionKey( entry.GetId(), entry.GetTimestamp(), _partitioningProvider );

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

      private string CreateGeneralFilter( string id, DateTime from, DateTime to )
      {
         var fromRowKey = AtsKeyCalculator.CalculateRowKey( from );
         var toRowKey = AtsKeyCalculator.CalculateRowKey( to );
         var fromPartitionKey = AtsKeyCalculator.CalculatePartitionKey( id, from, _partitioningProvider );
         var toPartitionKey = AtsKeyCalculator.CalculatePartitionKey( id, to.AddTicks( -1 ), _partitioningProvider ); // -1 tick because it is an approximation value and we use gte operation

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

      private string CreateBeforeFilter( string id, DateTime from )
      {
         var fromRowKey = AtsKeyCalculator.CalculateRowKey( from );
         var fromPartitionKey = AtsKeyCalculator.CalculatePartitionKey( id, from, _partitioningProvider ); // 7125
         var toPartitionKey = AtsKeyCalculator.CalculateMinPartitionKey( id, _partitioningProvider ); // 9999

         return TableQuery.CombineFilters(
            TableQuery.CombineFilters(
               TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.GreaterThanOrEqual, fromPartitionKey ),
               TableOperators.And,
               TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.LessThanOrEqual, toPartitionKey ) ),
            TableOperators.And,
            TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.GreaterThan, fromRowKey ) );
      }

      private string CreatePartitionFilter( string id )
      {
         var fromPartitionKey = AtsKeyCalculator.CalculateMaxPartitionKey( id, _partitioningProvider ); // 0000
         var toPartitionKey = AtsKeyCalculator.CalculateMinPartitionKey( id, _partitioningProvider ); // 9999

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
               _read.Dispose();
               _write.Dispose();
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Vibrant.Tsdb.Ats.Helpers;
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
      private Dictionary<string, CloudTable> _tables;
      private CloudStorageAccount _account;
      private CloudTableClient _client;
      private IPartitionProvider<TKey> _partitioningProvider;
      private ITableProvider _tableProvider;
      private IKeyConverter<TKey> _keyConverter;
      private IConcurrencyControl _cc;

      public AtsDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency, IPartitionProvider<TKey> partitioningProvider, ITableProvider tableProvider, IKeyConverter<TKey> keyConverter )
      {
         _cc = concurrency;
         _tableName = tableName;
         _account = CloudStorageAccount.Parse( connectionString );
         _client = _account.CreateCloudTableClient();
         _partitioningProvider = partitioningProvider;
         _tableProvider = tableProvider;
         _keyConverter = keyConverter;
         _defaultSelection = new[] { new StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>( this ) };
         _tables = new Dictionary<string, CloudTable>();

         _client.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;
      }

      public AtsDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency, IPartitionProvider<TKey> partitioningProvider, ITableProvider tableProvider )
         : this( tableName, connectionString, concurrency, partitioningProvider, tableProvider, DefaultKeyConverter<TKey>.Current )
      {
      }

      public AtsDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency )
         : this( tableName, connectionString, concurrency, new YearlyPartitioningProvider<TKey>(), new YearlyTableProvider() )
      {
      }

      public AtsDynamicStorage( string tableName, string connectionString )
         : this( tableName, connectionString, new ConcurrencyControl( DefaultReadParallelism, DefaultWriteParallelism ), new YearlyPartitioningProvider<TKey>(), new YearlyTableProvider() )
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

         var count = await DeleteInternal( id, fullQuery, DateTime.UtcNow ).ConfigureAwait( false );

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

         var count = await DeleteInternal( id, fullQuery, to ).ConfigureAwait( false );

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

         var count = await DeleteInternal( id, fullQuery, from, to ).ConfigureAwait( false );

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

         var currentTable = _tableProvider.GetTable( DateTime.UtcNow );
         var entries = await ReadWithUnknownEnd( fullQuery, currentTable, sort, count ).ConfigureAwait( false );

         return new ReadResult<TKey, TEntry>( id, sort, entries );
      }

      private async Task<List<TEntry>> ReadWithUnknownEnd( TableQuery<TsdbTableEntity> query, ITable currentTable, Sort sort, int? count )
      {
         List<List<TEntry>> entries = new List<List<TEntry>>();

         var maxTableMisses = _tableProvider.MaxTableMisses;
         int tableMisses = 0;
         bool queryMoreTables = true;
         while( queryMoreTables )
         {
            var foundEntries = await ReadInternal( query, currentTable, sort, count ).ConfigureAwait( false );
            entries.Add( foundEntries );

            // if we have not found everything
            if( !count.HasValue || entries.Sum( x => x.Count ) < count )
            {
               // determine if we should try more
               if( foundEntries.Count > 0 )
               {
                  tableMisses = 0;

                  // we want to keep trying as we found something in this table (likely there MAY be more in previous table)
                  currentTable = _tableProvider.GetPreviousTable( currentTable );
               }
               else
               {
                  // we did NOT find anything in this table
                  if( tableMisses <= maxTableMisses )
                  {
                     // ONLY look in previous table if this was our FIRST iteration
                     currentTable = _tableProvider.GetPreviousTable( currentTable );
                     tableMisses++;
                  }
                  else
                  {
                     queryMoreTables = false;
                  }
               }
            }
            else
            {
               // if we have found everything
               queryMoreTables = false;
            }
         }

         if( sort == Sort.Ascending )
         {
            entries.Reverse();
         }

         return entries.SelectMany( x => x ).ToList();
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

         var currentTable = _tableProvider.GetTable( DateTime.UtcNow );
         var entries = await ReadWithUnknownEnd( fullQuery, currentTable, sort, null ).ConfigureAwait( false );

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

         var currentTable = _tableProvider.GetTable( to );
         var entries = await ReadWithUnknownEnd( query, currentTable, sort, null ).ConfigureAwait( false );

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
            foreach( var table in _tableProvider.IterateTables( from, to ) )
            {
               var computedFrom = table.ComputeFrom( from );
               var computedTo = table.ComputeTo( to );

               foreach( var partitionRange in iterable.IteratePartitions( id, computedFrom, computedTo ) )
               {
                  var specificQuery = new TableQuery<TsdbTableEntity>()
                     .Where( CreateSpecificPartitionFilter( id, computedFrom, computedTo, partitionRange ) );

                  tasks.Add( ReadInternal( specificQuery, table, sort, null ) );
               }
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

            var tasks = new List<Task<List<TEntry>>>();
            foreach( var table in _tableProvider.IterateTables( from, to ) )
            {
               tasks.Add( ReadInternal( generalQuery, table, sort, null ) );
            }

            await Task.WhenAll( tasks ).ConfigureAwait( false );

            var queryResults = tasks.Select( x => x.Result );
            if( sort == Sort.Ascending )
            {
               queryResults = queryResults.Reverse();
            }

            return new ReadResult<TKey, TEntry>( id, sort, queryResults.SelectMany( x => x ).ToList() );
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

         if( from.HasValue && to.HasValue )
         {
            return ReadSegmentedInternal( id, generalQuery, from.Value, to.Value, segmentSize );
         }
         else if( to.HasValue )
         {
            return ReadSegmentedInternal( id, generalQuery, to.Value, segmentSize );
         }
         else
         {
            return ReadSegmentedInternal( id, generalQuery, DateTime.UtcNow, segmentSize );
         }
      }

      private async Task<List<TEntry>> ReadInternal( TableQuery<TsdbTableEntity> query, ITable suffix, Sort sort, int? take )
      {
         var table = GetTable( suffix );

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

      private async Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedInternal( TKey id, TableQuery<TsdbTableEntity> query, DateTime from, DateTime to, int segmentSize )
      {
         List<TEntry> results = new List<TEntry>( segmentSize );
         Dictionary<CloudTable, List<TsdbTableEntity>> allRows = new Dictionary<CloudTable, List<TsdbTableEntity>>();

         bool lastHasSome = true;

         foreach( var currentTable in _tableProvider.IterateTables( from, to ) )
         {
            var table = GetTable( currentTable );

            TableContinuationToken token = null;
            int read = 0;
            do
            {
               using( await _cc.ReadAsync().ConfigureAwait( false ) )
               {
                  var rows = await table.ExecuteQuerySegmentedAsync( query, token ).ConfigureAwait( false );
                  var entries = Convert( rows );

                  lastHasSome = rows.Results.Count > 0;

                  // add required items, and no more
                  if( segmentSize >= read + rows.Results.Count )
                  {
                     read += rows.Results.Count;
                     results.AddRange( entries );
                     allRows.AddRange( table, rows );
                  }
                  else
                  {
                     var take = segmentSize - read;

                     foreach( var entry in entries.Take( take ) )
                     {
                        read++;
                        results.Add( entry );
                     }

                     allRows.AddRange( table, rows.Take( take ) );
                  }

                  token = rows.ContinuationToken;
                  if( read == segmentSize ) // short circuit
                  {
                     token = null;
                  }
               }
            }
            while( token != null );
         }


         // calculate continuation token
         DateTime? continueTo = null;
         if( lastHasSome )
         {
            continueTo = results[ results.Count - 1 ].GetTimestamp();
         }
         else
         {
            continueTo = null;
         }

         return new SegmentedReadResult<TKey, TEntry>( id, Sort.Descending, new ContinuationToken( lastHasSome, continueTo ), results, () => DeleteInternal( id, allRows ) );
      }

      private async Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedInternal( TKey id, TableQuery<TsdbTableEntity> query, DateTime to, int segmentSize )
      {
         List<TEntry> results = new List<TEntry>( segmentSize );
         Dictionary<CloudTable, List<TsdbTableEntity>> allRows = new Dictionary<CloudTable, List<TsdbTableEntity>>();

         bool lastHasSome = true;
         int read = 0;

         var currentTable = _tableProvider.GetTable( to );

         var maxTableMisses = _tableProvider.MaxTableMisses;
         int tableMisses = 0;
         bool queryMoreTables = true;
         while( queryMoreTables )
         {
            var table = GetTable( currentTable );
            int foundInTable = 0;

            TableContinuationToken token = null;
            do
            {
               using( await _cc.ReadAsync().ConfigureAwait( false ) )
               {
                  var rows = await table.ExecuteQuerySegmentedAsync( query, token ).ConfigureAwait( false );
                  var entries = Convert( rows );

                  lastHasSome = rows.Results.Count > 0;

                  // add required items, and no more
                  if( segmentSize >= read + rows.Results.Count )
                  {
                     foundInTable += rows.Results.Count;
                     read += rows.Results.Count;
                     results.AddRange( entries );
                     allRows.AddRange( table, rows );
                  }
                  else
                  {
                     // calculate amount to take
                     int take = Math.Min( segmentSize - read, rows.Results.Count );

                     foundInTable += take;
                     read += take;
                     results.AddRange( entries.Take( take ) );
                     allRows.AddRange( table, rows.Take( take ) );
                  }

                  token = rows.ContinuationToken;
                  if( read == segmentSize ) // short circuit
                  {
                     token = null;
                  }
               }
            }
            while( token != null );

            if( read == segmentSize )
            {
               queryMoreTables = false;
            }
            else
            {
               // if we found SOMETHING (in this table), but not enough
               if( foundInTable > 0 )
               {
                  tableMisses = 0;

                  currentTable = _tableProvider.GetPreviousTable( currentTable );
               }
               else
               {
                  // found nothing in this table
                  if( tableMisses <= maxTableMisses )
                  {
                     tableMisses++;
                     currentTable = _tableProvider.GetPreviousTable( currentTable );
                  }
                  else
                  {
                     queryMoreTables = false;
                  }
               }
            }
         }


         // calculate continuation token
         DateTime? continueTo = null;
         if( lastHasSome )
         {
            continueTo = results[ results.Count - 1 ].GetTimestamp();
         }
         else
         {
            continueTo = null;
         }

         return new SegmentedReadResult<TKey, TEntry>( id, Sort.Descending, new ContinuationToken( lastHasSome, continueTo ), results, () => DeleteInternal( id, allRows ) );
      }

      private async Task<int> DeleteInternal( TKey id, Dictionary<CloudTable, List<TsdbTableEntity>> map )
      {
         int count = 0;

         TableContinuationToken token = null;
         do
         {
            // iterate by partition and 100s
            var tasks = new List<Task<int>>();
            foreach( var tableEntities in map )
            {
               var table = tableEntities.Key;
               var entities = tableEntities.Value;

               foreach( var kvp in IterateByPartition( entities ) )
               {
                  var partitionKey = kvp.Key;
                  var items = kvp.Value;

                  // schedule parallel execution
                  tasks.Add( DeleteInternalLocked( table, items ) );
               }
            }
            await Task.WhenAll( tasks ).ConfigureAwait( false );

            count += tasks.Sum( x => x.Result );
         }
         while( token != null );

         return count;
      }

      private async Task<int> DeleteInternal( TKey id, TableQuery<TsdbTableEntity> query, DateTime from, DateTime to )
      {
         int count = 0;

         foreach( var suffix in _tableProvider.IterateTables( from, to ) )
         {
            var table = GetTable( suffix );

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
                  tasks.Add( DeleteInternalLocked( table, items ) );
               }
               await Task.WhenAll( tasks ).ConfigureAwait( false );

               count += tasks.Sum( x => x.Result );
            }
            while( token != null );
         }

         return count;
      }

      private async Task<int> DeleteInternal( TKey id, TableQuery<TsdbTableEntity> query, DateTime to )
      {
         int count = 0;

         var currentTable = _tableProvider.GetTable( to );

         var maxTableMisses = _tableProvider.MaxTableMisses;
         int tableMisses = 0;
         bool queryMoreTables = true;
         while( queryMoreTables )
         {
            var table = GetTable( currentTable );

            int deletedFromTable = 0;
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
                  tasks.Add( DeleteInternalLocked( table, items ) );
               }
               await Task.WhenAll( tasks ).ConfigureAwait( false );

               deletedFromTable += tasks.Sum( x => x.Result );
            }
            while( token != null );

            count += deletedFromTable;

            // if we have found something
            if( deletedFromTable > 0 )
            {
               tableMisses = 0;

               currentTable = _tableProvider.GetPreviousTable( currentTable );
            }
            else
            {
               // we did NOT find anything in this table
               if( tableMisses <= maxTableMisses )
               {
                  tableMisses++;

                  // ONLY look in previous table if this was our FIRST iteration
                  currentTable = _tableProvider.GetPreviousTable( currentTable );
               }
               else
               {
                  queryMoreTables = false;
               }
            }
         }

         return count;
      }

      private async Task<int> DeleteInternalLocked( CloudTable table, List<TsdbTableEntity> entries )
      {
         int count = 0;

         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
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
            var key = kvp.Key;
            var items = kvp.Value;

            // schedule parallel execution
            tasks.Add( WriteInternalLocked( key.Table, key.PartitionKey, items ) );
         }

         await Task.WhenAll( tasks ).ConfigureAwait( false );
      }

      private async Task WriteInternalLocked( ITable suffix, string partitionKey, List<TEntry> entries )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            var table = GetTable( suffix );
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

      private IEnumerable<KeyValuePair<AtsTablePartition, List<TEntry>>> IterateByPartition( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         Dictionary<AtsTablePartition, List<TEntry>> lookup = new Dictionary<AtsTablePartition, List<TEntry>>();

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
                  var table = _tableProvider.GetTable( timestamp );
                  var pk = AtsKeyCalculator.CalculatePartitionKey( id, key, timestamp, _partitioningProvider );
                  var tpk = new AtsTablePartition( table, pk );

                  List<TEntry> items;
                  if( !lookup.TryGetValue( tpk, out items ) )
                  {
                     items = new List<TEntry>();
                     lookup.Add( tpk, items );
                  }
                  items.Add( entry );
                  if( items.Count == 100 )
                  {
                     lookup.Remove( tpk );
                     yield return new KeyValuePair<AtsTablePartition, List<TEntry>>( tpk, items );
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

      private CloudTable GetTable( ITable tableRef )
      {
         lock( _sync )
         {
            string fullTableName = _tableName + tableRef.Suffix;

            CloudTable table;
            if( _tables.TryGetValue( fullTableName, out table ) )
            {
               return table;
            }
            else
            {

               table = _client.GetTableReference( fullTableName );
               table.CreateIfNotExistsAsync().Wait();

               _tables.Add( fullTableName, table );
               return table;
            }
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;
using System.Reflection;
using System.Text;
using Vibrant.Tsdb.Helpers;
using System.Threading;

namespace Vibrant.Tsdb.InfluxDB
{
   public class InfluxDynamicStorage<TKey, TEntry> : IDynamicStorage<TKey, TEntry>, IDynamicStorageSelector<TKey, TEntry>, IDisposable
      where TEntry : IInfluxEntry<TKey>, new()
   {
      public const int DefaultReadParallelism = 20;
      public const int DefaultWriteParallelism = 5;

      private DateTime _maxFrom = new DateTime( 2050, 1, 1, 0, 0, 0, DateTimeKind.Utc );
      private object _sync = new object();
      private InfluxClient _client;
      private string _database;
      private Task _createDatabase;
      private EntryEqualityComparer<TKey, TEntry> _comparer;
      private IKeyConverter<TKey> _keyConverter;
      private SemaphoreSlim _read;
      private SemaphoreSlim _write;

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password, int readParallelism, int writeParallelism, IKeyConverter<TKey> keyConverter )
      {
         _client = new InfluxClient( endpoint, username, password );
         _database = database;

         _client.DefaultQueryOptions.Precision = TimestampPrecision.Nanosecond;
         _client.DefaultWriteOptions.Precision = TimestampPrecision.Nanosecond;

         _comparer = new EntryEqualityComparer<TKey, TEntry>();
         _keyConverter = keyConverter;

         _read = new SemaphoreSlim( readParallelism );
         _write = new SemaphoreSlim( writeParallelism );
      }

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password, IKeyConverter<TKey> keyConverter )
         : this( endpoint, database, username, password, DefaultReadParallelism, DefaultWriteParallelism, keyConverter )
      {
      }

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password, int readParallelism, int writeParallelism )
         : this(endpoint, database, username, password, readParallelism, writeParallelism, DefaultKeyConverter<TKey>.Current )
      {
      }

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password )
         : this( endpoint, database, username, password, DefaultKeyConverter<TKey>.Current )
      {
      }

      public InfluxDynamicStorage( Uri endpoint, string database, int readParallelism, int writeParallelism )
         : this( endpoint, database, null, null, DefaultReadParallelism, DefaultWriteParallelism, DefaultKeyConverter<TKey>.Current )
      {

      }

      public InfluxDynamicStorage( Uri endpoint, string database )
         : this( endpoint, database, null, null, DefaultKeyConverter<TKey>.Current )
      {

      }

      public IDynamicStorage<TKey, TEntry> GetStorage( TKey id )
      {
         return this;
      }

      public async Task Write( IEnumerable<TEntry> items )
      {
         await _write.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var uniqueEntries = Unique.Ensure<TKey, TEntry>( items, _comparer );
            await _client.WriteAsync( _database, uniqueEntries ).ConfigureAwait( false );
         }
         finally
         {
            _write.Release();
         }
      }

      public async Task Delete( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         await _write.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, from, to ) ).ConfigureAwait( false );
         }
         finally
         {
            _write.Release();
         }
      }

      public async Task Delete( IEnumerable<TKey> ids, DateTime to )
      {
         await _write.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, to ) ).ConfigureAwait( false );
         }
         finally
         {
            _write.Release();
         }
      }

      public async Task Delete( IEnumerable<TKey> ids )
      {
         await _write.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids ) ).ConfigureAwait( false );
         }
         finally
         {
            _write.Release();
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadLatest( IEnumerable<TKey> ids )
      {
         await _read.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateLatestSelectQuery( ids ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, Sort.Descending );
         }
         finally
         {
            _read.Release();
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         await _read.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, sort ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, sort );
         }
         finally
         {
            _read.Release();
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         await _read.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, to, sort ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, sort );
         }
         finally
         {
            _read.Release();
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         await _read.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, from, to, sort ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, sort );
         }
         finally
         {
            _read.Release();
         }
      }

      public async Task<SegmentedReadResult<TKey, TEntry>> Read( TKey id, DateTime to, int segmentSize, object continuationToken )
      {
         await _read.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var token = (ContinuationToken)continuationToken;
            to = token?.To ?? to;
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateUpperBoundSegmentedSelectQuery( id, to, segmentSize ) ).ConfigureAwait( false );
            bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
            return Convert( id, resultSet, segmentSize );
         }
         finally
         {
            _read.Release();
         }
      }

      public async Task<SegmentedReadResult<TKey, TEntry>> Read( TKey id, int segmentSize, object continuationToken )
      {
         await _read.WaitAsync().ConfigureAwait( false );
         try
         {
            await CreateDatabase().ConfigureAwait( false );
            var token = (ContinuationToken)continuationToken;
            var to = token?.To;
            InfluxResultSet<TEntry> resultSet;
            if( to.HasValue )
            {
               resultSet = await _client.ReadAsync<TEntry>( _database, CreateUpperBoundSegmentedSelectQuery( id, to.Value, segmentSize ) ).ConfigureAwait( false );
            }
            else
            {
               resultSet = await _client.ReadAsync<TEntry>( _database, CreateSegmentedSelectQuery( id, segmentSize ) ).ConfigureAwait( false );
            }
            bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
            return Convert( id, resultSet, segmentSize );
         }
         finally
         {
            _read.Release();
         }
      }

      private string CreateDeleteQuery( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{_keyConverter.Convert( id )}\" WHERE '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateDeleteQuery( IEnumerable<TKey> ids, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateDeleteQuery( IEnumerable<TKey> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{_keyConverter.Convert( id )}\";" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}' ORDER BY time {GetQuery( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateUpperBoundSegmentedSelectQuery( TKey id, DateTime to, int take )
      {
         return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{to.ToIso8601()}' ORDER BY time DESC LIMIT {take}";
      }

      private string CreateSegmentedSelectQuery( TKey id, int take )
      {
         return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{_maxFrom.ToIso8601()}' ORDER BY time DESC LIMIT {take}";
      }

      private string CreateSelectQuery( IEnumerable<TKey> ids, DateTime to, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{to.ToIso8601()}' ORDER BY time {GetQuery( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<TKey> ids, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{_maxFrom.ToIso8601()}' ORDER BY time {GetQuery( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateLatestSelectQuery( IEnumerable<TKey> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{_maxFrom.ToIso8601()}' ORDER BY time DESC LIMIT 1;" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string GetQuery( Sort sort )
      {
         if( sort == Sort.Ascending )
         {
            return "ASC";
         }
         else
         {
            return "DESC";
         }
      }

      private SegmentedReadResult<TKey, TEntry> Convert( TKey id, InfluxResultSet<TEntry> resultSet, int segmentSize )
      {
         var list = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows;
         var entries = (List<TEntry>)list;
         DateTime? to = null;
         if( entries.Count > 0 )
         {
            to = ( (IEntry<TKey>)entries[ entries.Count - 1 ] ).GetTimestamp();
         }
         var continuationToken = new ContinuationToken( entries.Count == segmentSize, to );

         return new SegmentedReadResult<TKey, TEntry>( id, Sort.Descending, continuationToken, entries, CreateDeleteFunction( id, continuationToken, entries ) );
      }

      private Func<Task> CreateDeleteFunction( TKey id, ContinuationToken token, List<TEntry> entries )
      {
         if( entries.Count == 0 )
         {
            return () => Task.FromResult( 0 );
         }
         else
         {
            var to = ( (IEntry<TKey>)entries[ 0 ] ).GetTimestamp().AddTicks( 1 );
            var from = ( (IEntry<TKey>)entries[ entries.Count - 1 ] ).GetTimestamp();
            return () => this.Delete( id, from, to );
         }
      }

      private MultiReadResult<TKey, TEntry> Convert( IEnumerable<TKey> requiredIds, InfluxResultSet<TEntry> resultSet, Sort sort )
      {
         MultiReadResult<TKey, TEntry> mr = new MultiReadResult<TKey, TEntry>();
         foreach( var id in requiredIds )
         {
            mr.AddOrMerge( new ReadResult<TKey, TEntry>( id, sort ) );
         }

         foreach( var result in resultSet.Results )
         {
            var serie = result.Series.FirstOrDefault();
            if( serie != null )
            {
               mr.AddOrMerge( new ReadResult<TKey, TEntry>( _keyConverter.Convert( serie.Name ), sort, (List<TEntry>)serie.Rows ) );
            }
         }

         return mr;
      }

      private Task CreateDatabase()
      {
         lock( _sync )
         {
            if( _createDatabase == null || _createDatabase.IsFaulted || _createDatabase.IsCanceled )
            {
               _createDatabase = _client.CreateDatabaseAsync( _database );
            }
         }
         return _createDatabase;
      }

      #region IDisposable Support

      private bool _dispose = false; // To detect redundant calls

      protected virtual void Dispose( bool disposing )
      {
         if( !_dispose )
         {
            if( disposing )
            {
               _client.Dispose();
               _read.Dispose();
               _write.Dispose();
            }

            _dispose = true;
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

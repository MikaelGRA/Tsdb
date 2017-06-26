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
   public class InfluxDynamicStorage<TKey, TEntry> : IDynamicStorage<TKey, TEntry>, IReversableDynamicStorage<TKey, TEntry>, IDynamicStorageSelector<TKey, TEntry>, IDisposable
      where TEntry : IInfluxEntry, new()
   {
      public const int DefaultReadParallelism = 20;
      public const int DefaultWriteParallelism = 5;

      private readonly StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>[] _defaultSelection;
      private readonly DateTime _maxTo = new DateTime( 2050, 1, 1, 0, 0, 0, DateTimeKind.Utc );
      private readonly object _sync = new object();
      private readonly InfluxClient _client;
      private readonly string _database;
      private readonly IKeyConverter<TKey> _keyConverter;
      private readonly IConcurrencyControl _cc;

      private Task _createDatabase;

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password, IConcurrencyControl concurrency, IKeyConverter<TKey> keyConverter )
      {
         _client = new InfluxClient( endpoint, username, password );
         _database = database;

         _client.DefaultQueryOptions.Precision = TimestampPrecision.Nanosecond;
         _client.DefaultWriteOptions.Precision = TimestampPrecision.Nanosecond;
         
         _keyConverter = keyConverter;
         _cc = concurrency;

         _defaultSelection = new[] { new StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>( this ) };
      }

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password, IKeyConverter<TKey> keyConverter )
         : this( endpoint, database, username, password, new ConcurrencyControl( DefaultReadParallelism, DefaultWriteParallelism ), keyConverter )
      {
      }

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password, IConcurrencyControl concurrency )
         : this( endpoint, database, username, password, concurrency, DefaultKeyConverter<TKey>.Current )
      {
      }

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password )
         : this( endpoint, database, username, password, DefaultKeyConverter<TKey>.Current )
      {
      }

      public InfluxDynamicStorage( Uri endpoint, string database, IConcurrencyControl concurrency )
         : this( endpoint, database, null, null, concurrency, DefaultKeyConverter<TKey>.Current )
      {

      }

      public InfluxDynamicStorage( Uri endpoint, string database )
         : this( endpoint, database, null, null, DefaultKeyConverter<TKey>.Current )
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

      public async Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            await _client.WriteAsync( _database, Convert( series ) ).ConfigureAwait( false );
         }
      }

      private IEnumerable<InfluxEntryAdapter<TEntry>> Convert( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         var keys = new HashSet<EntryKey<TKey>>();
         foreach( var serie in series )
         {
            var key = serie.GetKey();
            var id = _keyConverter.Convert( key );
            foreach( var entry in serie.GetEntries() )
            {
               var hashkey = new EntryKey<TKey>( key, ( (IEntry)entry ).GetTimestamp() );
               if( !keys.Contains( hashkey ) )
               {
                  yield return new InfluxEntryAdapter<TEntry>( id, entry );
                  keys.Add( hashkey );
               }
            }
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, from, to ) ).ConfigureAwait( false );
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids, DateTime to )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, to ) ).ConfigureAwait( false );
         }
      }

      public async Task DeleteAsync( IEnumerable<TKey> ids )
      {
         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids ) ).ConfigureAwait( false );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids, int count )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateLatestSelectQuery( ids, count ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, Sort.Descending );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, sort ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, sort );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, to, sort ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, sort );
         }
      }

      public async Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, from, to, sort ) ).ConfigureAwait( false );
            return Convert( ids, resultSet, sort );
         }
      }

      public async Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var token = (ContinuationToken)continuationToken;
            to = token?.At ?? to;
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSegmentedSelectQuery( id, from, to, segmentSize, false, token == null ) ).ConfigureAwait( false );
            bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
            return Convert( id, resultSet, segmentSize );
         }
      }

      public async Task<SegmentedReadResult<TKey, TEntry>> ReadReverseSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken )
      {
         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            await CreateDatabase().ConfigureAwait( false );
            var token = (ContinuationToken)continuationToken;
            from = token?.At ?? from;
            var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSegmentedSelectQuery( id, from, to, segmentSize, true, token == null ) ).ConfigureAwait( false );
            bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
            return Convert( id, resultSet, segmentSize );
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

      private string CreateSegmentedSelectQuery( TKey id, DateTime? from, DateTime? to, int take, bool reverse, bool isFirstSegment )
      {
         if( !reverse || isFirstSegment )
         {
            if( from.HasValue && to.HasValue )
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE '{from.Value.ToIso8601()}' <= time AND time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( !from.HasValue && to.HasValue )
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( from.HasValue && !to.HasValue )
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE '{from.Value.ToIso8601()}' <= time AND time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
         }
         else
         {
            if( from.HasValue && to.HasValue )
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE '{from.Value.ToIso8601()}' < time AND time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( !from.HasValue && to.HasValue )
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{to.Value.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else if( from.HasValue && !to.HasValue )
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE '{from.Value.ToIso8601()}' < time AND time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
            else
            {
               return $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{_maxTo.ToIso8601()}' ORDER BY time {( reverse ? "ASC" : "DESC" )} LIMIT {take}";
            }
         }
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
            sb.Append( $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{_maxTo.ToIso8601()}' ORDER BY time {GetQuery( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateLatestSelectQuery( IEnumerable<TKey> ids, int count )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{_keyConverter.Convert( id )}\" WHERE time < '{_maxTo.ToIso8601()}' ORDER BY time DESC LIMIT {count};" );
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
            to = ( (IEntry)entries[ entries.Count - 1 ] ).GetTimestamp();
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
            DateTime from;
            DateTime to;

            var ts1 = ( (IEntry)entries[ 0 ] ).GetTimestamp();
            var ts2 = ( (IEntry)entries[ entries.Count - 1 ] ).GetTimestamp();
            if( ts1 >= ts2 )
            {
               to = ts1;
               from = ts2;
            }
            else
            {
               to = ts2;
               from = ts1;
            }

            to = to.AddTicks( 1 );

            return () => this.DeleteAsync( id, from, to );
         }
      }

      private MultiReadResult<TKey, TEntry> Convert( IEnumerable<TKey> requiredIds, InfluxResultSet<TEntry> resultSet, Sort sort )
      {
         IDictionary<string, ReadResult<TKey, TEntry>> results = new Dictionary<string, ReadResult<TKey, TEntry>>();
         foreach( var id in requiredIds )
         {
            results[ _keyConverter.Convert( id ) ] = new ReadResult<TKey, TEntry>( id, sort );
         }

         foreach( var result in resultSet.Results )
         {
            var serie = result.Series.FirstOrDefault();
            if( serie != null )
            {
               ReadResult<TKey, TEntry> r;
               if( results.TryGetValue( serie.Name, out r ) )
               {
                  results[ serie.Name ] = new ReadResult<TKey, TEntry>( r.Key, sort, (List<TEntry>)serie.Rows );
               }
            }
         }

         return new MultiReadResult<TKey, TEntry>( results.Values.ToDictionary( x => x.Key ) );
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

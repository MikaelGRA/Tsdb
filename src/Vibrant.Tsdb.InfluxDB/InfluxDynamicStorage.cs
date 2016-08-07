using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vibrant.InfluxDB.Client;
using Vibrant.InfluxDB.Client.Rows;
using System.Reflection;
using System.Text;

namespace Vibrant.Tsdb.InfluxDB
{
   public class InfluxDynamicStorage<TEntry> : IDynamicStorage<TEntry>, IDynamicStorageSelector<TEntry>, IDisposable
      where TEntry : IEntry, IInfluxEntry, new()
   {
      private DateTime _maxFrom = new DateTime( 2050, 1, 1, 0, 0, 0, DateTimeKind.Utc );
      private object _sync = new object();
      private InfluxClient _client;
      private string _database;
      private Task _createDatabase;

      public InfluxDynamicStorage( Uri endpoint, string database, string username, string password )
      {
         _client = new InfluxClient( endpoint, username, password );
         _database = database;

         _client.DefaultQueryOptions.Precision = TimestampPrecision.Nanosecond;
         _client.DefaultWriteOptions.Precision = TimestampPrecision.Nanosecond;
      }

      public InfluxDynamicStorage( Uri endpoint, string database )
         : this( endpoint, database, null, null )
      {

      }

      public IDynamicStorage<TEntry> GetStorage( string id )
      {
         return this;
      }

      public async Task Write( IEnumerable<TEntry> items )
      {
         await CreateDatabase().ConfigureAwait( false );
         await _client.WriteAsync( _database, items ).ConfigureAwait( false );
      }

      public async Task Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, from, to ) ).ConfigureAwait( false );
      }

      public async Task Delete( IEnumerable<string> ids, DateTime to )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, to ) ).ConfigureAwait( false );
      }

      public async Task Delete( IEnumerable<string> ids )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids ) ).ConfigureAwait( false );
      }

      public async Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateLatestSelectQuery( ids ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, Sort.Descending );
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, sort ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, sort );
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, to, sort ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, sort );
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, from, to, sort ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, sort );
      }

      public async Task<SegmentedReadResult<TEntry>> Read( string id, DateTime to, int segmentSize, object continuationToken )
      {
         await CreateDatabase().ConfigureAwait( false );
         long skip = continuationToken != null ? (long)continuationToken : 0l;
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateUpperBoundSegmentedSelectQuery( id, to, skip, segmentSize ) ).ConfigureAwait( false );
         bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
         return Convert( id, resultSet, hasMore ? (object)( skip + segmentSize ) : null );
      }

      public async Task<SegmentedReadResult<TEntry>> Read( string id, int segmentSize, object continuationToken )
      {
         await CreateDatabase().ConfigureAwait( false );
         long skip = continuationToken != null ? (long)continuationToken : 0l;
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSegmentedSelectQuery( id, skip, segmentSize ) ).ConfigureAwait( false );
         bool hasMore = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows.Count == segmentSize;
         return Convert( id, resultSet, hasMore ? (object)( skip + segmentSize ) : null );
      }

      private string CreateDeleteQuery( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{id}\" WHERE '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateDeleteQuery( IEnumerable<string> ids, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{id}\" WHERE time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateDeleteQuery( IEnumerable<string> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"DELETE FROM \"{id}\";" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}' ORDER BY time {GetQuery( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateUpperBoundSegmentedSelectQuery( string id, DateTime to, long skip, int take )
      {
         return $"SELECT * FROM \"{id}\" WHERE time < '{to.ToIso8601()}' ORDER BY time DESC LIMIT {take} OFFSET {skip}";
      }

      private string CreateSegmentedSelectQuery( string id, long skip, int take )
      {
         return $"SELECT * FROM \"{id}\" WHERE time < '{_maxFrom.ToIso8601()}' ORDER BY time DESC LIMIT {take} OFFSET {skip}";
      }

      private string CreateSelectQuery( IEnumerable<string> ids, DateTime to, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE time < '{to.ToIso8601()}' ORDER BY time {GetQuery( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<string> ids, Sort sort )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE time < '{_maxFrom.ToIso8601()}' ORDER BY time {GetQuery( sort )};" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateLatestSelectQuery( IEnumerable<string> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE time < '{_maxFrom.ToIso8601()}' ORDER BY time DESC LIMIT 1;" );
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

      private SegmentedReadResult<TEntry> Convert( string id, InfluxResultSet<TEntry> resultSet, object continuationToken )
      {
         var list = resultSet.Results.FirstOrDefault()?.Series.FirstOrDefault()?.Rows;
         return new SegmentedReadResult<TEntry>( id, Sort.Descending, continuationToken, (List<TEntry>)list );
      }

      private MultiReadResult<TEntry> Convert( IEnumerable<string> requiredIds, InfluxResultSet<TEntry> resultSet, Sort sort )
      {
         MultiReadResult<TEntry> mr = new MultiReadResult<TEntry>();
         foreach( var id in requiredIds )
         {
            mr.AddOrMerge( new ReadResult<TEntry>( id, sort ) );
         }

         foreach( var result in resultSet.Results )
         {
            var serie = result.Series.FirstOrDefault();
            if( serie != null )
            {
               mr.AddOrMerge( new ReadResult<TEntry>( serie.Name, sort, (List<TEntry>)serie.Rows ) );
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

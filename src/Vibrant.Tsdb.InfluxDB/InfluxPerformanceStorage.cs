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
   public class InfluxPerformanceStorage<TEntry> : IPerformanceStorage<TEntry>
      where TEntry : IEntry, IInfluxEntry, new()
   {
      private DateTime _maxFrom = new DateTime( 2050, 1, 1, 0, 0, 0, DateTimeKind.Utc );
      private object _sync = new object();
      private InfluxClient _client;
      private string _database;
      private Task _createDatabase;

      public InfluxPerformanceStorage( Uri endpoint, string database, string username, string password )
      {
         _client = new InfluxClient( endpoint, username, password );
         _database = database;

         _client.DefaultQueryOptions.Precision = TimestampPrecision.Nanosecond;
         _client.DefaultWriteOptions.Precision = TimestampPrecision.Nanosecond;
      }

      public InfluxPerformanceStorage( Uri endpoint, string database )
         : this( endpoint, database, null, null )
      {

      }

      public async Task Write( IEnumerable<TEntry> items )
      {
         await CreateDatabase().ConfigureAwait( false );
         await _client.WriteAsync( _database, items ).ConfigureAwait( false );
      }

      public async Task<int> Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, from, to ) ).ConfigureAwait( false );
         return 0;
      }

      public async Task<int> Delete( IEnumerable<string> ids, DateTime to )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids, to ) ).ConfigureAwait( false );
         return 0;
      }

      public async Task<int> Delete( IEnumerable<string> ids )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateDeleteQuery( ids ) ).ConfigureAwait( false );
         return 0;
      }

      public async Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateLimitedSelectQuery( ids ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, Sort.Descending );
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, sort );
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, to ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, sort );
      }

      public async Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         await CreateDatabase().ConfigureAwait( false );
         var resultSet = await _client.ReadAsync<TEntry>( _database, CreateSelectQuery( ids, from, to ) ).ConfigureAwait( false );
         return Convert( ids, resultSet, sort );
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
            sb.Append( $"SELECT * FROM \"{id}\";" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE '{from.ToIso8601()}' <= time AND time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<string> ids, DateTime to )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE time < '{to.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateSelectQuery( IEnumerable<string> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE time < '{_maxFrom.ToIso8601()}';" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
      }

      private string CreateLimitedSelectQuery( IEnumerable<string> ids )
      {
         StringBuilder sb = new StringBuilder();
         foreach( var id in ids )
         {
            sb.Append( $"SELECT * FROM \"{id}\" WHERE time < '{_maxFrom.ToIso8601()}' LIMIT 1;" );
         }
         return sb.Remove( sb.Length - 1, 1 ).ToString();
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
   }
}

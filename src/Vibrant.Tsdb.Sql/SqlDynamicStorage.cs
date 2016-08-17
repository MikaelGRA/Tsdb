using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.SqlServer.Server;
using Vibrant.Tsdb.Helpers;
using Vibrant.Tsdb.Sql.Serialization;

namespace Vibrant.Tsdb.Sql
{
   public class SqlDynamicStorage<TKey, TEntry> : IDynamicStorage<TKey, TEntry>, IDynamicStorageSelector<TKey, TEntry>, IDisposable
      where TEntry : ISqlEntry, new()
   {
      private const int DefaultReadParallelism = 5;
      private const int DefaultWriteParallelism = 5;

      private readonly StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>[] _defaultSelection;
      private object _sync = new object();
      private string _tableName;
      private string _connectionString;
      private Task _createTable;
      private IKeyConverter<TKey> _keyConverter;
      private IConcurrencyControl _cc;

      public SqlDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency, IKeyConverter<TKey> keyConverter )
      {
         SqlMapper.AddTypeMap( typeof( DateTime ), DbType.DateTime2 );

         _tableName = tableName;
         _connectionString = connectionString;
         _cc = concurrency;
         _keyConverter = keyConverter;
         _defaultSelection = new[] { new StorageSelection<TKey, TEntry, IDynamicStorage<TKey, TEntry>>( this ) };
      }

      public SqlDynamicStorage( string tableName, string connectionString, IConcurrencyControl concurrency )
         : this( tableName, connectionString, concurrency, DefaultKeyConverter<TKey>.Current )
      {
      }

      public SqlDynamicStorage( string tableName, string connectionString )
         : this( tableName, connectionString, new ConcurrencyControl( DefaultReadParallelism, DefaultWriteParallelism ), DefaultKeyConverter<TKey>.Current )
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

      public Task WriteAsync( IEnumerable<ISerie<TKey, TEntry>> items )
      {
         return StoreForAll( items );
      }

      public Task DeleteAsync( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         return DeleteForIds( ids, from, to );
      }

      public Task DeleteAsync( IEnumerable<TKey> ids, DateTime to )
      {
         return DeleteForIds( ids, to );
      }

      public Task DeleteAsync( IEnumerable<TKey> ids )
      {
         return DeleteForIds( ids );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadLatestAsync( IEnumerable<TKey> ids )
      {
         return RetrieveLatestForIds( ids );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, sort );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, to, sort );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadAsync( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, from, to, sort );
      }

      public Task<SegmentedReadResult<TKey, TEntry>> ReadSegmentedAsync( TKey id, DateTime? from, DateTime? to, int segmentSize, IContinuationToken continuationToken )
      {
         return RetrieveForIdSegmented( id, from, to, segmentSize, (ContinuationToken)continuationToken );
      }

      private async Task CreateTableLocked()
      {
         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            await connection.ExecuteAsync(
               sql: Sql.GetCreateTableCommand( _tableName ),
               commandType: CommandType.Text ).ConfigureAwait( false );
         }
      }

      private Task CreateTable()
      {
         lock( _sync )
         {
            if( _createTable == null || _createTable.IsFaulted || _createTable.IsCanceled )
            {
               _createTable = CreateTableLocked();
            }
            return _createTable;
         }
      }

      private async Task StoreForAll( IEnumerable<ISerie<TKey, TEntry>> series )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );
               var keys = new HashSet<EntryKey<TKey>>();

               List<SqlDataRecord> records = new List<SqlDataRecord>();
               foreach( var serie in series )
               {
                  var key = serie.GetKey();
                  SqlSerializer.Serialize<TKey, TEntry>( serie.GetEntries(), ( entry, data ) =>
                  {
                     var timestamp = entry.GetTimestamp();
                     var hashkey = new EntryKey<TKey>( key, timestamp );

                     if( !keys.Contains( hashkey ) )
                     {
                        SqlDataRecord record = new SqlDataRecord( Sql.InsertParameterMetadata );
                        record.SetString( 0, _keyConverter.Convert( key ) );
                        record.SetDateTime( 1, timestamp );
                        record.SetSqlBinary( 2, new SqlBinary( data ) );
                        records.Add( record );

                        keys.Add( hashkey );
                     }
                  } );
               }

               if( records.Count > 0 )
               {
                  using( var tx = connection.BeginTransaction( IsolationLevel.ReadUncommitted ) )
                  {
                     using( var command = connection.CreateCommand() )
                     {
                        command.CommandText = Sql.GetInsertProcedureName( _tableName );
                        command.CommandType = CommandType.StoredProcedure;
                        command.Transaction = tx;
                        var parameter = command.Parameters.AddWithValue( "@Inserts", records );
                        parameter.SqlDbType = SqlDbType.Structured;
                        parameter.TypeName = Sql.GetInsertParameterType( _tableName );

                        await command.ExecuteNonQueryAsync().ConfigureAwait( false );
                     }

                     tx.Commit();
                  }
               }
            }
         }
      }

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveForIds( IEnumerable<TKey> ids, DateTime to, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );

               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  var sqlEntries = await connection.QueryAsync<SqlEntry>(
                     sql: Sql.GetBottomlessQuery( _tableName, sort ),
                     param: new { Ids = ids.Select( x => _keyConverter.Convert( x ) ).ToList(), To = to },
                     transaction: tx ).ConfigureAwait( false );

                  return CreateReadResult( sqlEntries, ids, sort );
               }
            }
         }
      }

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveForIds( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );

               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  var sqlEntries = await connection.QueryAsync<SqlEntry>(
                     sql: Sql.GetRangedQuery( _tableName, sort ),
                     param: new { Ids = ids.Select( x => _keyConverter.Convert( x ) ).ToList(), From = from, To = to },
                     transaction: tx ).ConfigureAwait( false );

                  return CreateReadResult( sqlEntries, ids, sort );
               }
            }
         }
      }

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveForIds( IEnumerable<TKey> ids, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );

               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  var sqlEntries = await connection.QueryAsync<SqlEntry>(
                     sql: Sql.GetQuery( _tableName, sort ),
                     param: new { Ids = ids.Select( x => _keyConverter.Convert( x ) ).ToList() },
                     transaction: tx ).ConfigureAwait( false );

                  return CreateReadResult( sqlEntries, ids, sort );
               }
            }
         }
      }

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveLatestForIds( IEnumerable<TKey> ids )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );


               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  List<Task<IEnumerable<SqlEntry>>> tasks = new List<Task<IEnumerable<SqlEntry>>>();

                  foreach( var id in ids )
                  {
                     tasks.Add( connection.QueryAsync<SqlEntry>(
                        sql: Sql.GetLatestQuery( _tableName ),
                        param: new { Id = _keyConverter.Convert( id ) },
                        transaction: tx ) );
                  }

                  await Task.WhenAll( tasks ).ConfigureAwait( false );

                  return CreateReadResult( tasks.SelectMany( x => x.Result ), ids, Sort.Descending );
               }
            }
         }
      }

      private async Task<SegmentedReadResult<TKey, TEntry>> RetrieveForIdSegmented( TKey key, DateTime? from, DateTime? to, int segmentSize, ContinuationToken continuationToken )
      {
         await CreateTable().ConfigureAwait( false );
         long skip = continuationToken?.Skip ?? 0;

         using( await _cc.ReadAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );

               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  var query = Sql.GetSegmentedQuery( _tableName, _keyConverter.Convert( key ), from, to, skip, segmentSize );

                  var sqlEntries = await connection.QueryAsync<SqlEntry>(
                     sql: query.Sql,
                     param: query.Args,
                     transaction: tx ).ConfigureAwait( false );

                  return CreateReadResult( key, sqlEntries, segmentSize, skip );
               }
            }
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );

               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  var count = await connection.ExecuteAsync(
                     sql: Sql.GetRangedDeleteCommand( _tableName ),
                     param: new { Ids = ids.Select( x => _keyConverter.Convert( x ) ).ToList(), From = from, To = to },
                     transaction: tx ).ConfigureAwait( false );

                  tx.Commit();

                  return count;
               }
            }
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<TKey> ids, DateTime to )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );

               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  var count = await connection.ExecuteAsync(
                     sql: Sql.GetBottomlessDeleteCommand( _tableName ),
                     param: new { Ids = ids.Select( x => _keyConverter.Convert( x ) ).ToList(), To = to },
                     transaction: tx ).ConfigureAwait( false );

                  tx.Commit();

                  return count;
               }
            }
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<TKey> ids )
      {
         await CreateTable().ConfigureAwait( false );

         using( await _cc.WriteAsync().ConfigureAwait( false ) )
         {
            using( var connection = new SqlConnection( _connectionString ) )
            {
               await connection.OpenAsync().ConfigureAwait( false );

               using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
               {
                  var count = await connection.ExecuteAsync(
                     sql: Sql.GetDeleteCommand( _tableName ),
                     param: new { Ids = ids.Select( x => _keyConverter.Convert( x ) ).ToList() },
                     transaction: tx ).ConfigureAwait( false );

                  tx.Commit();

                  return count;
               }
            }
         }
      }

      private MultiReadResult<TKey, TEntry> CreateReadResult( IEnumerable<SqlEntry> sqlEntries, IEnumerable<TKey> requiredIds, Sort sort )
      {
         IDictionary<TKey, ReadResult<TKey, TEntry>> results = new Dictionary<TKey, ReadResult<TKey, TEntry>>();
         foreach( var id in requiredIds )
         {
            results[ id ] = new ReadResult<TKey, TEntry>( id, sort );
         }

         ReadResult<TKey, TEntry> currentResult = null;

         foreach( var sqlEntry in sqlEntries )
         {
            var id = _keyConverter.Convert( sqlEntry.Id );
            var entry = SqlSerializer.Deserialize<TKey, TEntry>( sqlEntry );

            if( currentResult == null || !currentResult.Key.Equals( id ) )
            {
               currentResult = results[ id ];
            }

            currentResult.Entries.Add( entry );
         }

         return new MultiReadResult<TKey, TEntry>( results );
      }

      private SegmentedReadResult<TKey, TEntry> CreateReadResult( TKey id, IEnumerable<SqlEntry> sqlEntries, int segmentSize, long skip )
      {
         var entries = SqlSerializer.Deserialize<TKey, TEntry>( sqlEntries );
         var continuationToken = new ContinuationToken( entries.Count == segmentSize, skip + segmentSize, segmentSize );
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
            var to = entries[ 0 ].GetTimestamp().AddTicks( 1 );
            var from = entries[ entries.Count - 1 ].GetTimestamp();
            return async () =>
            {
               await this.DeleteAsync( id, from, to ).ConfigureAwait( false );
               token.SkippedWasDeleted();
            };
         }
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

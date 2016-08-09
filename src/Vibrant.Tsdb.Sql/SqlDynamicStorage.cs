using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.SqlServer.Server;
using Vibrant.Tsdb.Helpers;
using Vibrant.Tsdb.Sql.Serialization;

namespace Vibrant.Tsdb.Sql
{
   public class SqlDynamicStorage<TKey, TEntry> : IDynamicStorage<TKey, TEntry>, IDynamicStorageSelector<TKey, TEntry>
      where TEntry : ISqlEntry<TKey>, new()
   {
      private object _sync = new object();
      private string _tableName;
      private string _connectionString;
      private Task _createTable;
      private EntryEqualityComparer<TKey, TEntry> _comparer;
      private IKeyConverter<TKey> _keyConverter;

      public SqlDynamicStorage( string tableName, string connectionString, IKeyConverter<TKey> keyConverter )
      {
         SqlMapper.AddTypeMap( typeof( DateTime ), DbType.DateTime2 );

         _tableName = tableName;
         _connectionString = connectionString;
         _comparer = new EntryEqualityComparer<TKey, TEntry>();
         _keyConverter = keyConverter;
      }

      public SqlDynamicStorage( string tableName, string connectionString )
         : this( tableName, connectionString, DefaultKeyConverter<TKey>.Current )
      {
      }

      public IDynamicStorage<TKey, TEntry> GetStorage( TKey id )
      {
         return this;
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         var uniqueEntries = Unique.Ensure<TKey, TEntry>( items, _comparer );
         return StoreForAll( uniqueEntries );
      }

      public Task Delete( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         return DeleteForIds( ids, from, to );
      }

      public Task Delete( IEnumerable<TKey> ids, DateTime to )
      {
         return DeleteForIds( ids, to );
      }

      public Task Delete( IEnumerable<TKey> ids )
      {
         return DeleteForIds( ids );
      }

      public Task<MultiReadResult<TKey, TEntry>> ReadLatest( IEnumerable<TKey> ids )
      {
         return RetrieveLatestForIds( ids );
      }

      public Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, sort );
      }

      public Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, DateTime to, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, to, sort );
      }

      public Task<MultiReadResult<TKey, TEntry>> Read( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, from, to, sort );
      }

      public Task<SegmentedReadResult<TKey, TEntry>> Read( TKey id, DateTime to, int segmentSize, object continuationToken )
      {
         return RetrieveForIdSegmented( id, to, segmentSize, (ContinuationToken)continuationToken );
      }

      public Task<SegmentedReadResult<TKey, TEntry>> Read( TKey id, int segmentSize, object continuationToken )
      {
         return RetrieveForIdSegmented( id, segmentSize, (ContinuationToken)continuationToken );
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

      private async Task StoreForAll( IEnumerable<TEntry> entries )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            List<SqlDataRecord> records = new List<SqlDataRecord>();
            SqlSerializer.Serialize<TKey, TEntry>( entries, ( entry, data ) =>
            {
               SqlDataRecord record = new SqlDataRecord( Sql.InsertParameterMetadata );
               record.SetString( 0, _keyConverter.Convert( entry.GetKey() ) );
               record.SetDateTime( 1, entry.GetTimestamp() );
               record.SetSqlBinary( 2, new SqlBinary( data ) );
               records.Add( record );
            } );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
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

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveForIds( IEnumerable<TKey> ids, DateTime to, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetBottomlessQuery( _tableName, sort ),
               param: new { Ids = ids, To = to },
               transaction: tx ).ConfigureAwait( false );

            return CreateReadResult( sqlEntries, ids, sort );
         }
      }

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveForIds( IEnumerable<TKey> ids, DateTime from, DateTime to, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
            {
               var sqlEntries = await connection.QueryAsync<SqlEntry>(
                  sql: Sql.GetRangedQuery( _tableName, sort ),
                  param: new { Ids = ids, From = from, To = to },
                  transaction: tx ).ConfigureAwait( false );

               return CreateReadResult( sqlEntries, ids, sort );
            }
         }
      }

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveForIds( IEnumerable<TKey> ids, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
            {
               var sqlEntries = await connection.QueryAsync<SqlEntry>(
                  sql: Sql.GetQuery( _tableName, sort ),
                  param: new { Ids = ids },
                  transaction: tx ).ConfigureAwait( false );

               return CreateReadResult( sqlEntries, ids, sort );
            }
         }
      }

      private async Task<MultiReadResult<TKey, TEntry>> RetrieveLatestForIds( IEnumerable<TKey> ids )
      {
         await CreateTable().ConfigureAwait( false );

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
                     param: new { Id = id },
                     transaction: tx ) );
               }

               await Task.WhenAll( tasks ).ConfigureAwait( false );

               return CreateReadResult( tasks.SelectMany( x => x.Result ), ids, Sort.Descending );
            }
         }
      }

      private async Task<SegmentedReadResult<TKey, TEntry>> RetrieveForIdSegmented( TKey id, DateTime to, int segmentSize, ContinuationToken continuationToken )
      {
         await CreateTable().ConfigureAwait( false );
         long skip = continuationToken?.Skip ?? 0;

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
            {
               var sqlEntries = await connection.QueryAsync<SqlEntry>(
                  sql: Sql.GetUpperBoundSegmentedQuery( _tableName ),
                  param: new { Id = id, To = to, Take = segmentSize, Skip = skip },
                  transaction: tx ).ConfigureAwait( false );

               return CreateReadResult( id, sqlEntries, segmentSize, skip );
            }
         }
      }

      private async Task<SegmentedReadResult<TKey, TEntry>> RetrieveForIdSegmented( TKey id, int segmentSize, ContinuationToken continuationToken )
      {
         await CreateTable().ConfigureAwait( false );
         long skip = continuationToken?.Skip ?? 0;

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
            {
               var sqlEntries = await connection.QueryAsync<SqlEntry>(
                  sql: Sql.GetSegmentedQuery( _tableName ),
                  param: new { Id = id, Take = segmentSize, Skip = skip },
                  transaction: tx ).ConfigureAwait( false );

               return CreateReadResult( id, sqlEntries, segmentSize, skip );
            }
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<TKey> ids, DateTime from, DateTime to )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
            {
               var count = await connection.ExecuteAsync(
                  sql: Sql.GetRangedDeleteCommand( _tableName ),
                  param: new { Ids = ids, From = from, To = to },
                  transaction: tx ).ConfigureAwait( false );

               tx.Commit();

               return count;
            }
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<TKey> ids, DateTime to )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
            {
               var count = await connection.ExecuteAsync(
                  sql: Sql.GetBottomlessDeleteCommand( _tableName ),
                  param: new { Ids = ids, To = to },
                  transaction: tx ).ConfigureAwait( false );

               tx.Commit();

               return count;
            }
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<TKey> ids )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            using( var tx = connection.BeginTransaction( IsolationLevel.ReadCommitted ) )
            {
               var count = await connection.ExecuteAsync(
                  sql: Sql.GetDeleteCommand( _tableName ),
                  param: new { Ids = ids },
                  transaction: tx ).ConfigureAwait( false );

               tx.Commit();

               return count;
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

         foreach( var entry in SqlSerializer.Deserialize<TKey, TEntry>( sqlEntries, _keyConverter ) )
         {
            var id = entry.GetKey();
            if( currentResult == null || !currentResult.Id.Equals( id ) )
            {
               currentResult = results[ id ];
            }

            currentResult.Entries.Add( entry );
         }

         return new MultiReadResult<TKey, TEntry>( results );
      }

      private SegmentedReadResult<TKey, TEntry> CreateReadResult( TKey id, IEnumerable<SqlEntry> sqlEntries, int segmentSize, long skip )
      {
         var entries = SqlSerializer.Deserialize<TKey, TEntry>( sqlEntries, _keyConverter );
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
               await this.Delete( id, from, to ).ConfigureAwait( false );
               token.SkippedWasDeleted();
            };
         }
      }
   }
}

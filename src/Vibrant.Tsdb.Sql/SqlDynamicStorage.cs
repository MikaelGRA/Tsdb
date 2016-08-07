using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.SqlServer.Server;
using Vibrant.Tsdb.Sql.Serialization;

namespace Vibrant.Tsdb.Sql
{
   public class SqlDynamicStorage<TEntry> : IDynamicStorage<TEntry>, IDynamicStorageSelector<TEntry>
      where TEntry : ISqlEntry, new()
   {
      private object _sync = new object();
      private string _tableName;
      private string _connectionString;
      private Task _createTable;

      public SqlDynamicStorage( string tableName, string connectionString )
      {
         SqlMapper.AddTypeMap( typeof( DateTime ), DbType.DateTime2 );

         _tableName = tableName;
         _connectionString = connectionString;
      }

      public IDynamicStorage<TEntry> GetStorage( string id )
      {
         return this;
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         return StoreForAll( items );
      }

      public Task Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         return DeleteForIds( ids, from, to );
      }

      public Task Delete( IEnumerable<string> ids, DateTime to )
      {
         return DeleteForIds( ids, to );
      }

      public Task Delete( IEnumerable<string> ids )
      {
         return DeleteForIds( ids );
      }

      public Task<MultiReadResult<TEntry>> ReadLatest( IEnumerable<string> ids )
      {
         return RetrieveLatestForIds( ids );
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, sort );
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime to, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, to, sort );
      }

      public Task<MultiReadResult<TEntry>> Read( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort = Sort.Descending )
      {
         return RetrieveForIds( ids, from, to, sort );
      }

      public Task<SegmentedReadResult<TEntry>> Read( string id, DateTime to, int segmentSize, object continuationToken )
      {
         return RetrieveForIdSegmented( id, to, segmentSize, (ContinuationToken)continuationToken );
      }

      public Task<SegmentedReadResult<TEntry>> Read( string id, int segmentSize, object continuationToken )
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
            SqlSerializer.Serialize( entries, ( entry, data ) =>
            {
               SqlDataRecord record = new SqlDataRecord( Sql.InsertParameterMetadata );
               record.SetString( 0, entry.GetId() );
               record.SetDateTime( 1, entry.GetTimestamp() );
               record.SetSqlBinary( 2, new SqlBinary( data ) );
               records.Add( record );
            } );

            using( var command = connection.CreateCommand() )
            {
               command.CommandText = Sql.GetInsertProcedureName( _tableName );
               command.CommandType = CommandType.StoredProcedure;
               var parameter = command.Parameters.AddWithValue( "@Inserts", records );
               parameter.SqlDbType = SqlDbType.Structured;
               parameter.TypeName = Sql.GetInsertParameterType( _tableName );

               await command.ExecuteNonQueryAsync().ConfigureAwait( false );
            }
         }
      }

      private async Task<MultiReadResult<TEntry>> RetrieveForIds( IEnumerable<string> ids, DateTime to, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetBottomlessQuery( _tableName, sort ),
               param: new { Ids = ids, To = to } ).ConfigureAwait( false );

            return CreateReadResult( sqlEntries, ids, sort );
         }
      }

      private async Task<MultiReadResult<TEntry>> RetrieveForIds( IEnumerable<string> ids, DateTime from, DateTime to, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetRangedQuery( _tableName, sort ),
               param: new { Ids = ids, From = from, To = to } ).ConfigureAwait( false );

            return CreateReadResult( sqlEntries, ids, sort );
         }
      }

      private async Task<MultiReadResult<TEntry>> RetrieveForIds( IEnumerable<string> ids, Sort sort )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetQuery( _tableName, sort ),
               param: new { Ids = ids } ).ConfigureAwait( false );

            return CreateReadResult( sqlEntries, ids, sort );
         }
      }

      private async Task<MultiReadResult<TEntry>> RetrieveLatestForIds( IEnumerable<string> ids )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            List<Task<IEnumerable<SqlEntry>>> tasks = new List<Task<IEnumerable<SqlEntry>>>();

            foreach( var id in ids )
            {
               tasks.Add( connection.QueryAsync<SqlEntry>(
                  sql: Sql.GetLatestQuery( _tableName ),
                  param: new { Id = id } ) );
            }

            await Task.WhenAll( tasks ).ConfigureAwait( false );

            return CreateReadResult( tasks.SelectMany( x => x.Result ), ids, Sort.Descending );
         }
      }

      private async Task<SegmentedReadResult<TEntry>> RetrieveForIdSegmented( string id, DateTime to, int segmentSize, ContinuationToken continuationToken )
      {
         await CreateTable().ConfigureAwait( false );
         long skip = continuationToken?.Skip ?? 0;

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetUpperBoundSegmentedQuery( _tableName ),
               param: new { Id = id, To = to, Take = segmentSize, Skip = skip } ).ConfigureAwait( false );

            return CreateReadResult( id, sqlEntries, segmentSize, skip );
         }
      }

      private async Task<SegmentedReadResult<TEntry>> RetrieveForIdSegmented( string id, int segmentSize, ContinuationToken continuationToken )
      {
         await CreateTable().ConfigureAwait( false );
         long skip = continuationToken?.Skip ?? 0;

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetSegmentedQuery( _tableName ),
               param: new { Id = id, Take = segmentSize, Skip = skip } ).ConfigureAwait( false );

            return CreateReadResult( id, sqlEntries, segmentSize, skip );
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            return await connection.ExecuteAsync(
               sql: Sql.GetRangedDeleteCommand( _tableName ),
               param: new { Ids = ids, From = from, To = to } ).ConfigureAwait( false );
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<string> ids, DateTime to )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            return await connection.ExecuteAsync(
               sql: Sql.GetBottomlessDeleteCommand( _tableName ),
               param: new { Ids = ids, To = to } ).ConfigureAwait( false );
         }
      }

      private async Task<int> DeleteForIds( IEnumerable<string> ids )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            return await connection.ExecuteAsync(
               sql: Sql.GetDeleteCommand( _tableName ),
               param: new { Ids = ids } ).ConfigureAwait( false );
         }
      }

      private MultiReadResult<TEntry> CreateReadResult( IEnumerable<SqlEntry> sqlEntries, IEnumerable<string> requiredIds, Sort sort )
      {
         IDictionary<string, ReadResult<TEntry>> results = new Dictionary<string, ReadResult<TEntry>>();
         foreach( var id in requiredIds )
         {
            results[ id ] = new ReadResult<TEntry>( id, sort );
         }

         ReadResult<TEntry> currentResult = null;

         foreach( var entry in SqlSerializer.Deserialize<TEntry>( sqlEntries ) )
         {
            var id = entry.GetId();
            if( currentResult == null || currentResult.Id != id )
            {
               currentResult = results[ id ];
            }

            currentResult.Entries.Add( entry );
         }

         return new MultiReadResult<TEntry>( results );
      }

      private SegmentedReadResult<TEntry> CreateReadResult( string id, IEnumerable<SqlEntry> sqlEntries, int segmentSize, long skip )
      {
         var entries = SqlSerializer.Deserialize<TEntry>( sqlEntries );
         var to = entries[ 0 ].GetTimestamp();

         var continuationToken = new ContinuationToken( entries.Count == segmentSize, skip + segmentSize, segmentSize );
         return new SegmentedReadResult<TEntry>( id, Sort.Descending, continuationToken, entries, CreateDeleteFunction( id, continuationToken, entries ) );
      }

      private Func<Task> CreateDeleteFunction( string id, ContinuationToken token, List<TEntry> entries )
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

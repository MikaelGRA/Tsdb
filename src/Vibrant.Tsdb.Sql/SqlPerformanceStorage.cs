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
   public class SqlPerformanceStorage<TEntry> : IPerformanceStorage<TEntry>, IPerformanceStorageSelector<TEntry>
      where TEntry : ISqlEntry
   {
      private object _sync = new object();
      private string _tableName;
      private string _connectionString;
      private Task _createTable;

      public SqlPerformanceStorage( string tableName, string connectionString )
      {
         _tableName = tableName;
         _connectionString = connectionString;
      }

      public IPerformanceStorage<TEntry> GetStorage( string id )
      {
         return this;
      }

      public Task Write( IEnumerable<TEntry> items )
      {
         return StoreForAll( items );
      }

      public Task<int> Delete( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         return DeleteForIds( ids, from, to );
      }

      public Task<int> Delete( IEnumerable<string> ids, DateTime to )
      {
         return DeleteForIds( ids, to );
      }

      public Task<int> Delete( IEnumerable<string> ids )
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
   }
}

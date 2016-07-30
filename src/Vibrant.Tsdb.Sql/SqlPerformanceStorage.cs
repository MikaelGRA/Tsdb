using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Vibrant.Tsdb.Serialization;
using Vibrant.Tsdb.Sql.Serialization;

namespace Vibrant.Tsdb.Sql
{
   public class SqlPerformanceStorage : IPerformanceStorage
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

      public Task Write( IEnumerable<IEntry> items )
      {
         return StoreForAll( items );
      }

      public Task<int> Delete( string id )
      {
         return DeleteForIds( new[] { id } );
      }

      public Task<int> Delete( string id, DateTime from, DateTime to )
      {
         return DeleteForIds( new[] { id }, from, to );
      }

      public async Task<ReadResult<IEntry>> Read( string id )
      {
         var multiResult = await RetrieveForIds( new[] { id } ).ConfigureAwait( false );
         return multiResult.FindResult( id );
      }

      public async Task<ReadResult<IEntry>> Read( string id, DateTime from, DateTime to )
      {
         var multiResult = await RetrieveForIds( new[] { id }, from, to ).ConfigureAwait( false );
         return multiResult.FindResult( id );
      }

      public async Task<ReadResult<TEntry>> ReadAs<TEntry>( string id ) where TEntry : IEntry
      {
         var multiResult = await RetrieveForIds( new[] { id } ).ConfigureAwait( false );
         return multiResult.FindResult( id ).As<TEntry>();
      }

      public async Task<ReadResult<TEntry>> ReadAs<TEntry>( string id, DateTime from, DateTime to ) where TEntry : IEntry
      {
         var multiResult = await RetrieveForIds( new[] { id }, from, to ).ConfigureAwait( false );
         return multiResult.FindResult( id ).As<TEntry>();
      }

      public async Task<ReadResult<IEntry>> ReadLatest( string id )
      {
         var multiResult = await RetrieveLatestForIds( new[] { id } ).ConfigureAwait( false );
         return multiResult.FindResult( id );
      }

      public async Task<ReadResult<TEntry>> ReadLatestAs<TEntry>( string id ) where TEntry : IEntry
      {
         var multiResult = await RetrieveLatestForIds( new[] { id } ).ConfigureAwait( false );
         return multiResult.FindResult( id ).As<TEntry>();
      }

      public Task<int> DeleteMulti( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         return DeleteForIds( ids, from, to );
      }

      public Task<int> DeleteMulti( IEnumerable<string> ids )
      {
         return DeleteForIds( ids );
      }

      public Task<MultiReadResult<IEntry>> ReadLatestMulti( IEnumerable<string> ids )
      {
         return RetrieveLatestForIds( ids );
      }

      public async Task<MultiReadResult<TEntry>> ReadLatestMultiAs<TEntry>( IEnumerable<string> ids ) where TEntry : IEntry
      {
         var result = await RetrieveLatestForIds( ids ).ConfigureAwait( false );
         return result.As<TEntry>();
      }

      public Task<MultiReadResult<IEntry>> ReadMulti( IEnumerable<string> ids )
      {
         return RetrieveForIds( ids );
      }

      public async Task<MultiReadResult<TEntry>> ReadMultiAs<TEntry>( IEnumerable<string> ids ) where TEntry : IEntry
      {
         var result = await RetrieveForIds( ids ).ConfigureAwait( false );
         return result.As<TEntry>();
      }

      public Task<MultiReadResult<IEntry>> ReadMulti( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         return RetrieveForIds( ids, from, to );
      }

      public async Task<MultiReadResult<TEntry>> ReadMultiAs<TEntry>( IEnumerable<string> ids, DateTime from, DateTime to ) where TEntry : IEntry
      {
         var result = await RetrieveForIds( ids, from, to ).ConfigureAwait( false );
         return result.As<TEntry>();
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

      private async Task StoreForAll( IEnumerable<IEntry> entries )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            List<object> args = new List<object>();
            SqlSerializer.Serialize( entries, ( entry, data ) =>
            {
               args.Add( new
               {
                  Id = entry.GetId(),
                  Timestamp = entry.GetTimestamp(),
                  Data = data,
               } );
            } );

            await connection.ExecuteAsync( Sql.GetInsertCommand( _tableName ), args ).ConfigureAwait( false );
         }
      }

      private async Task<MultiReadResult<IEntry>> RetrieveForIds( IEnumerable<string> ids, DateTime from, DateTime to )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetRangedQuery( _tableName ),
               param: new { Ids = ids, From = from, To = to } ).ConfigureAwait( false );

            return CreateReadResult( sqlEntries, ids );
         }
      }

      private async Task<MultiReadResult<IEntry>> RetrieveForIds( IEnumerable<string> ids )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            var sqlEntries = await connection.QueryAsync<SqlEntry>(
               sql: Sql.GetQuery( _tableName ),
               param: new { Ids = ids } ).ConfigureAwait( false );

            return CreateReadResult( sqlEntries, ids );
         }
      }

      private async Task<MultiReadResult<IEntry>> RetrieveLatestForIds( IEnumerable<string> ids )
      {
         await CreateTable().ConfigureAwait( false );

         using( var connection = new SqlConnection( _connectionString ) )
         {
            await connection.OpenAsync().ConfigureAwait( false );

            List<Task<IEnumerable<SqlEntry>>> tasks = new List<Task<IEnumerable<SqlEntry>>>();

            foreach( var id in ids )
            {
               tasks.Add( connection.QueryAsync<SqlEntry>(
                  sql: Sql.GetQuery( _tableName ),
                  param: new { Id = id } ) );
            }

            await Task.WhenAll( tasks ).ConfigureAwait( false );

            return CreateReadResult( tasks.SelectMany( x => x.Result ), ids );
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

      private MultiReadResult<IEntry> CreateReadResult( IEnumerable<SqlEntry> sqlEntries, IEnumerable<string> requiredIds )
      {
         IDictionary<string, ReadResult<IEntry>> results = new Dictionary<string, ReadResult<IEntry>>();
         foreach( var id in requiredIds )
         {
            results[ id ] = new ReadResult<IEntry>( id );
         }

         foreach( var entry in SqlSerializer.Deserialize( sqlEntries ) )
         {
            var id = entry.GetId();

            ReadResult<IEntry> readResult;
            if( !results.TryGetValue( id, out readResult ) )
            {
               readResult = new ReadResult<IEntry>( id );
               results.Add( id, readResult );
            }
            readResult.Entries.Add( entry );
         }

         return new MultiReadResult<IEntry>( results );
      }
   }
}
